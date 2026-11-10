//! The durable host's persistence boundary. Storage adapters implement [`DurableStore`];
//! [`MemoryStore`] is the in-process reference implementation.
use crate::{
    Agent, Checkpoint, Error, HistoryEntry, Operation, Resolution, Result, RunId, RunState,
};
use async_trait::async_trait;
use serde_json::Value;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

mod memory;
pub use memory::MemoryStore;

/// Generation and revision identifying one authoritative unsettled checkpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CheckpointPosition {
    pub generation: u64,
    pub revision: u64,
}
impl From<&RunState> for CheckpointPosition {
    fn from(state: &RunState) -> Self {
        Self {
            generation: state.generation,
            revision: state.revision,
        }
    }
}

/// Exclusive residency of one host on a store. Dropping the lease releases the store.
pub struct StoreLease {
    _guard: Box<dyn Send + Sync>,
}
impl StoreLease {
    /// Wrap a guard whose `Drop` releases the store's residency.
    pub fn new(guard: impl Send + Sync + 'static) -> Self {
        Self {
            _guard: Box::new(guard),
        }
    }
}

/// A one-host slot for stores whose ownership is already exclusive to this process.
#[derive(Default)]
pub struct HostSlot(Arc<AtomicBool>);
impl HostSlot {
    /// Fails with [`Error::StoreBusy`] while another lease from this slot is alive.
    pub fn lease(&self) -> Result<StoreLease> {
        self.0
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| Error::StoreBusy)?;
        Ok(StoreLease::new(SlotGuard(self.0.clone())))
    }
}
struct SlotGuard(Arc<AtomicBool>);
impl Drop for SlotGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// An application request against a stored run, outside its execution.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum RunUpdate {
    /// Persist explicit cancellation. A settled or already cancelled run is unchanged.
    Cancel,
    /// Resolve one outstanding approval, input wait, or unknown outcome.
    Resolve {
        call_id: String,
        resolution: Resolution,
    },
    /// Clear a recorded host error so the run is discovered again.
    Retry,
}
/// What a store must persist after [`RunUpdate::apply`] changed a run.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct UpdateEffect {
    /// The state changed: advance its revision and persist it, fencing older workers.
    pub changed: bool,
    /// Clear the run's recorded error.
    pub clear_error: bool,
    /// Make the run due now regardless of `wake_at`, for one recovery pass.
    pub wake_now: bool,
}
impl RunUpdate {
    /// The one definition of update semantics. Stores load the run, apply this within the
    /// same atomic write, then persist the result as `UpdateEffect` describes. Adding an
    /// update kind here extends every store without a new trait method.
    pub fn apply(self, state: &mut RunState) -> Result<UpdateEffect> {
        let changed = UpdateEffect {
            changed: true,
            ..UpdateEffect::default()
        };
        match self {
            Self::Cancel if state.is_terminal() || state.cancel_requested => {
                Ok(UpdateEffect::default())
            }
            Self::Cancel => {
                state.cancel_requested = true;
                Ok(changed)
            }
            Self::Resolve {
                call_id,
                resolution,
            } => {
                state.resolve(&call_id, resolution)?;
                Ok(changed)
            }
            Self::Retry if state.is_terminal() => Err(Error::Stale),
            Self::Retry => Ok(UpdateEffect {
                clear_error: true,
                // Explicit repair gets one recovery pass even when a sibling waits.
                // `claim` recomputes normal eligibility before the engine runs.
                wake_now: state.needs_recovery(),
                ..changed
            }),
        }
    }
}

/// Durable runs for one resident [`Host`](crate::Host).
///
/// Implementors must meet these obligations; `tests/storage_conformance.rs` checks them
/// against every adapter, and `docs/durability.md` describes the contract.
///
/// - **Atomicity.** `accept`, `claim`, `update`, `record_error` and every
///   [`Checkpoint::commit`] are each one atomic write: the run's progress, its new history
///   entries, its operation evidence and its discovery fields (`RunState::wake_at`,
///   `RunState::needs_recovery`, recorded error) become durable together or not at all.
/// - **Immutable acceptance.** `accept` fixes the run's identity, conversation, agent,
///   binding, input, limits, expiry and bounded prior context. Later writes never change
///   them; a commit persists only progress (phase, usage, cancellation, pending response,
///   operations) and appends history. History is append-only.
/// - **Fencing.** A commit succeeds only if the stored run is unsettled and its generation
///   and revision equal the caller's; otherwise it fails with [`Error::Stale`] and writes
///   nothing. Success advances the revision by one. `claim` advances both generation and
///   revision; a changing `update` advances the revision; `record_error` changes neither.
/// - **Idempotent acceptance.** A repeated conversation and key with the same input,
///   binding and expiry returns the original run; any difference is
///   [`Error::IdempotencyConflict`]. A conversation has at most one unsettled run.
/// - **Single resident host.** `lease` grants exclusive residency until the lease drops.
///   Applications driving `claim`/`commit` directly must join earlier execution of a run
///   before claiming it again. This is not a distributed lease protocol.
/// - **Errors.** Failures to read or write are [`Error::Storage`], built with
///   [`Error::storage`] to keep the underlying error as the cause; requests the store refuses
///   use the variant named above or on the method, and out-of-bounds arguments are
///   [`Error::Input`].
#[async_trait]
pub trait DurableStore: Checkpoint {
    /// Take exclusive residency; fails with [`Error::StoreBusy`] while a lease is alive.
    fn lease(&self) -> Result<StoreLease>;
    /// Durably accept a run with bounded prior context from the conversation's history.
    async fn accept(
        &self,
        agent: &Agent,
        conversation: &str,
        key: &str,
        input: Value,
        expires_at_ms: Option<u64>,
    ) -> Result<RunId>;
    /// The authoritative state of a run, settled or not.
    async fn status(&self, id: RunId) -> Result<RunState>;
    /// Advance an unsettled run to a fresh generation and return it for execution.
    /// Recomputes eligibility, ending any one-off recovery wake. Settled runs are `Stale`.
    async fn claim(&self, id: RunId) -> Result<RunState>;
    /// Apply an application request atomically with [`RunUpdate::apply`].
    async fn update(&self, id: RunId, update: RunUpdate) -> Result<()>;
    /// Unsettled runs whose `wake_at` is at or before `now`, earliest first.
    async fn due(&self, now: u64, limit: usize) -> Result<Vec<RunId>>;
    /// Unsettled runs without a recorded error that need recovery, ordered by id after `after`.
    async fn recovery_page(&self, after: Option<RunId>, limit: usize) -> Result<Vec<RunId>>;
    /// The current position of an unsettled run; settled runs are `NotFound`.
    async fn position(&self, id: RunId) -> Result<CheckpointPosition>;
    /// Record a host error and stop automatic discovery, only at exactly `expected`.
    /// Returns false, changing nothing, if the run has moved on or settled.
    async fn record_error(
        &self,
        id: RunId,
        expected: CheckpointPosition,
        message: String,
    ) -> Result<bool>;
    async fn last_error(&self, id: RunId) -> Result<Option<String>>;
    /// The conversation's retained history in commit order, across all its runs.
    async fn history(&self, conversation: &str) -> Result<Vec<HistoryEntry>>;
    /// Every operation the run has recorded, across batches, in first-recorded order.
    async fn operations(&self, id: RunId) -> Result<Vec<Operation>>;

    async fn request_cancel(&self, id: RunId) -> Result<()> {
        self.update(id, RunUpdate::Cancel).await
    }
    async fn resolve(&self, id: RunId, call_id: &str, resolution: Resolution) -> Result<()> {
        self.update(
            id,
            RunUpdate::Resolve {
                call_id: call_id.to_owned(),
                resolution,
            },
        )
        .await
    }
    /// Explicitly retry a run after its unavailable binding or storage problem is repaired.
    async fn retry(&self, id: RunId) -> Result<()> {
        self.update(id, RunUpdate::Retry).await
    }
}

/// Bounds every store applies before acceptance. Timestamps and counters fit SQL integers.
pub(crate) fn check_acceptance(
    conversation: &str,
    key: &str,
    expires_at_ms: Option<u64>,
) -> Result<()> {
    if conversation.is_empty() || conversation.len() > 1024 || key.is_empty() || key.len() > 1024 {
        return Err(Error::input(
            "conversation and idempotency keys must contain 1..=1024 bytes",
        ));
    }
    if expires_at_ms.is_some_and(|at| at > i64::MAX as u64) {
        return Err(Error::input("expiry must fit signed 64-bit milliseconds"));
    }
    Ok(())
}
/// Advance a generation or revision, keeping it within a signed 64-bit SQL integer.
pub(crate) fn advance(value: u64, name: &str) -> Result<u64> {
    value
        .checked_add(1)
        .filter(|value| *value <= i64::MAX as u64)
        .ok_or_else(|| Error::storage(format!("{name} exhausted")))
}
