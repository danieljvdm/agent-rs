//! Durable steering: free-form instructions an application queues for an active run, which enter
//! its history at the next model dispatch. Stores that support it implement [`SteeringStore`];
//! a [`Host`] over such a store gains [`Host::steer`] and [`Host::steering`].
use crate::context::encoded_len;
use crate::types::now_ms;
use crate::{DurableStore, Error, HistoryEntry, Host, Phase, Result, RunId, RunState, Settlement};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Identifies one accepted instruction: a random UUID assigned at acceptance.
pub type SteeringId = uuid::Uuid;

/// The durable acknowledgement of one instruction, returned only after its acceptance commits.
/// Retrying [`SteeringStore::steer`] with the same run, key and instruction returns an equal
/// receipt, even after the run settled. Plain data, built by stores.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SteeringReceipt {
    /// The instruction's ID, also carried by its [`HistoryEntry::Steering`].
    pub id: SteeringId,
    /// The run it was accepted for.
    pub run_id: RunId,
    /// Its position in the run's acceptance order, from 0. Concurrent submissions are ordered by
    /// it, and queued instructions are applied in this order.
    pub sequence: u32,
}

/// Where an accepted instruction is.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub enum SteeringStatus {
    /// Accepted and waiting for the run's next model dispatch.
    Queued,
    /// Appended to the run's history by the model dispatch commit at this revision. This does not
    /// say the provider received the request or followed the instruction: a process can stop right
    /// after the commit, and recovery repeats the model attempt with the same history.
    Applied {
        /// The run's revision after that commit.
        revision: u64,
    },
    /// The run settled while the instruction was still queued. It stays inspectable but never
    /// enters any run's context.
    NotApplied,
}

/// One accepted instruction and its status, from [`SteeringStore::steering`]. Plain data, built by
/// stores.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SteeringInput {
    /// Its acknowledgement.
    pub receipt: SteeringReceipt,
    /// The instruction as accepted.
    pub instruction: String,
    /// Where it is.
    pub status: SteeringStatus,
}
impl SteeringInput {
    /// Instructions one run accepts, counting applied ones.
    pub const MAX_PER_RUN: usize = 128;
}

/// A [`DurableStore`] that accepts steering for its runs.
///
/// Implementors meet these obligations besides those of `DurableStore`; `tests/steering.rs` checks
/// them against every adapter, and `docs/steering.md` describes the behaviour.
///
/// - **Acceptance.** `steer` records the instruction for exactly the run named, in one atomic
///   write, and returns its receipt only after that write. It changes neither the run's
///   generation nor its revision, so it never fences or interrupts the execution that owns the
///   run, and it never makes a run due.
/// - **Idempotency.** Keys are scoped to the run, independently of acceptance keys. A repeated key
///   with the same instruction returns the original receipt, in any state of the run; a repeated
///   key with a different instruction is [`Error::IdempotencyConflict`].
/// - **Admission.** A new key is [`Error::RunNotActive`] once the run settled, has a cancellation
///   request or has expired, and [`Error::NotFound`] for an unknown run. It is [`Error::Input`]
///   for a key outside 1..=1024 bytes, an instruction that is empty or only whitespace, a run that
///   already accepted [`SteeringInput::MAX_PER_RUN`] instructions, or one whose input and accepted
///   instructions, all pinned in context, would no longer fit `max_context_bytes`.
/// - **Application.** Queued instructions change a run only through the commits that
///   [`Checkpoint`](crate::Checkpoint) allows: they are appended to history by the model dispatch
///   commit, and they turn a completed settlement back into `Ready`. Both happen atomically with
///   that commit and its fencing, and a failed commit leaves them queued.
#[async_trait]
pub trait SteeringStore: DurableStore {
    /// Durably queue `instruction` for run `id` under `key`.
    async fn steer(&self, id: RunId, key: &str, instruction: String) -> Result<SteeringReceipt>;
    /// Every instruction the run accepted, in acceptance order; [`Error::NotFound`] for an unknown
    /// run.
    async fn steering(&self, id: RunId) -> Result<Vec<SteeringInput>>;
}

/// Steering through the host's store. Available when the store implements [`SteeringStore`],
/// including `Host<dyn SteeringStore>`.
impl<S: SteeringStore + ?Sized + 'static> Host<S> {
    /// Durably queues `instruction` for run `id`, as [`SteeringStore::steer`] describes. The run's
    /// model request in flight, its tool calls and its waits continue unchanged; the instruction
    /// enters its history at the next model dispatch. Persist `key` before calling, so a retry
    /// after a lost acknowledgement returns the same receipt.
    pub async fn steer(
        &self,
        id: RunId,
        key: &str,
        instruction: impl Into<String>,
    ) -> Result<SteeringReceipt> {
        self.health()?;
        self.store().steer(id, key, instruction.into()).await
    }
    /// Every instruction run `id` accepted, with its status, in acceptance order. Authoritative
    /// after a reconnect, unlike progress events.
    pub async fn steering(&self, id: RunId) -> Result<Vec<SteeringInput>> {
        self.store().steering(id).await
    }
}

/// Which steering change a commit of `state` may make, as [`Checkpoint`](crate::Checkpoint)
/// documents. Stores decide with this alone, and only for commits made by an execution, never
/// for a [`RunUpdate`](crate::RunUpdate).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Boundary {
    /// The commit leaves queued instructions queued.
    None,
    /// The model dispatch commit: append queued instructions to history.
    Apply,
    /// A completed settlement: with instructions queued, commit `Ready` instead.
    Reopen,
}
impl Boundary {
    pub(crate) fn of(state: &RunState) -> Self {
        match state.phase {
            Phase::ModelInFlight => Self::Apply,
            Phase::Settled(Settlement::Completed(_)) => Self::Reopen,
            _ => Self::None,
        }
    }
}

/// Bounds on a request, checked before a store looks up its key.
pub(crate) fn check_request(key: &str, instruction: &str) -> Result<()> {
    if key.is_empty() || key.len() > 1024 {
        return Err(Error::input("steering keys must contain 1..=1024 bytes"));
    }
    if instruction.trim().is_empty() {
        return Err(Error::input("a steering instruction must not be empty"));
    }
    Ok(())
}
/// An instruction that passed admission, for a store to record.
pub(crate) struct Accepted {
    pub(crate) id: SteeringId,
    pub(crate) instruction: String,
    /// Encoded size of its [`HistoryEntry::Steering`].
    pub(crate) bytes: usize,
}
impl Accepted {
    pub(crate) fn entry(&self) -> HistoryEntry {
        HistoryEntry::Steering {
            id: self.id,
            instruction: self.instruction.clone(),
        }
    }
}
/// Checks a new key's instruction for `state` before a store records it, given how many
/// instructions the run already accepted and the sum of their encoded sizes.
pub(crate) fn admit(
    state: &RunState,
    instruction: String,
    accepted: usize,
    accepted_bytes: usize,
) -> Result<Accepted> {
    if state.is_terminal()
        || state.cancel_requested
        || state.expires_at_ms.is_some_and(|at| at <= now_ms())
    {
        return Err(Error::RunNotActive);
    }
    if accepted >= SteeringInput::MAX_PER_RUN {
        return Err(Error::input(
            "run accepted its maximum number of instructions",
        ));
    }
    let mut admitted = Accepted {
        id: SteeringId::new_v4(),
        instruction,
        bytes: 0,
    };
    admitted.bytes = encoded_len(&admitted.entry());
    // The pinned part of every later turn: the input and each instruction, in one JSON array.
    let input = encoded_len(&state.history[0]);
    let pinned = (2 + input)
        .saturating_add(accepted_bytes)
        .saturating_add(accepted)
        .saturating_add(admitted.bytes + 1);
    if pinned > state.limits.max_context_bytes {
        return Err(Error::input(
            "run input and its instructions exceed the context limit",
        ));
    }
    Ok(admitted)
}
/// The status of an instruction applied at `revision`, if at all, in a run that may have settled.
pub(crate) fn status(applied: Option<u64>, settled: bool) -> SteeringStatus {
    match applied {
        Some(revision) => SteeringStatus::Applied { revision },
        None if settled => SteeringStatus::NotApplied,
        None => SteeringStatus::Queued,
    }
}
