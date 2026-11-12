//! A process-local [`DurableStore`]: the reference implementation of the store contract.
//! Nothing survives the process, so it suits tests, examples and ephemeral hosts.
use super::{advance, check_acceptance};
use crate::steering::{self, Boundary};
use crate::{
    Agent, AttachedChild, Checkpoint, CheckpointPosition, DurableStore, Error, HistoryEntry,
    HostSlot, Operation, Phase, Result, RunId, RunState, RunUpdate, SteeringInput, SteeringReceipt,
    SteeringStore, StoreLease,
};
use async_trait::async_trait;
use serde_json::Value;

use std::{collections::HashMap, sync::Mutex};

mod automation;

/// Runs held in memory behind one lock. Each method is one atomic step.
#[derive(Default)]
pub struct MemoryStore {
    slot: HostSlot,
    inner: Mutex<Inner>,
}
#[derive(Default)]
struct Inner {
    runs: HashMap<RunId, Run>,
    keys: HashMap<(String, String), RunId>,
    unsettled: HashMap<String, RunId>,
    /// Conversation history in commit order.
    conversations: HashMap<String, Vec<HistoryEntry>>,
    /// Every operation each child recorded, as `Run::operations` holds a run's own.
    child_operations: HashMap<RunId, Vec<Operation>>,
    /// Automations, kept under the same lock so a firing is one atomic step.
    automations: automation::Automations,
}
struct Run {
    state: RunState,
    /// Encoded input and binding plus expiry, compared on idempotent acceptance.
    acceptance: (String, String, Option<u64>),
    wake_at: Option<u64>,
    last_error: Option<String>,
    operations: Vec<Operation>,
    /// Accepted steering in acceptance order.
    steering: Vec<Steered>,
}
struct Steered {
    key: String,
    receipt: SteeringReceipt,
    accepted: steering::Accepted,
    applied: Option<u64>,
}

fn encode(value: &impl serde::Serialize) -> Result<String> {
    serde_json::to_string(value).map_err(Error::storage)
}
/// The history each child whose revision advanced appended, by its index in `new`. A child
/// unknown to `stored` was admitted, and all of its history is new.
type ChildHistory = Vec<(usize, Vec<HistoryEntry>)>;
fn changed_children(stored: &[AttachedChild], new: &[AttachedChild]) -> Result<ChildHistory> {
    let mut changed = Vec::new();
    for (index, child) in new.iter().enumerate() {
        let durable = match stored.get(index) {
            None => 0,
            Some(old) if old.state.id != child.state.id => {
                return Err(Error::input("children are retained in admission order"));
            }
            Some(old) if old.state.revision == child.state.revision => continue,
            Some(old) => old.state.history.len(),
        };
        let appended = (child.state.history.get(durable..))
            .ok_or_else(|| Error::input("history is append-only"))?;
        changed.push((index, appended.to_vec()));
    }
    Ok(changed)
}
/// Persists a child's progress and appends its history; its acceptance never changes.
fn merge_child(stored: &mut Vec<AttachedChild>, index: usize, new: &AttachedChild) {
    let Some(old) = stored.get_mut(index) else {
        stored.push(new.clone());
        return;
    };
    let (old, new) = (&mut old.state, &new.state);
    let durable = old.history.len();
    old.history.extend_from_slice(&new.history[durable..]);
    old.revision = new.revision;
    old.cancel_requested = new.cancel_requested;
    old.phase = new.phase.clone();
    old.usage = new.usage.clone();
    old.pending_response = new.pending_response.clone();
    old.operations = new.operations.clone();
}

impl MemoryStore {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Inner>> {
        self.inner
            .lock()
            .map_err(|_| Error::storage("memory store lock poisoned"))
    }
}
impl Inner {
    fn run(&mut self, id: RunId) -> Result<&mut Run> {
        self.runs.get_mut(&id).ok_or(Error::NotFound)
    }
    /// [`DurableStore::accept`] under the store's lock, so an automation firing can accept
    /// under the same lock as its own writes. True when the run is new.
    fn accept(
        &mut self,
        agent: &Agent,
        conversation: &str,
        key: &str,
        input: Value,
        expires_at_ms: Option<u64>,
    ) -> Result<(RunId, bool)> {
        check_acceptance(conversation, key, expires_at_ms)?;
        let acceptance = (encode(&input)?, encode(agent.binding())?, expires_at_ms);
        let identity = (conversation.to_owned(), key.to_owned());
        if let Some(id) = self.keys.get(&identity) {
            return if self.runs[id].acceptance == acceptance {
                Ok((*id, false))
            } else {
                Err(Error::IdempotencyConflict)
            };
        }
        if self.unsettled.contains_key(conversation) {
            return Err(Error::ConversationBusy);
        }
        let mut prior = agent.prior_context();
        for entry in self
            .conversations
            .get(conversation)
            .into_iter()
            .flatten()
            .rev()
        {
            if !prior.push(entry.clone()) {
                break;
            }
        }
        let state = agent.new_run_from(conversation, input, prior, expires_at_ms)?;
        let id = state.id;
        let history = state.history.clone();
        self.keys.insert(identity, id);
        self.unsettled.insert(conversation.to_owned(), id);
        self.runs.insert(
            id,
            Run {
                state,
                acceptance,
                wake_at: None,
                last_error: None,
                operations: Vec::new(),
                steering: Vec::new(),
            },
        );
        self.persist(id, history, false, Vec::new());
        Ok((id, true))
    }
    /// After `run.state` changed: index its operations, discovery fields and new history, and
    /// those of the children that changed.
    fn persist(
        &mut self,
        id: RunId,
        history: Vec<HistoryEntry>,
        wake_now: bool,
        children: ChildHistory,
    ) {
        let run = self.runs.get_mut(&id).expect("run exists");
        retain(&mut run.operations, &run.state.operations);
        for (index, history) in children {
            let child = &run.state.children[index].state;
            retain(
                self.child_operations.entry(child.id).or_default(),
                &child.operations,
            );
            self.conversations
                .entry(child.conversation_id.clone())
                .or_default()
                .extend(history);
        }
        run.wake_at = if wake_now {
            Some(0)
        } else {
            run.state.wake_at()
        };
        if run.state.is_terminal() {
            run.last_error = None;
            // A settled run stays resolvable after a newer run took its conversation.
            if self.unsettled.get(&run.state.conversation_id) == Some(&id) {
                self.unsettled.remove(&run.state.conversation_id);
            }
        }
        self.conversations
            .entry(run.state.conversation_id.clone())
            .or_default()
            .extend(history);
    }
}
/// Retains every operation recorded, replacing earlier versions of the same call.
fn retain(retained: &mut Vec<Operation>, operations: &[Operation]) {
    for op in operations {
        match retained.iter_mut().find(|old| old.call.id == op.call.id) {
            Some(old) => *old = op.clone(),
            None => retained.push(op.clone()),
        }
    }
}

#[async_trait]
impl Checkpoint for MemoryStore {
    async fn commit(&self, state: &mut RunState) -> Result<()> {
        let mut inner = self.lock()?;
        let run = inner.run(state.id)?;
        let stored = &run.state;
        if stored.is_terminal()
            || stored.generation != state.generation
            || stored.revision != state.revision
        {
            return Err(Error::Stale);
        }
        if state.history.len() < stored.history.len() {
            return Err(Error::input("history is append-only"));
        }
        let revision = advance(state.revision, "revision")?;
        let mut appended = state.history[stored.history.len()..].to_vec();
        let children = changed_children(&stored.children, &state.children)?;
        // The steering changes `Checkpoint` allows, after the last step that can fail.
        let boundary = Boundary::of(state);
        let mut queued = run.steering.iter_mut().filter(|s| s.applied.is_none());
        match boundary {
            Boundary::Apply => {
                for steered in queued {
                    let entry = steered.accepted.entry();
                    steered.applied = Some(revision);
                    appended.push(entry.clone());
                    state.history.push(entry);
                }
            }
            Boundary::Reopen if queued.next().is_some() => state.phase = Phase::Ready,
            Boundary::Reopen | Boundary::None => (),
        }
        // Copy progress only: acceptance fields in the caller's state are never persisted.
        let stored = &mut run.state;
        stored.revision = revision;
        stored.history.extend(appended.iter().cloned());
        stored.cancel_requested = state.cancel_requested;
        stored.phase = state.phase.clone();
        stored.usage = state.usage.clone();
        stored.pending_response = state.pending_response.clone();
        stored.operations = state.operations.clone();
        stored.child_usage = state.child_usage.clone();
        for (index, _) in &children {
            merge_child(&mut stored.children, *index, &state.children[*index]);
        }
        inner.persist(state.id, appended, false, children);
        state.revision = revision;
        Ok(())
    }
}

#[async_trait]
impl DurableStore for MemoryStore {
    fn lease(&self) -> Result<StoreLease> {
        self.slot.lease()
    }
    async fn accept(
        &self,
        agent: &Agent,
        conversation: &str,
        key: &str,
        input: Value,
        expires_at_ms: Option<u64>,
    ) -> Result<RunId> {
        let mut inner = self.lock()?;
        let (id, _) = inner.accept(agent, conversation, key, input, expires_at_ms)?;
        Ok(id)
    }
    async fn status(&self, id: RunId) -> Result<RunState> {
        Ok(self.lock()?.run(id)?.state.clone())
    }
    async fn claim(&self, id: RunId) -> Result<RunState> {
        let mut inner = self.lock()?;
        let run = inner.run(id)?;
        if run.state.is_terminal() {
            return Err(Error::Stale);
        }
        let generation = advance(run.state.generation, "generation")?;
        let revision = advance(run.state.revision, "revision")?;
        run.state.generation = generation;
        run.state.revision = revision;
        run.wake_at = run.state.wake_at();
        Ok(run.state.clone())
    }
    async fn update(&self, id: RunId, update: RunUpdate) -> Result<()> {
        let mut inner = self.lock()?;
        let run = inner.run(id)?;
        let mut state = run.state.clone();
        let effect = update.apply(&mut state)?;
        if !effect.changed {
            return Ok(());
        }
        state.revision = advance(state.revision, "revision")?;
        let appended = state.history[run.state.history.len()..].to_vec();
        let children = changed_children(&run.state.children, &state.children)?;
        if effect.clear_error {
            run.last_error = None;
        }
        run.state = state;
        inner.persist(id, appended, effect.wake_now, children);
        Ok(())
    }
    async fn due(&self, now: u64, limit: usize) -> Result<Vec<RunId>> {
        let inner = self.lock()?;
        let mut due: Vec<_> = inner
            .runs
            .iter()
            .filter_map(|(id, run)| run.wake_at.filter(|at| *at <= now).map(|at| (at, *id)))
            .collect();
        due.sort();
        Ok(due.into_iter().take(limit).map(|(_, id)| id).collect())
    }
    async fn recovery_page(&self, after: Option<RunId>, limit: usize) -> Result<Vec<RunId>> {
        let inner = self.lock()?;
        let mut ids: Vec<_> = inner
            .runs
            .iter()
            .filter(|(id, run)| {
                after.is_none_or(|after| **id > after)
                    && run.last_error.is_none()
                    && run.state.needs_recovery()
            })
            .map(|(id, _)| *id)
            .collect();
        ids.sort();
        ids.truncate(limit);
        Ok(ids)
    }
    async fn position(&self, id: RunId) -> Result<CheckpointPosition> {
        let mut inner = self.lock()?;
        let run = inner.run(id)?;
        if run.state.is_terminal() {
            return Err(Error::NotFound);
        }
        Ok(CheckpointPosition::from(&run.state))
    }
    async fn record_error(
        &self,
        id: RunId,
        expected: CheckpointPosition,
        message: String,
    ) -> Result<bool> {
        let mut inner = self.lock()?;
        let run = inner.run(id)?;
        if run.state.is_terminal() || CheckpointPosition::from(&run.state) != expected {
            return Ok(false);
        }
        run.wake_at = None;
        run.last_error = Some(message);
        Ok(true)
    }
    async fn last_error(&self, id: RunId) -> Result<Option<String>> {
        Ok(self.lock()?.run(id)?.last_error.clone())
    }
    async fn history(&self, conversation: &str) -> Result<Vec<HistoryEntry>> {
        Ok(self
            .lock()?
            .conversations
            .get(conversation)
            .into_iter()
            .flatten()
            .cloned()
            .collect())
    }
    async fn operations(&self, id: RunId) -> Result<Vec<Operation>> {
        let inner = self.lock()?;
        let retained = match inner.runs.get(&id) {
            Some(run) => Some(&run.operations),
            None => inner.child_operations.get(&id),
        };
        Ok(retained.cloned().unwrap_or_default())
    }
}

#[async_trait]
impl SteeringStore for MemoryStore {
    async fn steer(&self, id: RunId, key: &str, instruction: String) -> Result<SteeringReceipt> {
        steering::check_request(key, &instruction)?;
        let mut inner = self.lock()?;
        let run = inner.run(id)?;
        if let Some(steered) = run.steering.iter().find(|s| s.key == key) {
            return if steered.accepted.instruction == instruction {
                Ok(steered.receipt.clone())
            } else {
                Err(Error::IdempotencyConflict)
            };
        }
        let bytes = run.steering.iter().map(|s| s.accepted.bytes).sum();
        let accepted = steering::admit(&run.state, instruction, run.steering.len(), bytes)?;
        let receipt = SteeringReceipt {
            id: accepted.id,
            run_id: id,
            sequence: run.steering.len() as u32,
        };
        run.steering.push(Steered {
            key: key.to_owned(),
            receipt: receipt.clone(),
            accepted,
            applied: None,
        });
        Ok(receipt)
    }
    async fn steering(&self, id: RunId) -> Result<Vec<SteeringInput>> {
        let mut inner = self.lock()?;
        let run = inner.run(id)?;
        let settled = run.state.is_terminal();
        Ok(run
            .steering
            .iter()
            .map(|steered| SteeringInput {
                receipt: steered.receipt.clone(),
                instruction: steered.accepted.instruction.clone(),
                status: steering::status(steered.applied, settled),
            })
            .collect())
    }
}
