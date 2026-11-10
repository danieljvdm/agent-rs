//! A process-local [`DurableStore`]: the reference implementation of the store contract.
//! Nothing survives the process, so it suits tests, examples and ephemeral hosts.
use super::{advance, check_acceptance};
use crate::{
    Agent, Checkpoint, CheckpointPosition, DurableStore, Error, HistoryEntry, HostSlot, Operation,
    Result, RunId, RunState, RunUpdate, StoreLease,
};
use async_trait::async_trait;
use serde_json::Value;

use std::{collections::HashMap, sync::Mutex};

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
    /// Conversation history with each entry's encoded size, for bounded prior context.
    conversations: HashMap<String, Vec<(HistoryEntry, usize)>>,
}
struct Run {
    state: RunState,
    /// Encoded input and binding plus expiry, compared on idempotent acceptance.
    acceptance: (String, String, Option<u64>),
    wake_at: Option<u64>,
    last_error: Option<String>,
    operations: Vec<Operation>,
}

fn encode(value: &impl serde::Serialize) -> Result<String> {
    serde_json::to_string(value).map_err(Error::storage)
}
fn sizes(entries: &[HistoryEntry]) -> Result<Vec<(HistoryEntry, usize)>> {
    entries
        .iter()
        .map(|entry| Ok((entry.clone(), encode(entry)?.len())))
        .collect()
}

impl MemoryStore {
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
    /// After `run.state` changed: index its operations, discovery fields and new history.
    fn persist(&mut self, id: RunId, history: Vec<(HistoryEntry, usize)>, wake_now: bool) {
        let run = self.runs.get_mut(&id).expect("run exists");
        for op in &run.state.operations {
            match run
                .operations
                .iter_mut()
                .find(|old| old.call.id == op.call.id)
            {
                Some(old) => *old = op.clone(),
                None => run.operations.push(op.clone()),
            }
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
        let appended = sizes(&state.history[stored.history.len()..])?;
        // Copy progress only: acceptance fields in the caller's state are never persisted.
        let stored = &mut run.state;
        stored.revision = revision;
        stored
            .history
            .extend(appended.iter().map(|(entry, _)| entry.clone()));
        stored.cancel_requested = state.cancel_requested;
        stored.phase = state.phase.clone();
        stored.usage = state.usage.clone();
        stored.pending_response = state.pending_response.clone();
        stored.operations = state.operations.clone();
        inner.persist(state.id, appended, false);
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
        check_acceptance(conversation, key, expires_at_ms)?;
        let acceptance = (encode(&input)?, encode(agent.binding())?, expires_at_ms);
        let mut inner = self.lock()?;
        let identity = (conversation.to_owned(), key.to_owned());
        if let Some(id) = inner.keys.get(&identity) {
            return if inner.runs[id].acceptance == acceptance {
                Ok(*id)
            } else {
                Err(Error::IdempotencyConflict)
            };
        }
        if inner.unsettled.contains_key(conversation) {
            return Err(Error::ConversationBusy);
        }
        let mut context = Vec::new();
        let mut bytes = 2usize;
        for (entry, size) in inner
            .conversations
            .get(conversation)
            .into_iter()
            .flatten()
            .rev()
        {
            bytes = bytes.saturating_add(*size).saturating_add(1);
            if bytes > agent.limits().max_context_bytes {
                break;
            }
            context.push(entry.clone());
        }
        context.reverse();
        let state = agent.new_run(conversation, input, &context, expires_at_ms)?;
        let id = state.id;
        let history = sizes(&state.history)?;
        inner.keys.insert(identity, id);
        inner.unsettled.insert(conversation.to_owned(), id);
        inner.runs.insert(
            id,
            Run {
                state,
                acceptance,
                wake_at: None,
                last_error: None,
                operations: Vec::new(),
            },
        );
        inner.persist(id, history, false);
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
        let appended = sizes(&state.history[run.state.history.len()..])?;
        if effect.clear_error {
            run.last_error = None;
        }
        run.state = state;
        inner.persist(id, appended, effect.wake_now);
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
            .map(|(entry, _)| entry.clone())
            .collect())
    }
    async fn operations(&self, id: RunId) -> Result<Vec<Operation>> {
        Ok(self
            .lock()?
            .runs
            .get(&id)
            .map(|run| run.operations.clone())
            .unwrap_or_default())
    }
}
