//! A single-owner application around the public durable-host interfaces.
//!
//! Serve [`router`] and drain HTTP requests before calling [`Application::shutdown`].
//! Keep the entire data directory: the catalogue journals admissions and the runtime
//! database owns execution, history, cancellation, and operation outcomes.
//!
//! The application uses only the library's public API: a [`Host`] over a [`SqliteStore`]
//! executes runs, [`DurableStore`] reads serve snapshots, and HTTP statuses follow the
//! library's [`Error`] variants.
mod catalog;
mod http;
pub mod notebook;

pub use catalog::{Conversation, Submission};
pub use http::{AccessToken, router};

use agent_rs::{sqlite::SqliteStore, *};
use catalog::{Admission, Catalog};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{path::Path, sync::Arc, time::Duration};
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PageRequest {
    pub after: i64,
    pub limit: usize,
}
impl Default for PageRequest {
    fn default() -> Self {
        Self {
            after: 0,
            limit: 50,
        }
    }
}
impl PageRequest {
    fn validate(self) -> Result<Self> {
        if self.after < 0 || !(1..=100).contains(&self.limit) {
            return Err(Error::input(
                "after must be nonnegative and limit must be 1..=100",
            ));
        }
        Ok(self)
    }
}

#[derive(Serialize)]
pub struct Page<T> {
    pub items: Vec<T>,
    /// Cursor over examined records, even when a filtered inbox page is empty.
    pub next_cursor: Option<i64>,
}

#[derive(Serialize)]
pub struct ConversationView {
    pub conversation: Conversation,
    pub submissions: Page<SubmissionView>,
}

#[derive(Serialize)]
pub struct SubmissionView {
    pub sequence: i64,
    pub conversation_id: Uuid,
    pub key: String,
    pub input: Value,
    pub admission: &'static str,
    pub error: Option<String>,
    pub run: Option<RunView>,
}

#[derive(Serialize)]
pub struct RunView {
    pub id: RunId,
    pub conversation_id: String,
    pub generation: u64,
    pub revision: u64,
    pub phase: Phase,
    pub input: Value,
    pub usage: Usage,
    pub history: Vec<HistoryEntry>,
    /// Durable evidence from all batches; actions come only from `decisions`.
    pub operations: Vec<Operation>,
    pub decisions: Vec<DecisionView>,
    pub cancel_requested: bool,
    pub can_cancel: bool,
    pub last_error: Option<String>,
}

#[derive(Serialize)]
pub struct DecisionView {
    pub run_id: RunId,
    pub call_id: String,
    pub generation: u64,
    pub revision: u64,
    pub tool: String,
    pub arguments: Value,
    pub kind: &'static str,
    pub prompt: String,
    pub actions: Vec<&'static str>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Decision {
    Approve,
    Deny,
    Input { value: Value },
    Reconcile { value: Value, is_error: bool },
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolveRequest {
    pub call_id: String,
    pub generation: u64,
    pub revision: u64,
    pub decision: Decision,
}

pub struct Application {
    /// Behind a lock only because [`Host::shutdown`] takes `&mut self`.
    host: RwLock<Host<SqliteStore>>,
    /// The host's own store, `host.store()`, kept to read snapshots without the host lock.
    store: Arc<SqliteStore>,
    catalog: Catalog,
    agent: Agent,
    admission_lock: Mutex<()>,
    decision_lock: Mutex<()>,
    stopping: CancellationToken,
    recovery: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    recovery_error: std::sync::Mutex<bool>,
}

impl Application {
    /// Opens the data directory and starts hosting `agent`. Fails with [`Error::StoreBusy`]
    /// while another application owns the directory.
    pub async fn open<I, O>(directory: impl AsRef<Path>, agent: Agent<I, O>) -> Result<Arc<Self>> {
        let directory = directory.as_ref();
        let agent = agent.erased();
        // This engine-owned process lock also guards the catalogue's admission protocol.
        // Opening a second application against the same directory is refused.
        let store = Arc::new(SqliteStore::open(directory.join("runtime.sqlite")).await?);
        let catalog = Catalog::open(directory.join("application.sqlite")).await?;
        let host = Host::start(store, agent.clone(), HostOptions::default())?;
        let store = host.store().clone();
        let application = Arc::new(Self {
            host: RwLock::new(host),
            store,
            catalog,
            agent,
            admission_lock: Mutex::new(()),
            decision_lock: Mutex::new(()),
            stopping: CancellationToken::new(),
            recovery: std::sync::Mutex::new(None),
            recovery_error: std::sync::Mutex::new(false),
        });
        let weak = Arc::downgrade(&application);
        let stopping = application.stopping.clone();
        let task = tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = stopping.cancelled() => break,
                    _ = tick.tick() => {
                        let Some(app) = weak.upgrade() else { break; };
                        let failed = app.recover_admissions().await.is_err();
                        *app.recovery_error.lock().expect("recovery status") = failed;
                    }
                }
            }
        });
        *application.recovery.lock().expect("recovery task") = Some(task);
        Ok(application)
    }

    pub fn agent_id(&self) -> &str {
        self.agent.id()
    }

    pub async fn healthy(&self) -> bool {
        self.host.read().await.health().is_ok()
            && !*self.recovery_error.lock().expect("recovery status")
    }

    pub async fn create_conversation(&self, id: Uuid, title: String) -> Result<Conversation> {
        if self.stopping.is_cancelled() {
            return Err(Error::Interrupted);
        }
        if title.trim().is_empty() || title.len() > 200 {
            return Err(Error::input("title must contain 1..=200 bytes"));
        }
        self.catalog.create_conversation(id, title).await
    }

    pub async fn conversations(&self, page: PageRequest) -> Result<Page<Conversation>> {
        let page = page.validate()?;
        let mut items = self
            .catalog
            .conversations(page.after, page.limit + 1)
            .await?;
        let has_more = items.len() > page.limit;
        items.truncate(page.limit);
        let next_cursor = has_more.then(|| items.last().expect("nonempty page").sequence);
        Ok(Page { items, next_cursor })
    }

    /// Returns only after both durable admission and catalogue association are committed.
    /// Retry a disconnected request with the identical conversation, key, and payload.
    pub async fn submit(&self, conversation: Uuid, submission: Submission) -> Result<RunId> {
        let _lock = self.admission_lock.lock().await;
        if self.stopping.is_cancelled() {
            return Err(Error::Interrupted);
        }
        self.host.read().await.health()?;
        self.catalog.conversation(conversation).await?;
        if submission.key.is_empty() || submission.key.len() > 200 {
            return Err(Error::input("key must contain 1..=200 bytes"));
        }
        let admission = if let Some(existing) = self
            .catalog
            .lookup(conversation, submission.key.clone())
            .await?
        {
            if existing.submission != submission {
                return Err(Error::IdempotencyConflict);
            }
            existing
        } else {
            // Reject invalid inputs before journaling: they must never become queued work.
            self.agent.new_run(
                conversation.to_string(),
                submission.input.clone(),
                &[],
                submission.expires_at_ms,
            )?;
            self.catalog
                .reserve(conversation, submission, self.agent.binding().clone())
                .await?
        };
        self.admit(admission).await
    }

    async fn admit(&self, admission: Admission) -> Result<RunId> {
        if let Some(id) = admission.run_id {
            return Ok(id);
        }
        if let Some(rejection) = admission.rejection {
            return Err(match rejection.as_str() {
                "conversation_busy" => Error::ConversationBusy,
                "idempotency_conflict" => Error::IdempotencyConflict,
                _ => Error::input(rejection),
            });
        }
        if &admission.binding != self.agent.binding() {
            return Err(Error::incompatible(
                "pending admission requires its original agent configuration",
            ));
        }
        let result = self
            .host
            .read()
            .await
            .submit(
                &admission.conversation_id.to_string(),
                &admission.submission.key,
                admission.submission.input,
                admission.submission.expires_at_ms,
            )
            .await;
        match result {
            Ok(id) => {
                self.catalog.accepted(admission.sequence, id).await?;
                Ok(id)
            }
            Err(error) => {
                let rejected = match &error {
                    Error::ConversationBusy => Some("conversation_busy"),
                    Error::IdempotencyConflict => Some("idempotency_conflict"),
                    Error::Input(_) => Some("invalid_input"),
                    _ => None,
                };
                if let Some(reason) = rejected {
                    self.catalog
                        .reject(admission.sequence, reason.into())
                        .await?;
                }
                // An ambiguous storage failure stays pending and is retried idempotently.
                Err(error)
            }
        }
    }

    async fn recover_admissions(&self) -> Result<()> {
        let _lock = self.admission_lock.lock().await;
        let mut after = 0;
        let mut error = None;
        loop {
            if self.stopping.is_cancelled() {
                break;
            }
            let pending = self.catalog.list(None, after, 50, true).await?;
            if pending.is_empty() {
                break;
            }
            for admission in pending {
                after = admission.sequence;
                if let Err(e) = self.admit(admission).await
                    && !matches!(
                        e,
                        Error::ConversationBusy | Error::IdempotencyConflict | Error::Input(_)
                    )
                {
                    error = Some(e);
                }
            }
        }
        error.map_or(Ok(()), Err)
    }

    pub async fn conversation(&self, id: Uuid, page: PageRequest) -> Result<ConversationView> {
        let conversation = self.catalog.conversation(id).await?;
        let submissions = self.submissions(Some(id), page, false).await?;
        Ok(ConversationView {
            conversation,
            submissions,
        })
    }

    /// Includes pending admissions and terminal runs whose effects still need reconciliation.
    pub async fn inbox(&self, page: PageRequest) -> Result<Page<SubmissionView>> {
        self.submissions(None, page, true).await
    }

    async fn submissions(
        &self,
        conversation: Option<Uuid>,
        page: PageRequest,
        inbox: bool,
    ) -> Result<Page<SubmissionView>> {
        let page = page.validate()?;
        let mut admissions = self
            .catalog
            .list(conversation, page.after, page.limit + 1, false)
            .await?;
        let has_more = admissions.len() > page.limit;
        admissions.truncate(page.limit);
        let next_cursor = has_more.then(|| admissions.last().expect("nonempty page").sequence);
        let mut items = Vec::with_capacity(admissions.len());
        for admission in admissions {
            let run = if let Some(id) = admission.run_id {
                let run = self.load_run(id).await?;
                if matches!(run.phase, Phase::Settled(_)) && run.decisions.is_empty() {
                    // Terminal state cannot resume. This durable index only excludes settled,
                    // fully reconciled runs; it is never an authority for a run's status.
                    self.catalog.retire(admission.sequence).await?;
                    if inbox {
                        continue;
                    }
                }
                Some(run)
            } else {
                None
            };
            let error = admission.rejection.clone().or_else(|| {
                (run.is_none() && &admission.binding != self.agent.binding()).then(|| {
                    "Restore the original agent configuration to recover this admission.".into()
                })
            });
            items.push(SubmissionView {
                sequence: admission.sequence,
                conversation_id: admission.conversation_id,
                key: admission.submission.key,
                input: admission.submission.input,
                admission: if run.is_some() {
                    "accepted"
                } else if admission.rejection.is_some() {
                    "rejected"
                } else {
                    "pending"
                },
                error,
                run,
            });
        }
        Ok(Page { items, next_cursor })
    }

    pub async fn run(&self, id: RunId) -> Result<RunView> {
        self.catalog.by_run(id).await?;
        self.load_run(id).await
    }

    async fn load_run(&self, id: RunId) -> Result<RunView> {
        let state = self.store.status(id).await?;
        let mut decisions = Vec::new();
        for operation in &state.operations {
            let (kind, prompt, actions) = match &operation.status {
                OperationStatus::Approval if !state.is_terminal() && !state.cancel_requested => (
                    "approval",
                    format!("Allow {}?", operation.call.name),
                    vec!["approve", "deny"],
                ),
                OperationStatus::Waiting(WaitRequest::Input { prompt })
                    if !state.is_terminal() && !state.cancel_requested =>
                {
                    ("input", prompt.clone(), vec!["input"])
                }
                OperationStatus::Unknown { reason } => {
                    ("reconciliation", reason.clone(), vec!["reconcile"])
                }
                _ => continue,
            };
            decisions.push(DecisionView {
                run_id: id,
                call_id: operation.call.id.clone(),
                generation: state.generation,
                revision: state.revision,
                tool: operation.call.name.clone(),
                arguments: operation.call.arguments.clone(),
                kind,
                prompt,
                actions,
            });
        }
        let operations = self.store.operations(id).await?;
        let last_error = self.store.last_error(id).await?;
        let can_cancel = !state.is_terminal() && !state.cancel_requested;
        Ok(RunView {
            id,
            conversation_id: state.conversation_id,
            generation: state.generation,
            revision: state.revision,
            phase: state.phase,
            input: state.input,
            usage: state.usage,
            history: state.history,
            operations,
            decisions,
            cancel_requested: state.cancel_requested,
            can_cancel,
            last_error,
        })
    }

    /// Applies a decision against the exact durable revision shown to the user.
    /// Stale or repeated decisions return a conflict; clients must refresh before deciding again.
    pub async fn resolve(&self, id: RunId, request: ResolveRequest) -> Result<()> {
        let _lock = self.decision_lock.lock().await;
        if self.stopping.is_cancelled() {
            return Err(Error::Interrupted);
        }
        self.catalog.by_run(id).await?;
        let mut state = self.store.status(id).await?;
        if state.generation != request.generation || state.revision != request.revision {
            return Err(Error::Stale);
        }
        if state.cancel_requested && !matches!(request.decision, Decision::Reconcile { .. }) {
            return Err(Error::NotWaiting);
        }
        let resolution = match request.decision {
            Decision::Approve => Resolution::Approve(true),
            Decision::Deny => Resolution::Approve(false),
            Decision::Input { value } => Resolution::Input(value),
            Decision::Reconcile { value, is_error } => Resolution::Reconcile(ToolResult {
                call_id: request.call_id.clone(),
                value,
                is_error,
            }),
        };
        if state.is_terminal() {
            // Only reconciliation is legal after settlement. There is no running worker;
            // the application decision lock serializes this terminal-only update.
            self.host
                .read()
                .await
                .resolve(id, &request.call_id, resolution)
                .await
        } else {
            state.resolve(&request.call_id, resolution)?;
            // The public checkpoint API fences the read/decide/write race with a worker
            // or cancellation. `Host::resolve` cannot be told the revision the user saw,
            // so this commits directly, and the host discovers it on its next poll.
            self.store.commit(&mut state).await
        }
    }

    pub async fn cancel(&self, id: RunId) -> Result<()> {
        if self.stopping.is_cancelled() {
            return Err(Error::Interrupted);
        }
        self.catalog.by_run(id).await?;
        self.host.read().await.cancel(id).await
    }

    /// Drain HTTP requests first. Accepted work is retained; no task is aborted.
    pub async fn shutdown(&self) -> Result<()> {
        self.stopping.cancel();
        let task = self.recovery.lock().expect("recovery task").take();
        if let Some(task) = task {
            task.await.map_err(Error::storage)?;
        }
        let _admissions = self.admission_lock.lock().await;
        let _decisions = self.decision_lock.lock().await;
        let host_result = self.host.write().await.shutdown().await;
        let store_result = self.store.close().await;
        let catalog_result = self.catalog.close().await;
        host_result.and(store_result).and(catalog_result)
    }
}

impl Drop for Application {
    fn drop(&mut self) {
        self.stopping.cancel();
    }
}

#[cfg(test)]
mod tests;
