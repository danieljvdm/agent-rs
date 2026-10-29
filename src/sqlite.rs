//! File-backed checkpoints. One bounded worker owns SQLite and the database file lock.
use crate::*;
use async_trait::async_trait;
use fs2::FileExt;
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde_json::{Value, json};
use std::{
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
};
use tokio::sync::{mpsc, oneshot};

const STORAGE_VERSION: i64 = 1;
type Job = Box<dyn FnOnce(&mut Database) + Send>;

#[derive(Clone, Copy, Debug, Default)]
pub struct StoreMetrics {
    pub transactions: u64,
    pub writes: u64,
}
/// Generation and revision identifying one authoritative active checkpoint.
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
#[derive(Default)]
struct Counters {
    transactions: AtomicU64,
    writes: AtomicU64,
}
struct Database {
    connection: Connection,
    _lock: File,
    counters: Arc<Counters>,
}
struct Worker {
    sender: Option<mpsc::Sender<Job>>,
    thread: Option<thread::JoinHandle<()>>,
}
/// Keep this store alive until all hosts using it have shut down. `close` joins its worker.
pub struct SqliteStore {
    worker: Mutex<Worker>,
    counters: Arc<Counters>,
    host_active: AtomicBool,
}

fn storage(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}
fn encode(value: &impl serde::Serialize) -> Result<String> {
    serde_json::to_string(value).map_err(storage)
}
fn decode_state(text: &str) -> Result<RunState> {
    let value: Value = serde_json::from_str(text).map_err(storage)?;
    if value.get("version").and_then(Value::as_u64) != Some(STATE_VERSION.into()) {
        return Err(Error::Incompatible("checkpoint state version".into()));
    }
    serde_json::from_value(value).map_err(storage)
}
fn wake_at(state: &RunState) -> Option<u64> {
    if state.is_terminal() {
        return None;
    }
    let ready = if state.cancel_requested {
        Some(0)
    } else {
        match state.phase {
            Phase::Ready | Phase::ModelInFlight => Some(0),
            Phase::Tools => {
                let mut latest = 0;
                let mut blocked = false;
                for op in &state.operations {
                    match &op.status {
                        OperationStatus::Approval
                        | OperationStatus::Unknown { .. }
                        | OperationStatus::Waiting(WaitRequest::Input { .. }) => blocked = true,
                        OperationStatus::Waiting(WaitRequest::Until { unix_ms }) => {
                            latest = latest.max(*unix_ms)
                        }
                        _ => (),
                    }
                }
                if blocked { None } else { Some(latest) }
            }
            Phase::Settled(_) => None,
        }
    };
    match (ready, state.expires_at_ms) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}
fn sql_time(value: Option<u64>) -> Option<i64> {
    value.map(|v| v.min(i64::MAX as u64) as i64)
}

impl Database {
    fn open(path: &Path, counters: Arc<Counters>) -> Result<Self> {
        if path.as_os_str().is_empty() || path == Path::new(":memory:") {
            return Err(Error::Invalid(
                "a file-backed database path is required".into(),
            ));
        }
        let database_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(storage)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if database_file.metadata().map_err(storage)?.nlink() != 1 {
                return Err(Error::Invalid("hard-linked SQLite databases are unsupported because WAL sidecars are path-specific".into()));
            }
        }
        let canonical = std::fs::canonicalize(path).map_err(storage)?;
        // A separate canonical sidecar avoids conflicting with SQLite's own advisory
        // locks (flock and fcntl interact on some Unix platforms). Hard links are
        // rejected above; symlink aliases therefore converge on this one owner lock.
        let mut lock_path = canonical.as_os_str().to_owned();
        lock_path.push(".lock");
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(PathBuf::from(lock_path))
            .map_err(storage)?;
        lock.try_lock_exclusive().map_err(|e| {
            if e.kind() == std::io::ErrorKind::WouldBlock {
                Error::DatabaseBusy
            } else {
                storage(e)
            }
        })?;
        let connection = Connection::open(canonical).map_err(storage)?;
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(storage)?;
        connection
            .pragma_update(None, "synchronous", "FULL")
            .map_err(storage)?;
        connection
            .pragma_update(None, "foreign_keys", "ON")
            .map_err(storage)?;
        let version: i64 = connection
            .pragma_query_value(None, "user_version", |r| r.get(0))
            .map_err(storage)?;
        if version != 0 && version != STORAGE_VERSION {
            return Err(Error::Incompatible(format!(
                "SQLite schema version {version}"
            )));
        }
        connection.execute_batch("BEGIN IMMEDIATE;
            CREATE TABLE IF NOT EXISTS runs (
                id TEXT PRIMARY KEY, conversation TEXT NOT NULL, idempotency_key TEXT NOT NULL,
                payload TEXT NOT NULL, terminal_state TEXT, last_error TEXT,
                UNIQUE(conversation, idempotency_key));
            CREATE TABLE IF NOT EXISTS active (
                run_id TEXT PRIMARY KEY REFERENCES runs(id), conversation TEXT NOT NULL UNIQUE,
                generation INTEGER NOT NULL, revision INTEGER NOT NULL, state TEXT NOT NULL,
                wake_at INTEGER);
            CREATE INDEX IF NOT EXISTS active_due ON active(wake_at) WHERE wake_at IS NOT NULL;
            CREATE TABLE IF NOT EXISTS history (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL REFERENCES runs(id),
                conversation TEXT NOT NULL, ordinal INTEGER NOT NULL, entry TEXT NOT NULL,
                UNIQUE(run_id, ordinal));
            CREATE INDEX IF NOT EXISTS history_conversation ON history(conversation, sequence);
            CREATE TABLE IF NOT EXISTS operations (
                run_id TEXT NOT NULL REFERENCES runs(id), call_id TEXT NOT NULL, operation TEXT NOT NULL,
                PRIMARY KEY(run_id, call_id));
            PRAGMA user_version=1; COMMIT;").map_err(storage)?;
        Ok(Self {
            connection,
            _lock: lock,
            counters,
        })
    }
    fn count(&self, writes: u64) {
        self.counters.transactions.fetch_add(1, Ordering::Relaxed);
        self.counters.writes.fetch_add(writes, Ordering::Relaxed);
    }
}

fn load(connection: &Connection, id: RunId) -> Result<RunState> {
    let text: Option<String> = connection.query_row(
        "SELECT COALESCE(a.state,r.terminal_state) FROM runs r LEFT JOIN active a ON a.run_id=r.id WHERE r.id=?1",
        [id.to_string()], |r| r.get(0)).optional().map_err(storage)?;
    decode_state(&text.ok_or(Error::NotFound)?)
}
fn append_evidence(tx: &Transaction<'_>, state: &RunState, from: usize) -> Result<u64> {
    let mut writes = 0;
    for (ordinal, entry) in state.history.iter().enumerate().skip(from) {
        tx.execute(
            "INSERT INTO history(run_id,conversation,ordinal,entry) VALUES(?1,?2,?3,?4)",
            params![
                state.id.to_string(),
                state.conversation_id,
                ordinal as i64,
                encode(entry)?
            ],
        )
        .map_err(storage)?;
        writes += 1;
    }
    for op in &state.operations {
        writes += tx.execute("INSERT INTO operations(run_id,call_id,operation) VALUES(?1,?2,?3)
            ON CONFLICT(run_id,call_id) DO UPDATE SET operation=excluded.operation WHERE operations.operation<>excluded.operation",
            params![state.id.to_string(), op.call.id, encode(op)?]).map_err(storage)? as u64;
    }
    Ok(writes)
}
fn write_state(tx: &Transaction<'_>, state: &RunState) -> Result<u64> {
    let encoded = encode(state)?;
    if state.is_terminal() {
        tx.execute(
            "UPDATE runs SET terminal_state=?2,last_error=NULL WHERE id=?1",
            params![state.id.to_string(), encoded],
        )
        .map_err(storage)?;
        let deleted = tx
            .execute("DELETE FROM active WHERE run_id=?1", [state.id.to_string()])
            .map_err(storage)?;
        Ok(1 + deleted as u64)
    } else {
        tx.execute(
            "UPDATE active SET generation=?2,revision=?3,state=?4,wake_at=?5 WHERE run_id=?1",
            params![
                state.id.to_string(),
                state.generation,
                state.revision,
                encoded,
                sql_time(wake_at(state))
            ],
        )
        .map_err(storage)?;
        Ok(1)
    }
}
fn immutable_matches(old: &RunState, new: &RunState) -> bool {
    old.id == new.id
        && old.conversation_id == new.conversation_id
        && old.agent_id == new.agent_id
        && old.binding == new.binding
        && old.input == new.input
        && old.limits == new.limits
        && old.expires_at_ms == new.expires_at_ms
        && old.prior_context == new.prior_context
        && old.version == new.version
        && new.history.starts_with(&old.history)
        && (!old.cancel_requested || new.cancel_requested)
}
fn bounded_history(
    connection: &Connection,
    conversation: &str,
    limit: usize,
) -> Result<Vec<HistoryEntry>> {
    let mut stmt = connection
        .prepare("SELECT entry FROM history WHERE conversation=?1 ORDER BY sequence DESC")
        .map_err(storage)?;
    let mut rows = stmt.query([conversation]).map_err(storage)?;
    let mut result = Vec::new();
    let mut bytes = 2usize;
    while let Some(row) = rows.next().map_err(storage)? {
        let text: String = row.get(0).map_err(storage)?;
        let next = bytes.saturating_add(text.len()).saturating_add(1);
        if next > limit {
            break;
        }
        bytes = next;
        result.push(serde_json::from_str(&text).map_err(storage)?);
    }
    result.reverse();
    Ok(result)
}

impl SqliteStore {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path: PathBuf = path.as_ref().to_owned();
        let counters = Arc::new(Counters::default());
        let worker_counters = counters.clone();
        let (sender, mut receiver) = mpsc::channel::<Job>(64);
        let (ready_tx, ready_rx) = oneshot::channel();
        let thread = thread::Builder::new()
            .name("yielded-sqlite".into())
            .spawn(move || match Database::open(&path, worker_counters) {
                Ok(mut database) => {
                    if ready_tx.send(Ok(())).is_err() {
                        return;
                    }
                    while let Some(job) = receiver.blocking_recv() {
                        job(&mut database);
                    }
                }
                Err(error) => {
                    let _ = ready_tx.send(Err(error));
                }
            })
            .map_err(storage)?;
        match ready_rx.await.map_err(storage)? {
            Ok(()) => Ok(Self {
                worker: Mutex::new(Worker {
                    sender: Some(sender),
                    thread: Some(thread),
                }),
                counters,
                host_active: AtomicBool::new(false),
            }),
            Err(error) => {
                let _ = thread.join();
                Err(error)
            }
        }
    }
    async fn call<T: Send + 'static>(
        &self,
        action: impl FnOnce(&mut Database) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let sender = self
            .worker
            .lock()
            .map_err(storage)?
            .sender
            .clone()
            .ok_or_else(|| Error::Storage("store is closed".into()))?;
        let (tx, rx) = oneshot::channel();
        sender
            .send(Box::new(move |database| {
                let _ = tx.send(action(database));
            }))
            .await
            .map_err(storage)?;
        rx.await.map_err(storage)?
    }
    pub(crate) fn take_host(&self) -> Result<()> {
        self.host_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| Error::DatabaseBusy)
    }
    pub(crate) fn release_host(&self) {
        self.host_active.store(false, Ordering::Release);
    }
    pub fn metrics(&self) -> StoreMetrics {
        StoreMetrics {
            transactions: self.counters.transactions.load(Ordering::Relaxed),
            writes: self.counters.writes.load(Ordering::Relaxed),
        }
    }
    /// Stop accepting work, drain queued transactions, and release the process file lock.
    pub async fn close(&self) -> Result<()> {
        let thread = {
            let mut worker = self.worker.lock().map_err(storage)?;
            worker.sender.take();
            worker.thread.take()
        };
        if let Some(thread) = thread {
            tokio::task::spawn_blocking(move || thread.join())
                .await
                .map_err(storage)?
                .map_err(|_| Error::Storage("SQLite worker panicked".into()))?;
        }
        Ok(())
    }
    pub async fn accept(
        &self,
        agent: &Agent,
        conversation: &str,
        key: &str,
        input: Value,
        expires_at_ms: Option<u64>,
    ) -> Result<RunId> {
        if conversation.is_empty()
            || conversation.len() > 1024
            || key.is_empty()
            || key.len() > 1024
        {
            return Err(Error::Invalid(
                "conversation and idempotency keys must contain 1..=1024 bytes".into(),
            ));
        }
        let (agent, conversation, key) = (agent.clone(), conversation.to_owned(), key.to_owned());
        let payload = encode(
            &json!({"input": input, "binding": agent.binding(), "expires_at_ms": expires_at_ms}),
        )?;
        self.call(move |db| {
            let tx = db.connection.transaction().map_err(storage)?;
            let existing: Option<(String, String)> = tx.query_row("SELECT id,payload FROM runs WHERE conversation=?1 AND idempotency_key=?2",
                params![conversation, key], |r| Ok((r.get(0)?, r.get(1)?))).optional().map_err(storage)?;
            if let Some((id, old_payload)) = existing {
                if old_payload != payload { return Err(Error::IdempotencyConflict); }
                return RunId::parse_str(&id).map_err(storage);
            }
            let busy: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM active WHERE conversation=?1)", [&conversation], |r| r.get(0)).map_err(storage)?;
            if busy { return Err(Error::ConversationBusy); }
            let context = bounded_history(&tx, &conversation, agent.limits().max_context_bytes)?;
            let state = agent.new_run(conversation.clone(), input, context, expires_at_ms)?;
            tx.execute("INSERT INTO runs(id,conversation,idempotency_key,payload) VALUES(?1,?2,?3,?4)",
                params![state.id.to_string(), conversation, key, payload]).map_err(storage)?;
            tx.execute("INSERT INTO active(run_id,conversation,generation,revision,state,wake_at) VALUES(?1,?2,?3,?4,?5,?6)",
                params![state.id.to_string(), state.conversation_id, state.generation, state.revision, encode(&state)?, sql_time(wake_at(&state))]).map_err(storage)?;
            let writes = 2 + append_evidence(&tx, &state, 0)?;
            tx.commit().map_err(storage)?;
            db.count(writes);
            Ok(state.id)
        }).await
    }
    pub async fn status(&self, id: RunId) -> Result<RunState> {
        self.call(move |db| load(&db.connection, id)).await
    }
    pub async fn history(&self, conversation: &str) -> Result<Vec<HistoryEntry>> {
        let conversation = conversation.to_owned();
        self.call(move |db| {
            let mut stmt = db
                .connection
                .prepare("SELECT entry FROM history WHERE conversation=?1 ORDER BY sequence")
                .map_err(storage)?;
            let rows = stmt
                .query_map([conversation], |r| r.get::<_, String>(0))
                .map_err(storage)?;
            rows.map(|row| serde_json::from_str(&row.map_err(storage)?).map_err(storage))
                .collect()
        })
        .await
    }
    /// Retained operations include earlier batches, and survive checkpoint retirement.
    pub async fn operations(&self, id: RunId) -> Result<Vec<Operation>> {
        self.call(move |db| {
            let mut stmt = db
                .connection
                .prepare("SELECT operation FROM operations WHERE run_id=?1 ORDER BY rowid")
                .map_err(storage)?;
            let rows = stmt
                .query_map([id.to_string()], |r| r.get::<_, String>(0))
                .map_err(storage)?;
            rows.map(|row| serde_json::from_str(&row.map_err(storage)?).map_err(storage))
                .collect()
        })
        .await
    }
    /// Claim a fresh execution generation. The host ensures old tasks have been joined first.
    pub async fn claim(&self, id: RunId) -> Result<RunState> {
        self.call(move |db| {
            let tx = db.connection.transaction().map_err(storage)?;
            let mut state = load(&tx, id)?;
            if state.is_terminal() {
                return Err(Error::Stale);
            }
            state.generation = state
                .generation
                .checked_add(1)
                .ok_or_else(|| storage("generation exhausted"))?;
            state.revision = state
                .revision
                .checked_add(1)
                .ok_or_else(|| storage("revision exhausted"))?;
            let writes = write_state(&tx, &state)?;
            tx.commit().map_err(storage)?;
            db.count(writes);
            Ok(state)
        })
        .await
    }
    pub async fn due(&self, now: u64, limit: usize) -> Result<Vec<RunId>> {
        self.call(move |db| {
            let mut stmt = db
                .connection
                .prepare(
                    "SELECT run_id FROM active WHERE wake_at<=?1 ORDER BY wake_at,run_id LIMIT ?2",
                )
                .map_err(storage)?;
            let rows = stmt
                .query_map(params![sql_time(Some(now)), limit.min(4096) as i64], |r| {
                    r.get::<_, String>(0)
                })
                .map_err(storage)?;
            rows.map(|row| RunId::parse_str(&row.map_err(storage)?).map_err(storage))
                .collect()
        })
        .await
    }
    pub async fn request_cancel(&self, id: RunId) -> Result<()> {
        self.call(move |db| {
            let tx = db.connection.transaction().map_err(storage)?;
            let mut state = load(&tx, id)?;
            if state.is_terminal() || state.cancel_requested {
                return Ok(());
            }
            state.cancel_requested = true;
            state.revision += 1;
            let writes = write_state(&tx, &state)?;
            tx.commit().map_err(storage)?;
            db.count(writes);
            Ok(())
        })
        .await
    }
    pub async fn resolve(&self, id: RunId, call_id: &str, resolution: Resolution) -> Result<()> {
        let call_id = call_id.to_owned();
        self.call(move |db| {
            let tx = db.connection.transaction().map_err(storage)?;
            let mut state = load(&tx, id)?;
            crate::resolve(&mut state, &call_id, resolution)?;
            state.revision += 1;
            let writes =
                append_evidence(&tx, &state, state.history.len())? + write_state(&tx, &state)?;
            tx.commit().map_err(storage)?;
            db.count(writes);
            Ok(())
        })
        .await
    }
    pub async fn position(&self, id: RunId) -> Result<CheckpointPosition> {
        self.call(move |db| {
            db.connection
                .query_row(
                    "SELECT generation,revision FROM active WHERE run_id=?1",
                    [id.to_string()],
                    |row| {
                        Ok(CheckpointPosition {
                            generation: row.get(0)?,
                            revision: row.get(1)?,
                        })
                    },
                )
                .optional()
                .map_err(storage)?
                .ok_or(Error::NotFound)
        })
        .await
    }
    /// One startup pass also discovers abandoned dispatched work behind an input wait.
    /// Paging and filtering use only active checkpoints, never retained history.
    pub(crate) async fn recovery_page(
        &self,
        after: Option<RunId>,
        limit: usize,
    ) -> Result<Vec<RunId>> {
        self.call(move |db| {
            let mut stmt = db.connection.prepare("SELECT a.run_id FROM active a JOIN runs r ON r.id=a.run_id
                WHERE a.run_id>?1 AND r.last_error IS NULL AND EXISTS
                (SELECT 1 FROM json_each(a.state,'$.operations') op WHERE json_extract(op.value,'$.status')='Dispatched')
                ORDER BY a.run_id LIMIT ?2").map_err(storage)?;
            let rows = stmt.query_map(params![after.map(|id|id.to_string()).unwrap_or_default(), limit.min(4096) as i64], |r| r.get::<_,String>(0)).map_err(storage)?;
            rows.map(|row| RunId::parse_str(&row.map_err(storage)?).map_err(storage)).collect()
        }).await
    }
    /// Suppress automatic retry only if this is still the failed worker's checkpoint.
    /// A late error cannot suppress a newer cancellation, resolution, claim, or repair.
    pub async fn record_error(
        &self,
        id: RunId,
        expected: CheckpointPosition,
        message: String,
    ) -> Result<bool> {
        self.call(move |db| {
            let tx = db.connection.transaction().map_err(storage)?;
            let changed = tx.execute("UPDATE active SET wake_at=NULL WHERE run_id=?1 AND generation=?2 AND revision=?3",
                params![id.to_string(), expected.generation, expected.revision]).map_err(storage)?;
            if changed == 0 { return Ok(false); }
            tx.execute("UPDATE runs SET last_error=?2 WHERE id=?1", params![id.to_string(), message]).map_err(storage)?;
            tx.commit().map_err(storage)?;
            db.count(2);
            Ok(true)
        }).await
    }
    pub async fn last_error(&self, id: RunId) -> Result<Option<String>> {
        self.call(move |db| {
            db.connection
                .query_row(
                    "SELECT last_error FROM runs WHERE id=?1",
                    [id.to_string()],
                    |r| r.get(0),
                )
                .optional()
                .map_err(storage)?
                .ok_or(Error::NotFound)
        })
        .await
    }
    /// Explicitly retry a run after its unavailable binding or storage problem is repaired.
    pub async fn retry(&self, id: RunId) -> Result<()> {
        self.call(move |db| {
            let tx = db.connection.transaction().map_err(storage)?;
            let mut state = load(&tx, id)?;
            if state.is_terminal() {
                return Err(Error::Stale);
            }
            state.revision += 1;
            tx.execute(
                "UPDATE runs SET last_error=NULL WHERE id=?1",
                [id.to_string()],
            )
            .map_err(storage)?;
            let mut writes = 1 + write_state(&tx, &state)?;
            if state
                .operations
                .iter()
                .any(|op| matches!(op.status, OperationStatus::Dispatched))
            {
                // Explicit repair must get one recovery pass even when a sibling waits.
                // Claim recomputes normal eligibility before invoking the engine.
                writes += tx
                    .execute(
                        "UPDATE active SET wake_at=0 WHERE run_id=?1",
                        [id.to_string()],
                    )
                    .map_err(storage)? as u64;
            }
            tx.commit().map_err(storage)?;
            db.count(writes);
            Ok(())
        })
        .await
    }
}
#[async_trait]
impl Checkpoint for SqliteStore {
    async fn commit(&self, state: &mut RunState) -> Result<()> {
        let candidate = state.clone();
        let committed = self
            .call(move |db| {
                let tx = db.connection.transaction().map_err(storage)?;
                let old = load(&tx, candidate.id)?;
                if old.is_terminal()
                    || candidate.generation != old.generation
                    || candidate.revision != old.revision
                {
                    return Err(Error::Stale);
                }
                if !immutable_matches(&old, &candidate) {
                    return Err(Error::Invalid(
                        "checkpoint changed immutable acceptance or appended history".into(),
                    ));
                }
                let mut new = candidate;
                new.revision = new
                    .revision
                    .checked_add(1)
                    .ok_or_else(|| storage("revision exhausted"))?;
                let writes =
                    append_evidence(&tx, &new, old.history.len())? + write_state(&tx, &new)?;
                tx.commit().map_err(storage)?;
                db.count(writes);
                Ok(new)
            })
            .await?;
        *state = committed;
        Ok(())
    }
}
impl Drop for SqliteStore {
    fn drop(&mut self) {
        if let Ok(worker) = self.worker.get_mut() {
            worker.sender.take();
            if let Some(thread) = worker.thread.take() {
                let _ = thread.join();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn failure_during_retirement_rolls_back_history_terminal_state_and_checkpoint() {
        let directory = tempfile::tempdir().unwrap();
        let db = SqliteStore::open(directory.path().join("atomic.sqlite"))
            .await
            .unwrap();
        let agent = Agent::new::<Value, Value>(
            "test",
            "test",
            Arc::new(ScriptedModel::new(vec![])),
            ModelConfig::default(),
            vec![],
            Limits::default(),
        )
        .unwrap();
        let id = db.accept(&agent, "c", "k", json!(1), None).await.unwrap();
        let mut state = db.claim(id).await.unwrap();
        let old_revision = state.revision;
        db.call(|db| {
            db.connection.execute_batch("CREATE TRIGGER fail_retirement BEFORE DELETE ON active BEGIN SELECT RAISE(FAIL, 'injected retirement failure'); END;").map_err(storage)
        }).await.unwrap();
        state.history.push(HistoryEntry::Exchange {
            response: ModelResponse::final_output(json!("done")),
            results: vec![],
        });
        state.phase = Phase::Settled(Settlement::Completed(json!("done")));
        assert!(matches!(
            db.commit(&mut state).await,
            Err(Error::Storage(_))
        ));
        assert_eq!(state.revision, old_revision);
        let retained = db.status(id).await.unwrap();
        assert_eq!(retained.phase, Phase::Ready);
        assert_eq!(retained.revision, old_revision);
        assert_eq!(db.history("c").await.unwrap().len(), 1);
        assert_eq!(db.due(now_ms(), 10).await.unwrap(), vec![id]);
        db.call(|db| {
            db.connection
                .execute_batch("DROP TRIGGER fail_retirement")
                .map_err(storage)
        })
        .await
        .unwrap();
        db.commit(&mut state).await.unwrap();
        assert!(db.status(id).await.unwrap().is_terminal());
        assert_eq!(db.history("c").await.unwrap().len(), 2);
        assert!(db.due(now_ms(), 10).await.unwrap().is_empty());
        db.close().await.unwrap();
    }
}
