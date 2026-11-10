//! File-backed [`DurableStore`]. One bounded worker owns SQLite and the database file lock.
//!
//! A run is split by mutability so that each write costs what it changes:
//!
//! - `runs` holds immutable acceptance (identity, binding, input, limits, expiry and bounded
//!   prior context). It is inserted once and never updated.
//! - `checkpoints` holds one small row per run: its position (generation, revision), progress
//!   (phase, usage, cancellation, pending response, current operations), the number of
//!   history entries the run owns, and the discovery fields `wake_at`, `recover` and
//!   `last_error`. Every commit rewrites only this row, fenced on generation and revision.
//! - `history` holds each entry once, appended in commit order; `operations` retains every
//!   operation the run recorded, across batches.
use crate::*;
use async_trait::async_trait;
use fs2::FileExt;
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    thread,
};
use tokio::sync::{mpsc, oneshot};

const STORAGE_VERSION: i64 = 2;
type Job = Box<dyn FnOnce(&mut Database) + Send>;

#[derive(Clone, Copy, Debug, Default)]
pub struct StoreMetrics {
    pub transactions: u64,
    pub writes: u64,
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
    slot: HostSlot,
    /// Durable history length per run as of its last claim or commit through this store.
    /// Only a hint for which entries to encode: the worker checks it against the stored
    /// length, and a missing hint encodes the run's whole history.
    history_hints: Mutex<HashMap<RunId, usize>>,
}

/// The part of a `RunState` a commit may change, other than appending history.
#[derive(Serialize)]
struct ProgressRef<'a> {
    cancel_requested: bool,
    phase: &'a Phase,
    usage: &'a Usage,
    pending_response: &'a Option<ModelResponse>,
    operations: &'a [Operation],
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Progress {
    cancel_requested: bool,
    phase: Phase,
    usage: Usage,
    pending_response: Option<ModelResponse>,
    operations: Vec<Operation>,
}

/// Everything one write persists, encoded from a borrow of the caller's state.
struct Delta {
    id: String,
    conversation: String,
    revision: u64,
    progress: String,
    settled: bool,
    wake_at: Option<i64>,
    recover: bool,
    history_len: usize,
    /// Encoded history entries from ordinal `history_from` to `history_len`.
    history_from: usize,
    history: Vec<String>,
    operations: Vec<(String, String)>,
}
impl Delta {
    fn encode(
        state: &RunState,
        revision: u64,
        history_from: usize,
        wake_now: bool,
    ) -> Result<Self> {
        Ok(Self {
            id: state.id.to_string(),
            conversation: state.conversation_id.clone(),
            revision,
            progress: encode(&ProgressRef {
                cancel_requested: state.cancel_requested,
                phase: &state.phase,
                usage: &state.usage,
                pending_response: &state.pending_response,
                operations: &state.operations,
            })?,
            settled: state.is_terminal(),
            wake_at: if wake_now {
                Some(0)
            } else {
                sql_time(state.wake_at())
            },
            recover: state.needs_recovery(),
            history_len: state.history.len(),
            history_from,
            history: state.history[history_from..]
                .iter()
                .map(encode)
                .collect::<Result<_>>()?,
            operations: state
                .operations
                .iter()
                .map(|op| Ok((op.call.id.clone(), encode(op)?)))
                .collect::<Result<_>>()?,
        })
    }
}
/// Which stored position a write must find.
#[derive(Clone, Copy)]
enum Fence {
    /// A worker's commit: the exact position of an unsettled run.
    Commit(CheckpointPosition),
    /// An update read in the same transaction; settled runs remain resolvable.
    Update(CheckpointPosition),
}
enum Written {
    Done {
        writes: u64,
    },
    /// The stored history is shorter than the delta's first encoded entry.
    NeedHistoryFrom(usize),
}

fn encode(value: &impl Serialize) -> Result<String> {
    serde_json::to_string(value).map_err(Error::storage)
}
fn decode<T: serde::de::DeserializeOwned>(text: &str) -> Result<T> {
    serde_json::from_str(text).map_err(Error::storage)
}
fn sql_time(value: Option<u64>) -> Option<i64> {
    value.map(|v| v.min(i64::MAX as u64) as i64)
}
fn run_id(text: &str) -> Result<RunId> {
    RunId::parse_str(text).map_err(Error::storage)
}

impl Database {
    fn open(path: &Path, counters: Arc<Counters>) -> Result<Self> {
        if path.as_os_str().is_empty() || path == Path::new(":memory:") {
            return Err(Error::config("a file-backed database path is required"));
        }
        let database_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(Error::storage)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if database_file.metadata().map_err(Error::storage)?.nlink() != 1 {
                return Err(Error::config(
                    "hard-linked SQLite databases are unsupported because WAL sidecars are path-specific",
                ));
            }
        }
        let canonical = std::fs::canonicalize(path).map_err(Error::storage)?;
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
            .map_err(Error::storage)?;
        lock.try_lock_exclusive().map_err(|e| {
            if e.kind() == std::io::ErrorKind::WouldBlock {
                Error::StoreBusy
            } else {
                Error::storage(e)
            }
        })?;
        let connection = Connection::open(canonical).map_err(Error::storage)?;
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(Error::storage)?;
        connection
            .pragma_update(None, "synchronous", "FULL")
            .map_err(Error::storage)?;
        connection
            .pragma_update(None, "foreign_keys", "ON")
            .map_err(Error::storage)?;
        let version: i64 = connection
            .pragma_query_value(None, "user_version", |r| r.get(0))
            .map_err(Error::storage)?;
        if version != 0 && version != STORAGE_VERSION {
            return Err(Error::incompatible(format!(
                "SQLite schema version {version}"
            )));
        }
        connection
            .execute_batch(
                "BEGIN IMMEDIATE;
            CREATE TABLE IF NOT EXISTS runs (
                id TEXT PRIMARY KEY, conversation TEXT NOT NULL, idempotency_key TEXT NOT NULL,
                version INTEGER NOT NULL, agent_id TEXT NOT NULL, binding TEXT NOT NULL,
                input TEXT NOT NULL, limits TEXT NOT NULL, expires_at INTEGER,
                prior_context TEXT NOT NULL,
                UNIQUE(conversation, idempotency_key));
            CREATE TABLE IF NOT EXISTS checkpoints (
                run_id TEXT PRIMARY KEY REFERENCES runs(id), conversation TEXT NOT NULL,
                generation INTEGER NOT NULL, revision INTEGER NOT NULL,
                history_len INTEGER NOT NULL, progress TEXT NOT NULL, settled INTEGER NOT NULL,
                wake_at INTEGER, recover INTEGER NOT NULL, last_error TEXT);
            CREATE UNIQUE INDEX IF NOT EXISTS checkpoints_unsettled
                ON checkpoints(conversation) WHERE settled=0;
            CREATE INDEX IF NOT EXISTS checkpoints_due ON checkpoints(wake_at)
                WHERE wake_at IS NOT NULL;
            CREATE INDEX IF NOT EXISTS checkpoints_recovery ON checkpoints(run_id)
                WHERE recover=1 AND last_error IS NULL;
            CREATE TABLE IF NOT EXISTS history (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL REFERENCES runs(id),
                conversation TEXT NOT NULL, ordinal INTEGER NOT NULL, entry TEXT NOT NULL,
                UNIQUE(run_id, ordinal));
            CREATE INDEX IF NOT EXISTS history_conversation ON history(conversation, sequence);
            CREATE TABLE IF NOT EXISTS operations (
                run_id TEXT NOT NULL REFERENCES runs(id), call_id TEXT NOT NULL, operation TEXT NOT NULL,
                PRIMARY KEY(run_id, call_id));
            PRAGMA user_version=2; COMMIT;",
            )
            .map_err(Error::storage)?;
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

/// Reassemble a run from its acceptance, checkpoint and history rows.
fn load(connection: &Connection, id: RunId) -> Result<RunState> {
    let key = id.to_string();
    let row = connection
        .prepare_cached(
            "SELECT r.version,r.conversation,r.agent_id,r.binding,r.input,r.limits,r.expires_at,
                r.prior_context,c.generation,c.revision,c.history_len,c.progress
            FROM runs r JOIN checkpoints c ON c.run_id=r.id WHERE r.id=?1",
        )
        .map_err(Error::storage)?
        .query_row([&key], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, String>(5)?,
                r.get::<_, Option<i64>>(6)?,
                r.get::<_, String>(7)?,
                r.get::<_, i64>(8)?,
                r.get::<_, i64>(9)?,
                r.get::<_, i64>(10)?,
                r.get::<_, String>(11)?,
            ))
        })
        .optional()
        .map_err(Error::storage)?
        .ok_or(Error::NotFound)?;
    let (version, conversation_id, agent_id, binding, input, limits, expires_at, prior_context) =
        (row.0, row.1, row.2, row.3, row.4, row.5, row.6, row.7);
    let (generation, revision, history_len, progress) = (row.8, row.9, row.10, row.11);
    if version != i64::from(STATE_VERSION) {
        return Err(Error::incompatible(format!(
            "checkpoint state version {version}"
        )));
    }
    let history_len = usize::try_from(history_len).map_err(Error::storage)?;
    let history = {
        let mut stmt = connection
            .prepare_cached(
                "SELECT entry FROM history WHERE run_id=?1 AND ordinal<?2 ORDER BY ordinal",
            )
            .map_err(Error::storage)?;
        let rows = stmt
            .query_map(params![key, history_len as i64], |r| r.get::<_, String>(0))
            .map_err(Error::storage)?;
        rows.map(|row| decode(&row.map_err(Error::storage)?))
            .collect::<Result<Vec<HistoryEntry>>>()?
    };
    if history.len() != history_len {
        return Err(Error::storage(format!(
            "run {id} records {history_len} history entries but {} are stored",
            history.len()
        )));
    }
    let progress: Progress = decode(&progress)?;
    Ok(RunState {
        version: STATE_VERSION,
        id,
        conversation_id,
        agent_id,
        binding: decode(&binding)?,
        input: decode(&input)?,
        limits: decode(&limits)?,
        expires_at_ms: expires_at
            .map(|at| u64::try_from(at).map_err(Error::storage))
            .transpose()?,
        generation: u64::try_from(generation).map_err(Error::storage)?,
        revision: u64::try_from(revision).map_err(Error::storage)?,
        cancel_requested: progress.cancel_requested,
        phase: progress.phase,
        usage: progress.usage,
        prior_context: decode(&prior_context)?,
        history,
        pending_response: progress.pending_response,
        operations: progress.operations,
    })
}
fn insert_history(
    tx: &Transaction<'_>,
    id: &str,
    conversation: &str,
    first_ordinal: usize,
    entries: &[String],
) -> Result<u64> {
    let mut stmt = tx
        .prepare_cached(
            "INSERT INTO history(run_id,conversation,ordinal,entry) VALUES(?1,?2,?3,?4)",
        )
        .map_err(Error::storage)?;
    for (offset, entry) in entries.iter().enumerate() {
        stmt.execute(params![
            id,
            conversation,
            (first_ordinal + offset) as i64,
            entry
        ])
        .map_err(Error::storage)?;
    }
    Ok(entries.len() as u64)
}
fn upsert_operations(
    tx: &Transaction<'_>,
    id: &str,
    operations: &[(String, String)],
) -> Result<u64> {
    let mut stmt = tx
        .prepare_cached(
            "INSERT INTO operations(run_id,call_id,operation) VALUES(?1,?2,?3)
            ON CONFLICT(run_id,call_id) DO UPDATE SET operation=excluded.operation
            WHERE operations.operation<>excluded.operation",
        )
        .map_err(Error::storage)?;
    let mut writes = 0;
    for (call_id, operation) in operations {
        writes += stmt
            .execute(params![id, call_id, operation])
            .map_err(Error::storage)? as u64;
    }
    Ok(writes)
}
/// Persist one delta: a fenced update of the checkpoint row, then new history and changed
/// operation evidence. The caller commits the transaction only on `Written::Done`.
fn write_delta(
    tx: &Transaction<'_>,
    delta: &Delta,
    fence: Fence,
    clear_error: bool,
) -> Result<Written> {
    let stored: i64 = tx
        .prepare_cached("SELECT history_len FROM checkpoints WHERE run_id=?1")
        .map_err(Error::storage)?
        .query_row([&delta.id], |r| r.get(0))
        .optional()
        .map_err(Error::storage)?
        .ok_or(Error::NotFound)?;
    let stored = usize::try_from(stored).map_err(Error::storage)?;
    let (expected, allow_settled) = match fence {
        Fence::Commit(position) => (position, false),
        Fence::Update(position) => (position, true),
    };
    let changed = tx
        .prepare_cached(
            "UPDATE checkpoints SET revision=?4,history_len=?5,progress=?6,settled=?7,wake_at=?8,
                recover=?9,last_error=CASE WHEN ?10 THEN NULL ELSE last_error END
            WHERE run_id=?1 AND generation=?2 AND revision=?3 AND (?11 OR settled=0)",
        )
        .map_err(Error::storage)?
        .execute(params![
            delta.id,
            expected.generation,
            expected.revision,
            delta.revision,
            delta.history_len as i64,
            delta.progress,
            delta.settled,
            delta.wake_at,
            delta.recover,
            clear_error || delta.settled,
            allow_settled,
        ])
        .map_err(Error::storage)?;
    if changed == 0 {
        return Err(Error::Stale);
    }
    if delta.history_len < stored {
        return Err(Error::input("history is append-only"));
    }
    if stored < delta.history_from {
        return Ok(Written::NeedHistoryFrom(stored));
    }
    let new = &delta.history[stored - delta.history_from..];
    let writes = 1
        + insert_history(tx, &delta.id, &delta.conversation, stored, new)?
        + upsert_operations(tx, &delta.id, &delta.operations)?;
    Ok(Written::Done { writes })
}
fn bounded_history(
    connection: &Connection,
    conversation: &str,
    limit: usize,
) -> Result<Vec<HistoryEntry>> {
    let mut stmt = connection
        .prepare_cached("SELECT entry FROM history WHERE conversation=?1 ORDER BY sequence DESC")
        .map_err(Error::storage)?;
    let mut rows = stmt.query([conversation]).map_err(Error::storage)?;
    let mut result = Vec::new();
    let mut bytes = 2usize;
    while let Some(row) = rows.next().map_err(Error::storage)? {
        let text: String = row.get(0).map_err(Error::storage)?;
        let next = bytes.saturating_add(text.len()).saturating_add(1);
        if next > limit {
            break;
        }
        bytes = next;
        result.push(decode(&text)?);
    }
    result.reverse();
    Ok(result)
}
/// Accept within the caller's transaction, so optional admission adapters can commit their
/// own records atomically with the run. Returns the run and the rows written (0 on replay).
fn accept_in(
    tx: &Transaction<'_>,
    agent: &Agent,
    conversation: &str,
    key: &str,
    input: Value,
    expires_at_ms: Option<u64>,
) -> Result<(RunId, u64)> {
    store::check_acceptance(conversation, key, expires_at_ms)?;
    let existing: Option<(String, String, String, Option<i64>)> = tx
        .prepare_cached(
            "SELECT id,input,binding,expires_at FROM runs WHERE conversation=?1 AND idempotency_key=?2",
        )
        .map_err(Error::storage)?
        .query_row(params![conversation, key], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
        })
        .optional()
        .map_err(Error::storage)?;
    if let Some((id, old_input, old_binding, old_expiry)) = existing {
        if old_input != encode(&input)?
            || old_expiry != sql_time(expires_at_ms)
            || old_binding != encode(agent.binding())?
        {
            return Err(Error::IdempotencyConflict);
        }
        return Ok((run_id(&id)?, 0));
    }
    let busy: bool = tx
        .prepare_cached(
            "SELECT EXISTS(SELECT 1 FROM checkpoints WHERE conversation=?1 AND settled=0)",
        )
        .map_err(Error::storage)?
        .query_row([conversation], |r| r.get(0))
        .map_err(Error::storage)?;
    if busy {
        return Err(Error::ConversationBusy);
    }
    let context = bounded_history(tx, conversation, agent.limits().max_context_bytes)?;
    let state = agent.new_run(conversation, input, &context, expires_at_ms)?;
    let id = state.id.to_string();
    tx.prepare_cached(
        "INSERT INTO runs(id,conversation,idempotency_key,version,agent_id,binding,input,limits,
            expires_at,prior_context) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
    )
    .map_err(Error::storage)?
    .execute(params![
        id,
        conversation,
        key,
        state.version,
        state.agent_id,
        encode(&state.binding)?,
        encode(&state.input)?,
        encode(&state.limits)?,
        sql_time(state.expires_at_ms),
        encode(&state.prior_context)?,
    ])
    .map_err(Error::storage)?;
    let delta = Delta::encode(&state, state.revision, 0, false)?;
    tx.prepare_cached(
        "INSERT INTO checkpoints(run_id,conversation,generation,revision,history_len,progress,
            settled,wake_at,recover) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
    )
    .map_err(Error::storage)?
    .execute(params![
        id,
        conversation,
        state.generation,
        state.revision,
        delta.history_len as i64,
        delta.progress,
        delta.settled,
        delta.wake_at,
        delta.recover,
    ])
    .map_err(Error::storage)?;
    let writes = 2
        + insert_history(tx, &id, conversation, 0, &delta.history)?
        + upsert_operations(tx, &id, &delta.operations)?;
    Ok((state.id, writes))
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
            .map_err(Error::storage)?;
        match ready_rx.await.map_err(Error::storage)? {
            Ok(()) => Ok(Self {
                worker: Mutex::new(Worker {
                    sender: Some(sender),
                    thread: Some(thread),
                }),
                counters,
                slot: HostSlot::default(),
                history_hints: Mutex::new(HashMap::new()),
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
            .map_err(|_| Error::storage("SQLite worker lock poisoned"))?
            .sender
            .clone()
            .ok_or_else(|| Error::storage("store is closed"))?;
        let (tx, rx) = oneshot::channel();
        sender
            .send(Box::new(move |database| {
                let _ = tx.send(action(database));
            }))
            .await
            .map_err(|_| Error::storage("store is closed"))?;
        rx.await.map_err(Error::storage)?
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
            let mut worker = self
                .worker
                .lock()
                .map_err(|_| Error::storage("SQLite worker lock poisoned"))?;
            worker.sender.take();
            worker.thread.take()
        };
        if let Some(thread) = thread {
            tokio::task::spawn_blocking(move || thread.join())
                .await
                .map_err(Error::storage)?
                .map_err(|_| Error::storage("SQLite worker panicked"))?;
        }
        Ok(())
    }
    fn hint(&self, id: RunId, length: Option<usize>) {
        if let Ok(mut hints) = self.history_hints.lock() {
            match length {
                Some(length) => hints.insert(id, length),
                None => hints.remove(&id),
            };
        }
    }
    fn hinted(&self, state: &RunState) -> usize {
        self.history_hints
            .lock()
            .ok()
            .and_then(|hints| hints.get(&state.id).copied())
            .filter(|length| *length <= state.history.len())
            .unwrap_or(0)
    }
}
#[async_trait]
impl DurableStore for SqliteStore {
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
        let (agent, conversation, key) = (agent.clone(), conversation.to_owned(), key.to_owned());
        self.call(move |db| {
            let tx = db.connection.transaction().map_err(Error::storage)?;
            let (id, writes) = accept_in(&tx, &agent, &conversation, &key, input, expires_at_ms)?;
            if writes > 0 {
                tx.commit().map_err(Error::storage)?;
                db.count(writes);
            }
            Ok(id)
        })
        .await
    }
    async fn status(&self, id: RunId) -> Result<RunState> {
        self.call(move |db| load(&db.connection, id)).await
    }
    async fn history(&self, conversation: &str) -> Result<Vec<HistoryEntry>> {
        let conversation = conversation.to_owned();
        self.call(move |db| {
            let mut stmt = db
                .connection
                .prepare_cached("SELECT entry FROM history WHERE conversation=?1 ORDER BY sequence")
                .map_err(Error::storage)?;
            let rows = stmt
                .query_map([conversation], |r| r.get::<_, String>(0))
                .map_err(Error::storage)?;
            rows.map(|row| decode(&row.map_err(Error::storage)?))
                .collect()
        })
        .await
    }
    /// Retained operations include earlier batches, and survive settlement.
    async fn operations(&self, id: RunId) -> Result<Vec<Operation>> {
        self.call(move |db| {
            let mut stmt = db
                .connection
                .prepare_cached("SELECT operation FROM operations WHERE run_id=?1 ORDER BY rowid")
                .map_err(Error::storage)?;
            let rows = stmt
                .query_map([id.to_string()], |r| r.get::<_, String>(0))
                .map_err(Error::storage)?;
            rows.map(|row| decode(&row.map_err(Error::storage)?))
                .collect()
        })
        .await
    }
    /// Claim a fresh execution generation. The host ensures old tasks have been joined first.
    async fn claim(&self, id: RunId) -> Result<RunState> {
        let state = self
            .call(move |db| {
                let tx = db.connection.transaction().map_err(Error::storage)?;
                let mut state = load(&tx, id)?;
                if state.is_terminal() {
                    return Err(Error::Stale);
                }
                let old = CheckpointPosition::from(&state);
                state.generation = store::advance(state.generation, "generation")?;
                state.revision = store::advance(state.revision, "revision")?;
                let changed = tx
                    .prepare_cached(
                        "UPDATE checkpoints SET generation=?4,revision=?5,wake_at=?6,recover=?7
                        WHERE run_id=?1 AND generation=?2 AND revision=?3 AND settled=0",
                    )
                    .map_err(Error::storage)?
                    .execute(params![
                        id.to_string(),
                        old.generation,
                        old.revision,
                        state.generation,
                        state.revision,
                        sql_time(state.wake_at()),
                        state.needs_recovery(),
                    ])
                    .map_err(Error::storage)?;
                if changed == 0 {
                    return Err(Error::Stale);
                }
                tx.commit().map_err(Error::storage)?;
                db.count(1);
                Ok(state)
            })
            .await?;
        self.hint(id, Some(state.history.len()));
        Ok(state)
    }
    async fn update(&self, id: RunId, update: RunUpdate) -> Result<()> {
        self.call(move |db| {
            let tx = db.connection.transaction().map_err(Error::storage)?;
            let mut state = load(&tx, id)?;
            let fence = Fence::Update(CheckpointPosition::from(&state));
            let durable = state.history.len();
            let effect = update.apply(&mut state)?;
            if !effect.changed {
                return Ok(());
            }
            let revision = store::advance(state.revision, "revision")?;
            let delta = Delta::encode(&state, revision, durable, effect.wake_now)?;
            match write_delta(&tx, &delta, fence, effect.clear_error)? {
                Written::Done { writes } => {
                    tx.commit().map_err(Error::storage)?;
                    db.count(writes);
                    Ok(())
                }
                Written::NeedHistoryFrom(_) => {
                    Err(Error::storage("run history changed during update"))
                }
            }
        })
        .await
    }
    async fn due(&self, now: u64, limit: usize) -> Result<Vec<RunId>> {
        self.call(move |db| {
            let mut stmt = db
                .connection
                .prepare_cached(
                    "SELECT run_id FROM checkpoints WHERE wake_at<=?1 ORDER BY wake_at,run_id LIMIT ?2",
                )
                .map_err(Error::storage)?;
            let rows = stmt
                .query_map(params![sql_time(Some(now)), limit.min(4096) as i64], |r| {
                    r.get::<_, String>(0)
                })
                .map_err(Error::storage)?;
            rows.map(|row| run_id(&row.map_err(Error::storage)?)).collect()
        })
        .await
    }
    async fn position(&self, id: RunId) -> Result<CheckpointPosition> {
        self.call(move |db| {
            db.connection
                .prepare_cached(
                    "SELECT generation,revision FROM checkpoints WHERE run_id=?1 AND settled=0",
                )
                .map_err(Error::storage)?
                .query_row([id.to_string()], |row| {
                    Ok(CheckpointPosition {
                        generation: row.get(0)?,
                        revision: row.get(1)?,
                    })
                })
                .optional()
                .map_err(Error::storage)?
                .ok_or(Error::NotFound)
        })
        .await
    }
    /// One startup pass also discovers abandoned dispatched work behind an input wait.
    /// Discovery reads the `recover` column written with each checkpoint, never its contents.
    async fn recovery_page(&self, after: Option<RunId>, limit: usize) -> Result<Vec<RunId>> {
        self.call(move |db| {
            let mut stmt = db
                .connection
                .prepare_cached(
                    "SELECT run_id FROM checkpoints WHERE recover=1 AND last_error IS NULL
                    AND run_id>?1 ORDER BY run_id LIMIT ?2",
                )
                .map_err(Error::storage)?;
            let after = after.map(|id| id.to_string()).unwrap_or_default();
            let rows = stmt
                .query_map(params![after, limit.min(4096) as i64], |r| {
                    r.get::<_, String>(0)
                })
                .map_err(Error::storage)?;
            rows.map(|row| run_id(&row.map_err(Error::storage)?))
                .collect()
        })
        .await
    }
    /// Suppress automatic retry only if this is still the failed worker's checkpoint.
    /// A late error cannot suppress a newer cancellation, resolution, claim, or repair.
    async fn record_error(
        &self,
        id: RunId,
        expected: CheckpointPosition,
        message: String,
    ) -> Result<bool> {
        self.call(move |db| {
            let changed = db
                .connection
                .prepare_cached(
                    "UPDATE checkpoints SET wake_at=NULL,last_error=?4
                    WHERE run_id=?1 AND generation=?2 AND revision=?3 AND settled=0",
                )
                .map_err(Error::storage)?
                .execute(params![
                    id.to_string(),
                    expected.generation,
                    expected.revision,
                    message
                ])
                .map_err(Error::storage)?;
            if changed > 0 {
                db.count(1);
            }
            Ok(changed > 0)
        })
        .await
    }
    async fn last_error(&self, id: RunId) -> Result<Option<String>> {
        self.call(move |db| {
            db.connection
                .prepare_cached("SELECT last_error FROM checkpoints WHERE run_id=?1")
                .map_err(Error::storage)?
                .query_row([id.to_string()], |r| r.get(0))
                .optional()
                .map_err(Error::storage)?
                .ok_or(Error::NotFound)
        })
        .await
    }
}
#[async_trait]
impl Checkpoint for SqliteStore {
    /// Writes only the run's progress, new history and changed operations, encoded here from
    /// a borrow. The stored acceptance is never compared or rewritten.
    async fn commit(&self, state: &mut RunState) -> Result<()> {
        let fence = Fence::Commit(CheckpointPosition::from(&*state));
        let revision = store::advance(state.revision, "revision")?;
        let mut from = self.hinted(state);
        loop {
            let delta = Delta::encode(state, revision, from, false)?;
            let written = self
                .call(move |db| {
                    let tx = db.connection.transaction().map_err(Error::storage)?;
                    let written = write_delta(&tx, &delta, fence, false)?;
                    if let Written::Done { writes } = written {
                        tx.commit().map_err(Error::storage)?;
                        db.count(writes);
                    }
                    Ok(written)
                })
                .await;
            match written {
                Ok(Written::Done { .. }) => break,
                // The hint ran ahead of durable history; resend from the stored length.
                Ok(Written::NeedHistoryFrom(stored)) if stored < from => from = stored,
                Ok(Written::NeedHistoryFrom(_)) => {
                    return Err(Error::storage("checkpoint history changed during commit"));
                }
                Err(error) => {
                    self.hint(state.id, None);
                    return Err(error);
                }
            }
        }
        state.revision = revision;
        let settled = state.is_terminal();
        self.hint(state.id, (!settled).then_some(state.history.len()));
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
    use serde_json::json;
    #[tokio::test]
    async fn failure_during_retirement_rolls_back_history_terminal_state_and_checkpoint() {
        let directory = tempfile::tempdir().unwrap();
        let db = SqliteStore::open(directory.path().join("atomic.sqlite"))
            .await
            .unwrap();
        let agent = Agent::builder("test", Arc::new(ScriptedModel::new(vec![])))
            .instructions("test")
            .build()
            .unwrap();
        let id = db.accept(&agent, "c", "k", json!(1), None).await.unwrap();
        let mut state = db.claim(id).await.unwrap();
        let old_revision = state.revision;
        // Fail the final history append, after the fenced checkpoint update has run.
        db.call(|db| {
            db.connection.execute_batch("CREATE TRIGGER fail_retirement BEFORE INSERT ON history WHEN NEW.ordinal=1 BEGIN SELECT RAISE(FAIL, 'injected retirement failure'); END;").map_err(Error::storage)
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
        assert_eq!(retained.history.len(), 1);
        assert_eq!(db.history("c").await.unwrap().len(), 1);
        assert_eq!(db.due(now_ms(), 10).await.unwrap(), vec![id]);
        assert!(matches!(
            db.accept(&agent, "c", "next", json!(2), None).await,
            Err(Error::ConversationBusy)
        ));
        db.call(|db| {
            db.connection
                .execute_batch("DROP TRIGGER fail_retirement")
                .map_err(Error::storage)
        })
        .await
        .unwrap();
        db.commit(&mut state).await.unwrap();
        assert!(db.status(id).await.unwrap().is_terminal());
        assert_eq!(db.history("c").await.unwrap().len(), 2);
        assert!(db.due(now_ms(), 10).await.unwrap().is_empty());
        db.accept(&agent, "c", "next", json!(2), None)
            .await
            .unwrap();
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn commits_with_a_stale_history_hint_append_each_entry_once() {
        let directory = tempfile::tempdir().unwrap();
        let db = SqliteStore::open(directory.path().join("hint.sqlite"))
            .await
            .unwrap();
        let agent = Agent::builder("test", Arc::new(ScriptedModel::new(vec![])))
            .instructions("test")
            .build()
            .unwrap();
        let id = db.accept(&agent, "c", "k", json!(1), None).await.unwrap();
        let mut state = db.claim(id).await.unwrap();
        let exchange = |n: u64| HistoryEntry::Exchange {
            response: ModelResponse::final_output(json!(n)),
            results: vec![],
        };
        // Ahead of durable history: the worker asks for the missing entries.
        db.hint(id, Some(3));
        state
            .history
            .extend([exchange(1), exchange(2), exchange(3)]);
        db.commit(&mut state).await.unwrap();
        // No hint: the whole run history is sent and only new entries are appended.
        db.hint(id, None);
        state.history.push(exchange(4));
        db.commit(&mut state).await.unwrap();
        assert_eq!(db.status(id).await.unwrap().history, state.history);
        assert_eq!(db.history("c").await.unwrap(), state.history);
        db.close().await.unwrap();
    }
}
