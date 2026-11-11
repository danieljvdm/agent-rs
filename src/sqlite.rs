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
//! - `steering` holds each accepted instruction once, with its idempotency key and the revision
//!   that applied it. A queued instruction is not history yet: the model dispatch commit appends
//!   it to `history` at the run's next ordinals, in the same transaction that marks it applied.
//!   The table is created additively in `steering_queue`, within schema version 2.
//! - `children` holds one progress row per child a run's delegations admitted; see `children`.
//! - `automations`, `automation_cursors` and `automation_occurrences` hold interval
//!   automations and their firings. They are created additively in `automation`, within schema
//!   version 2, and no run write touches them.
mod children;
use crate::steering::Boundary;
use crate::store;
use crate::types::STATE_VERSION;
use crate::{
    Agent, Checkpoint, CheckpointPosition, DurableStore, Error, HistoryEntry, HostSlot,
    ModelResponse, Operation, Phase, Result, RunId, RunState, RunUpdate, StoreLease, Usage,
};
use async_trait::async_trait;
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::HashMap,
    fs::{File, OpenOptions, TryLockError},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    thread,
};
use tokio::sync::{mpsc, oneshot};

mod automation;
#[cfg(feature = "memory")]
mod memory;
mod steering_queue;

const STORAGE_VERSION: i64 = 2;
type Job = Box<dyn FnOnce(&mut Database) + Send>;

/// Work a [`SqliteStore`] has committed since it was opened, from [`SqliteStore::metrics`].
#[derive(Clone, Copy, Debug, Default)]
#[non_exhaustive]
pub struct StoreMetrics {
    /// Committed write transactions.
    pub transactions: u64,
    /// Rows inserted or updated by those transactions.
    pub writes: u64,
}
/// A 256-bit key for an encrypted database file.
///
/// The key is used as given, with no key derivation applied to it, so it must come from a
/// cryptographic random source or from a key-derivation function of the caller's. The store
/// holds it only while opening; where it is kept between runs is up to the caller.
#[derive(Clone)]
pub struct SqliteKey([u8; 32]);
impl SqliteKey {
    /// A key of these 32 bytes.
    pub fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}
impl std::fmt::Debug for SqliteKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SqliteKey(..)")
    }
}
/// How [`SqliteStore::open_with`] opens its database.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct SqliteOptions {
    /// Encrypts the database file and its write-ahead log with this key, through
    /// [SQLCipher](https://www.zetetic.net/sqlcipher/). Defaults to `None`: a plain SQLite file.
    ///
    /// A key needs a SQLite built with SQLCipher, which the `sqlcipher` feature provides.
    /// Without one, opening fails rather than leave the file unencrypted. A file keeps the key
    /// it was created with: one created without a key cannot be opened with one.
    pub key: Option<SqliteKey>,
}
impl SqliteOptions {
    /// Sets [`key`](Self::key).
    pub fn with_key(mut self, key: SqliteKey) -> Self {
        self.key = Some(key);
        self
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
    slot: HostSlot,
    /// Durable history length per run as of its last claim or commit through this store.
    /// Only a hint for which entries to encode: the worker checks it against the stored
    /// length, and a missing hint encodes the run's whole history.
    history_hints: Mutex<HashMap<RunId, usize>>,
    /// The same for children: the revision and history length stored for each, so a commit
    /// encodes only the children it changed. A missing hint encodes the whole child.
    child_hints: Mutex<children::Hints>,
}

/// The part of a `RunState` a commit may change, other than appending history. A run that has
/// admitted children also records their usage and how many there are.
#[derive(Serialize)]
struct ProgressRef<'a> {
    cancel_requested: bool,
    phase: &'a Phase,
    usage: &'a Usage,
    pending_response: &'a Option<ModelResponse>,
    operations: &'a [Operation],
    #[serde(skip_serializing_if = "is_zero")]
    children: usize,
    #[serde(skip_serializing_if = "Usage::is_zero")]
    child_usage: &'a Usage,
}
fn is_zero(count: &usize) -> bool {
    *count == 0
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Progress {
    cancel_requested: bool,
    phase: Phase,
    usage: Usage,
    pending_response: Option<ModelResponse>,
    operations: Vec<Operation>,
    #[serde(default)]
    children: usize,
    #[serde(default)]
    child_usage: Usage,
}
impl<'a> ProgressRef<'a> {
    fn of(state: &'a RunState) -> Self {
        Self {
            cancel_requested: state.cancel_requested,
            phase: &state.phase,
            usage: &state.usage,
            pending_response: &state.pending_response,
            operations: &state.operations,
            children: state.children.len(),
            child_usage: &state.child_usage,
        }
    }
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
    /// What queued steering does to this write, as `Checkpoint` allows.
    steering: Boundary,
    /// The children this write changed; empty unless the run has children.
    children: Vec<children::ChildDelta>,
}
impl Delta {
    fn encode(
        state: &RunState,
        revision: u64,
        history_from: usize,
        wake_now: bool,
        steering: Boundary,
    ) -> Result<Self> {
        Ok(Self {
            id: state.id.to_string(),
            conversation: state.conversation_id.clone(),
            revision,
            progress: encode(&ProgressRef::of(state))?,
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
            steering,
            children: Vec::new(),
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
        /// Queued steering this write appended to the run's history, for the caller's state.
        applied: Vec<HistoryEntry>,
    },
    /// The stored history is shorter than the delta's first encoded entry.
    NeedHistoryFrom(usize),
    /// A completed settlement found steering queued: nothing was written, and the caller
    /// commits `Ready` instead.
    Reopen,
    /// A child's stored progress does not match the hints it was encoded from.
    NeedChildren,
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

/// Keys the connection, which must not have read the file yet.
fn apply_key(connection: &Connection, key: &SqliteKey) -> Result<()> {
    // Plain SQLite ignores `PRAGMA key`, which would leave the file unencrypted without any
    // error. Only SQLCipher answers `cipher_version`.
    let cipher: Option<String> = connection
        .query_row("PRAGMA cipher_version", [], |row| row.get(0))
        .optional()
        .map_err(Error::storage)?;
    if cipher.is_none() {
        return Err(Error::config(
            "a database key was given, but SQLite was built without encryption; enable the `sqlcipher` feature",
        ));
    }
    // SQLCipher's raw-key form: the 32 bytes are the key, with no passphrase derivation.
    let mut literal = String::with_capacity(67);
    literal.push_str("x'");
    for byte in key.0 {
        literal.push_str(&format!("{byte:02x}"));
    }
    literal.push('\'');
    // The error is replaced, not wrapped, so that nothing derived from the key leaves here.
    connection
        .pragma_update(None, "key", &literal)
        .map_err(|_| Error::storage("SQLite refused the database key"))
}
/// The first read of a file opened with a key. SQLite cannot tell a wrong key from a file that
/// is not a database, so with a key in hand the key is what the caller has to fix.
fn first_keyed_read(error: rusqlite::Error) -> Error {
    if error.sqlite_error_code() == Some(rusqlite::ErrorCode::NotADatabase) {
        Error::Config(crate::ErrorDetail::caused_by(
            "the database key does not open this file, which was created with another key or without one",
            error,
        ))
    } else {
        Error::storage(error)
    }
}
impl Database {
    fn open(path: &Path, options: &SqliteOptions, counters: Arc<Counters>) -> Result<Self> {
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
        // An exclusive, non-blocking lock (flock on Unix, LockFileEx on Windows), released when
        // the file closes: when the store's worker drops the database, or the process exits.
        lock.try_lock().map_err(|error| match error {
            TryLockError::WouldBlock => Error::StoreBusy,
            TryLockError::Error(error) => Error::storage(error),
        })?;
        let connection = Connection::open(canonical).map_err(Error::storage)?;
        if let Some(key) = &options.key {
            apply_key(&connection, key)?;
        }
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(if options.key.is_some() {
                first_keyed_read
            } else {
                Error::storage
            })?;
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
            CREATE TABLE IF NOT EXISTS children (
                child_id TEXT PRIMARY KEY REFERENCES runs(id), root_id TEXT NOT NULL REFERENCES runs(id),
                ordinal INTEGER NOT NULL, call_id TEXT NOT NULL, revision INTEGER NOT NULL,
                history_len INTEGER NOT NULL, progress TEXT NOT NULL, UNIQUE(root_id, ordinal));
            PRAGMA user_version=2; COMMIT;",
            )
            .map_err(Error::storage)?;
        steering_queue::create(&connection)?;
        automation::create_tables(&connection)?;
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
    let history = load_history(connection, id, history_len)?;
    let progress: Progress = decode(&progress)?;
    let mut state = RunState {
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
        parent: None,
        children: Vec::new(),
        child_usage: progress.child_usage,
    };
    if progress.children > 0 {
        state.children = children::load(connection, &state, progress.children)?;
    }
    Ok(state)
}
/// The first `len` history entries of run `id`, in order.
fn load_history(connection: &Connection, id: RunId, len: i64) -> Result<Vec<HistoryEntry>> {
    let history_len = usize::try_from(len).map_err(Error::storage)?;
    let mut stmt = connection
        .prepare_cached("SELECT entry FROM history WHERE run_id=?1 AND ordinal<?2 ORDER BY ordinal")
        .map_err(Error::storage)?;
    let rows = stmt
        .query_map(params![id.to_string(), len], |r| r.get::<_, String>(0))
        .map_err(Error::storage)?;
    let history = rows
        .map(|row| decode(&row.map_err(Error::storage)?))
        .collect::<Result<Vec<HistoryEntry>>>()?;
    if history.len() != history_len {
        return Err(Error::storage(format!(
            "run {id} records {history_len} history entries but {} are stored",
            history.len()
        )));
    }
    Ok(history)
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
    // The one read main makes here. Only a dispatch or completion commit also asks, in the same
    // statement, whether steering is queued; `CASE` leaves every other commit's work unchanged.
    let (stored, steered): (i64, bool) = tx
        .prepare_cached(
            "SELECT history_len, CASE WHEN ?2 THEN EXISTS(SELECT 1 FROM steering
                WHERE run_id=?1 AND applied_revision IS NULL) ELSE 0 END
            FROM checkpoints WHERE run_id=?1",
        )
        .map_err(Error::storage)?
        .query_row(params![delta.id, delta.steering != Boundary::None], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .optional()
        .map_err(Error::storage)?
        .ok_or(Error::NotFound)?;
    let stored = usize::try_from(stored).map_err(Error::storage)?;
    let (expected, allow_settled) = match fence {
        Fence::Commit(position) => (position, false),
        Fence::Update(position) => (position, true),
    };
    let queued = match delta.steering {
        Boundary::Apply if steered => steering_queue::queued(tx, &delta.id)?,
        Boundary::Apply | Boundary::Reopen | Boundary::None => Vec::new(),
    };
    // Applied steering takes the run's next ordinals, after the delta's own entries.
    let history_len = match delta.steering {
        Boundary::Apply => delta.history_len + queued.len(),
        Boundary::None | Boundary::Reopen => delta.history_len,
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
            history_len as i64,
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
    if delta.steering == Boundary::Reopen && steered {
        return Ok(Written::Reopen);
    }
    if delta.history_len < stored {
        return Err(Error::input("history is append-only"));
    }
    if stored < delta.history_from {
        return Ok(Written::NeedHistoryFrom(stored));
    }
    let new = &delta.history[stored - delta.history_from..];
    let mut writes = 1
        + insert_history(tx, &delta.id, &delta.conversation, stored, new)?
        + upsert_operations(tx, &delta.id, &delta.operations)?;
    if !delta.children.is_empty() {
        match children::write(tx, &delta.id, &delta.children)? {
            Some(children) => writes += children,
            None => return Ok(Written::NeedChildren),
        }
    }
    let (applied_writes, applied) = steering_queue::apply(tx, delta, queued)?;
    writes += applied_writes;
    Ok(Written::Done { writes, applied })
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
    let delta = Delta::encode(&state, state.revision, 0, false, Boundary::None)?;
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
    /// Opens or creates the database file at `path` and takes its process lock, a `.lock` file
    /// beside it. Fails with [`Error::StoreBusy`] while another store holds the lock, with
    /// [`Error::Config`] for an in-memory or hard-linked path, with [`Error::Incompatible`] for
    /// another schema version, and with [`Error::Storage`] otherwise, which includes a file
    /// that is not a database or is encrypted.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(path, SqliteOptions::default()).await
    }
    /// [`open`](Self::open) with `options`, such as a key that encrypts the file.
    ///
    /// Beyond the failures of `open`, fails with [`Error::Config`] when a key is given to a
    /// SQLite built without encryption, and when the key does not open the file: it was created
    /// with another key, or without one.
    pub async fn open_with(path: impl AsRef<Path>, options: SqliteOptions) -> Result<Self> {
        let path: PathBuf = path.as_ref().to_owned();
        let counters = Arc::new(Counters::default());
        let worker_counters = counters.clone();
        let (sender, mut receiver) = mpsc::channel::<Job>(64);
        let (ready_tx, ready_rx) = oneshot::channel();
        let thread = thread::Builder::new()
            .name("agent-rs-sqlite".into())
            .spawn(
                move || match Database::open(&path, &options, worker_counters) {
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
                },
            )
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
                child_hints: Mutex::new(HashMap::new()),
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
    /// Write transactions and rows committed since the store was opened.
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
    /// Records what is now stored for each of the run's children, or forgets them.
    fn hint_children(&self, state: &RunState, stored: bool) {
        if let Ok(mut hints) = self.child_hints.lock() {
            for child in &state.children {
                let child = &child.state;
                if stored && !state.is_terminal() {
                    hints.insert(child.id, (child.revision, child.history.len()));
                } else {
                    hints.remove(&child.id);
                }
            }
        }
    }
    fn encode_children(&self, state: &RunState) -> Result<Vec<children::ChildDelta>> {
        let hints = self
            .child_hints
            .lock()
            .map_err(|_| Error::storage("hint lock poisoned"))?;
        children::changed(state, |id| hints.get(&id).copied())
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
        if !state.children.is_empty() {
            self.hint_children(&state, true);
        }
        Ok(state)
    }
    async fn update(&self, id: RunId, update: RunUpdate) -> Result<()> {
        self.call(move |db| {
            let tx = db.connection.transaction().map_err(Error::storage)?;
            let mut state = load(&tx, id)?;
            let fence = Fence::Update(CheckpointPosition::from(&state));
            let durable = state.history.len();
            let stored = children::stored(&state);
            let effect = update.apply(&mut state)?;
            if !effect.changed {
                return Ok(());
            }
            let revision = store::advance(state.revision, "revision")?;
            // Updates never apply steering: only an execution's commits may (see `Checkpoint`).
            let mut delta =
                Delta::encode(&state, revision, durable, effect.wake_now, Boundary::None)?;
            if !state.children.is_empty() {
                delta.children = children::changed(&state, |id| stored.get(&id).copied())?;
            }
            match write_delta(&tx, &delta, fence, effect.clear_error)? {
                Written::Done { writes, .. } => {
                    tx.commit().map_err(Error::storage)?;
                    db.count(writes);
                    Ok(())
                }
                Written::NeedHistoryFrom(_) | Written::Reopen | Written::NeedChildren => {
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
    ///
    /// Whether steering is queued is asked inside the same transaction, by the write's existing
    /// first statement and only for a dispatch or completion commit. A completion that finds
    /// some is rolled back and resent as `Ready`; the queue cannot drain in between, because only
    /// this run's dispatch commit applies it.
    async fn commit(&self, state: &mut RunState) -> Result<()> {
        let fence = Fence::Commit(CheckpointPosition::from(&*state));
        let revision = store::advance(state.revision, "revision")?;
        let mut from = self.hinted(state);
        let mut settled = None;
        let mut retried_children = false;
        let result = loop {
            let mut delta = Delta::encode(state, revision, from, false, Boundary::of(state))?;
            if !state.children.is_empty() {
                delta.children = self.encode_children(state)?;
            }
            let written = self
                .call(move |db| {
                    let tx = db.connection.transaction().map_err(Error::storage)?;
                    let written = write_delta(&tx, &delta, fence, false)?;
                    if let Written::Done { writes, .. } = &written {
                        tx.commit().map_err(Error::storage)?;
                        db.count(*writes);
                    }
                    Ok(written)
                })
                .await;
            match written {
                Ok(Written::Done { applied, .. }) => {
                    state.history.extend(applied);
                    break Ok(());
                }
                // The hint ran ahead of durable history; resend from the stored length.
                Ok(Written::NeedHistoryFrom(stored)) if stored < from => from = stored,
                Ok(Written::NeedHistoryFrom(_)) => {
                    break Err(Error::storage("checkpoint history changed during commit"));
                }
                Ok(Written::Reopen) => {
                    settled = Some(std::mem::replace(&mut state.phase, Phase::Ready));
                }
                // A child's hint was wrong; resend every child whole.
                Ok(Written::NeedChildren) if !retried_children => {
                    retried_children = true;
                    self.hint_children(state, false);
                }
                Ok(Written::NeedChildren) => {
                    break Err(Error::storage("checkpoint children changed during commit"));
                }
                Err(error) => {
                    self.hint(state.id, None);
                    if !state.children.is_empty() {
                        self.hint_children(state, false);
                    }
                    break Err(error);
                }
            }
        };
        if let Err(error) = result {
            // A failed commit leaves the caller's state as it was.
            if let Some(phase) = settled {
                state.phase = phase;
            }
            return Err(error);
        }
        state.revision = revision;
        let settled = state.is_terminal();
        self.hint(state.id, (!settled).then_some(state.history.len()));
        if !state.children.is_empty() {
            self.hint_children(state, true);
        }
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
    use crate::{ScriptedModel, Settlement, types::now_ms};
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
