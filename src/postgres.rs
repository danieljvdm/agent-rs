//! [`DurableStore`] on PostgreSQL, so a resident host can restart on another machine.
//!
//! One store owns one schema through one dedicated session. Opening takes a session advisory
//! lock for the schema; a second owner fails with [`Error::StoreBusy`]. All work runs serially
//! on that session through a bounded queue, each job under [`PostgresOptions::operation_timeout`].
//! Losing the session or timing out fails the store permanently: it never reconnects, and
//! [`DurableStore::lease`] refuses a new host. Call [`Host::shutdown`](crate::Host::shutdown),
//! then [`PostgresStore::close`].
//!
//! The layout mirrors the SQLite adapter, split by mutability so each write costs what it
//! changes:
//!
//! - `runs` holds immutable acceptance (identity, binding, input, limits, expiry and bounded
//!   prior context), inserted once and never updated.
//! - `checkpoints` holds one small row per run: its position (generation, revision), progress
//!   (phase, usage, cancellation, pending response, current operations), the number of history
//!   entries the run owns, and the discovery fields `wake_at`, `recover` and `last_error`.
//! - `history` holds each entry once, appended in order; `operations` retains every operation
//!   the run recorded, across batches, upserted only when it changed.
//! - `children` holds one progress row per child a run's delegations admitted. A child's
//!   acceptance is a `runs` row written once, at admission, under its conversation
//!   `child:{root}:{call}`, and its history and operations are rows like a run's. A child has no
//!   `checkpoints` row: it is never discovered, claimed or read on its own, and its root's
//!   `wake_at` and `recover` columns account for it.
//!
//! Every write is a single statement, so it is one implicit transaction and one network round
//! trip: a commit's fenced `UPDATE` of the checkpoint row, its history append and its operation
//! upserts are data-modifying parts of one `WITH` query, and only reach the history and
//! operations tables when the fence matched. A commit that changed children writes them in the
//! same query, behind the same fence; one that changed none sends the plain statement. Reads
//! that precede a write in the same job (the load before a claim or an update) cannot race
//! another writer, because only the owning session writes the schema, and the write is fenced
//! regardless. JSON is stored as text,
//! byte for byte; discovery reads the stored `wake_at` and `recover` columns, never JSON.
use crate::store;
use crate::types::STATE_VERSION;
use crate::{
    Agent, AttachedChild, Checkpoint, CheckpointPosition, DurableStore, Error, ErrorDetail,
    HistoryEntry, HostSlot, ModelResponse, Operation, ParentRun, Phase, Result, RunId, RunState,
    RunUpdate, StoreLease, Usage,
};
use async_trait::async_trait;
use futures::future::{BoxFuture, try_join_all};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::HashMap,
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Mutex as AsyncMutex, mpsc, oneshot},
    task::JoinHandle,
    time::timeout,
};
use tokio_postgres::{Client, Row, Socket, Statement, tls::MakeTlsConnect, types::ToSql};
pub use tokio_postgres::{Config, NoTls};

/// The layout version in `store_metadata`. Versions 1 to 3 were pre-release layouts (1 and 2
/// kept whole states in an `active` table; 3 had no `children` table); they are refused as
/// incompatible, not migrated.
const STORAGE_VERSION: i32 = 4;
/// Jobs that may wait for the session before callers feel backpressure.
const QUEUE: usize = 64;
/// History rows read per round trip while selecting a new run's bounded prior context.
const CONTEXT_PAGE: usize = 64;

/// How a [`PostgresStore`] uses its session. Start from [`PostgresOptions::default`]:
///
/// ```
/// # use std::time::Duration;
/// # use yielded_agent::postgres::PostgresOptions;
/// let options = PostgresOptions::default().with_operation_timeout(Duration::from_secs(5));
/// ```
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct PostgresOptions {
    /// Deadline for connecting, for initializing the schema, for each queued job once it
    /// starts, and for closing. Waiting in the queue is backpressure and not counted. A timeout
    /// fails the store permanently; a write whose reply was lost may still have committed.
    /// Nonzero. Default 30 seconds.
    pub operation_timeout: Duration,
}
impl Default for PostgresOptions {
    fn default() -> Self {
        Self {
            operation_timeout: Duration::from_secs(30),
        }
    }
}
impl PostgresOptions {
    /// Sets [`operation_timeout`](Self::operation_timeout).
    pub fn with_operation_timeout(mut self, timeout: Duration) -> Self {
        self.operation_timeout = timeout;
        self
    }
}

/// Work a [`PostgresStore`] has done since it connected, from [`PostgresStore::metrics`].
/// Every count is deterministic for a given sequence of calls.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct StoreMetrics {
    /// Committed write transactions. Each write is one statement, so one transaction.
    pub transactions: u64,
    /// Rows inserted or updated by those transactions.
    pub writes: u64,
    /// Statements executed on the session after it was opened, reads included. Each is one
    /// network round trip.
    pub round_trips: u64,
    /// Bytes of JSON text those transactions wrote: acceptance, progress, history entries and
    /// operations. Identifiers and integer columns are not counted.
    pub payload_bytes: u64,
}
#[derive(Default)]
struct Counters {
    transactions: AtomicU64,
    writes: AtomicU64,
    round_trips: AtomicU64,
    payload_bytes: AtomicU64,
}

/// Statements prepared once per session, so each later use is a single round trip.
struct Statements {
    load: Statement,
    load_children: Statement,
    find: Statement,
    context: Statement,
    insert: Statement,
    claim: Statement,
    write: Statement,
    write_children: Statement,
    inspect: Statement,
    due: Statement,
    recovery: Statement,
    position: Statement,
    record_error: Statement,
    last_error: Statement,
    history: Statement,
    operations: Statement,
}
struct Database {
    client: Client,
    sql: Statements,
    counters: Arc<Counters>,
    /// The advisory-lock namespace, released explicitly only after an orderly drain.
    namespace: String,
}
type Params<'a> = &'a [&'a (dyn ToSql + Sync)];
impl Database {
    fn trip(&self) {
        self.counters.round_trips.fetch_add(1, Ordering::Relaxed);
    }
    async fn query(&self, statement: &Statement, params: Params<'_>) -> Result<Vec<Row>> {
        self.trip();
        self.client
            .query(statement, params)
            .await
            .map_err(Error::storage)
    }
    async fn query_opt(&self, statement: &Statement, params: Params<'_>) -> Result<Option<Row>> {
        self.trip();
        self.client
            .query_opt(statement, params)
            .await
            .map_err(Error::storage)
    }
    async fn query_one(&self, statement: &Statement, params: Params<'_>) -> Result<Row> {
        self.trip();
        self.client
            .query_one(statement, params)
            .await
            .map_err(Error::storage)
    }
    async fn execute(&self, statement: &Statement, params: Params<'_>) -> Result<u64> {
        self.trip();
        self.client
            .execute(statement, params)
            .await
            .map_err(Error::storage)
    }
    fn count(&self, writes: u64, bytes: u64) {
        self.counters.transactions.fetch_add(1, Ordering::Relaxed);
        self.counters.writes.fetch_add(writes, Ordering::Relaxed);
        self.counters
            .payload_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }
}

type Job = Box<dyn FnOnce(Arc<Database>) -> BoxFuture<'static, ()> + Send>;
type Failure = Arc<Mutex<Option<Arc<Error>>>>;

/// Aborts the connection driver if its owner is dropped: on a cancelled `connect`, a failed
/// initialization, or runtime shutdown.
struct ConnectionTask(JoinHandle<std::result::Result<(), tokio_postgres::Error>>);
impl Drop for ConnectionTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Durable runs in one PostgreSQL schema, owned by one session.
///
/// Use a direct or session-pooled connection to a writable primary; transaction pooling cannot
/// hold the session's advisory lock. Only this adapter may write its schema. TLS is supplied
/// by the application; [`NoTls`] suits only a trusted local connection. Nothing is read from
/// the environment. Keep the store alive until hosts using it have shut down, then call
/// [`close`](Self::close).
pub struct PostgresStore {
    sender: Mutex<Option<mpsc::Sender<Job>>>,
    worker: AsyncMutex<Option<JoinHandle<()>>>,
    /// Why the session ended, once it has failed.
    failure: Failure,
    counters: Arc<Counters>,
    slot: HostSlot,
    /// Durable history length per run as of its last claim or commit through this store.
    /// Only a hint for which entries to send: the commit's fence checks it against the stored
    /// length, and a missing hint sends the run's whole history.
    history_hints: Mutex<HashMap<RunId, usize>>,
    /// The same for children: the revision and history length stored for each, so a commit
    /// sends only the children it changed. A missing hint sends the whole child, acceptance
    /// included; the statement skips what is already stored.
    child_hints: Mutex<HashMap<RunId, (u64, usize)>>,
}

/// The part of a `RunState` a commit may change, other than appending history. A run that has
/// admitted children also records their usage and how many there are; a run without children
/// encodes exactly as it did before children existed.
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

/// Everything one write sends, encoded from a borrow of the caller's state.
struct Delta {
    id: String,
    revision: i64,
    progress: String,
    settled: bool,
    wake_at: Option<i64>,
    recover: bool,
    history_len: i64,
    /// Encoded history entries from ordinal `history_from` to `history_len`.
    history_from: i64,
    history: Vec<String>,
    call_ids: Vec<String>,
    operations: Vec<String>,
    /// The children this write changed; empty unless the run has children.
    children: Children,
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
            revision: int(revision)?,
            progress: encode(&ProgressRef::of(state))?,
            settled: state.is_terminal(),
            wake_at: if wake_now {
                Some(0)
            } else {
                sql_time(state.wake_at())
            },
            recover: state.needs_recovery(),
            history_len: int(state.history.len() as u64)?,
            history_from: int(history_from as u64)?,
            history: state.history[history_from..]
                .iter()
                .map(encode)
                .collect::<Result<_>>()?,
            call_ids: state
                .operations
                .iter()
                .map(|op| op.call.id.clone())
                .collect(),
            operations: state.operations.iter().map(encode).collect::<Result<_>>()?,
            children: Children::default(),
        })
    }
    /// JSON bytes this delta sends, as counted by [`StoreMetrics::payload_bytes`].
    fn bytes(&self) -> u64 {
        let texts = self.history.iter().chain(&self.operations);
        (self.progress.len() + texts.map(String::len).sum::<usize>()) as u64 + self.children.bytes()
    }
}

/// The children one write changed, flattened into the array parameters of `WRITE_CHILDREN`:
/// one element per child in the first group, then one per new history entry and one per
/// current operation, each naming its child by 1-based position in the first group.
#[derive(Default)]
struct Children {
    ids: Vec<String>,
    /// Admission order within the root.
    ordinals: Vec<i64>,
    call_ids: Vec<String>,
    conversations: Vec<String>,
    revisions: Vec<i64>,
    progress: Vec<String>,
    history_lens: Vec<i64>,
    /// The ordinal of each child's first entry sent.
    history_froms: Vec<i64>,
    /// A child's acceptance, sent only when this store does not know it is stored; otherwise
    /// `None`. A child with a binding and `history_from` 0 may be admitted by the write.
    agent_ids: Vec<Option<String>>,
    bindings: Vec<Option<String>>,
    inputs: Vec<Option<String>>,
    limits: Vec<Option<String>>,
    expires_at: Vec<Option<i64>>,
    entry_children: Vec<i64>,
    entry_ordinals: Vec<i64>,
    entries: Vec<String>,
    operation_children: Vec<i64>,
    operation_call_ids: Vec<String>,
    operations: Vec<String>,
}
impl Children {
    /// Encodes every child whose revision differs from what `known` says is stored. A child
    /// `known` does not know is encoded whole, acceptance included.
    fn encode(state: &RunState, known: impl Fn(RunId) -> Option<(u64, usize)>) -> Result<Self> {
        let mut out = Self::default();
        for (ordinal, attached) in state.children.iter().enumerate() {
            let child = &attached.state;
            let known = known(child.id);
            if known.is_some_and(|(revision, _)| revision == child.revision) {
                continue;
            }
            let from = known
                .map(|(_, len)| len)
                .filter(|len| *len <= child.history.len())
                .unwrap_or(0);
            let position = int(out.ids.len() as u64 + 1)?;
            out.ids.push(child.id.to_string());
            out.ordinals.push(int(ordinal as u64)?);
            out.call_ids.push(attached.call_id.clone());
            out.conversations.push(child.conversation_id.clone());
            out.revisions.push(int(child.revision)?);
            out.progress.push(encode(&ProgressRef::of(child))?);
            out.history_lens.push(int(child.history.len() as u64)?);
            out.history_froms.push(int(from as u64)?);
            let whole = known.is_none();
            out.agent_ids.push(whole.then(|| child.agent_id.clone()));
            out.bindings
                .push(whole.then(|| encode(&child.binding)).transpose()?);
            out.inputs
                .push(whole.then(|| encode(&child.input)).transpose()?);
            out.limits
                .push(whole.then(|| encode(&child.limits)).transpose()?);
            out.expires_at
                .push(sql_time(child.expires_at_ms).filter(|_| whole));
            for (ordinal, entry) in child.history.iter().enumerate().skip(from) {
                out.entry_children.push(position);
                out.entry_ordinals.push(int(ordinal as u64)?);
                out.entries.push(encode(entry)?);
            }
            for op in &child.operations {
                out.operation_children.push(position);
                out.operation_call_ids.push(op.call.id.clone());
                out.operations.push(encode(op)?);
            }
        }
        Ok(out)
    }
    fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }
    fn bytes(&self) -> u64 {
        let acceptance = [&self.bindings, &self.inputs, &self.limits]
            .into_iter()
            .flatten()
            .flatten();
        let texts = (self.progress.iter())
            .chain(acceptance)
            .chain(&self.entries)
            .chain(&self.operations);
        texts.map(String::len).sum::<usize>() as u64
    }
}
enum Written {
    Done,
    /// The stored history is shorter than the delta's first encoded entry.
    NeedHistoryFrom(usize),
    /// A child's stored progress does not match the hint it was encoded from.
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
/// A counter as a signed 64-bit SQL integer; `store::advance` keeps counters within it.
fn int(value: u64) -> Result<i64> {
    i64::try_from(value).map_err(Error::storage)
}
fn uint(value: i64) -> Result<u64> {
    u64::try_from(value).map_err(Error::storage)
}
fn get<'a, T: tokio_postgres::types::FromSql<'a>>(row: &'a Row, index: usize) -> Result<T> {
    row.try_get(index).map_err(Error::storage)
}
fn run_id(text: &str) -> Result<RunId> {
    RunId::parse_str(text).map_err(Error::storage)
}
fn valid_schema(schema: &str) -> bool {
    let bytes = schema.as_bytes();
    (1..=63).contains(&bytes.len())
        && bytes[0].is_ascii_lowercase()
        && bytes
            .iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'_')
        && !schema.starts_with("pg_")
        && schema != "information_schema"
}

/// The schema's tables. `$schema.` is replaced by the quoted, validated schema name. Identity
/// columns use the "C" collation so comparisons and ordering are bytewise, as in SQLite.
const TABLES: &str = r#"
CREATE TABLE $schema.store_metadata (
    singleton BOOLEAN PRIMARY KEY CHECK (singleton), version INTEGER NOT NULL);
CREATE TABLE $schema.runs (
    id TEXT COLLATE "C" PRIMARY KEY,
    seq BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    conversation TEXT COLLATE "C" NOT NULL, idempotency_key TEXT COLLATE "C" NOT NULL,
    version INTEGER NOT NULL, agent_id TEXT NOT NULL, binding TEXT NOT NULL,
    input TEXT NOT NULL, limits TEXT NOT NULL, expires_at BIGINT, prior_context TEXT NOT NULL,
    UNIQUE (conversation, idempotency_key));
CREATE TABLE $schema.checkpoints (
    run_id TEXT COLLATE "C" PRIMARY KEY REFERENCES $schema.runs(id),
    run_seq BIGINT NOT NULL, conversation TEXT COLLATE "C" NOT NULL,
    generation BIGINT NOT NULL, revision BIGINT NOT NULL, history_len BIGINT NOT NULL,
    progress TEXT NOT NULL, settled BOOLEAN NOT NULL, wake_at BIGINT,
    recover BOOLEAN NOT NULL, last_error TEXT);
CREATE UNIQUE INDEX checkpoints_unsettled ON $schema.checkpoints (conversation)
    WHERE NOT settled;
CREATE INDEX checkpoints_due ON $schema.checkpoints (wake_at, run_id)
    WHERE wake_at IS NOT NULL;
CREATE INDEX checkpoints_recovery ON $schema.checkpoints (run_id)
    WHERE recover AND last_error IS NULL;
CREATE TABLE $schema.history (
    run_id TEXT COLLATE "C" NOT NULL REFERENCES $schema.runs(id), ordinal BIGINT NOT NULL,
    conversation TEXT COLLATE "C" NOT NULL, run_seq BIGINT NOT NULL, entry TEXT NOT NULL,
    PRIMARY KEY (run_id, ordinal));
CREATE INDEX history_conversation ON $schema.history (conversation, run_seq, ordinal);
CREATE TABLE $schema.operations (
    run_id TEXT COLLATE "C" NOT NULL REFERENCES $schema.runs(id),
    call_id TEXT COLLATE "C" NOT NULL, first_revision BIGINT NOT NULL,
    batch_index BIGINT NOT NULL, operation TEXT NOT NULL,
    PRIMARY KEY (run_id, call_id));
CREATE TABLE $schema.children (
    child_id TEXT COLLATE "C" PRIMARY KEY REFERENCES $schema.runs(id),
    root_id TEXT COLLATE "C" NOT NULL REFERENCES $schema.runs(id),
    ordinal BIGINT NOT NULL, call_id TEXT COLLATE "C" NOT NULL, revision BIGINT NOT NULL,
    history_len BIGINT NOT NULL, progress TEXT NOT NULL,
    UNIQUE (root_id, ordinal));
"#;

/// Reassemble a run from its acceptance, checkpoint and history rows, in one round trip.
const LOAD: &str = "
SELECT r.version, r.conversation, r.agent_id, r.binding, r.input, r.limits, r.expires_at,
    r.prior_context, c.generation, c.revision, c.history_len, c.progress,
    ARRAY(SELECT h.entry FROM $schema.history h
        WHERE h.run_id = r.id AND h.ordinal < c.history_len ORDER BY h.ordinal)
FROM $schema.runs r JOIN $schema.checkpoints c ON c.run_id = r.id
WHERE r.id = $1::text";
/// A root's children in admission order, each with its acceptance and history, in one round
/// trip. Read only for a run whose progress records children.
const LOAD_CHILDREN: &str = "
SELECT c.child_id, c.call_id, c.revision, c.history_len, c.progress, r.version, r.conversation,
    r.agent_id, r.binding, r.input, r.limits, r.expires_at,
    ARRAY(SELECT h.entry FROM $schema.history h
        WHERE h.run_id = c.child_id AND h.ordinal < c.history_len ORDER BY h.ordinal)
FROM $schema.children c JOIN $schema.runs r ON r.id = c.child_id
WHERE c.root_id = $1::text ORDER BY c.ordinal";
/// An earlier acceptance under this key, if any, and whether the conversation is busy.
const FIND: &str = "
SELECT r.id, r.input, r.binding, r.expires_at,
    EXISTS (SELECT 1 FROM $schema.checkpoints c
        WHERE c.conversation = $1::text AND NOT c.settled)
FROM (SELECT 1) AS one
LEFT JOIN $schema.runs r ON r.conversation = $1::text AND r.idempotency_key = $2::text";
/// One page of a conversation's history, newest first, before a (run, ordinal) position.
const CONTEXT: &str = "
SELECT run_seq, ordinal, entry FROM $schema.history
WHERE conversation = $1::text AND (run_seq, ordinal) < ($2::int8, $3::int8)
ORDER BY run_seq DESC, ordinal DESC LIMIT $page";
/// Acceptance: the immutable run, its first checkpoint, history and operations in one
/// statement. Conversation history is ordered by the run's acceptance sequence, then ordinal:
/// a conversation has one unsettled run at a time and settled runs append no history.
const INSERT: &str = "
WITH r AS (
    INSERT INTO $schema.runs (id, conversation, idempotency_key, version, agent_id, binding,
        input, limits, expires_at, prior_context)
    VALUES ($1::text, $2::text, $3::text, $4::int4, $5::text, $6::text, $7::text, $8::text,
        $9::int8, $10::text)
    RETURNING seq),
c AS (
    INSERT INTO $schema.checkpoints (run_id, run_seq, conversation, generation, revision,
        history_len, progress, settled, wake_at, recover)
    SELECT $1::text, r.seq, $2::text, $11::int8, $12::int8, $13::int8, $14::text, $15::bool,
        $16::int8, $17::bool
    FROM r
    RETURNING 1),
h AS (
    INSERT INTO $schema.history (run_id, ordinal, conversation, run_seq, entry)
    SELECT $1::text, t.i - 1, $2::text, r.seq, t.entry
    FROM r, UNNEST($18::text[]) WITH ORDINALITY AS t(entry, i)
    RETURNING 1),
o AS (
    INSERT INTO $schema.operations (run_id, call_id, first_revision, batch_index, operation)
    SELECT $1::text, t.call_id, $12::int8, t.i, t.operation
    FROM r, UNNEST($19::text[], $20::text[]) WITH ORDINALITY AS t(call_id, operation, i)
    RETURNING 1)
SELECT (SELECT count(*) FROM c), (SELECT count(*) FROM h), (SELECT count(*) FROM o)";
/// A claim: a fresh generation and recomputed eligibility, at exactly the loaded position.
const CLAIM: &str = "
UPDATE $schema.checkpoints
SET generation = $4::int8, revision = $5::int8, wake_at = $6::int8, recover = $7::bool
WHERE run_id = $1::text AND generation = $2::int8 AND revision = $3::int8 AND NOT settled";
/// A commit or an update. The checkpoint row changes only at exactly the caller's position,
/// unsettled unless the write is an update, and only if the stored history length lies between
/// the first entry sent and the new length. History and operations are written only through
/// that row, so a refused fence writes nothing. Entries already stored are skipped by
/// `ON CONFLICT`; operations are rewritten only when they changed.
const WRITE: &str = "
WITH cp AS (
    UPDATE $schema.checkpoints
    SET revision = $4::int8, history_len = $5::int8, progress = $6::text, settled = $7::bool,
        wake_at = $8::int8, recover = $9::bool,
        last_error = CASE WHEN $10::bool THEN NULL ELSE last_error END
    WHERE run_id = $1::text AND generation = $2::int8 AND revision = $3::int8
        AND ($11::bool OR NOT settled) AND history_len BETWEEN $12::int8 AND $5::int8
    RETURNING run_seq, conversation),
h AS (
    INSERT INTO $schema.history (run_id, ordinal, conversation, run_seq, entry)
    SELECT $1::text, $12::int8 + t.i - 1, cp.conversation, cp.run_seq, t.entry
    FROM cp, UNNEST($13::text[]) WITH ORDINALITY AS t(entry, i)
    ON CONFLICT (run_id, ordinal) DO NOTHING
    RETURNING 1),
op AS (
    INSERT INTO $schema.operations AS o (run_id, call_id, first_revision, batch_index,
        operation)
    SELECT $1::text, t.call_id, $4::int8, t.i, t.operation
    FROM cp, UNNEST($14::text[], $15::text[]) WITH ORDINALITY AS t(call_id, operation, i)
    ON CONFLICT (run_id, call_id) DO UPDATE SET operation = excluded.operation
        WHERE o.operation <> excluded.operation
    RETURNING 1)
SELECT (SELECT count(*) FROM cp), (SELECT count(*) FROM h), (SELECT count(*) FROM op)";
/// `WRITE` for a delta that changed children: the root's parts are the same, and its fence also
/// requires every child sent to match how it was encoded. `d` pairs each child sent with its
/// stored progress row, read from the statement's snapshot: a child with no row is admitted if
/// its acceptance was sent and its history sent whole; a child whose stored revision equals the
/// one sent is unchanged and skipped; any other must have a stored history length between its
/// first entry sent and its new length. Children are written only through the fenced row, so a
/// refused fence writes no child either. An admitted child gets its acceptance as a `runs` row
/// and its progress row; a changed child gets its progress updated; both append their new
/// history and upsert their operations, which first appear at the root's new revision. A
/// child's history rows carry its root's acceptance sequence: its conversation holds only it.
/// The last two columns classify children for a refusal: those whose stored history is longer
/// than the one sent, and every child that does not match.
const WRITE_CHILDREN: &str = "
WITH d AS (
    SELECT t.k, t.id, t.ordinal, t.call_id, t.conversation, t.revision, t.progress,
        t.history_len, t.agent_id, t.binding, t.input, t.limits, t.expires_at,
        s.child_id IS NULL AS fresh,
        s.child_id IS NULL OR s.revision <> t.revision AS changed,
        CASE WHEN s.child_id IS NULL THEN t.binding IS NOT NULL AND t.history_from = 0
            WHEN s.revision = t.revision THEN true
            ELSE s.history_len BETWEEN t.history_from AND t.history_len END AS ok,
        s.child_id IS NOT NULL AND s.revision <> t.revision AND s.history_len > t.history_len
            AS shrunk
    FROM UNNEST($16::text[], $17::int8[], $18::text[], $19::text[], $20::int8[], $21::text[],
            $22::int8[], $23::int8[], $24::text[], $25::text[], $26::text[], $27::text[],
            $28::int8[])
        WITH ORDINALITY AS t(id, ordinal, call_id, conversation, revision, progress,
            history_len, history_from, agent_id, binding, input, limits, expires_at, k)
    LEFT JOIN $schema.children s ON s.child_id = t.id AND s.root_id = $1::text),
cp AS (
    UPDATE $schema.checkpoints
    SET revision = $4::int8, history_len = $5::int8, progress = $6::text, settled = $7::bool,
        wake_at = $8::int8, recover = $9::bool,
        last_error = CASE WHEN $10::bool THEN NULL ELSE last_error END
    WHERE run_id = $1::text AND generation = $2::int8 AND revision = $3::int8
        AND ($11::bool OR NOT settled) AND history_len BETWEEN $12::int8 AND $5::int8
        AND NOT EXISTS (SELECT 1 FROM d WHERE d.ok IS NOT TRUE)
    RETURNING run_seq, conversation),
h AS (
    INSERT INTO $schema.history (run_id, ordinal, conversation, run_seq, entry)
    SELECT $1::text, $12::int8 + t.i - 1, cp.conversation, cp.run_seq, t.entry
    FROM cp, UNNEST($13::text[]) WITH ORDINALITY AS t(entry, i)
    ON CONFLICT (run_id, ordinal) DO NOTHING
    RETURNING 1),
op AS (
    INSERT INTO $schema.operations AS o (run_id, call_id, first_revision, batch_index,
        operation)
    SELECT $1::text, t.call_id, $4::int8, t.i, t.operation
    FROM cp, UNNEST($14::text[], $15::text[]) WITH ORDINALITY AS t(call_id, operation, i)
    ON CONFLICT (run_id, call_id) DO UPDATE SET operation = excluded.operation
        WHERE o.operation <> excluded.operation
    RETURNING 1),
cr AS (
    INSERT INTO $schema.runs (id, conversation, idempotency_key, version, agent_id, binding,
        input, limits, expires_at, prior_context)
    SELECT d.id, d.conversation, d.call_id, $29::int4, d.agent_id, d.binding, d.input,
        d.limits, d.expires_at, '[]'
    FROM cp, d WHERE d.fresh
    RETURNING 1),
cc AS (
    INSERT INTO $schema.children (child_id, root_id, ordinal, call_id, revision, history_len,
        progress)
    SELECT d.id, $1::text, d.ordinal, d.call_id, d.revision, d.history_len, d.progress
    FROM cp, d WHERE d.fresh
    RETURNING 1),
cu AS (
    UPDATE $schema.children AS c
    SET revision = d.revision, history_len = d.history_len, progress = d.progress
    FROM cp, d WHERE c.child_id = d.id AND d.changed AND NOT d.fresh
    RETURNING 1),
ch AS (
    INSERT INTO $schema.history (run_id, ordinal, conversation, run_seq, entry)
    SELECT d.id, e.ordinal, d.conversation, cp.run_seq, e.entry
    FROM cp CROSS JOIN UNNEST($30::int8[], $31::int8[], $32::text[]) AS e(k, ordinal, entry)
        JOIN d ON d.k = e.k
    WHERE d.changed
    ON CONFLICT (run_id, ordinal) DO NOTHING
    RETURNING 1),
co AS (
    INSERT INTO $schema.operations AS o (run_id, call_id, first_revision, batch_index,
        operation)
    SELECT d.id, e.call_id, $4::int8, e.i, e.operation
    FROM cp CROSS JOIN UNNEST($33::int8[], $34::text[], $35::text[])
            WITH ORDINALITY AS e(k, call_id, operation, i)
        JOIN d ON d.k = e.k
    WHERE d.changed
    ON CONFLICT (run_id, call_id) DO UPDATE SET operation = excluded.operation
        WHERE o.operation <> excluded.operation
    RETURNING 1)
SELECT (SELECT count(*) FROM cp), (SELECT count(*) FROM h), (SELECT count(*) FROM op),
    (SELECT count(*) FROM cr) + (SELECT count(*) FROM cc) + (SELECT count(*) FROM cu)
        + (SELECT count(*) FROM ch) + (SELECT count(*) FROM co),
    (SELECT count(*) FROM d WHERE d.shrunk),
    (SELECT count(*) FROM d WHERE d.ok IS NOT TRUE)";
/// Why a write was refused; read only after a refusal.
const INSPECT: &str = "
SELECT generation, revision, settled, history_len FROM $schema.checkpoints
WHERE run_id = $1::text";
const DUE: &str = "
SELECT run_id FROM $schema.checkpoints WHERE wake_at <= $1::int8
ORDER BY wake_at, run_id LIMIT $2::int8";
const RECOVERY: &str = "
SELECT run_id FROM $schema.checkpoints
WHERE recover AND last_error IS NULL AND run_id > $1::text ORDER BY run_id LIMIT $2::int8";
const POSITION: &str = "
SELECT generation, revision FROM $schema.checkpoints WHERE run_id = $1::text AND NOT settled";
const RECORD_ERROR: &str = "
UPDATE $schema.checkpoints SET wake_at = NULL, last_error = $4::text
WHERE run_id = $1::text AND generation = $2::int8 AND revision = $3::int8 AND NOT settled";
const LAST_ERROR: &str = "SELECT last_error FROM $schema.checkpoints WHERE run_id = $1::text";
const HISTORY: &str = "
SELECT entry FROM $schema.history WHERE conversation = $1::text ORDER BY run_seq, ordinal";
const OPERATIONS: &str = "
SELECT operation FROM $schema.operations WHERE run_id = $1::text
ORDER BY first_revision, batch_index";

impl Database {
    /// Take ownership of `schema`, create or check its tables, and prepare every statement.
    async fn open(mut client: Client, schema: &str, counters: Arc<Counters>) -> Result<Self> {
        let namespace = format!("yielded-agent:{schema}");
        let locked: bool = client
            .query_one(
                "SELECT pg_catalog.pg_try_advisory_lock(pg_catalog.hashtextextended($1::text, 0))",
                &[&namespace],
            )
            .await
            .map_err(Error::storage)?
            .get(0);
        if !locked {
            return Err(Error::StoreBusy);
        }
        let encoding: String = client
            .query_one("SELECT pg_catalog.current_setting('server_encoding')", &[])
            .await
            .map_err(Error::storage)?
            .get(0);
        if encoding != "UTF8" {
            return Err(Error::config(format!(
                "PostgreSQL server encoding {encoding}; UTF8 is required"
            )));
        }
        // Acknowledged commits must be durable on the primary.
        client
            .batch_execute("SET synchronous_commit TO on")
            .await
            .map_err(Error::storage)?;
        // `schema` was validated as a lowercase ASCII identifier, so quoting it is exact.
        let qualified = format!("\"{schema}\".");
        let sql = |text: &str| {
            text.replace("$schema.", &qualified)
                .replace("$page", &CONTEXT_PAGE.to_string())
        };
        let tx = client.transaction().await.map_err(Error::storage)?;
        let exists: bool = tx
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = $1::text)",
                &[&schema],
            )
            .await
            .map_err(Error::storage)?
            .get(0);
        if !exists {
            tx.batch_execute(&format!("CREATE SCHEMA \"{schema}\""))
                .await
                .map_err(Error::storage)?;
        }
        let metadata = format!("{qualified}store_metadata");
        let initialized: bool = tx
            .query_one("SELECT to_regclass($1::text) IS NOT NULL", &[&metadata])
            .await
            .map_err(Error::storage)?
            .get(0);
        if initialized {
            let version: Option<i32> = tx
                .query_opt(
                    &format!("SELECT version FROM {metadata} WHERE singleton"),
                    &[],
                )
                .await
                .map_err(Error::storage)?
                .map(|row| row.get(0));
            if version != Some(STORAGE_VERSION) {
                return Err(Error::incompatible(format!(
                    "PostgreSQL schema version {version:?}"
                )));
            }
        } else {
            let foreign: bool = tx
                .query_one(
                    "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_class c
                        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                        WHERE n.nspname = $1::text)",
                    &[&schema],
                )
                .await
                .map_err(Error::storage)?
                .get(0);
            if foreign {
                return Err(Error::incompatible(format!(
                    "schema {schema} holds relations this adapter did not create"
                )));
            }
            tx.batch_execute(&sql(TABLES))
                .await
                .map_err(Error::storage)?;
            tx.execute(
                &format!("INSERT INTO {metadata} (singleton, version) VALUES (true, $1::int4)"),
                &[&STORAGE_VERSION],
            )
            .await
            .map_err(Error::storage)?;
        }
        tx.commit().await.map_err(Error::storage)?;
        // Prepared together: the client pipelines them in one exchange.
        let texts = [
            LOAD,
            LOAD_CHILDREN,
            FIND,
            CONTEXT,
            INSERT,
            CLAIM,
            WRITE,
            WRITE_CHILDREN,
            INSPECT,
            DUE,
            RECOVERY,
            POSITION,
            RECORD_ERROR,
            LAST_ERROR,
            HISTORY,
            OPERATIONS,
        ]
        .map(sql);
        let prepared = try_join_all(texts.iter().map(|text| client.prepare(text)))
            .await
            .map_err(Error::storage)?;
        let [
            load,
            load_children,
            find,
            context,
            insert,
            claim,
            write,
            write_children,
            inspect,
            due,
            recovery,
            position,
            record_error,
            last_error,
            history,
            operations,
        ] = <[Statement; 16]>::try_from(prepared).map_err(|_| Error::storage("prepare"))?;
        Ok(Self {
            client,
            sql: Statements {
                load,
                load_children,
                find,
                context,
                insert,
                claim,
                write,
                write_children,
                inspect,
                due,
                recovery,
                position,
                record_error,
                last_error,
                history,
                operations,
            },
            counters,
            namespace,
        })
    }
}

async fn load(db: &Database, id: RunId) -> Result<RunState> {
    let key = id.to_string();
    let row = db
        .query_opt(&db.sql.load, &[&key])
        .await?
        .ok_or(Error::NotFound)?;
    let version: i32 = get(&row, 0)?;
    if i64::from(version) != i64::from(STATE_VERSION) {
        return Err(Error::incompatible(format!(
            "checkpoint state version {version}"
        )));
    }
    let history_len = usize::try_from(get::<i64>(&row, 10)?).map_err(Error::storage)?;
    let entries: Vec<&str> = get(&row, 12)?;
    if entries.len() != history_len {
        return Err(Error::storage(format!(
            "run {id} records {history_len} history entries but {} are stored",
            entries.len()
        )));
    }
    let history = entries
        .into_iter()
        .map(decode)
        .collect::<Result<Vec<HistoryEntry>>>()?;
    let progress: Progress = decode(get(&row, 11)?)?;
    let mut state = RunState {
        version: STATE_VERSION,
        id,
        conversation_id: get(&row, 1)?,
        agent_id: get(&row, 2)?,
        binding: decode(get(&row, 3)?)?,
        input: decode(get(&row, 4)?)?,
        limits: decode(get(&row, 5)?)?,
        expires_at_ms: get::<Option<i64>>(&row, 6)?.map(uint).transpose()?,
        generation: uint(get(&row, 8)?)?,
        revision: uint(get(&row, 9)?)?,
        cancel_requested: progress.cancel_requested,
        phase: progress.phase,
        usage: progress.usage,
        prior_context: decode(get(&row, 7)?)?,
        history,
        pending_response: progress.pending_response,
        operations: progress.operations,
        parent: None,
        children: Vec::new(),
        child_usage: progress.child_usage,
    };
    if progress.children > 0 {
        state.children = load_children(db, &state, progress.children).await?;
    }
    Ok(state)
}
/// The `count` children of `root`, in admission order, in one round trip.
async fn load_children(db: &Database, root: &RunState, count: usize) -> Result<Vec<AttachedChild>> {
    let rows = db
        .query(&db.sql.load_children, &[&root.id.to_string()])
        .await?;
    if rows.len() != count {
        return Err(Error::storage(format!(
            "run {} records {count} children but {} are stored",
            root.id,
            rows.len()
        )));
    }
    let mut children = Vec::with_capacity(count);
    for row in &rows {
        let version: i32 = get(row, 5)?;
        if i64::from(version) != i64::from(STATE_VERSION) {
            return Err(Error::incompatible(format!(
                "child state version {version}"
            )));
        }
        let id = run_id(get(row, 0)?)?;
        let call_id: String = get(row, 1)?;
        let history_len = usize::try_from(get::<i64>(row, 3)?).map_err(Error::storage)?;
        let entries: Vec<&str> = get(row, 12)?;
        if entries.len() != history_len {
            return Err(Error::storage(format!(
                "child {id} records {history_len} history entries but {} are stored",
                entries.len()
            )));
        }
        let progress: Progress = decode(get(row, 4)?)?;
        let state = RunState {
            version: STATE_VERSION,
            id,
            conversation_id: get(row, 6)?,
            agent_id: get(row, 7)?,
            binding: decode(get(row, 8)?)?,
            input: decode(get(row, 9)?)?,
            limits: decode(get(row, 10)?)?,
            expires_at_ms: get::<Option<i64>>(row, 11)?.map(uint).transpose()?,
            // Children are fenced by their root, never claimed themselves.
            generation: 0,
            revision: uint(get(row, 2)?)?,
            cancel_requested: progress.cancel_requested,
            phase: progress.phase,
            usage: progress.usage,
            prior_context: Vec::new(),
            history: entries.into_iter().map(decode).collect::<Result<_>>()?,
            pending_response: progress.pending_response,
            operations: progress.operations,
            parent: Some(ParentRun {
                run_id: root.id,
                call_id: call_id.clone(),
            }),
            children: Vec::new(),
            child_usage: progress.child_usage,
        };
        children.push(AttachedChild {
            call_id,
            state: Box::new(state),
        });
    }
    Ok(children)
}
/// The conversation's newest history that fits `limit` encoded bytes, oldest first, read in
/// pages of `CONTEXT_PAGE` rows: the same rule as SQLite and `MemoryStore`.
async fn bounded_history(
    db: &Database,
    conversation: &str,
    limit: usize,
) -> Result<Vec<HistoryEntry>> {
    let mut result = Vec::new();
    let mut bytes = 2usize;
    let (mut run, mut ordinal) = (i64::MAX, i64::MAX);
    'pages: loop {
        let rows = db
            .query(&db.sql.context, &[&conversation, &run, &ordinal])
            .await?;
        for row in &rows {
            (run, ordinal) = (get(row, 0)?, get(row, 1)?);
            let text: &str = get(row, 2)?;
            let next = bytes.saturating_add(text.len()).saturating_add(1);
            if next > limit {
                break 'pages;
            }
            bytes = next;
            result.push(decode(text)?);
        }
        if rows.len() < CONTEXT_PAGE {
            break;
        }
    }
    result.reverse();
    Ok(result)
}
async fn accept_on(
    db: &Database,
    agent: &Agent,
    conversation: &str,
    key: &str,
    input: Value,
    expires_at_ms: Option<u64>,
) -> Result<RunId> {
    store::check_acceptance(conversation, key, expires_at_ms)?;
    let row = db.query_one(&db.sql.find, &[&conversation, &key]).await?;
    if let Some(id) = get::<Option<&str>>(&row, 0)? {
        let (old_input, old_binding): (&str, &str) = (get(&row, 1)?, get(&row, 2)?);
        if old_input != encode(&input)?
            || get::<Option<i64>>(&row, 3)? != sql_time(expires_at_ms)
            || old_binding != encode(agent.binding())?
        {
            return Err(Error::IdempotencyConflict);
        }
        return run_id(id);
    }
    if get::<bool>(&row, 4)? {
        return Err(Error::ConversationBusy);
    }
    let context = bounded_history(db, conversation, agent.limits().max_context_bytes).await?;
    let state = agent.new_run(conversation, input, &context, expires_at_ms)?;
    let delta = Delta::encode(&state, state.revision, 0, false)?;
    let acceptance = [
        encode(&state.binding)?,
        encode(&state.input)?,
        encode(&state.limits)?,
        encode(&state.prior_context)?,
    ];
    let version = i32::try_from(state.version).map_err(Error::storage)?;
    let (generation, expires_at) = (int(state.generation)?, sql_time(state.expires_at_ms));
    let row = db
        .query_one(
            &db.sql.insert,
            &[
                &delta.id,
                &conversation,
                &key,
                &version,
                &state.agent_id,
                &acceptance[0],
                &acceptance[1],
                &acceptance[2],
                &expires_at,
                &acceptance[3],
                &generation,
                &delta.revision,
                &delta.history_len,
                &delta.progress,
                &delta.settled,
                &delta.wake_at,
                &delta.recover,
                &delta.history,
                &delta.call_ids,
                &delta.operations,
            ],
        )
        .await?;
    let (checkpoints, history, operations): (i64, i64, i64) =
        (get(&row, 0)?, get(&row, 1)?, get(&row, 2)?);
    if checkpoints != 1 {
        return Err(Error::storage("acceptance wrote no checkpoint"));
    }
    let bytes = acceptance.iter().map(String::len).sum::<usize>() as u64 + delta.bytes();
    db.count(2 + uint(history)? + uint(operations)?, bytes);
    Ok(state.id)
}
/// Persist one delta in one statement. The fence is the caller's `expected` position; an update
/// (`allow_settled`) may also change a settled run, so its uncertainty stays resolvable.
async fn write_delta(
    db: &Database,
    delta: &Delta,
    expected: CheckpointPosition,
    allow_settled: bool,
    clear_error: bool,
) -> Result<Written> {
    let (generation, revision) = (int(expected.generation)?, int(expected.revision)?);
    let clear_error = clear_error || delta.settled;
    let root: [&(dyn ToSql + Sync); 15] = [
        &delta.id,
        &generation,
        &revision,
        &delta.revision,
        &delta.history_len,
        &delta.progress,
        &delta.settled,
        &delta.wake_at,
        &delta.recover,
        &clear_error,
        &allow_settled,
        &delta.history_from,
        &delta.history,
        &delta.call_ids,
        &delta.operations,
    ];
    // A write that changed no child sends the plain statement, unchanged by children.
    let children = &delta.children;
    let row = if children.is_empty() {
        db.query_one(&db.sql.write, &root).await?
    } else {
        let version = i32::try_from(STATE_VERSION).map_err(Error::storage)?;
        let child: [&(dyn ToSql + Sync); 20] = [
            &children.ids,
            &children.ordinals,
            &children.call_ids,
            &children.conversations,
            &children.revisions,
            &children.progress,
            &children.history_lens,
            &children.history_froms,
            &children.agent_ids,
            &children.bindings,
            &children.inputs,
            &children.limits,
            &children.expires_at,
            &version,
            &children.entry_children,
            &children.entry_ordinals,
            &children.entries,
            &children.operation_children,
            &children.operation_call_ids,
            &children.operations,
        ];
        let params: Vec<_> = root.into_iter().chain(child).collect();
        db.query_one(&db.sql.write_children, &params).await?
    };
    let (checkpoints, history, operations): (i64, i64, i64) =
        (get(&row, 0)?, get(&row, 1)?, get(&row, 2)?);
    // Children's rows written, children with a longer stored history, children that differ.
    let (child_writes, shrunk, mismatched): (i64, i64, i64) = if children.is_empty() {
        (0, 0, 0)
    } else {
        (get(&row, 3)?, get(&row, 4)?, get(&row, 5)?)
    };
    if checkpoints == 1 {
        let writes = 1 + uint(history)? + uint(operations)? + uint(child_writes)?;
        db.count(writes, delta.bytes());
        return Ok(Written::Done);
    }
    // Refused: one more read says why, in the order SQLite checks.
    let row = db
        .query_opt(&db.sql.inspect, &[&delta.id])
        .await?
        .ok_or(Error::NotFound)?;
    let (stored_generation, stored_revision, settled, stored): (i64, i64, bool, i64) =
        (get(&row, 0)?, get(&row, 1)?, get(&row, 2)?, get(&row, 3)?);
    if stored_generation != generation || stored_revision != revision || settled && !allow_settled {
        return Err(Error::Stale);
    }
    if delta.history_len < stored {
        return Err(Error::input("history is append-only"));
    }
    if stored < delta.history_from {
        return Ok(Written::NeedHistoryFrom(
            usize::try_from(stored).map_err(Error::storage)?,
        ));
    }
    // The run itself matched, so its children refused the write.
    if shrunk > 0 {
        return Err(Error::input("history is append-only"));
    }
    if mismatched > 0 {
        return Ok(Written::NeedChildren);
    }
    Err(Error::storage(
        "checkpoint write refused at a matching position",
    ))
}
fn ids(rows: Vec<Row>) -> Result<Vec<RunId>> {
    rows.iter().map(|row| run_id(get(row, 0)?)).collect()
}
fn decoded<T: serde::de::DeserializeOwned>(rows: Vec<Row>) -> Result<Vec<T>> {
    rows.iter().map(|row| decode(get(row, 0)?)).collect()
}

/// Drive the session: run queued jobs one at a time until the store closes, the session ends,
/// or a job outlives its deadline. Never reconnects.
async fn work(
    database: Database,
    mut connection: ConnectionTask,
    mut receiver: mpsc::Receiver<Job>,
    deadline: Duration,
    failure: Failure,
) {
    let database = Arc::new(database);
    let stopped: Result<()> = loop {
        tokio::select! {
            biased;
            ended = &mut connection.0 => {
                let cause = match ended {
                    Ok(Err(error)) => Error::storage(error),
                    Err(error) => Error::storage(error),
                    Ok(Ok(())) => Error::storage("connection closed"),
                };
                break Err(Error::Storage(ErrorDetail::caused_by(
                    "PostgreSQL ownership session ended", cause,
                )));
            }
            job = receiver.recv() => match job {
                Some(job) => {
                    if timeout(deadline, job(database.clone())).await.is_err() {
                        break Err(Error::storage(
                            "PostgreSQL operation timed out; the store gave up its session",
                        ));
                    }
                }
                None => break Ok(()),
            }
        }
    };
    let closed = match stopped {
        Ok(()) => {
            // Drained in order: no job can follow, so release ownership before disconnecting
            // and a successor can take it at once. Dropping the client ends the session.
            let unlocked = timeout(
                deadline,
                database.client.query_one(
                    "SELECT pg_catalog.pg_advisory_unlock(pg_catalog.hashtextextended($1::text, 0))",
                    &[&database.namespace],
                ),
            )
            .await;
            drop(database);
            let ended = timeout(deadline, &mut connection.0).await;
            match (unlocked, ended) {
                (Ok(Ok(_)), Ok(Ok(Ok(())))) => Ok(()),
                (Err(_), _) | (_, Err(_)) => Err(Error::storage("PostgreSQL close timed out")),
                (Ok(Err(error)), _) => Err(Error::storage(error)),
                (_, Ok(Ok(Err(error)))) => Err(Error::storage(error)),
                (_, Ok(Err(error))) => Err(Error::storage(error)),
            }
        }
        Err(error) => {
            // Record the cause before queued callers see their jobs dropped.
            *failure.lock().expect("PostgreSQL failure lock") = Some(Arc::new(error));
            drop(receiver);
            drop(database);
            connection.0.abort();
            let _ = (&mut connection.0).await;
            return;
        }
    };
    if let Err(error) = closed {
        *failure.lock().expect("PostgreSQL failure lock") = Some(Arc::new(error));
    }
}

impl PostgresStore {
    /// Connects with `config` and `tls`, takes ownership of `schema`, and creates its tables on
    /// first use. `schema` is 1..=63 lowercase ASCII letters, digits or underscores, starting
    /// with a letter and not `pg_`; the role needs `CREATE` on the database to create it.
    ///
    /// Fails with [`Error::Config`] for an invalid schema name, a zero timeout, or a server
    /// encoding other than UTF8; with [`Error::StoreBusy`] while another session owns the
    /// schema; with [`Error::Incompatible`] for another layout version or a schema holding
    /// other relations; and with [`Error::Storage`] otherwise.
    pub async fn connect<T>(
        config: &Config,
        schema: &str,
        tls: T,
        options: PostgresOptions,
    ) -> Result<Self>
    where
        T: MakeTlsConnect<Socket>,
        T::Stream: Send + 'static,
    {
        if !valid_schema(schema) {
            return Err(Error::config(
                "PostgreSQL schema names are 1..=63 lowercase ASCII letters, digits or \
                 underscores, starting with a letter and not pg_",
            ));
        }
        let deadline = options.operation_timeout;
        if deadline.is_zero() {
            return Err(Error::config(
                "PostgreSQL operation timeout must be nonzero",
            ));
        }
        let (client, connection) = timeout(deadline, config.connect(tls))
            .await
            .map_err(|_| Error::storage("PostgreSQL connection timed out"))?
            .map_err(Error::storage)?;
        let mut connection = ConnectionTask(tokio::spawn(connection));
        let counters = Arc::new(Counters::default());
        let opened = timeout(deadline, Database::open(client, schema, counters.clone()))
            .await
            .map_err(|_| Error::storage("PostgreSQL initialization timed out"))
            .and_then(|opened| opened);
        let database = match opened {
            Ok(database) => database,
            Err(error) => {
                // The client is gone; end the session before reporting.
                connection.0.abort();
                let _ = (&mut connection.0).await;
                return Err(error);
            }
        };
        let (sender, receiver) = mpsc::channel(QUEUE);
        let failure = Failure::default();
        let worker = tokio::spawn(work(
            database,
            connection,
            receiver,
            deadline,
            failure.clone(),
        ));
        Ok(Self {
            sender: Mutex::new(Some(sender)),
            worker: AsyncMutex::new(Some(worker)),
            failure,
            counters,
            slot: HostSlot::default(),
            history_hints: Mutex::new(HashMap::new()),
            child_hints: Mutex::new(HashMap::new()),
        })
    }
    /// Work done since the store connected.
    pub fn metrics(&self) -> StoreMetrics {
        StoreMetrics {
            transactions: self.counters.transactions.load(Ordering::Relaxed),
            writes: self.counters.writes.load(Ordering::Relaxed),
            round_trips: self.counters.round_trips.load(Ordering::Relaxed),
            payload_bytes: self.counters.payload_bytes.load(Ordering::Relaxed),
        }
    }
    /// Stop accepting work, drain queued jobs, release the schema and end the session. Fails with
    /// [`Error::Storage`], caused by the failure, if the session had already failed. Dropping
    /// the store also drains and disconnects, but cannot be awaited.
    pub async fn close(&self) -> Result<()> {
        self.sender
            .lock()
            .map_err(|_| Error::storage("PostgreSQL sender lock poisoned"))?
            .take();
        let mut worker = self.worker.lock().await;
        if let Some(handle) = worker.as_mut() {
            // Kept in the mutex until it finishes, so a cancelled close leaves a later close
            // something to join.
            let joined = handle.await;
            worker.take();
            joined.map_err(|error| {
                Error::Storage(ErrorDetail::caused_by("PostgreSQL worker failed", error))
            })?;
        }
        match self.failed() {
            Some(cause) => Err(stopped(cause)),
            None => Ok(()),
        }
    }
    fn failed(&self) -> Option<Arc<Error>> {
        self.failure.lock().ok().and_then(|failure| failure.clone())
    }
    fn unavailable(&self) -> Error {
        match self.failed() {
            Some(cause) => stopped(cause),
            None => Error::storage("PostgreSQL store is closed"),
        }
    }
    async fn call<T, F, Fut>(&self, action: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(Arc<Database>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        let sender = self
            .sender
            .lock()
            .map_err(|_| Error::storage("PostgreSQL sender lock poisoned"))?
            .clone()
            .ok_or_else(|| self.unavailable())?;
        let (tx, rx) = oneshot::channel();
        let job: Job = Box::new(move |database| {
            Box::pin(async move {
                let _ = tx.send(action(database).await);
            })
        });
        // The job runs even if this caller stops waiting: dropping a future never rolls back
        // or skips work the store has queued.
        sender.send(job).await.map_err(|_| self.unavailable())?;
        rx.await.map_err(|_| self.unavailable())?
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
    fn encode_children(&self, state: &RunState) -> Result<Children> {
        let hints = self
            .child_hints
            .lock()
            .map_err(|_| Error::storage("PostgreSQL hint lock poisoned"))?;
        Children::encode(state, |id| hints.get(&id).copied())
    }
}
fn stopped(cause: Arc<Error>) -> Error {
    Error::Storage(ErrorDetail::caused_by("PostgreSQL store failed", cause))
}

#[async_trait]
impl DurableStore for PostgresStore {
    /// Residency for one host, while this store still owns its session. A store whose session
    /// failed or that was closed refuses, and never reconnects; open a new store instead.
    fn lease(&self) -> Result<StoreLease> {
        let open = self
            .sender
            .lock()
            .map(|sender| sender.as_ref().is_some_and(|sender| !sender.is_closed()))
            .unwrap_or(false);
        if !open || self.failed().is_some() {
            return Err(self.unavailable());
        }
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
        self.call(move |db| async move {
            accept_on(&db, &agent, &conversation, &key, input, expires_at_ms).await
        })
        .await
    }
    async fn status(&self, id: RunId) -> Result<RunState> {
        self.call(move |db| async move { load(&db, id).await })
            .await
    }
    async fn history(&self, conversation: &str) -> Result<Vec<HistoryEntry>> {
        let conversation = conversation.to_owned();
        self.call(
            move |db| async move { decoded(db.query(&db.sql.history, &[&conversation]).await?) },
        )
        .await
    }
    /// Retained operations include earlier batches, and survive settlement.
    async fn operations(&self, id: RunId) -> Result<Vec<Operation>> {
        self.call(move |db| async move {
            decoded(db.query(&db.sql.operations, &[&id.to_string()]).await?)
        })
        .await
    }
    /// Claim a fresh execution generation. The host ensures old tasks have been joined first.
    async fn claim(&self, id: RunId) -> Result<RunState> {
        let state = self
            .call(move |db| async move {
                let mut state = load(&db, id).await?;
                if state.is_terminal() {
                    return Err(Error::Stale);
                }
                let old = CheckpointPosition::from(&state);
                state.generation = store::advance(state.generation, "generation")?;
                state.revision = store::advance(state.revision, "revision")?;
                let changed = db
                    .execute(
                        &db.sql.claim,
                        &[
                            &id.to_string(),
                            &int(old.generation)?,
                            &int(old.revision)?,
                            &int(state.generation)?,
                            &int(state.revision)?,
                            &sql_time(state.wake_at()),
                            &state.needs_recovery(),
                        ],
                    )
                    .await?;
                if changed == 0 {
                    return Err(Error::Stale);
                }
                db.count(1, 0);
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
        self.call(move |db| async move {
            let mut state = load(&db, id).await?;
            let fence = CheckpointPosition::from(&state);
            let durable = state.history.len();
            let stored: HashMap<RunId, (u64, usize)> = (state.children.iter())
                .map(|child| {
                    (
                        child.state.id,
                        (child.state.revision, child.state.history.len()),
                    )
                })
                .collect();
            let effect = update.apply(&mut state)?;
            if !effect.changed {
                return Ok(());
            }
            let revision = store::advance(state.revision, "revision")?;
            let mut delta = Delta::encode(&state, revision, durable, effect.wake_now)?;
            if !state.children.is_empty() {
                delta.children = Children::encode(&state, |id| stored.get(&id).copied())?;
            }
            match write_delta(&db, &delta, fence, true, effect.clear_error).await? {
                Written::Done => Ok(()),
                Written::NeedHistoryFrom(_) => {
                    Err(Error::storage("run history changed during update"))
                }
                Written::NeedChildren => Err(Error::storage("run children changed during update")),
            }
        })
        .await
    }
    async fn due(&self, now: u64, limit: usize) -> Result<Vec<RunId>> {
        self.call(move |db| async move {
            let (now, limit) = (sql_time(Some(now)), limit.min(4096) as i64);
            ids(db.query(&db.sql.due, &[&now, &limit]).await?)
        })
        .await
    }
    async fn position(&self, id: RunId) -> Result<CheckpointPosition> {
        self.call(move |db| async move {
            let row = db
                .query_opt(&db.sql.position, &[&id.to_string()])
                .await?
                .ok_or(Error::NotFound)?;
            Ok(CheckpointPosition {
                generation: uint(get(&row, 0)?)?,
                revision: uint(get(&row, 1)?)?,
            })
        })
        .await
    }
    /// One startup pass also discovers abandoned dispatched work behind an input wait.
    /// Discovery reads the `recover` column written with each checkpoint, never its contents.
    async fn recovery_page(&self, after: Option<RunId>, limit: usize) -> Result<Vec<RunId>> {
        self.call(move |db| async move {
            let after = after.map(|id| id.to_string()).unwrap_or_default();
            let limit = limit.min(4096) as i64;
            ids(db.query(&db.sql.recovery, &[&after, &limit]).await?)
        })
        .await
    }
    /// Suppress automatic retry only if this is still the failed worker's checkpoint.
    async fn record_error(
        &self,
        id: RunId,
        expected: CheckpointPosition,
        message: String,
    ) -> Result<bool> {
        self.call(move |db| async move {
            let changed = db
                .execute(
                    &db.sql.record_error,
                    &[
                        &id.to_string(),
                        &int(expected.generation)?,
                        &int(expected.revision)?,
                        &message,
                    ],
                )
                .await?;
            if changed > 0 {
                db.count(1, 0);
            }
            Ok(changed > 0)
        })
        .await
    }
    async fn last_error(&self, id: RunId) -> Result<Option<String>> {
        self.call(move |db| async move {
            let row = db
                .query_opt(&db.sql.last_error, &[&id.to_string()])
                .await?
                .ok_or(Error::NotFound)?;
            get(&row, 0)
        })
        .await
    }
}
#[async_trait]
impl Checkpoint for PostgresStore {
    /// One statement, one round trip: the run's progress, its new history and its changed
    /// operations, encoded here from a borrow, and for each child whose revision advanced its
    /// progress, new history and operations (with its acceptance when it was just admitted).
    /// The stored acceptance is never compared or rewritten.
    async fn commit(&self, state: &mut RunState) -> Result<()> {
        let fence = CheckpointPosition::from(&*state);
        let revision = store::advance(state.revision, "revision")?;
        let mut from = self.hinted(state);
        let mut resent_children = false;
        loop {
            let mut delta = Delta::encode(state, revision, from, false)?;
            if !state.children.is_empty() {
                delta.children = self.encode_children(state)?;
            }
            let written = self
                .call(move |db| async move { write_delta(&db, &delta, fence, false, false).await })
                .await;
            let failed = match written {
                Ok(Written::Done) => break,
                // The hint ran ahead of durable history; resend from the stored length.
                Ok(Written::NeedHistoryFrom(stored)) if stored < from => {
                    from = stored;
                    continue;
                }
                // A child's hint was wrong; forget them all and resend every child whole, once.
                Ok(Written::NeedChildren) if !resent_children => {
                    resent_children = true;
                    self.hint_children(state, false);
                    continue;
                }
                Ok(Written::NeedHistoryFrom(_)) => {
                    Error::storage("checkpoint history changed during commit")
                }
                Ok(Written::NeedChildren) => {
                    Error::storage("checkpoint children changed during commit")
                }
                Err(error) => error,
            };
            self.hint(state.id, None);
            if !state.children.is_empty() {
                self.hint_children(state, false);
            }
            return Err(failed);
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
impl Drop for PostgresStore {
    fn drop(&mut self) {
        // Closing the queue lets the worker drain, release the schema and end its session.
        if let Ok(sender) = self.sender.get_mut() {
            sender.take();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Limits, RunControl, ScriptedModel, Settlement, Subagent, Tool, ToolCall, ToolOutput,
        ToolSpec, typed_tool,
    };
    use serde_json::json;

    /// Bytes of JSON one four-round run sends from claim through settlement (with 2% headroom
    /// for wall-clock digits in `active_ms`). Prior context is sent once, at acceptance.
    const PAYLOAD_BYTES_FOUR_TOOL_ROUNDS: u64 = 19_655;
    /// Commits (exact) from claim to settlement of a parent that delegates once to a child that
    /// answers at once: SQLite's 10 transactions in `tests/cost_budget.rs` less the claim.
    const COMMITS_DELEGATE_ONCE: u64 = 9;
    /// Of those, commits that change the child and so send `WRITE_CHILDREN` (exact): its
    /// admission, its model intent, and its settlement with the parent's result.
    const CHILD_COMMITS_DELEGATE_ONCE: u64 = 3;
    /// Commits (exact) each tool round of the child adds, every one of them the child's: the
    /// six of a run's own tool round.
    const COMMITS_PER_CHILD_TOOL_ROUND: u64 = 6;
    /// JSON bytes (with 2% headroom) the delegating parent sends from claim to settlement when
    /// its child answers at once, the child's acceptance included, sent once.
    const PAYLOAD_BYTES_DELEGATE_ONCE: u64 = 9_998;
    /// JSON bytes (with 2% headroom) each tool round of the child adds, the same for its first
    /// round and its eighth: a commit sends the child it changed, never every child whole.
    const PAYLOAD_BYTES_PER_CHILD_TOOL_ROUND: u64 = 12_839;

    /// What the commits of one drive would send.
    #[derive(Debug, Default)]
    struct Sent {
        commits: u64,
        bytes: u64,
        /// The run's own history entries.
        entries: u64,
        /// Commits that sent children, so `WRITE_CHILDREN` rather than `WRITE`.
        child_commits: u64,
        /// Children sent, summed over commits.
        children: u64,
        acceptances: u64,
        child_entries: u64,
    }
    /// Encodes each commit's delta exactly as `PostgresStore::commit` does, with the hints a
    /// claim leaves and each successful commit updates, and counts what it would send. No
    /// server is involved.
    struct Encoder {
        hint: Mutex<usize>,
        child_hints: Mutex<HashMap<RunId, (u64, usize)>>,
        sent: Mutex<Sent>,
    }
    impl Encoder {
        fn claimed(state: &RunState) -> Self {
            assert!(state.children.is_empty());
            Self {
                hint: Mutex::new(state.history.len()),
                child_hints: Mutex::new(HashMap::new()),
                sent: Mutex::new(Sent::default()),
            }
        }
        fn sent(self) -> Sent {
            self.sent.into_inner().unwrap()
        }
    }
    #[async_trait]
    impl Checkpoint for Encoder {
        async fn commit(&self, state: &mut RunState) -> Result<()> {
            let mut hint = self.hint.lock().unwrap();
            let mut child_hints = self.child_hints.lock().unwrap();
            let from = (*hint).min(state.history.len());
            let mut delta = Delta::encode(state, state.revision + 1, from, false)?;
            if state.children.is_empty() {
                // A run without children encodes its progress exactly as before children.
                let progress: Value = serde_json::from_str(&delta.progress).unwrap();
                let keys: Vec<_> = progress.as_object().unwrap().keys().cloned().collect();
                assert_eq!(
                    keys,
                    [
                        "cancel_requested",
                        "operations",
                        "pending_response",
                        "phase",
                        "usage"
                    ]
                );
            } else {
                delta.children = Children::encode(state, |id| child_hints.get(&id).copied())?;
            }
            let mut sent = self.sent.lock().unwrap();
            sent.commits += 1;
            sent.bytes += delta.bytes();
            sent.entries += delta.history.len() as u64;
            let children = &delta.children;
            if !children.is_empty() {
                sent.child_commits += 1;
                sent.children += children.ids.len() as u64;
                sent.acceptances += children.bindings.iter().flatten().count() as u64;
                sent.child_entries += children.entries.len() as u64;
            }
            *hint = state.history.len();
            for child in &state.children {
                let child = &child.state;
                if state.is_terminal() {
                    child_hints.remove(&child.id);
                } else {
                    child_hints.insert(child.id, (child.revision, child.history.len()));
                }
            }
            state.revision += 1;
            Ok(())
        }
    }
    fn responses(rounds: usize) -> Vec<ModelResponse> {
        let mut responses: Vec<_> = (0..rounds)
            .map(|round| {
                ModelResponse::tools(vec![ToolCall {
                    id: format!("round-{round}"),
                    name: "echo".into(),
                    arguments: json!({"n": round}),
                }])
            })
            .collect();
        responses.push(ModelResponse::final_output(json!("done")));
        responses
    }
    fn echo() -> Arc<dyn Tool> {
        typed_tool::<Value, Value, _, _>(
            ToolSpec::new("echo", "1").with_description("Echo the arguments."),
            |args, _| async move { Ok(ToolOutput::Complete(args)) },
        )
    }
    fn agent(rounds: usize) -> Agent {
        Agent::builder("cost", Arc::new(ScriptedModel::new(responses(rounds))))
            .instructions("Answer with the scripted result.")
            .tools(vec![echo()])
            .build()
            .unwrap()
    }
    async fn sent(prior: usize) -> (u64, u64, u64) {
        let agent = agent(4);
        let context: Vec<_> = (0..prior)
            .map(|n| {
                HistoryEntry::Input(json!({"retained": format!("{n:04}{}", "x".repeat(1024))}))
            })
            .collect();
        let mut state = agent.new_run("c", json!("go"), &context, None).unwrap();
        assert_eq!(state.prior_context.len(), prior);
        let encoder = Encoder::claimed(&state);
        agent
            .drive(&mut state, &encoder, RunControl::default())
            .await
            .unwrap();
        assert_eq!(
            state.phase,
            Phase::Settled(Settlement::Completed(json!("done")))
        );
        let sent = encoder.sent();
        // Every entry after the accepted input is sent exactly once, by the plain statement.
        assert_eq!(sent.entries, state.history.len() as u64 - 1);
        assert_eq!(sent.child_commits, 0);
        (sent.bytes, sent.entries, sent.commits)
    }
    /// Drives a parent that delegates once to a child doing `rounds` rounds of one tool, as
    /// `tests/cost_budget.rs` does for SQLite.
    async fn delegated(rounds: usize) -> Sent {
        let child = Agent::builder(
            "cost-child",
            Arc::new(ScriptedModel::new(responses(rounds))),
        )
        .instructions("Answer with the scripted result.")
        .tools(vec![echo()])
        .limits(
            Limits::default()
                .with_max_model_calls(16)
                .with_max_tool_calls(16)
                .with_max_tokens(10_000)
                .with_active_budget(Duration::from_secs(120)),
        )
        .build()
        .unwrap();
        let parent = Agent::builder(
            "cost-parent",
            Arc::new(ScriptedModel::new(vec![
                ModelResponse::tools(vec![ToolCall {
                    id: "delegate".into(),
                    name: "delegate".into(),
                    arguments: json!("go"),
                }]),
                ModelResponse::final_output(json!("done")),
            ])),
        )
        .instructions("Delegate, then answer.")
        .subagents(1, [Subagent::new("delegate", "Do the task.", child)])
        .build()
        .unwrap();
        let mut state = parent.new_run("c", json!("go"), &[], None).unwrap();
        let encoder = Encoder::claimed(&state);
        parent
            .drive(&mut state, &encoder, RunControl::default())
            .await
            .unwrap();
        assert_eq!(
            state.phase,
            Phase::Settled(Settlement::Completed(json!("done")))
        );
        let child = &state.children[0].state;
        assert_eq!(child.usage.tool_calls as usize, rounds);
        let sent = encoder.sent();
        // The child's acceptance is sent once, and each of its history entries once.
        assert_eq!(sent.acceptances, 1);
        assert_eq!(sent.child_entries, child.history.len() as u64);
        // Every commit that sent children sent the one child it changed.
        assert_eq!(sent.children, sent.child_commits);
        sent
    }

    #[tokio::test]
    async fn commits_send_progress_and_new_history_only() {
        let (empty, entries, commits) = sent(0).await;
        eprintln!("postgres_payload_bytes_four_tool_rounds: {empty} over {commits} commits");
        assert_eq!(commits, 26);
        assert_eq!(entries, 5);
        assert!(
            empty <= PAYLOAD_BYTES_FOUR_TOOL_ROUNDS + PAYLOAD_BYTES_FOUR_TOOL_ROUNDS.div_ceil(50),
            "payload bytes exceed budget plus 2%"
        );
        // Sixty-four KiB of retained context add nothing after acceptance.
        let (retained, _, _) = sent(64).await;
        assert!(retained.abs_diff(empty) < 64, "{retained} vs {empty}");
    }

    #[tokio::test]
    async fn commits_send_only_the_children_they_changed() {
        let headroom = |budget: u64| budget + budget.div_ceil(50);
        let mut bytes = Vec::new();
        for rounds in [0, 1, 4, 8] {
            let sent = delegated(rounds).await;
            eprintln!("postgres_delegation_child_rounds_{rounds}: {sent:?}");
            let added = rounds as u64 * COMMITS_PER_CHILD_TOOL_ROUND;
            assert_eq!(
                sent.commits,
                COMMITS_DELEGATE_ONCE + added,
                "{rounds} rounds"
            );
            assert_eq!(
                sent.child_commits,
                CHILD_COMMITS_DELEGATE_ONCE + added,
                "{rounds} rounds"
            );
            bytes.push(sent.bytes);
        }
        eprintln!(
            "postgres_payload_bytes_delegate_once: {} (budget {PAYLOAD_BYTES_DELEGATE_ONCE})",
            bytes[0]
        );
        assert!(bytes[0] <= headroom(PAYLOAD_BYTES_DELEGATE_ONCE));
        // Every child round costs about the same, however many came before it.
        for (from, to, (before, after)) in [
            (0, 1, (bytes[0], bytes[1])),
            (1, 4, (bytes[1], bytes[2])),
            (4, 8, (bytes[2], bytes[3])),
        ] {
            let per_round = (after - before) / (to - from);
            eprintln!(
                "postgres_payload_bytes_per_child_tool_round ({from} to {to}): {per_round} \
                 (budget {PAYLOAD_BYTES_PER_CHILD_TOOL_ROUND})"
            );
            assert!(per_round <= headroom(PAYLOAD_BYTES_PER_CHILD_TOOL_ROUND));
        }
    }

    #[test]
    fn schema_names_are_plain_identifiers() {
        for valid in ["a", "workflows", "tenant_7", &"x".repeat(63)] {
            assert!(valid_schema(valid), "{valid}");
        }
        for invalid in [
            "",
            "pg_catalog",
            "information_schema",
            "MixedCase",
            "7up",
            "_x",
            "x-y",
            "x\"; DROP SCHEMA public; --",
            &"x".repeat(64),
        ] {
            assert!(!valid_schema(invalid), "{invalid}");
        }
    }
}
