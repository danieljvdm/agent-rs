# PostgreSQL resident host

Enable `postgres` explicitly. It is independent of `sqlite` and `openai`; the default features
and the engine-only build are unchanged.

```toml
[dependencies]
yielded-agent = { path = "../agent-rs", default-features = false, features = ["postgres"] }
```

The first consumer is the Rust personal-workflow service: durable acceptance and
approval/input/deadline waits must survive restarting on a different machine. `PostgresStore`
is one more `DurableStore` adapter beside `MemoryStore` and `SqliteStore`, so `Host` runs over
it unchanged. The application supplies its connection settings and TLS connector:

```rust,ignore
use std::sync::Arc;
use yielded_agent::{Host, HostOptions, postgres::{PostgresOptions, PostgresStore}};

// `config`: an explicit tokio_postgres::Config; `tls`: the application's verified connector.
// The schema belongs to this adapter and is created on first connect.
let store = Arc::new(
    PostgresStore::connect(&config, "personal_workflows", tls, PostgresOptions::default()).await?,
);
let mut host = Host::start(store.clone(), agent, HostOptions::default())?;
let id = host.submit("conversation-1", "client-request-1", input, None).await?;
let status = host.status(id).await?;
// The same idempotency key safely recovers an acceptance reply lost in transit.
host.shutdown().await?;
store.close().await?;
```

`connect` and `close` are the store's own methods, as for SQLite, and `metrics()` reports
deterministic work counts (`postgres::StoreMetrics`). `postgres::Config` and `postgres::NoTls`
re-export the `tokio-postgres` types the constructor takes; a TLS connector must implement
`tokio_postgres::tls::MakeTlsConnect` from the same `tokio-postgres` version.

## Storage layout

The layout mirrors SQLite schema v2, split by mutability so each write costs what it changes:

| Table | Holds | Written |
| --- | --- | --- |
| `runs` | Immutable acceptance: identity, idempotency key, binding, input, limits, expiry, bounded prior context, and an acceptance sequence | Once, at acceptance |
| `checkpoints` | One small row per run: generation, revision, progress (phase, usage, cancellation, pending response, current operations), history length, `settled`, and the discovery columns `wake_at`, `recover`, `last_error` | Every claim, commit and update |
| `history` | Each entry once, keyed by run and ordinal | Appended |
| `operations` | Every operation the run recorded, across batches | Upserted only when changed |

JSON is stored as text, byte for byte, so accepted JSON that PostgreSQL's JSON types reject
(an escaped U+0000) round-trips and cannot poison discovery. Identifiers use the `"C"`
collation, so ordering is bytewise as in SQLite. A conversation's history is ordered by its
runs' acceptance sequence, then ordinal: a conversation has one unsettled run at a time and a
settled run appends no history. `store_metadata` records layout version 3; any other version
(including this PR's pre-release versions 1 and 2), and a schema holding relations the adapter
did not create, is refused with `Error::Incompatible`. There is no migration.

## How a write works

Every write is a single statement, so it is one implicit transaction and one network round
trip. Statements are prepared once per session.

- **Commit** (`Checkpoint::commit`) encodes a delta from a borrow of the caller's state:
  progress, the history entries after this store's per-run history-length hint, and the current
  operations. One `WITH` statement runs a fenced
  `UPDATE checkpoints … WHERE run_id=$1 AND generation=$2 AND revision=$3 AND NOT settled AND
  history_len BETWEEN $from AND $len`, and appends history and upserts operations **only from
  that update's returned row**, so a refused fence writes nothing. Its row counts decide the
  result. On a refusal, one more read classifies it as `Stale`, `Input` ("history is
  append-only"), or a hint that ran ahead of durable history, in which case the commit resends
  from the stored length. Entries already stored (no hint, for example after a restart) are
  skipped by `ON CONFLICT DO NOTHING`, so the hint is purely an optimisation.
- **Claim** reads the run (one query reassembles acceptance, checkpoint and history), advances
  generation and revision, and recomputes `wake_at` / `recover` with
  `RunState::wake_at` / `RunState::needs_recovery`, in a fenced update: two round trips.
- **Cancel, resolve and retry** go through `DurableStore::update`: load, `RunUpdate::apply`,
  then the commit statement with an update fence that also admits settled runs, so uncertain
  outcomes stay resolvable after settlement.
- **Acceptance** looks up the key and whether the conversation is busy in one query, reads the
  bounded prior context newest-first in pages of 64 rows, and inserts the run, its checkpoint,
  first history and operations in one statement: three round trips for a short conversation.
- **Discovery** (`due`, `recovery_page`) reads only the indexed `wake_at` and `recover` columns.

The read before a claim or update is safe without an explicit transaction because only the
owning session writes the schema, every job runs serially on it, and the write is fenced anyway.

## Ownership

`connect` takes a PostgreSQL **session advisory lock** for the schema on the store's dedicated
session, before reading or creating tables; another owner gets `Error::StoreBusy`. Different
schemas are independent. `DurableStore::lease` then admits one resident host through a
`HostSlot`, and only while the session is alive: a store whose session failed or that was
closed refuses with `Error::Storage`, caused by the failure.

All store operations run serially on that session through a 64-slot queue. Connecting,
initialising, each job once it starts, and closing each have `PostgresOptions::operation_timeout`
(30 seconds by default); waiting in the queue is backpressure, not part of the timeout. Losing
the session or a timeout fails the store permanently: it **never reconnects**. Its host stops on
the next failed discovery or commit and joins its operations. Open a new store, and a new host,
after shutting the old one down. A dropped caller does not cancel or roll back a queued job.

`close` drains queued jobs in order, releases the advisory lock, and ends the session, so a
successor can connect at once. After a lost session, PostgreSQL releases the lock when it ends
the old backend; a network partition can delay that until server or TCP failure detection.

This slice targets **PostgreSQL 18** and a resident Tokio process. It has no distributed
scheduler, expiring or renewable per-run lease, connection pool, reconnect, replicas, SQLite
migration or multi-primary support. Use a direct connection or session pooling; transaction
pooling cannot hold the ownership lock. The application owns credentials, TLS, schema
privileges, backups, server durability and failover. Only this adapter may write its schema; do
not release its lock by hand. The adapter sets `synchronous_commit=on` and requires a UTF8
server encoding. A promotion that loses acknowledged transactions breaks the contract.

No external effect receives an exactly-once guarantee: an old effect can finish after its
session is lost. Never-retry effects become `Unknown`; read-only and idempotent effects keep the
existing replay rules and original keys. See the
[ownership protocol](deployment-targets.md#postgresql-ownership-protocol).

## Not supported yet

`PostgresStore` implements the store contract, `DurableStore` and `Checkpoint`, and none of the
optional store extensions. Each is a separate trait, so what is missing fails to compile rather
than failing at run time:

- **Steering.** It does not implement [`SteeringStore`](steering.md), so `Host<PostgresStore>`
  has no `steer` or `steering` method. Use `SqliteStore` or `MemoryStore` to steer active runs.
- **Automations.** It does not implement `AutomationStore`, the store extension automations run
  on.
- **Memory sources.** It does not implement the [memory](memory.md) `SourceStore`. A
  `MemoryPolicy` takes its sources from any `SourceStore`, such as `SqliteStore` or
  `InMemorySourceStore`, independently of the run store.

Nothing in the host or engine requires an extension: runs, waits and recovery work on
`PostgresStore` alone.

## Reproduce behaviour

With PostgreSQL binaries (`initdb`, `pg_ctl`) on `PATH`:

```sh
scripts/test-postgres.sh
```

This creates a temporary database on a private Unix socket and removes it on exit. It runs the
shared durability contract (`tests/storage_conformance.rs`) against every store, the
PostgreSQL-only tests (`tests/postgres.rs`), and the PostgreSQL workflow example.
Alternatively set `YIELDED_TEST_POSTGRES_URL` to an **isolated scratch database**; each test
and example creates its own schema and leaves it for inspection. Never point the tests at
personal or production data.

Tests that need a server are `#[ignore]`d in ordinary `cargo test`; the
[PostgreSQL CI job](../.github/workflows/postgres.yml) runs them with `--include-ignored`
against a PostgreSQL 18 service, where a missing database is an error. The PostgreSQL-specific
tests terminate the owner backend, block a statement past its timeout, drop an acceptance
caller, inject a failure into a commit's history append, commit without a history hint, and
reject other layout and state versions and foreign schemas.

Both `durable_workflow` and `postgres_workflow` run [one workflow](../examples/support/durable_workflow.rs):
approval → input → deadline → lost mutation result → explicit reconciliation, closing and
reopening storage at each boundary.

## Cost guards

Each count is deterministic; none is a timing.

| Guard | Where | Runs |
| --- | --- | --- |
| JSON bytes one four-round run sends from claim to settlement, and that 64 KiB of retained prior context adds none of them; each history entry sent exactly once | `postgres::tests::commits_send_progress_and_new_history_only` | Every `cargo test --all-features`, no server |
| Transactions per run equal SQLite's (3, 9, 27, 12 for the `tests/cost_budget.rs` workloads); round trips are 2 per claim and 1 per commit; 3 per new acceptance, 1 per replay, 1 per status | `tests/postgres.rs` budget table | PostgreSQL CI |
| Transactions, rows, round trips and JSON bytes from claim to settlement do not change with 128 KiB of retained context | `tests/postgres.rs` | PostgreSQL CI |

The earlier version of this adapter rewrote the whole run state on every commit, inside an
explicit transaction of several statements. Its CI timing samples were informational only and
are not carried forward; timings over a loopback service container are not an estimate of a
remote database. Every round trip costs network latency, which is why a commit is now one.
