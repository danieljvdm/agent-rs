# Optional deployment targets

The first consumer is the resident Rust personal-workflow service demonstrated by
`durable_workflow`: accept a request, wait for approval/input/time, and retain the
obligation when the process moves to another machine. The first implementation
slice is a PostgreSQL checkpoint store behind the existing bounded resident host.
SQLite remains the default local store. This does not expand the
Rust-only runtime into a distributed one.

| Target | Consumer and supported behavior | Independent slice |
| --- | --- | --- |
| PostgreSQL/shared storage | The Rust workflow service can restart on another machine using the same database, with one active owner per schema. | Optional `postgres` adapter, the same scheduler and engine, shared durability conformance, and a restart example. |
| Distributed workers | A future multi-tenant service needs several machines processing independent conversations. | Not implemented. Add transactionally claimed, expiring per-run leases, renewal, and owner epochs before enabling multiple schedulers in one namespace. |
| Node/Bun | A TypeScript application needs to host the same typed workflows and await durable acceptance and cleanup. | Not implemented. A separate binding package should first expose one native resident host and JSON-compatible typed inputs/outputs; callbacks and streaming can follow separately. |
| Cloudflare Workers/Durable Objects | An event-driven reminder service needs to sleep without a resident process and recover on requests or alarms. | Not implemented. A separate adapter should first support one conversation per SQLite-backed Durable Object, durable input waits and alarms, then provider/tool integration. |

## PostgreSQL ownership protocol

These constraints are the design boundary for the first slice:

1. Opening a store acquires a PostgreSQL **session advisory lock** for its schema
   on a dedicated connection before creating or reading its tables. A competing
   owner fails with `Error::StoreBusy`; separate schemas are independent. All store
   operations run serially on that same connection. No pool or reconnect can
   silently replace it. `DurableStore::lease` admits one host through a
   `HostSlot`, and only while that session is alive.
2. Acceptance commits the input, immutable binding, deduplication key, initial
   checkpoint and history together. Claims advance the generation; checkpoint
   commits compare generation and revision in a conditional `UPDATE` whose
   affected-row count decides between success and `Stale`. Cancellation,
   resolutions and retry are one `RunUpdate` and advance the revision. Progress,
   appended history, operation evidence, settlement and the discovery columns
   share one statement, so one transaction.
3. The session is the ownership lease; it has **no application TTL**. Connection
   loss or an operation timeout permanently closes that store. The host stops
   when discovery/checkpoint work fails and signals and joins owned operations.
   Reopening requires a new store and host. A stale connection cannot write after
   PostgreSQL ends its session. Generation/revision checks fence stale work within
   a live session. Only an orderly `close` releases the lock explicitly, after
   draining every queued job.
4. PostgreSQL releases the lock when its session ends. A network partition can
   delay that release until server/TCP failure detection; there is no promised
   failover time. Old external requests can still complete after ownership loss.
   Recovery consequently retains `Never` operations as `Unknown`, retries only
   declared `ReadOnly`/`Idempotent` work, and retains stable effect keys and charged
   budgets. Database fencing cannot fence an external service by itself.
5. Use one authoritative writable primary with durable acknowledged commits and
   `synchronous_commit=on`. Transaction-pooling proxies, writes outside this
   adapter, manually releasing its lock, split-brain primaries, and failover that
   loses acknowledged commits are unsupported. The application configures TLS,
   credentials, database permissions and backups. Schemas belong to this adapter;
   it does not migrate SQLite files.
6. Shut down the host, then await `PostgresStore::close`. It drains accepted
   database jobs, releases the lock and joins its connection task. Dropping the
   store initiates closure but cannot be an asynchronous cleanup barrier.

Session locks remain held through transaction rollback and are released on
session termination ([PostgreSQL advisory-lock documentation](https://www.postgresql.org/docs/18/explicit-locking.html#ADVISORY-LOCKS)).
This is shared durable storage with a single owner, not distributed worker
coordination.

## Before adding distributed workers

A run lease must contain an owner ID, monotonically increasing epoch and expiry
based on database time. Claim, renew and every progress/terminal/error mutation
must atomically verify owner, epoch and a live lease. A late renewal must not
revive an expired epoch. Admission still serializes by conversation and key;
notification delivery remains an optimization over indexed durable discovery.

On renewal failure, stop admission/dispatch, signal local operations, and join
cleanup. A new owner may recover only after the old lease expires. Old effects
can outlive that expiry: keep uncertain ordinary mutations for reconciliation,
and reuse original idempotency keys on safe replay. Commit outcomes may be
ambiguous after disconnect; reload authoritative state instead of retrying a
mutation blindly. Tests must race claims/renewals/cancellation and lose replies
around each transaction boundary. These are prerequisites, not guarantees of
the first slice.

## Node/Bun boundary contract to implement

Use JSON-compatible values with generated TypeScript types and runtime schema
validation on both sides. Run IDs and operation IDs are strings; represent any
64-bit counters/deadlines outside JavaScript's safe integer range as decimal
strings. Reject unsupported values instead of lossy coercion. Acceptance returns
a `Promise<RunId>` only after commit; a retried request uses its original key.
Expose errors as a stable discriminated code plus message, preserving conflict,
not-found, stale, incompatible, interrupted and storage distinctions. Rust panics
must not unwind into JavaScript.

Separate aborting an observation/request from durable `cancel(runId)`: dropping
a Promise must not cancel accepted work. Persist cancellation before signalling
handlers. A model/tool callback receives an `AbortSignal` and the original effect
key; its result remains subject to revision fencing. Define `close(): Promise<void>`
as a stop-admission, signal, join-and-cleanup barrier; reject new calls after
closure begins. Garbage collection is best-effort shutdown, never proof of
cleanup. JavaScript exceptions become typed callback failures. A callback that
ignores cancellation cannot hold cleanup indefinitely or permit a late result
to bypass fencing. Native-library loading, Node-API/Bun compatibility and any
platform prebuilds need their own runtime matrix before claiming support.

## Cloudflare boundary contract to implement

Map conversation identity to a Durable Object ID. Persist acceptance, checkpoint,
history, operation evidence and the earliest next wakeup transactionally in that
object's storage. Use short event-driven execution segments; do not serialize
futures, keep a resident polling loop, or assume memory survives eviction. Input
and approval replies address the exact run/call ID. On each event, reload state,
claim a generation and obey the same replay and cumulative-budget rules. Gate
overlapping handlers around claims and commits without holding a storage
transaction across a network call. External effects still require idempotency
or reconciliation.

A Durable Object has one alarm. Recompute it from all pending deadlines and use
at-least-once alarm delivery only as a wakeup hint. Automatic retries are bounded
(six with exponential backoff); persist/re-arm unfinished work and expose stalled
work instead of treating that retry loop as a durable queue ([alarm contract](https://developers.cloudflare.com/durable-objects/api/alarms/)).

As of 2026-10-29, SQLite-backed objects have a 10 GB per-object storage limit,
2 MB maximum row/value size, and a default 30-second CPU limit configurable up to
five minutes. Alarm handlers have a 15-minute wall-time limit; plan/account quotas
also apply ([Durable Object limits](https://developers.cloudflare.com/durable-objects/platform/limits/)).
The existing Rust checkpoint can exceed that row limit: a delivered adapter must
enforce smaller input/context/output caps or atomically chunk its storage format.
CPU limits differ from the engine's elapsed active budget; both must be bounded.
The Tokio, filesystem-lock and rusqlite host is not a Workers binary. A Wasm/event
bridge and storage adapter need explicit compatibility and deployed evidence.

Every future slice needs the shared durability scenarios, its own real runtime
end-to-end example, interruption/cleanup tests and deterministic cost guards.
Listing a target here does not mark it implemented.
