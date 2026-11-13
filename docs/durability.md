# Execution and durability contract

The engine owns a serializable `RunState`, with phases `Ready`, `ModelInFlight`, `Tools`, and `Settled`. The durable host drives that same engine over any `DurableStore` (`src/store.rs`), which extends the engine's atomic `Checkpoint` boundary with acceptance, claiming, application updates, and discovery. `MemoryStore` is the in-process reference implementation; SQLite is an optional file-backed adapter and [PostgreSQL](postgres.md) an optional shared one. `tests/storage_conformance.rs` runs the same contract against each. Futures are never serialized.

[Attached subagents](attached-subagents.md) commit inside their parent's checkpoint and fence. A child's admission commits with its input, identity, allocation and the parent's dependency on it; its settlement commits with the parent's result for the delegation and the reconciliation of its budget. Children are never discovered or claimed on their own.

## Commit protocol

1. Acceptance atomically stores the immutable acceptance (input, binding, limits, expiry, and bounded prior context), initial history, idempotency mapping, and the run's first checkpoint. The caller receives a RunId only after commit. Idempotency keys are scoped to conversations; conflicting payloads are rejected, including changed binding or expiry. Only one run may be unsettled per conversation.
2. Before model work, the engine commits its phase, charged attempt count, and active-time reservation. A complete response is retained before tools can start. All calls in the response must pass argument, identity, and tool validation.
3. Before tool work, the engine commits the declared contract, stable external idempotency key, and dispatched intent. Parallel tools execute in bounded segments separated by sequential barriers. Each outcome commits as it arrives; the next model turn sees a complete batch in declaration order.
4. Every checkpoint transaction appends new history and updates operation evidence together with checkpoint advancement. Settlement and the run's discovery fields commit together. Operations survive settlement for inspection and reconciliation.

A commit persists only progress: phase, usage, cancellation, the pending response, the current operations, and history appended since the last commit. Acceptance data in the caller's state is never compared or rewritten; it is fixed when the run is accepted. History is append-only, so a commit that drops entries is rejected. In SQLite, acceptance is one immutable `runs` row, each commit rewrites one small `checkpoints` row and inserts only new `history` rows, and loading reassembles the full `RunState` from the three. Bytes written per commit therefore do not depend on retained context (`tests/cost_budget.rs` pins this).

A commit changes nothing in the caller's state beyond its revision, except in a store that accepts [steering](steering.md): there the model dispatch commit appends queued instructions to history, and a completed settlement with instructions queued commits `Ready` instead, keeping its exchange. `Checkpoint` documents both, and the engine rejects any other change. Acceptance of an instruction changes neither generation nor revision.

A failed checkpoint stops that execution. Callers must reload authoritative state before resuming. A failure while driving a run is classified once, by `Error::fails_run`: a failure of the run's own work (the model, its output, a tool contract, authorization, context) settles the run as `Failed` with the bounded error message, while `Storage`, `Stale`, `Incompatible` and `Interrupted` are returned with the run left at its last checkpoint. A checkpoint's error is always one of the latter: any other kind is reported as `Storage`, with the original as its cause. A committed state can outlive loss of its acknowledgement; acceptance retries are safe through the caller's key.

## Recovery and external effects

The host claims a new internal generation before driving a checkpoint. A commit succeeds only while the stored run is unsettled at exactly the caller's generation and revision; in SQLite this is a conditional `UPDATE … WHERE run_id=? AND generation=? AND revision=?` whose affected-row count decides between success and `Stale`. Cancellation, resolution, and retry are one `RunUpdate`, applied by the same core function in every store, and advance the revision, so late results cannot overwrite them. A late host error is recorded only at the failed worker's exact position.

Eligibility has one definition, `RunState::wake_at` (and `RunState::needs_recovery` for dispatched work hidden behind a sibling wait). Stores persist both at every write, and discovery queries those stored fields rather than the checkpoint's contents. A single process owns the database, and `DurableStore::lease` admits one resident host at a time; this is not a distributed lease protocol.

The SQLite adapter uses WAL with full synchronization and a process lock beside the canonical database path. Symlink aliases share that lock. Hard-linked database files are rejected because their distinct WAL paths are unsafe after a crash. Keep the database and sidecars together on a local filesystem, and do not remove the lock file while an owner is running.

The [PostgreSQL adapter](postgres.md) instead holds a session advisory lock for one schema on a dedicated session, and never reconnects after losing it. Its layout mirrors SQLite's, and each commit is one fenced statement. The [ownership protocol](deployment-targets.md#postgresql-ownership-protocol) defines failure detection, fencing and the limits of failover; it adds no distributed workers and does not change the external-effect rules below.

Recorded tool outcomes are reused. Incomplete model attempts may repeat, with prior attempt charges retained. For an interrupted tool:

| Original contract | Recovery |
| --- | --- |
| `Never` | Retain `Unknown`; wait for application reconciliation. |
| `ReadOnly` | May retry under the same tool contract. |
| `Idempotent { contract }` | May retry with the original stable idempotency key, under the exact same contract. |

`Idempotent` is an application assertion about its external system. The handler must actually pass `ToolContext::idempotency_key` to a system that deduplicates the effect. The runtime does not manufacture external exactly-once behavior. Even the gap after intent commit but before actual dispatch is conservatively uncertain for ordinary mutations.

Resolve unknown outcomes with the factual result discovered by the application. Do not invent success simply to unblock the run. Explicit cancellation cannot undo an effect, and outstanding uncertainty remains inspectable and resolvable after cancellation.

Tool-call IDs must be unique throughout a run. This makes a delayed approval or input reply unable to resolve a different later operation with the same ID.

## Waiting and time

Approval is persisted before dispatch. Tool handlers may return `WaitRequest::Input` or `WaitRequest::Until`. The host releases the execution slot while the run waits. Replies target a specific operation; due deadlines are UTC Unix milliseconds stored by the store. Startup and periodic indexed discovery resume eligible work without relying on notifications.

Each operation or parallel segment reserves its operation timeout plus cleanup allowance against the cumulative active budget **before external work**. On orderly completion it refunds unused time. An interrupted process keeps the entire reservation charged. This conservative policy prevents crashes from replenishing the budget. Awaiting approval, input, or a deadline consumes no active budget. Optional run expiry is a separate wall-clock deadline.

Model/tool attempts and token use accumulate across recovery. Attempts can be charged without external dispatch when authorization, a commit, or shutdown intervenes. Provider usage unavailable after a broken stream cannot be reconstructed; model attempts and the conservative active reservation still bound retry work. Live provider billing may include repeated requests.

## Ownership and shutdown

One host may serve several agents over one store (`Host::start_agents`, `Host::submit_to`). Runs record their agent ID at acceptance, and each claimed run executes with the registered agent of that ID; a run whose agent is not registered keeps a visible `Incompatible` error, like an incompatible binding, until a host with that agent retries it.

Use `Host::shutdown().await` and then the adapter's `close().await` (`SqliteStore::close`, `PostgresStore::close`). Shutdown stops admission, signals operations, and joins owned tasks; accepted work remains available to a later host. `Host::cancel` first persists explicit cancellation and then signals active work. Dropping the host signals shutdown but is not an asynchronous cleanup barrier.

Model/tool implementations must not detach child tasks. If they own asynchronous resources, implement `cleanup`, which the engine invokes after each attempt, including interruptions. Cleanup has an explicit timeout; Rust cannot force an uncooperative external operation to undo itself. Handlers must yield to Tokio, honor cancellation, and bound CPU work; substantial blocking work belongs on an application-owned bounded blocking executor. SQLite uses its own bounded worker.

## Compatibility and observation

Schema/state versions and immutable agent bindings are checked explicitly. A database or run written with another schema or state version is rejected with `Error::Incompatible`, and corrupt or foreign data with `Error::Storage`; there is no compatibility decoding. Instructions, model configuration, schemas, tool versions, retry contracts, and limits are retained. Change the agent/tool version when implementation semantics change; function bodies and credentials are not serialized. An incompatible binding leaves the accepted obligation retained with a visible host/store error until the application repairs the binding and retries it.

Status and history are authoritative after reconnect. Progress events and text deltas are bounded, transient, and may be dropped; each names its run, and none is persisted. A run carries bounded prior context, stored once at acceptance, and current-run evidence; recovery does not replay the conversation's entire lifetime. Context retains whole model/tool exchanges and the current input; an oversized newest group fails explicitly. An agent with a [context window](context-compaction.md) also appends `HistoryEntry::Compaction` entries: a compaction's summary call follows the model-call protocol above, and acceptance reads back no further than the latest compaction and the entries it kept.

## Deliberate differences from Yielded Agent

This implementation takes its behavioral reference from `yielded-dev/agent` at `549daa3c7aa65dfbaeded9dccc6dd342f1706bd7`, without TypeScript API or data-format compatibility. It uses one public RunId per run or child, one active run per conversation, ordinary tools and typed final output, straightforward budgets, and constructors/traits at real boundaries. It supports queued steering at model boundaries and [summarizing compaction](context-compaction.md), and omits in-flight preemption, input joining, multiple completion protocols, detached observation modes, ambient service graphs, distributed ownership, tool-result pruning and context rollover inside a run, nested or detached subagents, and MCP.
