# Durable steering of an active run

`Host::steer` durably queues a free-form user instruction for a run that is already active, without the agent having asked for input. The instruction takes effect at the run's next model dispatch, after the current model/tool exchange completes. It is available on a `Host<S>` whose store implements `SteeringStore`: `MemoryStore` and `SqliteStore` do, and so does `Host<dyn SteeringStore>`. The engine and stores that do not implement the trait are unchanged.

```rust
// Persist the key at the caller so retries use the same identity.
let receipt = host
    .steer(run_id, "title-change-1", "Change the reminder title to Book annual checkup.")
    .await?;

// A lost acknowledgement can be retried, including after the run settles.
let retry = host
    .steer(run_id, "title-change-1", "Change the reminder title to Book annual checkup.")
    .await?;
assert_eq!(receipt, retry);

// Ordered, authoritative status after reconnect; progress events are best effort.
let inputs = host.steering(run_id).await?;
let input = inputs.iter().find(|input| input.receipt == receipt).unwrap();
println!("{:?}", input.status);
```

Applications driving the engine directly call the same operations on the store (`SteeringStore::steer`, `SteeringStore::steering`). An instruction becomes a `HistoryEntry::Steering { id, instruction }`: user input, separate from the run's typed input and the agent's immutable instructions. Both OpenAI adapters, Chat Completions and Responses, send it the same way: a user message with the instruction as written, in its place in history.

## Acceptance and identity

Acceptance atomically records the instruction and its idempotency key, and the receipt is returned only after that commit. A receipt holds a unique `SteeringId`, the targeted `RunId`, and the instruction's `sequence` in the run's acceptance order, from 0; concurrent submissions are ordered by it. Reusing a key with different text is `Error::IdempotencyConflict`. Keys are scoped to the run, independently of `Host::submit` keys.

The exact run is always targeted. A new key for a settled, cancellation-requested or expired run is `Error::RunNotActive`, without creating a successor run. A matching retry returns the original receipt in every state. An unknown run is `Error::NotFound`. To start another run after settlement, submit one with a new request key.

Steering does not claim an execution generation, advance the revision, cancel the worker's token, or make a run due. There is still one execution owner and one active run per conversation, so an accepted instruction cannot fence a dispatched tool's valid outcome or make it replay.

## When instructions take effect

Queued instructions change a run only through the two commits that the `Checkpoint` contract allows a `SteeringStore` to change, each atomically with that commit and its fencing:

- **The model dispatch commit** (a commit in `Phase::ModelInFlight`) appends every queued instruction to the run's history, in acceptance order, and marks it applied at that revision. The engine assembles the turn's context after this commit, so the request includes them.
- **A completed settlement** with instructions queued commits `Ready` instead and hands the caller `Ready`. The completed exchange stays in history and the run goes on to another model turn, which applies them.

The engine checks every commit against this list, reports any other change as `Error::Storage`, and emits `ProgressKind::Settled` only for a commit that really settled.

| State at acceptance | Behavior |
| --- | --- |
| Ready, before dispatch commits | The next dispatch commit appends the queued instructions in acceptance order. |
| Model request in flight | The request continues with its original context. Its complete response is retained. A tool batch it proposes completes under its existing contracts before the instructions enter context. |
| Tool work in progress | Every call in the batch keeps its plan, stable key, approval and outcome record. Steering does not cancel or skip them. |
| Approval, input, deadline or reconciliation wait | The instruction stays queued. It does not answer the question, approve the operation, bypass the deadline or resolve uncertainty, and it does not make the run due. Waiting still releases the execution slot. |
| Successful completion racing acceptance | The two writes serialize. If steering commits first, the completed response is retained and the same run returns to `Ready`. If completion commits first, a new key is `RunNotActive`. |
| Failure, cancellation, expiry or exhausted budget | Settlement is final. Instructions still queued become `NotApplied`; they never replenish budgets or cause an implicit retry. A reopened completion that then hits a limit stops as usual. |

If steering changes a plan with an outstanding approval, the application can still deny that exact operation through the existing resolution API. Steering grants no permission. An uncertain dispatched mutation still needs reconciliation before the run continues, and read-only or idempotent recovery follows its original retry contract.

An accepted instruction may therefore wait indefinitely for an explicit resolution. Preempting model requests, replacing an outstanding tool batch, cancel-and-replan, and arbitrary input joining are not supported; each would need its own contract for the effect in flight.

## Inspection, history and context

`SteeringStatus::Queued` means acceptance committed and no dispatch commit has applied the instruction. `Applied { revision }` names the dispatch commit that appended it. It does not say the provider received the request or followed it: a process can stop right after the commit, and recovery repeats the model attempt with the same history, never appending the instruction twice. `NotApplied` means the run settled while it was queued. Limits, token totals, call counters and active-time reservations carry through all of these transitions.

A queued instruction is not history. It appears in `RunState::history` and the conversation's `DurableStore::history` when it is applied, at the position where the model first sees it: after the exchanges completed before that turn. Run history and conversation history therefore stay one sequence in one order, and an instruction accepted during a model request follows that request's response. Use `steering` to see queued and unapplied instructions.

Every steering entry of the run's own history is pinned in its context, in the default selection in `src/context.rs`, alongside the current input and the latest exchange; older exchanges can be dropped as whole groups. This holds with or without a `ContextPolicy`, so a policy (for example [memory](memory.md)'s `MemoryPolicy`) never needs to pin steering. If the pinned entries cannot fit, the turn fails with `Error::Context` rather than dropping an instruction. Admission keeps that from happening through steering alone: a run accepts at most `SteeringInput::MAX_PER_RUN` (128) instructions, applied ones included, and their combined encoded size plus the run's input must fit `max_context_bytes`. Empty instructions, oversized submissions and keys outside 1..=1024 bytes are `Error::Input` and record nothing; a matching retry stays valid at the limit. In later runs of the conversation, applied instructions are ordinary prior context and are not pinned.

Steering does not clear a recorded host error; repair and `retry` stay explicit.

## Storage

`MemoryStore` keeps each run's instructions beside the run and applies them under the store's lock. In SQLite, `src/sqlite/steering_queue.rs` adds a `steering` table (one row per instruction, with its key, size, and the revision that applied it) and a partial index of queued rows. Both are created with `IF NOT EXISTS` at every open, within schema version 2, so a version 2 database from before steering opens and gains them; no version number changes. Acceptance inserts one row and changes nothing else. The delta write's first statement, which already reads the checkpoint's `history_len`, also asks whether steering is queued, but only for a dispatch or completion commit; every other commit, and every `RunUpdate`, executes exactly what it did before. A dispatch commit that finds steering queued reads it, appends it to `history` at the run's next ordinals, after the delta's own entries, counts it in the checkpoint's `history_len`, marks it applied, and returns it so the store's history-length hint includes it. A completion that finds instructions queued is rolled back and resent as `Ready`; the queue cannot drain in between, because only this run's dispatch commit applies it. Both checks run in the commit's own transaction on the store's single worker, so they hold across restarts and against `steer` racing a commit. A binary from before this feature would open such a database and ignore its queue; keep one binary per database.

## Verification and cost

`tests/steering.rs` runs one contract against every `SteeringStore`: acceptance ordering and idempotency, active model and tool work, both completion-race orders, explicit waits, cancellation, cumulative budgets, context bounds, restart before or after application, and exactly what each commit may change. A lost dispatch acknowledgement proves application is not duplicated. Tests in `src/sqlite/steering_queue.rs` prove that acceptance and application roll back with their transactions, that applied instructions take the next ordinals whatever the history-length hint, and that a version 2 database from before steering gains the table. The [personal-agent eval](personal-workflow-eval.md) includes a model-driven title change with real synthetic reminder files and a negative grading case.

`tests/cost_budget.rs` pins the cost: one transaction to accept an instruction; no extra transaction to apply it (a run steered before dispatch costs the unsteered run's three plus the acceptance); six for a run steered during its model call, whose completion reopens; the record bytes one instruction adds; and the allocations of a turn with sixteen pinned instructions. Without steering every existing budget is unchanged, and a guard in `src/sqlite/steering_queue.rs` pins the SQL statements each commit of an unsteered run executes to main's: 4 for a progress-only commit, 4 for the model dispatch commit and 5 for a completed settlement that appends its exchange. What remains on a dispatch or completion commit is one `EXISTS` probe of the empty partial index, evaluated inside the statement main already runs.

`cargo bench --bench steering_schedule` compares delivering an instruction in the original input against queueing it during an active model request, for 64 runs with two model calls and one read-only tool each, on 1, 16 and 64 execution slots. It measures the cost of the new path, not a speedup: the two cases do equal work and produce equal output, every second request must see the instruction and the tool's receipt, and every queued instruction must be applied once. Setup, verification and shutdown are outside the measured interval. No timing conclusions are recorded; timings stay informational, as described in [benchmarks](benchmarks.md).
