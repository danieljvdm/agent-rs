# Persistent interval automation

An automation creates **new runs** on a fixed interval. A persisted `WaitRequest::Until` still resumes an existing run; it does not create a recurrence.

Automations are an extension of the storage contract. `AutomationStore` (implemented by `MemoryStore` and `sqlite::SqliteStore`) keeps automations beside the runs, and a `Host` over such a store gains `Host::fire_automations`. The scheduling rules and the firing protocol are defined once in the crate; each store supplies only the primitive reads and writes inside one atomic write. Nothing changes in the engine, and the host spawns no automation task: the application calls `fire_automations` from its own poll loop.

```rust
use std::{sync::Arc, time::Duration};
use serde_json::json;
use tokio_util::sync::CancellationToken;
use agent_rs::{sqlite::SqliteStore, *};

async fn serve(agent: Agent, stop: CancellationToken) -> Result<()> {
    let store = Arc::new(SqliteStore::open("personal.sqlite").await?);
    // Provision once, or retry with these exact persisted values. Do not use a fresh clock
    // reading as first_at_ms on every restart of an existing definition.
    let hourly = IntervalAutomation::new(
        "personal-inbox",
        json!("Review my task inbox"),
        1_793_404_800_000,
        3_600_000,
    );
    store.create_automation(&agent, "personal-inbox", hourly).await?;
    let mut host = Host::start(store.clone(), agent, HostOptions::default())?;
    let mut poll = tokio::time::interval(Duration::from_secs(1));
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let options = AutomationOptions::default();
    let result = loop {
        tokio::select! {
            _ = stop.cancelled() => break Ok(()),
            _ = poll.tick() => {
                if let Err(error) = host.fire_automations(options).await {
                    break Err(error);
                }
            }
        }
    };
    // Stop polling before shutting down the host; accepted work survives.
    let shutdown = host.shutdown().await;
    let close = store.close().await;
    shutdown?;
    close?;
    result
}
```

Applications own the poll cadence, error reporting, management authorization and process lifetime. `Host::fire_automations` reads the current UTC time and wakes the host for the runs it admits; the host would also discover them at its next poll, because an admitted run is an ordinary durable run returned by `DurableStore::due`. Repeating a call is safe. One call examines at most `max_per_fire` automations, so a large set of due schedules may need several polls. `AutomationStore::fire_due(agents, now_ms, options)` is the same operation with an explicit clock, for tests and demonstrations.

## Which agent fires

An automation records the ID and the binding of the agent it was created (or last updated) with. `Host::fire_automations` fires only automations whose recorded agent the host serves (see `Host::start_agents`), and runs each with that agent. An automation of an agent the host does not serve is left due and unchanged, as a run of an unregistered agent is left for a host that has it. If the registered agent's binding differs from the recorded one, the firing pauses the automation with `last_error` instead of admitting a run with another definition; an `AutomationChange::Update` adopts the new agent explicitly. The binding includes the model's adapter route ([`Model::binding`](model-adapters.md)), so moving an agent to another API or endpoint pauses its automations too, while rotating a credential does not.

## Admission and recovery contract

An automation persists its schedule (conversation, input, first time and fixed interval), its next due time, its agent ID and binding, a management revision and a schedule version. Creation validates the input against the agent's input type and context limit, exactly as run admission will. Repeating `create_automation` with the same ID, schedule and agent returns the current record; anything else, including a deleted ID, is `Error::IdempotencyConflict`. IDs are store-wide, 1–256 bytes.

The occurrence identity is `(automation ID, schedule version, scheduled UTC millisecond)`. A firing accepts its run exactly as `DurableStore::accept` does, with the idempotency key `automation/{id}/{version}/{scheduled}` in the automation's conversation, and records the occurrence and the next due time in the **same atomic write**. `SqliteStore` commits a whole `fire_due` call as one transaction, and a unique index also protects the identity. A crash before commit leaves everything unchanged; a committed firing remains authoritative even if its acknowledgement is lost, and retrying cannot create a second run for that occurrence. Reserve the `automation/` key prefix for automations when submitting runs yourself.

Occurrence inspection records the selected due time, the observed clock time, the number of earlier intervals coalesced, and either the admitted `RunId` or `SkippedOverlap`. Coalesced intervals allocate no rows. Inspection pages by a store-wide sequence number, which is not the identity.

Admitted runs are ordinary runs under the [durability contract](durability.md): one unsettled run per conversation, generation/revision fencing, immutable acceptance and binding, tool replay rules, cumulative **per-run** budgets, explicit waits, cancellation and cleanup. Each occurrence is a distinct run with its own budget. They are [steered](steering.md) like any other run: steering changes neither the automation nor its occurrences, and a run that queued steering keeps active still counts as unsettled for overlap and capacity. Automations do not make arbitrary external tool effects exactly once.

A firing of an agent with [attached subagents](attached-subagents.md) admits only the root run: its occurrence and `last_run_id` name the root, and the engine admits children later, inside the root's commits. A child is not a run of the store, so it is never an occurrence and never counts toward capacity. A child that waits keeps its root unsettled, so later intervals are skipped until `Host::resolve_child` (or `RunUpdate::ResolveChild`) lets the root finish. The children's definitions are part of the parent's binding, so changing a child pauses the parent's automations until an `Update` adopts the new agent.

## Policies in this slice

These policies are fixed rather than configurable modes:

| Condition | Behavior |
| --- | --- |
| Due or missed intervals | Admit only the latest due occurrence. With first time 100, interval 100, and a firing at 450, select 400, report three earlier intervals coalesced, and advance to 500. No catch-up burst is materialized; years of downtime cost constant arithmetic. |
| The automation's previous run is unsettled | Record `SkippedOverlap` and advance. Ready, executing, waiting for input/approval/time/reconciliation (its own or a child's), cancellation pending, and host-error runs all count, even after an update moved the automation to another conversation. |
| The conversation has another unsettled run | The same skip, including runs submitted directly rather than by an automation. |
| Capacity reached | Keep the cursor; create no run and no occurrence. A later call coalesces again. |
| A run completes, fails, is cancelled, or exhausts its budget | Keep its outcome; future intervals continue. A settled run is never retried and its occurrence never replayed. |
| A host error leaves a run unsettled | Keep skipping until the application retries or cancels the run (`DurableStore::last_error`, `Host::retry`, `Host::cancel`). Automations never reset a budget. |
| Storage failure during a call | `SqliteStore` rolls back the whole call. The application may retry; a lost successful acknowledgement is safe too. |
| Binding change, or an input the agent no longer admits | Pause that automation with `last_error`; other automations in the call proceed. `Resume` alone does not adopt a binding: the next firing pauses again until an `Update` adopts it. |
| Clock moves backward | Wait for the durable next due time; never replay earlier occurrences. |
| Clock jumps forward | The same coalescing policy. |
| The next interval exceeds the signed 64-bit millisecond range | Pause with an error before admitting that occurrence. Update the schedule before resuming. |

`max_in_flight` bounds **all unsettled automation runs in the store**, including those of paused and deleted automations, and not runs submitted directly. It counts root runs only: a delegating root counts once however many children it has admitted, because its children run inside its execution and spend its budget, and `AgentBuilder::subagents(max_parallel, …)` bounds them per root. Concurrent model work from automations is therefore at most `max_in_flight × (1 + max_parallel)`. Use the same options for every caller sharing a store; they are deployment limits, not persisted schedule policy. Both options must be 1–1024. At capacity there is at most one implicit pending interval per automation, represented by its cursor; there is no separate queue. Management can provision any number of trusted automations, so applications exposing this API should impose their own tenant quotas. Occurrences and run history are retained; compaction is outside this slice.

## Inspect and manage

```rust
use agent_rs::{sqlite::SqliteStore, *};

async fn manage(host: &Host<SqliteStore>, agent: Agent) -> Result<()> {
    let store = host.store();
    let current = store.automation("personal-inbox").await?;
    let paused = store
        .change_automation(&current.id, current.revision, AutomationChange::Pause)
        .await?;
    let longer = paused.schedule.clone().with_interval_ms(7_200_000);
    let update = AutomationChange::Update { agent, schedule: longer };
    let updated = store.change_automation(&paused.id, paused.revision, update).await?;
    let enabled = store
        .change_automation(&updated.id, updated.revision, AutomationChange::Resume)
        .await?;
    for occurrence in store.occurrences(&enabled.id, 0, 100).await? {
        println!("{} {:?}", occurrence.scheduled_at_ms, occurrence.outcome);
    }
    if let Some(id) = enabled.last_run_id {
        println!("{:?}", host.status(id).await?.phase);
        // Cancelling a run (host.cancel(id)) is separate from pausing or deleting.
    }
    store
        .change_automation(&enabled.id, enabled.revision, AutomationChange::Delete)
        .await?;
    Ok(())
}
```

- `automation`, the paginated `automations` and the paginated `occurrences` include tombstones and history. All methods are trusted application APIs; authenticate and authorize callers before exposing management over a network.
- Every change is fenced on the automation's revision: a stale revision is `Error::Stale`. Reload to reconcile a lost acknowledgement. Firings never advance the revision, except by pausing an automation.
- `Pause` stops future admission. `Resume` coalesces time spent paused into one firing. Neither changes an accepted run.
- `Update` keeps the pause state and the last run, clears the error, resets the cursor to the new first time and starts a new schedule version, so even an identical replacement starts a new occurrence namespace and a past first time may fire at the next poll. Changing the conversation does not bypass overlap with the previous run. Accepted runs keep their original binding; finish or repair them with their original agent before switching the host to a new definition.
- `Delete` leaves a tombstone: no later firing admits from it, its ID cannot be reused, and accepted work and history remain inspectable. If a firing commits first, its run continues; if the deletion commits first, the firing cannot admit.

## Storage

`SqliteStore` adds three tables, created idempotently at open beside the run tables. The schema version stays 2: the tables are additive, so a database opened by a build without automations is unaffected and one created before gains them when opened. As with runs, the tables are split by mutability so a firing writes only what changes:

- `automations`: the managed definition (agent ID, binding, schedule, state, revision, schedule version, error), rewritten only by management and automatic pauses.
- `automation_cursors`: one small row per automation (next due time, latest run, and the agent ID and enabled flag the due index needs), rewritten by each firing.
- `automation_occurrences`: one row per occurrence, unique by identity and by run.

One admitted firing writes five rows in its call's single transaction: the run's acceptance, checkpoint and input history entry (exactly what `accept` writes), its occurrence and its cursor. A skipped firing writes two. A call with nothing due, and a store with no automations, writes nothing, and runs cost exactly what they cost without automations: an ordinary run's commits execute the same statements in a database holding automations, and the automation tables coexist with the steering and memory tables whichever were created first. `tests/cost_budget.rs` pins these counts and the record bytes of 16 firings beyond accepting the same runs directly. Due discovery uses an index and a row limit per served agent; the capacity count scans unsettled checkpoints with an indexed occurrence lookup.

`MemoryStore` keeps automations under the same lock as its runs, so each firing is one atomic step; it suits tests and ephemeral hosts.

## Calendar and external-event follow-ups

Scheduling is broader than this interval implementation:

| Slice | Remaining contract/work |
| --- | --- |
| Calendar/cron with time zones | Persist an IANA zone and an explicit calendar policy. Test spring gaps and fall folds, zone-data changes, and schedule updates. Proposed default: skip nonexistent local times and choose the earlier instant for an ambiguous local time, once. This policy is **not implemented** here; calendar inputs are not accepted. |
| Event/webhook subscriptions | Persist subscription and provider event identities; authenticate the sender and authorize its subscription/tenant **before** admitting work. Validate signature/replay window, schema, body size and payload-to-input mapping; bound pending work and apply backpressure. Duplicate and restart delivery tests must cover provider identities. No public event endpoint or event admission API is exposed by this slice. |

Intervals measure elapsed UTC milliseconds. A 24-hour interval is not "the same local time every day"; daylight-saving changes do not alter its duration. There is no calendar or DST behavior hidden behind the interval fields.

## Runnable example and verification

`cargo run --locked --example recurring_personal_agent` uses an in-memory task inbox fixture, `ScriptedModel`, a typed read-only tool, and a real durable host over a temporary database. It admits an inbox review, closes and reopens the store before execution, retries the old wakeup, completes three distinct reviews, coalesces a downtime gap, and pauses the schedule. An explicit demonstration clock (`fire_due`) avoids waiting for real hourly intervals. It needs no credentials and writes to no external service.

`tests/automation.rs` runs the same consumer, and runs one contract against `MemoryStore` and `SqliteStore` alike: concurrent duplicate firings, restarts, backward clocks, busy conversations and waiting runs, pause/update/resume/delete, binding, model route and child definition changes, steering a fired run, delegating agents whose child waits, settled and failed runs, capacity, validation, and a host serving several agents. Scheduling rules are unit-tested as pure functions of a timestamp. A SQLite abort injected into the cursor update proves the run and occurrence roll back with it. The subprocess harness in `tests/process_recovery.rs` SIGKILLs and relaunches the owner after admission and after completion, verifying one occurrence, one run and one model invocation.
