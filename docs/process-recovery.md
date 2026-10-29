# Process termination and restart tests

`tests/process_recovery.rs` exercises recovery after actual process death on macOS and Linux. It uses the public engine, host, and SQLite APIs and runs without credentials or network access.

```sh
cargo test --locked --test process_recovery -- --nocapture
```

Each parent test launches the same compiled test executable as a separate process. The child commits the required boundary, atomically writes a readiness marker, and remains alive with SQLite open. The parent waits for that marker, calls `Child::kill`, waits for the process to exit, and asserts that its exit signal was `SIGKILL`. The next step launches a new process against the same database. The marker avoids relying on a guessed delay to hit the crash boundary; a missing marker or premature child exit fails with the child log after a bounded timeout. Unwinding the parent also kills and reaps any surviving child.

The default suite covers these cases:

| Boundary or workflow | Required observation after restart |
| --- | --- |
| Acceptance committed, execution not started | A competing process is denied database ownership while the child lives; after it dies, the same idempotency key returns the original RunId and the accepted work completes. |
| Model intent and active-time reservation committed | Restart does not replenish the model-attempt limit or active-time reservation. A one-attempt run stops before making another model call. |
| Ordinary tool intent committed, handler not entered | Recovery exposes `Unknown`; no effect occurs without application reconciliation. |
| External mutation completed, outcome not committed | Recovery exposes `Unknown`, preserves the effect, and does not invoke the handler again. |
| Idempotent mutation completed, outcome not committed | The handler can be invoked again with the identical retained key, while the independent effect service deduplicates it. |
| Tool outcome committed, batch not advanced | Recovery reuses the recorded result without executing the tool again. |
| Approval → input → timer | Each wait survives process termination; the final fresh host discovers the persisted UTC timer without an old notification. |
| A timer scheduled 24 hours ahead, six successive restarts | Deadline, pending operations, and usage remain unchanged; cancellation is then persisted and observed. |

The external-effect fixture is a separate, fsynced file ledger outside the runtime database. It records handler attempts separately from effects. The idempotent variant stores one receipt per key, modeling the explicit deduplication contract an application must obtain from its real external service. These tests establish recovery behavior; they do not claim that the runtime makes arbitrary external mutations exactly once.

For wait boundaries, the fixture first awaits `Engine::drive` returning so the checkpoint's active-time refund is complete, then kills the process while the store remains open. Recovery to completion or uncertainty uses the background `Host`. This avoids confusing an intermediate, already-visible wait checkpoint with the execution loop's quiescence boundary.

## Real-time restart soak

An opt-in soak keeps a real 24-hour deadline pending while repeatedly killing and relaunching its owner for a configurable real-time interval. It asserts that no model/tool work is repeated, no active budget is consumed while waiting, and the exact pending operation remains retained. It finishes by explicitly cancelling the run.

```sh
AGENT_RS_SOAK_SECONDS=60 cargo test --locked --test process_recovery \
  persisted_wait_process_restart_soak -- --ignored --exact --nocapture
```

The default is 60 seconds; supported values are 1–3,600 seconds. The 250 ms interval is a deliberate soak cadence; each kill still waits on the child's committed readiness marker. `process_child` is an ignored implementation entrypoint and should only be launched by its parent fixture, so use the exact soak name rather than running every ignored test.

A 24-hour persisted deadline is tested without waiting a full day. The real-time soak measures its configured duration only. These are process-failure tests, not power-loss or storage-device fault tests; the latter require validation on the intended deployment filesystem.

## Retained local verification

On 2026-10-29, the default process suite passed all five parent tests in 4.10 seconds, and its targeted Clippy check passed with warnings denied. The explicit 60-second soak passed after 179 SIGKILL/relaunch cycles in 60.190 seconds. The pending run's active-work budget stayed unchanged at 46 ms through the soak. The two ignored entries in the default suite are the subprocess implementation entrypoint and this opt-in real-time soak; neither is silently substituted for an ordinary passing test.
