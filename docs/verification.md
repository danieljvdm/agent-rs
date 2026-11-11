# Verification

The implementation was built from an empty repository. The initial source revision is `555b124`; the benchmark's additional seeded-context assertions are in `4388ae4`.

## Functional checks

The integrated `cargo test --all-features --locked --offline` run passed all 45 tests:

- 21 engine tests: complete-response validation, argument/identity validation, authorization after approval, bounded parallelism and sequential barriers, retained waits and context, pre-dispatch commit failure, usage preservation, expiry, cancellation, mutation uncertainty, cleanup, and settlement-event ordering.
- 14 file-backed durability tests: durable/idempotent acceptance, exclusive ownership, generation/revision fencing, interrupted mutations, compatible retries, partial batches, persisted waits, missing-notification discovery, repair retries, and stale error suppression.
- 5 provider parser tests and 4 loopback HTTP tests: fragmented SSE, complete JSON/tool calls, bounds, truncation, cancellation, history/schema conversion, and request token cap.
- 1 SQLite transaction fault-injection test: failed terminal checkpoint retirement rolls back settlement and related evidence together.

Also passed:

```sh
cargo clippy --all-targets --all-features --locked --offline -- -D warnings
cargo fmt --all --check
cargo check --no-default-features --locked --offline
cargo test --locked --offline --bench agent_loop -- --test
cargo run --offline --example durable_workflow
```

The benchmark correctness pass covers 19 workloads. Separate diagnostics cover 23 cases, including streaming observation and startup; allocation instrumentation has its own executable.

## Restart workflow

[Retained workflow output](https://github.com/danieljvdm/agent-rs/tree/evidence-2026-10-29/docs/evidence/workflow.txt) comes from `examples/durable_workflow.rs`. It uses a real temporary SQLite file and closes/reopens storage at each boundary:

1. Accept input durably and persist an approval wait.
2. Reopen, approve, and persist an explicit input request.
3. Reopen, reply, and persist a wall-clock deadline.
4. Reopen after the deadline; confirm waiting did not consume the active budget.
5. Execute a simulated external mutation, then inject failure before recording its result.
6. Reopen and expose `Unknown`; the effect counter remains one.
7. Reconcile the factual result and complete with five model calls and four tool calls.

This proves the local recovery protocol for the scripted workflow. It is not an external exactly-once claim. The example's injected commit loss models a process dying between effect and outcome commit; the tests also inspect the persisted dispatch boundary directly.

## Benchmark evidence

See [the benchmark guide](benchmarks.md) for measurement boundaries, raw output format, and the repeated AB/BA comparison command. The initial capture compares the same source on the same machine because no earlier Rust implementation exists. These short exploratory windows establish a retained starting point and reveal variation; they are not timing regression thresholds.

The [completed comparison](https://github.com/danieljvdm/agent-rs/tree/evidence-2026-10-29/benchmark-results/initial-2026-10-29/COMPARISON.md) contains four successful captures with one matching source fingerprint, 19 workloads per capture, and 115 CPU/latency plus 115 allocation samples per capture. The script completed with exit status zero, and the saved statuses, workload counts, diagnostic sample counts, and source fingerprints were independently checked. Normal timing and allocation instrumentation ran separately; this session ran no builds or tests concurrently with measurement. The final source also passed Clippy and formatting checks after capture.

This initial verification used a local HTTP server for the OpenAI adapter. The subsequent [personal workflow evaluation](personal-workflow-eval.md) and [process termination suite](process-recovery.md) extend that evidence with live-model behavior and real process death. The repository was subsequently published privately at `danieljvdm/agent-rs`; no production deployment has been created.

## Follow-up verification

The follow-up at `0b2098a` adds five subprocess recovery tests and three personal-workflow tests. The full Rust suite passes **53 tests**, with the subprocess helper and opt-in soak explicitly ignored by the default command. Clippy with warnings denied, rustfmt, and the engine-only build pass on pinned Rust 1.99.1. The benchmark reporter has three additional integrity tests. The CI failure under newer Clippy was fixed by upgrading to async-trait 0.1.92, which removes the problematic generated attribute; no lint was suppressed.

The separate [60-second soak log](https://github.com/danieljvdm/agent-rs/tree/evidence-2026-10-29/docs/evidence/process-soak.txt) records **179 SIGKILL/relaunch cycles**, preserving a 24-hour pending deadline and unchanged active-work budget. Default process tests also kill the owner at durable acceptance, model intent, tool intent, before outcome recording, and after a recorded outcome, then verify recovery and external-effect counts.

The [v2 live workflow results](personal-workflow-eval.md#recorded-results) passed all **nine attempts** against `gpt-4.1-2025-04-14`. It exercised real tool selection and file effects, missing-input waits, approval/denial, and two host restarts per attempt. It used 24,782 input tokens and 1,593 output tokens across 36 model calls. The negative deterministic cases reject wrong schedules, invented success, and a correct guess after a failed calendar read. The earlier three-case smoke run also passed and remains separately retained with its original grader source.

The subsequent `agent-loop-v2` fixture corrects delayed SQLite batch observation. Two regression tests verify observation continues while later admissions are blocked and that a real 64-run batch retains every completion and exact work count. These bring the Rust suite to **55 tests**; the reporter now has four integrity tests, including explicit rejection of v1 data as v2 evidence.

[Hosted CI at `83b187c`](https://github.com/danieljvdm/agent-rs/actions/runs/40953366853) passed the full 55-test Rust suite, all four reporter tests, the 19 benchmark correctness cases, Clippy with warnings denied, formatting, and the engine-only build on the pinned compiler.

The [full hosted v2 comparison and same-source calibration](benchmark-results.md#full-hosted-comparison-and-calibration) both passed. All eight captures contain the expected 19 timed workloads and complete CPU/allocation diagnostics, with matching fixtures and explicit source/toolchain identities. The comparison's median changes range from -2.1% to +1.9%; the separate calibration shows workload-dependent noise up to 14.65%. Timing gates remain informational pending independent repeated calibration. Failed setup and superseded/cancelled attempts remain in the evidence ledger.

## Recovery decoder optimization

[CI at `5cd69a7`](https://github.com/danieljvdm/agent-rs/actions/runs/40961977208) passed **59 Rust tests**, including four new checkpoint-format compatibility tests, all eight benchmark-controller integrity tests, and all 19 benchmark correctness cases. Formatting, Clippy with warnings denied, and the engine-only build also passed on Rust 1.99.1. The subprocess helper and opt-in soak remain the two tests ignored by the default command.

A separate [60-second soak](https://github.com/danieljvdm/agent-rs/tree/evidence-2026-10-29/benchmark-results/recovery-2026-10-29/process-soak.txt) completed **217 SIGKILL/relaunch cycles** with the pending deadline and active-work budget preserved. The three scripted personal-workflow tests passed; live-model evaluation was not rerun for this decoder change. The [recovery optimization results](benchmark-results.md#recovery-allocation-optimization) summarize the allocation profiles and matched timing evidence.

Two independent hosted comparisons verify eight complete captures with unchanged fixture bytes and work counts. Long-history recovery improves by 28.7% and 30.5% in elapsed time and 36.5% and 37.9% in CPU time. Both show 44.5% less cumulative allocation traffic. The first job's candidate-only durable latency spikes did not recur in the larger repeat (200 normal samples per side); the report retains both findings and the unresolved cause instead of discarding slow samples.
