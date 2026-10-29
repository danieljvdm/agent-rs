# YIE-245 verification

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

[Retained workflow output](evidence/workflow.txt) comes from `examples/durable_workflow.rs`. It uses a real temporary SQLite file and closes/reopens storage at each boundary:

1. Accept input durably and persist an approval wait.
2. Reopen, approve, and persist an explicit input request.
3. Reopen, reply, and persist a wall-clock deadline.
4. Reopen after the deadline; confirm waiting did not consume the active budget.
5. Execute a simulated external mutation, then inject failure before recording its result.
6. Reopen and expose `Unknown`; the effect counter remains one.
7. Reconcile the factual result and complete with five model calls and four tool calls.

This proves the local recovery protocol for the scripted workflow. It is not an external exactly-once claim. The example's injected commit loss models a process dying between effect and outcome commit; the tests also inspect the persisted dispatch boundary directly.

## Benchmark evidence

See [the benchmark guide](../benchmark-results/README.md) for measurement boundaries, raw output format, and the repeated AB/BA comparison command. The initial capture compares the same source on the same machine because no earlier Rust implementation exists. These short exploratory windows establish a retained starting point and reveal variation; they are not timing regression thresholds.

The [completed comparison](../benchmark-results/initial-2026-10-29/COMPARISON.md) contains four successful captures with one matching source fingerprint, 19 workloads per capture, and 115 CPU/latency plus 115 allocation samples per capture. The script completed with exit status zero, and the saved statuses, workload counts, diagnostic sample counts, and source fingerprints were independently checked. Normal timing and allocation instrumentation ran separately; this session ran no builds or tests concurrently with measurement. The final source also passed Clippy and formatting checks after capture.

The OpenAI adapter was tested against a local HTTP server, without a live provider request. Live model quality, provider latency/billing, deployment filesystem guarantees, and a product-specific personal-agent workflow remain application validation work. No remote repository or deployment was created.
