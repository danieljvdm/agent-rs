# Runtime benchmarks

Run `cargo bench --bench agent_loop` for the uninstrumented Criterion suite. It uses four Tokio workers, no network, deterministic scripts and tools, and no artificial sleeps. Tools make one cooperative yield so bounded concurrency is observable. The public runtime APIs validate final output, exact model/tool/cleanup counts, complete batches and declaration-order history on every iteration. Skipping work fails the benchmark.

The suite covers a minimal final response; four tool rounds; batches of eight tools with bounds 1/4/8; identical 1,024-byte streaming text split into 1/64/1,024 chunks; 0/1,024 prior history entries; fixed batches of 64 conversations with 1/16/64 active runs; file-backed SQLite execution through the background host, also in a conversation retaining 0/1,024 KiB of earlier input (`agent-loop-v3`); and restart-to-completion from a checkpoint after the first recorded tool round, with 0/512 older runs. Recovery must complete with exactly four new model calls and three new tool calls. Stored results cannot be silently replayed.

Fixture preparation, schema construction, initial history, checkpoint template creation, and report serialization are outside the measured intervals. In-memory intervals start at typed admission and state creation, and include driving the run through validated settlement and cleanup. Raw input and starting-history preparation remain outside the timer. Durable intervals include public submission, commit, scheduling, execution, and status read after the settlement event. Recovery intervals additionally include opening SQLite and acquiring ownership. SQLite startup and Tokio startup are reported separately in diagnostics. Each SQLite sample starts with a fresh resident host/database and one fixed warm-up run completed outside the timer. The timed work is one additional run or one fixed batch of 64 runs, so adaptive iteration counts cannot change the starting database size. Host/database shutdown is outside the timer; run-owned cleanup completes inside it.

## Capturing artifacts

Capture a complete release artifact (new output directory required):

```sh
python3 scripts/benchmark.py --out benchmark-results/initial
```

Compare two checkouts on this runner, prepared before measurements, in repeated AB/BA order:

```sh
python3 scripts/benchmark.py --base /absolute/base-checkout --candidate . --out benchmark-results/comparison
```

`benchmark-results/` is ignored by Git. Keep artifacts you want to cite outside the repository, for example as workflow artifacts or under a dedicated tag, and record the conclusions in [benchmark results](benchmark-results.md). The hosted [comparison and calibration workflow](benchmark-calibration.md) uploads its artifacts instead of committing them.

For a new repository with no previous implementation, use `--base . --candidate .` to retain initial baselines and measure variation. Identical-source comparisons suppress improvement percentages. `--quick --samples 5` is an exploratory smoke baseline, not a calibrated regression gate. Default runs retain 30 Criterion samples and 30 diagnostic samples per case. Do not run builds, tests, or another benchmark concurrently.

## Artifact contents

Each artifact retains source-file SHA-256 fingerprints including untracked files, revision when available, lockfile, toolchain, release flags, machine, worker counts, raw Criterion samples and estimate intervals, command logs, CPU/latency diagnostics, and separately instrumented allocations. A changed source during build/capture fails. Existing artifacts are never overwritten. Failures keep their logs and incomplete metadata.

`metrics.json` records process CPU across all worker threads, per-operation latencies for sustained conversation batches, first public text-delta observation separately from completion, persisted event counts, and SQLite transaction/write counts. CPU sampling uses the process CPU clock around each workload; its clock-read overhead is included rather than subtracted. Batch CPU/allocations/elapsed time can be normalized by `completed_runs`; individual latency samples include queueing from their own submission. In `agent-loop-v2`, SQLite batch completion is observed concurrently with ongoing submission and timed through each public status-read completion. The original v1 fixture delayed observation until all submissions finished; its per-operation latency includes that artificial delay. Compare only identical fixture versions and bytes. Streaming observation uses a separate Tokio task and a bounded channel, so it measures observer-visible output timing. Latency measurement and correctness instrumentation are part of this fixture and are held constant across revisions.

`allocations.json` is collected in a separate release executable with `allocation-metrics`; it counts successful allocations/reallocations and requested bytes across all threads only during the operation interval. Those instrumented timings are diagnostic and must not replace Criterion timings. They do not claim retained/peak heap size. Ordinary tracing calls run with no subscriber in both modes.

## Gates

Timing thresholds remain informational until repeated matched runs establish variance; see [calibrating a future timing gate](benchmark-calibration.md#calibrating-a-future-timing-gate). Correctness, cleanup and exact work counts are immediate gates. This suite measures runtime execution with scripted dependencies; it does not estimate live provider latency or subtract provider/tool overhead.

`tests/cost_budget.rs` holds the deterministic cost gates that run with `cargo test`: SQLite transactions per run, full copies of retained context written per durable run, virtual elapsed time for tool batches under paused Tokio time, and per-thread allocation counts for fixed engine workloads. A change that lowers a cost updates its constant in that file's budget table.
