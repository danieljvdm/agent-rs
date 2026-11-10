# Runtime performance comparison and calibration

The scripted benchmark is the correctness and runtime-cost loop. The live personal-agent evaluation is a separate measure of provider behavior; it does not replace these deterministic workloads.

The [completed hosted v2 comparison and calibration](benchmark-results.md#full-hosted-comparison-and-calibration) summarize full-window results; their raw samples and complete attempt ledger are [retained at the `evidence-2026-10-29` tag](https://github.com/danieljvdm/agent-rs/tree/evidence-2026-10-29/benchmark-results/hosted-2026-10-29).

The [initial local capture](benchmark-results.md#initial-local-capture) used short exploratory windows. It retained substantial same-source variation, so it does not justify a timing gate. The hosted workflow adds full-window measurements, paired cohort variability, explicit work-count checks, and a repeatable runner.

The current fixture is `agent-loop-v4`. Since `agent-loop-v2` it observes SQLite settlements and reads each run's public status concurrently with later admissions, recording the timestamp immediately when that status read completes. Version 1 drained settlements only after submitting the entire batch, so its SQLite per-operation latency includes artificial observer delay. Retained v1 elapsed/throughput measurements remain useful within matched v1 comparisons, but its per-operation latency is not a precise settlement distribution. Version 2 changes observation work during execution; do not compare timings across fixture versions. Both sides of a comparison must contain identical fixture bytes, including when benchmarking an older runtime revision.

The workflow's default baseline is `7a4b5e0cd2c896ed951b41f2929f4a822c9a36a5` on `benchmark-baseline-v2`: the original `6b67138` runtime with only `benches/support/mod.rs` updated to v2. Its runtime source and dependency lockfile are unchanged. This keeps the default comparison compatible with the current fixture.

## Full comparison on a single runner

Run the **Runtime performance** workflow manually. Supply full 40-character base and candidate commit hashes when producing evidence for a change. Its `compare` mode checks out both revisions, reads the candidate's pinned Rust version, and builds both revisions before measurement. All captures then run sequentially on one `ubuntu-24.04` runner with four Tokio workers and the same compiler. Two cohorts mean `base, candidate, candidate, base`; higher cohort counts must be even.

The default uses 30 Criterion samples with one-second warmup and two-second target measurement windows, plus 30 CPU and allocation samples per diagnostic case. SQLite can require longer than its target window to complete the required samples. Allocation instrumentation runs in a separate executable. The job uploads raw samples, per-capture summaries, comparison JSON, metadata and logs even after a failure.

The hosted runner is one reproducible runner class, not a claim of exclusive physical hardware. Other tenants and storage variability can still affect it. The workflow serializes jobs within each mode. A comparison and calibration can run concurrently on [separate hosted VMs](https://docs.github.com/en/actions/how-tos/write-workflows/choose-where-workflows-run/choose-the-runner-for-a-job), while each job keeps its own captures sequential. Do not compare timings across those VMs as if they measured a code change. Do not run unrelated builds or tests within the measurement job; there are no laptop-wide CPU locks or idle-machine waits.

The same controller works locally with two prepared clean checkouts and an output directory outside them:

```sh
RUSTUP_TOOLCHAIN=1.99.1 PATH="$HOME/.cargo/bin:$PATH" \
python3 scripts/benchmark.py \
  --base /absolute/base-checkout --candidate /absolute/candidate-checkout \
  --out /tmp/agent-rs-full-comparison --cohorts 2 --samples 30 --require-clean
```

Both revisions retain their own lockfiles. Fixture bytes, runtime settings and compiler identity must match. The script refuses mismatched fixtures, environments, incomplete captures, missing cases, or changed model/tool work counts. It records binary hashes and all source hashes, including untracked source in local exploratory mode. `--require-clean` rejects dirty Git checkouts.

## Reading the evidence

`COMPARISON.md` reports median elapsed time per completed run and the range of changes from every paired cohort. `comparison.json` retains those individual relative and absolute deltas, base/candidate ranges, and median absolute deviations. A paired range is descriptive, not a confidence interval or significance test. A very fast sample cannot erase a slow cohort. Each capture also retains normalized Criterion iteration ranges and separate CPU, allocation, first-output and per-operation latency data.

Same-source detection compares runtime, manifest/lockfile and benchmark fixture sources, excluding this reporting controller and unrelated examples. It suppresses improvement percentages when the benchmarked source is identical; binary hashes remain available to distinguish changed build artifacts.

## Calibrating a future timing gate

Choose workflow mode `calibrate` to run the same candidate commit on both sides. Its report includes the maximum observed absolute delta and relative delta for each workload. These values describe the noise observed in that job; they are not automatically accepted regression thresholds.

Run full same-source calibration jobs on separate runner allocations before proposing a gate. At least three independent jobs give an initial view of cross-runner variability, but more may be necessary for noisy SQLite or concurrency workloads. Compare only matching fixture/toolchain/runner configurations. Retain every failed, slow or inconclusive job; do not select only stable runs.

A future workload-specific gate should require a slowdown to exceed **both** a relative tolerance and an absolute time tolerance, with tolerances chosen above observed repeated-job noise. Reproduce a suspected breach in a fresh matched comparison before treating it as a regression. If the repeated measurements cannot distinguish the expected regression size from noise, leave that workload's timing gate disabled and investigate with CPU/allocation evidence.

The controller deliberately keeps `timing_gate_enabled: false`. Correct completed output, model/tool counts, ordering, retained history and run-owned cleanup remain immediate hard gates. Enabling a timing gate is a reviewed policy change backed by independent calibration artifacts, not a side effect of running one benchmark.
