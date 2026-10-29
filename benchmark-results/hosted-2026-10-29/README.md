# Full hosted performance evidence — 2026-10-29

Both full v2 jobs completed successfully: [matched comparison](https://github.com/danieljvdm/agent-rs/actions/runs/40953367110) and [same-source calibration](https://github.com/danieljvdm/agent-rs/actions/runs/40953797286). Raw artifacts are retained in this directory, including unsuccessful earlier attempts.

## Revisions and checks

- Comparison base: `7a4b5e0cd2c896ed951b41f2929f4a822c9a36a5`, the original `6b67138` runtime with only the v2 benchmark fixture applied.
- Comparison candidate and both calibration sides: `83b187ca9423f429fd46990d693aba23fd60768b`.
- The runtime files under `src/` are identical between revisions. The production dependency difference is the async-trait update used to fix Clippy compatibility. This is validation evidence, not an optimization claim.
- Both jobs used Rust/Cargo 1.99.1, four Tokio workers, and the `agent-loop-v2` fixture on separate Ubuntu hosted VMs reporting AMD EPYC 7763 and two logical CPUs. Each job built both sides before its sequential `base, candidate, candidate, base` captures.
- All eight captures are complete and clean. Each contains 19 Criterion workloads with 30 samples and 23 diagnostic cases with 30 CPU/latency and 30 separately instrumented allocation samples. Total: 5,520 CPU/latency samples and 5,520 allocation samples.
- Source/fixture hashes, compiler/worker settings, expected model/tool work counts, 64 per-run latency samples for every conversation-batch sample, and capture completeness were checked. [Independent calibration verification](calibration-verification.json) records its detailed checks.

## Matched comparison

Across all 19 workloads, candidate medians range from -2.1% to +1.9% relative to the base. The complete [comparison table](comparison-v2/COMPARISON.md) and [machine-readable deltas](comparison-v2/comparison.json) retain both cohorts and their variation. Representative elapsed times follow; these are medians of cohort medians, not per-operation tail latencies.

| Workload | Base µs/run | Candidate µs/run | Change |
| --- | ---: | ---: | ---: |
| Minimal | 13.825 | 13.812 | -0.1% |
| Four tool rounds | 58.818 | 58.921 | +0.2% |
| Streaming, 1,024 chunks | 74.310 | 73.185 | -1.5% |
| Durable four rounds | 18546.618 | 18151.045 | -2.1% |
| SQLite, 64 active | 12330.832 | 12112.173 | -1.8% |
| Recovery, 512 prior runs | 62018.329 | 63215.384 | +1.9% |

CPU, allocation, first-output and per-operation latency evidence remains in each capture’s `DIAGNOSTICS.md`, `diagnostics.json`, `metrics.json`, and `allocations.json`. For example, both comparison sides recorded 1,177 allocations and 133,636 allocated bytes per four-tool-round run in both cohorts. Instrumented allocation timings are not used as headline results.

## Same-source noise and gate decision

The [calibration](calibration-v2/COMPARISON.md) compares identical source. Its paired changes are observed noise, not improvements or regressions. These ranges are descriptive, not confidence intervals.

| Workload | Paired change range | Largest absolute paired delta |
| --- | ---: | ---: |
| Minimal | -1.06% to +0.63% | 0.148 µs |
| Four tool rounds | -0.92% to -0.12% | 0.547 µs |
| Streaming, 1,024 chunks | -0.27% to +14.65% | 11.007 µs |
| Durable four rounds | +4.91% to +13.64% | 2236.131 µs |
| SQLite, 64 active | -0.87% to +6.27% | 694.937 µs |
| Recovery, 512 prior runs | -0.09% to +4.74% | 2739.914 µs |

Timing gates remain disabled. One independent same-source job cannot establish cross-runner tolerances, and observed noise differs substantially by workload. The comparison and calibration ran on separate VMs, so their absolute times are not a paired test of a code change. Repeat full calibration on independent allocations before choosing workload-specific relative and absolute thresholds; reproduce suspected regressions in a fresh matched comparison. Correctness and exact work counts are immediate hard gates.

## Complete attempt accounting

The [attempt ledger](attempts.json) separates workflow revisions from the actual benchmark refs. Two early jobs failed checkout because an abbreviated SHA was supplied; no timing data was collected. Two queued calibration jobs were cancelled before measurement while moving to v2 and then independent VM scheduling. One running v1 comparison was cancelled as superseded by the corrected v2 observation fixture; its partial raw artifact is retained under `cancelled-v1/` and its workflow log under `attempts/`. The partial capture still says `running` because cancellation interrupted finalization; it is not a completed or passing comparison. Neither incomplete results nor v1 latencies enter the v2 summaries.

All failed/cancelled attempts are retained in the ledger; the final two jobs both passed.
