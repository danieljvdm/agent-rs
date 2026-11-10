# Benchmark results

This page records the conclusions drawn from benchmark runs. Raw artifacts (Criterion samples, per-capture metrics and allocation reports, logs, audits, and DHAT profiles) are not tracked on `main`. Everything captured up to 2026-10-29 is preserved at the [`evidence-2026-10-29` tag](https://github.com/danieljvdm/agent-rs/tree/evidence-2026-10-29/benchmark-results), including the original full write-ups this page condenses. See the [benchmark guide](benchmarks.md) for how to capture new artifacts.

Timing gates remain disabled throughout. Correctness, cleanup, and exact model/tool work counts are the hard gates.

## Recovery allocation optimization

Commit `5cd69a7` decodes canonical checkpoints directly into `RunState` instead of building an intermediate JSON `Value` tree. It verifies the version and an exact serialization round trip, and falls back to the unchanged compatibility decoder for any other representation. Checkpoint bytes, transactions, fencing, and recovery work are unchanged. Base `82839973a01b86db8e57bb700b47184760401472` adds only profiling and the focused benchmark suite; candidate `5cd69a71c09d3987532904c3162899c5023a838b` adds the decoder and its compatibility tests.

**Allocation profile** (DHAT 0.3.3, one diagnostic capture per side, Apple M4 Max, resuming one run after 512 completed runs with 1,024 retained context entries):

| Measure | Base | Candidate | Change |
| --- | ---: | ---: | ---: |
| Cumulative allocated bytes | 109,324,972 | 60,688,426 | -44.5% |
| Peak tracked bytes | 4,894,205 | 3,346,371 | -31.6% |
| Allocation count | 381,197 | 181,538 | -52.4% |

Checkpoint decoding fell from 76.1 MB to 27.5 MB of that traffic; `RunState` cloning, checkpoint encoding, and other engine work were unchanged. The zero-history control fell 25.4% in cumulative bytes. These are allocation measurements, not timings, and exclude SQLite's direct C allocations and whole-process RSS. The [profiling guide](recovery-profiling.md) describes the measurement window and reproduction.

**Matched timing** in two independent hosted jobs ([first](https://github.com/danieljvdm/agent-rs/actions/runs/40961976724), [repeat](https://github.com/danieljvdm/agent-rs/actions/runs/40963163553)), each with four sequential AB/BA captures on an AMD EPYC 9V45 runner. Values are medians of cohort medians, in ms per run:

| Workload | First job | Change | Repeat job | Change |
| --- | ---: | ---: | ---: | ---: |
| Minimal in-memory control | 0.009472 → 0.009224 | -2.6% | 0.010037 → 0.010040 | +0.0% |
| Recovery, 0 prior runs | 10.033 → 9.475 | -5.6% | 10.548 → 9.786 | -7.2% |
| Recovery, 512 prior runs | 39.673 → 28.284 | **-28.7%** | 43.476 → 30.236 | **-30.5%** |
| Ordinary durable execution | 12.923 → 12.644 | -2.2% | 13.590 → 12.982 | -4.5% |

Long-history CPU time fell 36.5% and 37.9% in the two jobs. The first job showed candidate-only durable samples of roughly 100–121 ms (7 of 60 above 50 ms). The repeat, with 100 normal and 100 allocation samples per case per capture, did not reproduce them: no candidate durable sample exceeded 25 ms, while one base sample reached 62.9 ms. No trace established the cause of the first job's stalls, so rare stalls are not ruled out. No samples were discarded.

The [full recovery report](https://github.com/danieljvdm/agent-rs/tree/evidence-2026-10-29/benchmark-results/recovery-2026-10-29/README.md) retains the attribution table, tail analysis, audits, raw profiles, both comparisons, and the 60-second process soak.

## Full hosted comparison and calibration

Both full `agent-loop-v2` jobs passed: a [matched comparison](https://github.com/danieljvdm/agent-rs/actions/runs/40953367110) of the original runtime (`7a4b5e0`, with only the v2 fixture applied) against `83b187c`, and a [same-source calibration](https://github.com/danieljvdm/agent-rs/actions/runs/40953797286) of `83b187c` against itself. The runtime files under `src/` are identical between the compared revisions, so this is validation of the harness, not an optimization claim. Each job ran on its own Ubuntu VM (AMD EPYC 7763, two logical CPUs) with Rust 1.99.1 and four Tokio workers. All eight captures are complete: 19 Criterion workloads each, and 5,520 CPU/latency plus 5,520 allocation samples in total.

| Workload | Base µs/run | Candidate µs/run | Change | Same-source paired range |
| --- | ---: | ---: | ---: | ---: |
| Minimal | 13.825 | 13.812 | -0.1% | -1.06% to +0.63% |
| Four tool rounds | 58.818 | 58.921 | +0.2% | -0.92% to -0.12% |
| Streaming, 1,024 chunks | 74.310 | 73.185 | -1.5% | -0.27% to +14.65% |
| Durable four rounds | 18546.618 | 18151.045 | -2.1% | +4.91% to +13.64% |
| SQLite, 64 active | 12330.832 | 12112.173 | -1.8% | -0.87% to +6.27% |
| Recovery, 512 prior runs | 62018.329 | 63215.384 | +1.9% | -0.09% to +4.74% |

Across all 19 workloads, candidate medians range from -2.1% to +1.9% of base. The calibration column is observed noise between identical builds on a different VM; it is descriptive, not a confidence interval, and it varies substantially by workload. One same-source job cannot establish cross-runner tolerances, so timing gates stay disabled until repeated independent calibration supports workload-specific thresholds.

Earlier attempts (two checkout failures, two cancelled calibrations, and one superseded v1 comparison) collected no usable v2 data. The [full hosted report](https://github.com/danieljvdm/agent-rs/tree/evidence-2026-10-29/benchmark-results/hosted-2026-10-29/README.md) retains the attempt ledger, calibration verification, and every capture.

## Initial local capture

The first capture at `4388ae4` compared identical source in four AB/BA captures on one local machine, using the quick exploratory profile (10 Criterion samples and 5 diagnostic samples per case per capture): all 19 workloads, 460 CPU/latency samples, and 460 allocation samples. Its large same-source swings in some workloads ruled out a timing gate or speedup claim and motivated the hosted runner. The [comparison table and raw captures](https://github.com/danieljvdm/agent-rs/tree/evidence-2026-10-29/benchmark-results/initial-2026-10-29) are retained at the tag.
