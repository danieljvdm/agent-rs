# Recovery allocation optimization — 2026-10-29

Recovery after 512 prior runs improves by **28.7% and 30.5% in two independent matched hosted jobs**. A separate allocation-stack profile shows **44.5% less cumulative allocation traffic** and **31.6% lower peak tracked Rust allocations**. Both revisions perform the same work. Suspected durable-control tail regressions in the first job did not recur in the larger second job; all samples and that limitation remain below.

The checkpoint decoder now avoids building an intermediate JSON `Value` tree for the exact encoding produced by the current writer. It decodes directly into `RunState`, checks the version, and verifies an exact serialization round trip. Every other representation uses the unchanged compatibility decoder. Checkpoint bytes, transactions, fencing, and recovery work are unchanged.

Base: `82839973a01b86db8e57bb700b47184760401472`. Candidate: `5cd69a71c09d3987532904c3162899c5023a838b`. The base adds profiling and focused benchmark tooling to the previous runtime; the candidate adds the decoder change and compatibility tests. Both use identical production dependencies and benchmark fixtures.

## Allocation profiles

The long-history fixture resumes one interrupted run in a conversation with 512 completed runs and 1,024 retained context entries. Both revisions complete four new model calls, three new tool calls, and all corresponding cleanup. The zero-history fixture is a control with the same resumed work.

| Prior runs | Cumulative allocated bytes, base | Candidate | Change | Peak tracked bytes, base | Candidate | Change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 1,867,450 | 1,393,722 | -25.4% | 83,473 | 66,379 | -20.5% |
| 512 | 109,324,972 | 60,688,426 | -44.5% | 4,894,205 | 3,346,371 | -31.6% |

Long-history allocation count falls from 381,197 to 181,538 (-52.4%). Live tracked bytes at the end are unchanged: 17,427 for zero history and 785,427 for long history. These are single diagnostic captures with DHAT 0.3.3, Rust 1.99.1, release optimizations and debug information, on an Apple M4 Max with four Tokio workers. They are not timing measurements.

The [raw profiles](profiles/), [source/environment metadata](profiles/after-metadata.json), and [exact totals](profile-comparison.json) retain the evidence. Each allocation stack is attributed once, taking its first match in this order: checkpoint decoder, checkpoint writer, `RunState::clone`, other engine work, other work.

| Long-history allocation attribution | Base bytes | Candidate bytes |
| --- | ---: | ---: |
| Checkpoint decoding, including the new round-trip check | 76,118,437 | 27,481,893 |
| `RunState` cloning | 18,016,812 | 18,016,812 |
| Other engine work | 8,812,188 | 8,812,186 |
| Checkpoint encoding outside the decoder | 6,289,284 | 6,289,284 |
| Other work | 88,251 | 88,251 |

Peak/live values cover Rust allocations originating inside the SQLite open/claim/resume window. They exclude pre-existing allocations, runtime and fixture setup, SQLite's direct C allocations, and whole-process RSS. Cumulative allocation traffic is not retained heap size. See the [profiling guide](../../docs/recovery-profiling.md) for reproduction commands and measurement boundaries.

## Matched ordinary timing

The [focused hosted comparison](https://github.com/danieljvdm/agent-rs/actions/runs/40961976724) passed. It measures recovery with 0 and 512 prior runs, plus minimal in-memory and ordinary durable execution controls, using unchanged `agent-loop-v2` fixtures. All four captures ran sequentially in base/candidate/candidate/base order on one Ubuntu hosted VM reporting AMD EPYC 9V45 and two logical CPUs, with Rust/Cargo 1.99.1 and four Tokio workers. Both revisions built before measurement. This is a different machine from the allocation-stack profiles and the earlier full-suite baseline; only matched results within this run establish these changes.

Each capture contains all four Criterion workloads with 30 samples, plus 30 normal and 30 separately instrumented allocation samples per diagnostic case. Across the four captures that is 480 normal diagnostic samples and 480 allocation samples. Normal Criterion timing is the headline measurement; allocation-instrumented elapsed times are excluded.

| Workload | Base ms/run | Candidate ms/run | Change | Paired change range |
| --- | ---: | ---: | ---: | ---: |
| Minimal in-memory control | 0.009472 | 0.009224 | -2.6% | -4.6% to -0.5% |
| Recovery, 0 prior runs | 10.033 | 9.475 | -5.6% | -5.8% to -5.3% |
| Recovery, 512 prior runs | 39.673 | 28.284 | -28.7% | -31.0% to -26.4% |
| Ordinary durable execution | 12.923 | 12.644 | -2.2% | -5.5% to +1.0% |

The values are medians of the two cohort medians. The paired ranges retain both cohorts and are descriptive, not confidence intervals. Long-history recovery improves in both orders with the same work completed. Control medians alone do not establish absence of regressions: raw normal diagnostics contain candidate-only durable samples around 100–121 ms, which prompted the larger independent repeat below. Timing gates remain disabled because these jobs do not establish calibrated variation across runner allocations. Each focused run covers four workloads, while CI exercises correctness for the full 19-workload matrix.

The [complete timing table](comparison/COMPARISON.md), [cohort values and deltas](comparison/comparison.json), and per-capture raw Criterion, CPU/latency, and allocation reports remain available under [comparison/](comparison/). No attempts or samples were discarded. The hosted run completed on its first attempt.

The [independent artifact audit](comparison-audit.json) validates exact source and fixture fingerprints, capture order, all sample counts, raw/merged diagnostic identity, and unchanged model/tool/event/transaction/write counts. It also retains the initial durable tail concern: 7 of 60 candidate normal samples exceed 50 ms, including 5 above 90 ms, versus none of 60 baseline samples. Normal durable mean elapsed rises from 13.37 ms to 24.81 ms in this run. Those slow samples still use approximately 6.4–8.1 ms CPU, and neither allocation-instrumented samples nor the individual Criterion sample totals contain comparable stalls. This supports investigating waits, but does not establish their cause.

## Independent repeat and tail investigation

The [second matched job](https://github.com/danieljvdm/agent-rs/actions/runs/40963163553) passed with the same revisions and fixtures on a fresh EPYC 9V45 runner. It retains four complete AB/BA captures with 30 Criterion samples per workload and 100 normal plus 100 allocation samples per diagnostic case: **1,600 normal and 1,600 allocation samples**. It changes only the diagnostic sample count, not the benchmark work or candidate code.

| Workload | Base ms/run | Candidate ms/run | Change | Paired change range |
| --- | ---: | ---: | ---: | ---: |
| Minimal in-memory control | 0.010037 | 0.010040 | +0.0% | -0.5% to +0.5% |
| Recovery, 0 prior runs | 10.548 | 9.786 | -7.2% | -7.7% to -6.8% |
| Recovery, 512 prior runs | 43.476 | 30.236 | -30.5% | -31.3% to -29.6% |
| Ordinary durable execution | 13.590 | 12.982 | -4.5% | -6.1% to -2.9% |

The suspected durable tail regression did **not reproduce**. Neither candidate cohort has a normal durable sample above 25 ms; one baseline sample reaches 62.9 ms. Across 200 samples per side, candidate mean durable completion is 13.07 ms versus baseline 13.67 ms; the descriptive pooled p95 is 14.445 ms versus 14.320 ms. No sample on either side exceeds 90 ms.

| Repeat durable normal samples | Count | Mean ms | Maximum ms | Above 50 ms |
| --- | ---: | ---: | ---: | ---: |
| Base, cohort 0 | 100 | 13.850 | 62.895 | 1 |
| Candidate, cohort 0 | 100 | 13.447 | 19.956 | 0 |
| Candidate, cohort 1 | 100 | 12.700 | 24.784 | 0 |
| Base, cohort 1 | 100 | 13.481 | 22.960 | 0 |

The [repeat timing table](repeat/COMPARISON.md), [raw reports](repeat/), and [independent audit](repeat-audit.json) retain every sample and verify the exact revisions, source/fixture/dependency bytes, binary hashes, sample counts, work counts, and sequential capture order. Long-history CPU time falls by 36.5% in the first job and 37.9% in the second; cumulative allocated bytes fall by 44.5% in both. The [historical check](historical-durable-tails.json) found no comparable stalls in 240 earlier normal durable samples, but those jobs used a different processor and full-suite layout. Code inspection found no 100 ms application timer in this fixture: its host polls every 1 ms. SQLite uses synchronous durable writes through one worker thread. Storage, allocator, or scheduler waits remain possible explanations; no trace established which occurred.

The change is retained because long-history elapsed time improves in both independent jobs, allocation and CPU reductions agree with the decoder profile, and the larger repeat shows no reproduced durable regression. This does not prove rare stalls are impossible or identify the first job's cause. The initial audit remains marked provisional to preserve its original finding; the second audit records the additional evidence. No slow samples were removed and no timing gate was enabled.

## Compatibility and durability

Four SQLite reopen tests cover canonical and alternate encodings, duplicate keys, unknown fields, malformed data, version precedence, dynamic number semantics, and the accepted/rejected nesting boundary. A separate temporary probe compared 620 nested checkpoint cases with the old decoder and found no acceptance or decoded-state differences. Existing engine, durability, personal workflow, and real-process recovery tests remain part of CI.

[Candidate CI](https://github.com/danieljvdm/agent-rs/actions/runs/40961977208) passed all 59 Rust tests, all eight benchmark-controller tests, all 19 benchmark correctness cases, formatting, Clippy with warnings denied, and the engine-only build on Rust 1.99.1. The helper subprocess and optional soak remain ignored in the default Rust test command. A separate [60-second soak](process-soak.txt) passed 217 SIGKILL/relaunch cycles with the pending deadline and active-work budget preserved. The three scripted personal-workflow tests passed; the live-model evaluation was not rerun for this decoder-only change.
