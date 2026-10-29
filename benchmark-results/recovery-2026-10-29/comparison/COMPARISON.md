Repeated AB/BA comparison. Timing differences remain informational pending independent variance calibration.

| Workload | Base median µs/run | Candidate median µs/run | Change | Paired change range |
| --- | ---: | ---: | ---: | ---: |
| engine/minimal | 9.472 | 9.224 | -2.6% | -4.6% to -0.5% |
| recovery/restart_to_completion/0 | 10033.010 | 9475.068 | -5.6% | -5.8% to -5.3% |
| recovery/restart_to_completion/512 | 39672.558 | 28283.767 | -28.7% | -31.0% to -26.4% |
| sqlite/durable_four_tool_rounds | 12922.509 | 12644.284 | -2.2% | -5.5% to +1.0% |

Each side has 2 cohort medians from one sequential job. The paired range retains every cohort, including slow ones; it is descriptive, not a confidence interval.
A timing gate is deliberately disabled. One job cannot establish runner-to-runner variance. Before enabling a workload-specific gate, repeat full same-source jobs, choose both relative and absolute tolerances above observed noise, and verify suspected regressions in a fresh matched job.
See comparison.json for raw paired deltas and median absolute deviations; each capture retains Criterion samples plus separate CPU/allocation diagnostics. Correctness and work counts are enforced immediately.
