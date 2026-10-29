Repeated AB/BA comparison. Timing differences remain informational pending independent variance calibration.

| Workload | Base median µs/run | Candidate median µs/run | Change | Paired change range |
| --- | ---: | ---: | ---: | ---: |
| engine/minimal | 10.037 | 10.040 | +0.0% | -0.5% to +0.5% |
| recovery/restart_to_completion/0 | 10548.404 | 9785.743 | -7.2% | -7.7% to -6.8% |
| recovery/restart_to_completion/512 | 43476.016 | 30235.753 | -30.5% | -31.3% to -29.6% |
| sqlite/durable_four_tool_rounds | 13590.490 | 12982.014 | -4.5% | -6.1% to -2.9% |

Each side has 2 cohort medians from one sequential job. The paired range retains every cohort, including slow ones; it is descriptive, not a confidence interval.
A timing gate is deliberately disabled. One job cannot establish runner-to-runner variance. Before enabling a workload-specific gate, repeat full same-source jobs, choose both relative and absolute tolerances above observed noise, and verify suspected regressions in a fresh matched job.
See comparison.json for raw paired deltas and median absolute deviations; each capture retains Criterion samples plus separate CPU/allocation diagnostics. Correctness and work counts are enforced immediately.
