Repeated AB/BA comparison. Timing differences remain informational pending independent variance calibration.

| Workload | Base median µs/run | Candidate median µs/run | Change | Paired change range |
| --- | ---: | ---: | ---: | ---: |
| conversations/engine_workers_4/1 | 171.351 | 168.311 | -1.8% | -2.7% to -0.8% |
| conversations/engine_workers_4/16 | 68.181 | 67.485 | -1.0% | -2.2% to +0.1% |
| conversations/engine_workers_4/64 | 62.678 | 62.691 | +0.0% | -0.3% to +0.4% |
| engine/context_entries_0 | 13.819 | 13.738 | -0.6% | -1.4% to +0.2% |
| engine/context_entries_1024 | 773.373 | 776.871 | +0.5% | -0.2% to +1.2% |
| engine/four_tool_rounds | 58.818 | 58.921 | +0.2% | +0.1% to +0.3% |
| engine/minimal | 13.825 | 13.812 | -0.1% | -0.2% to -0.0% |
| engine/stream_chunks_1 | 15.550 | 15.630 | +0.5% | +0.3% to +0.7% |
| engine/stream_chunks_1024 | 74.310 | 73.185 | -1.5% | -2.6% to -0.4% |
| engine/stream_chunks_64 | 19.697 | 19.693 | -0.0% | -0.2% to +0.2% |
| engine/tools_8_bound_1 | 76.250 | 75.965 | -0.4% | -0.7% to +0.0% |
| engine/tools_8_bound_4 | 70.696 | 70.204 | -0.7% | -1.2% to -0.2% |
| engine/tools_8_bound_8 | 70.890 | 71.391 | +0.7% | +0.3% to +1.1% |
| recovery/restart_to_completion/0 | 13384.462 | 13349.302 | -0.3% | -1.0% to +0.5% |
| recovery/restart_to_completion/512 | 62018.329 | 63215.384 | +1.9% | +0.9% to +3.0% |
| sqlite/durable_four_tool_rounds | 18546.618 | 18151.045 | -2.1% | -5.8% to +1.6% |
| sqlite_conversations/host_workers_4/1 | 13764.524 | 13764.723 | +0.0% | -0.7% to +0.7% |
| sqlite_conversations/host_workers_4/16 | 12221.221 | 12033.586 | -1.5% | -2.3% to -0.8% |
| sqlite_conversations/host_workers_4/64 | 12330.832 | 12112.173 | -1.8% | -2.3% to -1.2% |

Each side has 2 cohort medians from one sequential job. The paired range retains every cohort, including slow ones; it is descriptive, not a confidence interval.
A timing gate is deliberately disabled. One job cannot establish runner-to-runner variance. Before enabling a workload-specific gate, repeat full same-source jobs, choose both relative and absolute tolerances above observed noise, and verify suspected regressions in a fresh matched job.
See comparison.json for raw paired deltas and median absolute deviations; each capture retains Criterion samples plus separate CPU/allocation diagnostics. Correctness and work counts are enforced immediately.
