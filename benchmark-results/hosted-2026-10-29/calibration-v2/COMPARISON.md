Repeated AB/BA comparison of identical benchmarked source. Observed differences estimate noise; they are not improvement evidence.

| Workload | Base median µs/run | Candidate median µs/run | Change | Paired change range |
| --- | ---: | ---: | ---: | ---: |
| conversations/engine_workers_4/1 | 166.902 | 168.398 | same source | +0.3% to +1.5% |
| conversations/engine_workers_4/16 | 67.486 | 67.350 | same source | -1.0% to +0.6% |
| conversations/engine_workers_4/64 | 61.714 | 61.347 | same source | -1.2% to +0.1% |
| engine/context_entries_0 | 13.930 | 13.733 | same source | -1.8% to -1.1% |
| engine/context_entries_1024 | 765.877 | 771.140 | same source | +0.7% to +0.7% |
| engine/four_tool_rounds | 59.519 | 59.209 | same source | -0.9% to -0.1% |
| engine/minimal | 13.891 | 13.861 | same source | -1.1% to +0.6% |
| engine/stream_chunks_1 | 15.629 | 15.533 | same source | -1.1% to -0.1% |
| engine/stream_chunks_1024 | 75.068 | 80.469 | same source | -0.3% to +14.6% |
| engine/stream_chunks_64 | 19.852 | 20.126 | same source | -0.3% to +3.1% |
| engine/tools_8_bound_1 | 75.859 | 75.785 | same source | -0.2% to +0.0% |
| engine/tools_8_bound_4 | 70.632 | 70.417 | same source | -0.8% to +0.2% |
| engine/tools_8_bound_8 | 71.347 | 71.618 | same source | -0.8% to +1.6% |
| recovery/restart_to_completion/0 | 12896.113 | 13044.914 | same source | -0.5% to +2.8% |
| recovery/restart_to_completion/512 | 58853.031 | 60196.333 | same source | -0.1% to +4.7% |
| sqlite/durable_four_tool_rounds | 16796.081 | 18336.330 | same source | +4.9% to +13.6% |
| sqlite_conversations/host_workers_4/1 | 13075.058 | 13326.540 | same source | -0.1% to +4.0% |
| sqlite_conversations/host_workers_4/16 | 11364.890 | 11554.598 | same source | -1.4% to +4.8% |
| sqlite_conversations/host_workers_4/64 | 11286.286 | 11583.850 | same source | -0.9% to +6.3% |

Each side has 2 cohort medians from one sequential job. The paired range retains every cohort, including slow ones; it is descriptive, not a confidence interval.
A timing gate is deliberately disabled. One job cannot establish runner-to-runner variance. Before enabling a workload-specific gate, repeat full same-source jobs, choose both relative and absolute tolerances above observed noise, and verify suspected regressions in a fresh matched job.
See comparison.json for raw paired deltas and median absolute deviations; each capture retains Criterion samples plus separate CPU/allocation diagnostics. Correctness and work counts are enforced immediately.
