Scripted runtime measurement. Timing gates are not calibrated.

| Workload | Median µs/completed run | Median estimate interval µs |
| --- | ---: | ---: |
| conversations/engine_workers_4/1 | 167.814 | 167.582–168.430 |
| conversations/engine_workers_4/16 | 67.032 | 66.648–67.482 |
| conversations/engine_workers_4/64 | 61.565 | 61.254–61.813 |
| engine/context_entries_0 | 14.038 | 14.022–14.067 |
| engine/context_entries_1024 | 768.688 | 767.643–770.304 |
| engine/four_tool_rounds | 59.318 | 59.231–59.418 |
| engine/minimal | 13.983 | 13.960–14.011 |
| engine/stream_chunks_1 | 15.718 | 15.691–15.732 |
| engine/stream_chunks_1024 | 75.151 | 75.010–75.278 |
| engine/stream_chunks_64 | 19.882 | 19.851–19.919 |
| engine/tools_8_bound_1 | 75.794 | 75.645–75.911 |
| engine/tools_8_bound_4 | 70.867 | 70.712–71.264 |
| engine/tools_8_bound_8 | 70.909 | 70.761–71.159 |
| recovery/restart_to_completion/0 | 12777.304 | 12601.989–12838.217 |
| recovery/restart_to_completion/512 | 57819.020 | 57462.016–58141.280 |
| sqlite/durable_four_tool_rounds | 16399.533 | 16318.816–16521.297 |
| sqlite_conversations/host_workers_4/1 | 12924.177 | 12849.992–12960.037 |
| sqlite_conversations/host_workers_4/16 | 11226.934 | 11140.676–11263.854 |
| sqlite_conversations/host_workers_4/64 | 11083.327 | 11015.530–11133.938 |

Intervals describe Criterion's median estimate, not per-operation tail latency. See metrics.json for raw CPU, latency, first-output, and database samples; allocations.json is separately instrumented. Raw Criterion samples retain outliers.
