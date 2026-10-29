Scripted runtime measurement. Timing gates are not calibrated.

| Workload | Median µs/completed run | Median estimate interval µs |
| --- | ---: | ---: |
| conversations/engine_workers_4/1 | 169.733 | 168.773–170.578 |
| conversations/engine_workers_4/16 | 67.762 | 67.599–67.902 |
| conversations/engine_workers_4/64 | 62.284 | 62.091–62.532 |
| engine/context_entries_0 | 13.831 | 13.802–13.846 |
| engine/context_entries_1024 | 769.251 | 766.933–771.519 |
| engine/four_tool_rounds | 58.781 | 58.679–59.090 |
| engine/minimal | 13.895 | 13.885–13.934 |
| engine/stream_chunks_1 | 15.560 | 15.529–15.611 |
| engine/stream_chunks_1024 | 75.194 | 75.043–75.311 |
| engine/stream_chunks_64 | 19.830 | 19.781–19.896 |
| engine/tools_8_bound_1 | 76.710 | 76.384–76.915 |
| engine/tools_8_bound_4 | 71.009 | 70.893–71.189 |
| engine/tools_8_bound_8 | 70.819 | 70.412–70.995 |
| recovery/restart_to_completion/0 | 13344.805 | 13068.863–13469.545 |
| recovery/restart_to_completion/512 | 62687.427 | 61245.741–63600.767 |
| sqlite/durable_four_tool_rounds | 18459.238 | 18107.503–18722.736 |
| sqlite_conversations/host_workers_4/1 | 13778.067 | 13651.906–13837.546 |
| sqlite_conversations/host_workers_4/16 | 12134.536 | 12086.663–12226.916 |
| sqlite_conversations/host_workers_4/64 | 12317.755 | 12118.380–12479.006 |

Intervals describe Criterion's median estimate, not per-operation tail latency. See metrics.json for raw CPU, latency, first-output, and database samples; allocations.json is separately instrumented. Raw Criterion samples retain outliers.
