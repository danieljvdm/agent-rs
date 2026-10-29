Scripted runtime measurement. Timing gates are not calibrated.

| Workload | Median µs/completed run | Median estimate interval µs |
| --- | ---: | ---: |
| conversations/engine_workers_4/1 | 168.328 | 167.805–168.950 |
| conversations/engine_workers_4/16 | 67.858 | 67.724–68.267 |
| conversations/engine_workers_4/64 | 62.070 | 61.782–62.320 |
| engine/context_entries_0 | 13.856 | 13.837–13.878 |
| engine/context_entries_1024 | 778.140 | 777.063–778.990 |
| engine/four_tool_rounds | 58.827 | 58.749–59.103 |
| engine/minimal | 13.892 | 13.873–13.917 |
| engine/stream_chunks_1 | 15.673 | 15.647–15.703 |
| engine/stream_chunks_1024 | 73.252 | 73.083–73.326 |
| engine/stream_chunks_64 | 19.788 | 19.751–19.855 |
| engine/tools_8_bound_1 | 76.137 | 75.990–76.441 |
| engine/tools_8_bound_4 | 70.159 | 70.031–70.595 |
| engine/tools_8_bound_8 | 71.591 | 71.438–71.816 |
| recovery/restart_to_completion/0 | 13405.868 | 13307.820–13563.774 |
| recovery/restart_to_completion/512 | 63264.924 | 62235.026–66031.742 |
| sqlite/durable_four_tool_rounds | 18751.077 | 18514.786–19688.727 |
| sqlite_conversations/host_workers_4/1 | 13687.172 | 13590.115–13896.771 |
| sqlite_conversations/host_workers_4/16 | 12040.237 | 11999.775–12184.757 |
| sqlite_conversations/host_workers_4/64 | 12168.973 | 11998.523–12303.281 |

Intervals describe Criterion's median estimate, not per-operation tail latency. See metrics.json for raw CPU, latency, first-output, and database samples; allocations.json is separately instrumented. Raw Criterion samples retain outliers.
