Scripted runtime measurement. Timing gates are not calibrated.

| Workload | Median µs/completed run | Median estimate interval µs |
| --- | ---: | ---: |
| conversations/engine_workers_4/1 | 168.537 | 168.075–168.847 |
| conversations/engine_workers_4/16 | 67.294 | 67.136–67.496 |
| conversations/engine_workers_4/64 | 61.092 | 60.912–61.211 |
| engine/context_entries_0 | 13.673 | 13.651–13.702 |
| engine/context_entries_1024 | 768.381 | 767.662–771.219 |
| engine/four_tool_rounds | 59.173 | 58.998–59.237 |
| engine/minimal | 13.886 | 13.876–13.919 |
| engine/stream_chunks_1 | 15.517 | 15.456–15.529 |
| engine/stream_chunks_1024 | 74.779 | 74.693–74.806 |
| engine/stream_chunks_64 | 19.763 | 19.730–19.800 |
| engine/tools_8_bound_1 | 75.763 | 75.596–75.892 |
| engine/tools_8_bound_4 | 70.556 | 70.412–70.738 |
| engine/tools_8_bound_8 | 71.216 | 71.092–71.350 |
| recovery/restart_to_completion/0 | 12950.506 | 12846.096–13053.921 |
| recovery/restart_to_completion/512 | 59833.733 | 59561.221–60658.635 |
| sqlite/durable_four_tool_rounds | 18036.995 | 17258.393–18447.420 |
| sqlite_conversations/host_workers_4/1 | 13209.025 | 13113.168–13250.510 |
| sqlite_conversations/host_workers_4/16 | 11347.068 | 11303.955–11441.635 |
| sqlite_conversations/host_workers_4/64 | 11389.437 | 11328.345–11566.165 |

Intervals describe Criterion's median estimate, not per-operation tail latency. See metrics.json for raw CPU, latency, first-output, and database samples; allocations.json is separately instrumented. Raw Criterion samples retain outliers.
