Scripted runtime measurement. Timing gates are not calibrated.

| Workload | Median µs/completed run | Median estimate interval µs |
| --- | ---: | ---: |
| conversations/engine_workers_4/1 | 168.293 | 167.785–168.726 |
| conversations/engine_workers_4/16 | 67.112 | 66.724–67.280 |
| conversations/engine_workers_4/64 | 63.312 | 62.993–64.029 |
| engine/context_entries_0 | 13.619 | 13.601–13.640 |
| engine/context_entries_1024 | 775.602 | 774.840–778.290 |
| engine/four_tool_rounds | 59.015 | 58.892–59.109 |
| engine/minimal | 13.732 | 13.709–13.783 |
| engine/stream_chunks_1 | 15.588 | 15.565–15.608 |
| engine/stream_chunks_1024 | 73.119 | 72.965–73.203 |
| engine/stream_chunks_64 | 19.597 | 19.570–19.671 |
| engine/tools_8_bound_1 | 75.793 | 75.676–75.899 |
| engine/tools_8_bound_4 | 70.250 | 70.149–70.405 |
| engine/tools_8_bound_8 | 71.191 | 70.976–71.409 |
| recovery/restart_to_completion/0 | 13292.736 | 13188.789–13425.994 |
| recovery/restart_to_completion/512 | 63165.843 | 62318.728–64436.730 |
| sqlite/durable_four_tool_rounds | 17551.013 | 17229.906–17979.022 |
| sqlite_conversations/host_workers_4/1 | 13842.274 | 13731.669–13980.767 |
| sqlite_conversations/host_workers_4/16 | 12026.934 | 11940.497–12078.599 |
| sqlite_conversations/host_workers_4/64 | 12055.373 | 11942.781–12131.378 |

Intervals describe Criterion's median estimate, not per-operation tail latency. See metrics.json for raw CPU, latency, first-output, and database samples; allocations.json is separately instrumented. Raw Criterion samples retain outliers.
