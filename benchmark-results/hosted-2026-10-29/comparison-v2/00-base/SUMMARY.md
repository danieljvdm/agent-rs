Scripted runtime measurement. Timing gates are not calibrated.

| Workload | Median µs/completed run | Median estimate interval µs |
| --- | ---: | ---: |
| conversations/engine_workers_4/1 | 172.969 | 172.204–173.343 |
| conversations/engine_workers_4/16 | 68.601 | 68.421–68.945 |
| conversations/engine_workers_4/64 | 63.072 | 62.848–63.425 |
| engine/context_entries_0 | 13.807 | 13.779–13.831 |
| engine/context_entries_1024 | 777.494 | 776.224–778.545 |
| engine/four_tool_rounds | 58.855 | 58.759–58.975 |
| engine/minimal | 13.755 | 13.721–13.774 |
| engine/stream_chunks_1 | 15.540 | 15.512–15.561 |
| engine/stream_chunks_1024 | 73.426 | 73.361–73.653 |
| engine/stream_chunks_64 | 19.564 | 19.523–19.594 |
| engine/tools_8_bound_1 | 75.791 | 75.578–76.335 |
| engine/tools_8_bound_4 | 70.382 | 70.233–70.567 |
| engine/tools_8_bound_8 | 70.962 | 70.827–71.302 |
| recovery/restart_to_completion/0 | 13424.119 | 13307.397–13676.399 |
| recovery/restart_to_completion/512 | 61349.232 | 60531.119–62221.464 |
| sqlite/durable_four_tool_rounds | 18633.998 | 18001.017–18910.273 |
| sqlite_conversations/host_workers_4/1 | 13750.980 | 13685.913–13834.001 |
| sqlite_conversations/host_workers_4/16 | 12307.906 | 12194.226–12394.748 |
| sqlite_conversations/host_workers_4/64 | 12343.908 | 12185.760–12477.388 |

Intervals describe Criterion's median estimate, not per-operation tail latency. See metrics.json for raw CPU, latency, first-output, and database samples; allocations.json is separately instrumented. Raw Criterion samples retain outliers.
