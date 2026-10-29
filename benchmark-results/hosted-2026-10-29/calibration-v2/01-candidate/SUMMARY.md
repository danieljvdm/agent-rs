Scripted runtime measurement. Timing gates are not calibrated.

| Workload | Median µs/completed run | Median estimate interval µs |
| --- | ---: | ---: |
| conversations/engine_workers_4/1 | 168.258 | 167.715–169.137 |
| conversations/engine_workers_4/16 | 67.406 | 67.290–67.987 |
| conversations/engine_workers_4/64 | 61.603 | 61.459–61.820 |
| engine/context_entries_0 | 13.792 | 13.785–13.811 |
| engine/context_entries_1024 | 773.898 | 772.509–774.701 |
| engine/four_tool_rounds | 59.245 | 59.080–59.353 |
| engine/minimal | 13.835 | 13.822–13.847 |
| engine/stream_chunks_1 | 15.550 | 15.526–15.586 |
| engine/stream_chunks_1024 | 86.159 | 85.986–86.256 |
| engine/stream_chunks_64 | 20.489 | 20.472–20.517 |
| engine/tools_8_bound_1 | 75.807 | 75.634–75.987 |
| engine/tools_8_bound_4 | 70.278 | 70.165–70.573 |
| engine/tools_8_bound_8 | 72.020 | 71.954–72.247 |
| recovery/restart_to_completion/0 | 13139.322 | 13020.908–13253.567 |
| recovery/restart_to_completion/512 | 60558.933 | 60323.390–60835.872 |
| sqlite/durable_four_tool_rounds | 18635.664 | 18316.217–19350.982 |
| sqlite_conversations/host_workers_4/1 | 13444.055 | 13344.495–13529.638 |
| sqlite_conversations/host_workers_4/16 | 11762.128 | 11699.094–11844.976 |
| sqlite_conversations/host_workers_4/64 | 11778.264 | 11683.723–11908.281 |

Intervals describe Criterion's median estimate, not per-operation tail latency. See metrics.json for raw CPU, latency, first-output, and database samples; allocations.json is separately instrumented. Raw Criterion samples retain outliers.
