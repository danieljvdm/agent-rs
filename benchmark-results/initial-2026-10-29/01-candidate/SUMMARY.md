Scripted runtime measurement. Timing gates are not calibrated.

| Workload | Median µs/completed run | Median estimate interval µs |
| --- | ---: | ---: |
| conversations/engine_workers_4/1 | 172.375 | 82.637–257.766 |
| conversations/engine_workers_4/16 | 79.206 | 45.640–95.886 |
| conversations/engine_workers_4/64 | 62.636 | 30.101–87.005 |
| engine/context_entries_0 | 13.306 | 9.110–25.911 |
| engine/context_entries_1024 | 517.261 | 478.521–639.449 |
| engine/four_tool_rounds | 45.718 | 42.453–54.422 |
| engine/minimal | 10.550 | 9.451–11.729 |
| engine/stream_chunks_1 | 11.308 | 10.432–13.165 |
| engine/stream_chunks_1024 | 38.653 | 35.175–68.279 |
| engine/stream_chunks_64 | 19.740 | 14.842–32.681 |
| engine/tools_8_bound_1 | 47.540 | 45.058–49.988 |
| engine/tools_8_bound_4 | 38.121 | 36.323–162.691 |
| engine/tools_8_bound_8 | 134.357 | 40.933–300.618 |
| recovery/restart_to_completion/0 | 27520.021 | 23268.709–38518.583 |
| recovery/restart_to_completion/512 | 153860.854 | 135005.728–160855.812 |
| sqlite/durable_four_tool_rounds | 56340.666 | 48308.771–67253.834 |
| sqlite_conversations/host_workers_4/1 | 32741.797 | 18634.656–52487.475 |
| sqlite_conversations/host_workers_4/16 | 9336.275 | 7134.652–11058.417 |
| sqlite_conversations/host_workers_4/64 | 13648.984 | 10025.893–18532.452 |

Intervals describe Criterion's median estimate, not per-operation tail latency. See metrics.json for raw CPU, latency, first-output, and database samples; allocations.json is separately instrumented. Raw Criterion samples retain outliers.
