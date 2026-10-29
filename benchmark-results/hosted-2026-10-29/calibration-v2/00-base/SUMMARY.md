Scripted runtime measurement. Timing gates are not calibrated.

| Workload | Median µs/completed run | Median estimate interval µs |
| --- | ---: | ---: |
| conversations/engine_workers_4/1 | 165.990 | 165.575–166.631 |
| conversations/engine_workers_4/16 | 67.941 | 67.797–68.127 |
| conversations/engine_workers_4/64 | 61.863 | 61.804–62.090 |
| engine/context_entries_0 | 13.822 | 13.785–13.847 |
| engine/context_entries_1024 | 763.065 | 761.387–765.275 |
| engine/four_tool_rounds | 59.721 | 59.627–59.838 |
| engine/minimal | 13.800 | 13.770–13.829 |
| engine/stream_chunks_1 | 15.539 | 15.527–15.598 |
| engine/stream_chunks_1024 | 74.985 | 74.878–75.074 |
| engine/stream_chunks_64 | 19.822 | 19.748–19.868 |
| engine/tools_8_bound_1 | 75.923 | 75.824–76.098 |
| engine/tools_8_bound_4 | 70.397 | 70.325–70.565 |
| engine/tools_8_bound_8 | 71.784 | 71.628–71.930 |
| recovery/restart_to_completion/0 | 13014.922 | 12815.224–13212.813 |
| recovery/restart_to_completion/512 | 59887.041 | 58909.275–61310.643 |
| sqlite/durable_four_tool_rounds | 17192.629 | 16865.668–17553.642 |
| sqlite_conversations/host_workers_4/1 | 13225.940 | 13154.566–13328.151 |
| sqlite_conversations/host_workers_4/16 | 11502.847 | 11415.357–11567.208 |
| sqlite_conversations/host_workers_4/64 | 11489.246 | 11427.048–11560.504 |

Intervals describe Criterion's median estimate, not per-operation tail latency. See metrics.json for raw CPU, latency, first-output, and database samples; allocations.json is separately instrumented. Raw Criterion samples retain outliers.
