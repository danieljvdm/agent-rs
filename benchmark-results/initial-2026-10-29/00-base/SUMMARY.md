Scripted runtime measurement. Timing gates are not calibrated.

| Workload | Median µs/completed run | Median estimate interval µs |
| --- | ---: | ---: |
| conversations/engine_workers_4/1 | 102.715 | 80.237–138.365 |
| conversations/engine_workers_4/16 | 37.726 | 22.972–52.467 |
| conversations/engine_workers_4/64 | 34.216 | 29.622–41.992 |
| engine/context_entries_0 | 8.530 | 8.390–8.949 |
| engine/context_entries_1024 | 427.058 | 412.956–531.880 |
| engine/four_tool_rounds | 39.393 | 36.104–39.591 |
| engine/minimal | 8.708 | 8.192–9.134 |
| engine/stream_chunks_1 | 9.051 | 8.955–9.127 |
| engine/stream_chunks_1024 | 29.958 | 29.306–30.837 |
| engine/stream_chunks_64 | 10.691 | 10.354–10.812 |
| engine/tools_8_bound_1 | 47.100 | 45.185–50.204 |
| engine/tools_8_bound_4 | 40.177 | 38.385–40.938 |
| engine/tools_8_bound_8 | 39.317 | 38.263–40.724 |
| recovery/restart_to_completion/0 | 28040.896 | 24649.896–44175.562 |
| recovery/restart_to_completion/512 | 108313.333 | 103211.833–111471.355 |
| sqlite/durable_four_tool_rounds | 36552.938 | 29192.146–46066.937 |
| sqlite_conversations/host_workers_4/1 | 30933.446 | 22162.710–33792.843 |
| sqlite_conversations/host_workers_4/16 | 14892.074 | 12609.655–16964.366 |
| sqlite_conversations/host_workers_4/64 | 15631.701 | 11627.008–18604.431 |

Intervals describe Criterion's median estimate, not per-operation tail latency. See metrics.json for raw CPU, latency, first-output, and database samples; allocations.json is separately instrumented. Raw Criterion samples retain outliers.
