Scripted runtime measurement. Timing gates are not calibrated.

| Workload | Median µs/completed run | Median estimate interval µs |
| --- | ---: | ---: |
| conversations/engine_workers_4/1 | 77.319 | 65.365–118.689 |
| conversations/engine_workers_4/16 | 28.684 | 22.253–50.716 |
| conversations/engine_workers_4/64 | 34.421 | 26.544–40.294 |
| engine/context_entries_0 | 8.900 | 8.568–9.017 |
| engine/context_entries_1024 | 421.062 | 410.533–440.211 |
| engine/four_tool_rounds | 37.857 | 36.603–38.442 |
| engine/minimal | 8.904 | 8.695–9.110 |
| engine/stream_chunks_1 | 9.950 | 9.672–10.474 |
| engine/stream_chunks_1024 | 30.571 | 30.047–32.759 |
| engine/stream_chunks_64 | 10.839 | 10.316–11.751 |
| engine/tools_8_bound_1 | 42.339 | 41.016–44.357 |
| engine/tools_8_bound_4 | 42.080 | 40.111–42.959 |
| engine/tools_8_bound_8 | 40.326 | 39.752–40.852 |
| recovery/restart_to_completion/0 | 37838.896 | 33501.896–44320.542 |
| recovery/restart_to_completion/512 | 107453.958 | 96856.645–117509.105 |
| sqlite/durable_four_tool_rounds | 26214.541 | 20298.688–37791.479 |
| sqlite_conversations/host_workers_4/1 | 16563.812 | 12523.205–28298.460 |
| sqlite_conversations/host_workers_4/16 | 12323.410 | 11451.015–15859.510 |
| sqlite_conversations/host_workers_4/64 | 14413.110 | 11196.817–18002.850 |

Intervals describe Criterion's median estimate, not per-operation tail latency. See metrics.json for raw CPU, latency, first-output, and database samples; allocations.json is separately instrumented. Raw Criterion samples retain outliers.
