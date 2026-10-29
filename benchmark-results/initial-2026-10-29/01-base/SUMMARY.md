Scripted runtime measurement. Timing gates are not calibrated.

| Workload | Median µs/completed run | Median estimate interval µs |
| --- | ---: | ---: |
| conversations/engine_workers_4/1 | 101.706 | 66.721–192.902 |
| conversations/engine_workers_4/16 | 48.230 | 40.013–52.337 |
| conversations/engine_workers_4/64 | 36.537 | 30.103–47.429 |
| engine/context_entries_0 | 9.681 | 8.861–9.924 |
| engine/context_entries_1024 | 453.771 | 444.699–467.331 |
| engine/four_tool_rounds | 38.564 | 36.710–39.538 |
| engine/minimal | 9.445 | 8.372–10.530 |
| engine/stream_chunks_1 | 9.425 | 9.246–9.792 |
| engine/stream_chunks_1024 | 31.183 | 30.038–32.474 |
| engine/stream_chunks_64 | 10.693 | 10.572–11.111 |
| engine/tools_8_bound_1 | 41.890 | 41.510–42.449 |
| engine/tools_8_bound_4 | 39.831 | 38.351–41.198 |
| engine/tools_8_bound_8 | 41.029 | 39.253–42.743 |
| recovery/restart_to_completion/0 | 35096.395 | 22739.542–37912.479 |
| recovery/restart_to_completion/512 | 110053.417 | 102202.666–124038.292 |
| sqlite/durable_four_tool_rounds | 35219.874 | 23729.146–49448.333 |
| sqlite_conversations/host_workers_4/1 | 32379.397 | 23699.213–33558.687 |
| sqlite_conversations/host_workers_4/16 | 19881.210 | 16541.239–21588.307 |
| sqlite_conversations/host_workers_4/64 | 21217.449 | 16469.711–25807.090 |

Intervals describe Criterion's median estimate, not per-operation tail latency. See metrics.json for raw CPU, latency, first-output, and database samples; allocations.json is separately instrumented. Raw Criterion samples retain outliers.
