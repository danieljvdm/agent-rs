Scripted runtime measurement. Timing gates are not calibrated.

| Workload | Median µs/completed run | Median estimate interval µs |
| --- | ---: | ---: |
| engine/minimal | 9.574 | 9.439–9.683 |
| recovery/restart_to_completion/0 | 10053.306 | 9916.064–10152.973 |
| recovery/restart_to_completion/512 | 40320.632 | 40163.733–40415.192 |
| sqlite/durable_four_tool_rounds | 13155.249 | 12974.515–13511.086 |

Intervals describe Criterion's median estimate, not per-operation tail latency. See metrics.json for raw CPU, latency, first-output, and database samples; allocations.json is separately instrumented. Raw Criterion samples retain outliers.
