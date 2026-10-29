Scripted runtime measurement. Timing gates are not calibrated.

| Workload | Median µs/completed run | Median estimate interval µs |
| --- | ---: | ---: |
| engine/minimal | 9.989 | 9.964–10.017 |
| recovery/restart_to_completion/0 | 10683.983 | 10490.208–10806.858 |
| recovery/restart_to_completion/512 | 43023.313 | 42386.463–43298.400 |
| sqlite/durable_four_tool_rounds | 13624.368 | 13471.111–13775.602 |

Intervals describe Criterion's median estimate, not per-operation tail latency. See metrics.json for raw CPU, latency, first-output, and database samples; allocations.json is separately instrumented. Raw Criterion samples retain outliers.
