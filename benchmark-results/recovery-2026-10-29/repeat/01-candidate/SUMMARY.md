Scripted runtime measurement. Timing gates are not calibrated.

| Workload | Median µs/completed run | Median estimate interval µs |
| --- | ---: | ---: |
| engine/minimal | 10.041 | 10.014–10.090 |
| recovery/restart_to_completion/0 | 9863.218 | 9714.915–9949.323 |
| recovery/restart_to_completion/512 | 30284.599 | 30192.349–30517.487 |
| sqlite/durable_four_tool_rounds | 13230.842 | 12301.002–13493.661 |

Intervals describe Criterion's median estimate, not per-operation tail latency. See metrics.json for raw CPU, latency, first-output, and database samples; allocations.json is separately instrumented. Raw Criterion samples retain outliers.
