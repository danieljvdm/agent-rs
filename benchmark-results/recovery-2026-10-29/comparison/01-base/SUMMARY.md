Scripted runtime measurement. Timing gates are not calibrated.

| Workload | Median µs/completed run | Median estimate interval µs |
| --- | ---: | ---: |
| engine/minimal | 9.370 | 9.291–9.420 |
| recovery/restart_to_completion/0 | 10012.715 | 9886.126–10194.751 |
| recovery/restart_to_completion/512 | 39024.483 | 38433.580–39396.849 |
| sqlite/durable_four_tool_rounds | 12689.769 | 12600.301–12877.632 |

Intervals describe Criterion's median estimate, not per-operation tail latency. See metrics.json for raw CPU, latency, first-output, and database samples; allocations.json is separately instrumented. Raw Criterion samples retain outliers.
