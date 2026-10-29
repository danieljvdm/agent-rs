Scripted runtime measurement. Timing gates are not calibrated.

| Workload | Median µs/completed run | Median estimate interval µs |
| --- | ---: | ---: |
| engine/minimal | 10.040 | 10.000–10.082 |
| recovery/restart_to_completion/0 | 9708.268 | 9622.825–9850.156 |
| recovery/restart_to_completion/512 | 30186.909 | 29931.853–30516.045 |
| sqlite/durable_four_tool_rounds | 12733.185 | 12630.358–12830.067 |

Intervals describe Criterion's median estimate, not per-operation tail latency. See metrics.json for raw CPU, latency, first-output, and database samples; allocations.json is separately instrumented. Raw Criterion samples retain outliers.
