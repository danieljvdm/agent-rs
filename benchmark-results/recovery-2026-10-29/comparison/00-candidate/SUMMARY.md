Scripted runtime measurement. Timing gates are not calibrated.

| Workload | Median µs/completed run | Median estimate interval µs |
| --- | ---: | ---: |
| engine/minimal | 9.129 | 9.007–9.273 |
| recovery/restart_to_completion/0 | 9522.929 | 9366.525–9671.769 |
| recovery/restart_to_completion/512 | 27832.770 | 27601.240–28154.904 |
| sqlite/durable_four_tool_rounds | 13292.587 | 13087.918–13572.451 |

Intervals describe Criterion's median estimate, not per-operation tail latency. See metrics.json for raw CPU, latency, first-output, and database samples; allocations.json is separately instrumented. Raw Criterion samples retain outliers.
