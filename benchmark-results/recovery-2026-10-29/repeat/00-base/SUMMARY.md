Scripted runtime measurement. Timing gates are not calibrated.

| Workload | Median µs/completed run | Median estimate interval µs |
| --- | ---: | ---: |
| engine/minimal | 10.085 | 10.030–10.160 |
| recovery/restart_to_completion/0 | 10412.825 | 10149.466–10514.290 |
| recovery/restart_to_completion/512 | 43928.720 | 42881.805–44344.035 |
| sqlite/durable_four_tool_rounds | 13556.611 | 13373.896–13698.558 |

Intervals describe Criterion's median estimate, not per-operation tail latency. See metrics.json for raw CPU, latency, first-output, and database samples; allocations.json is separately instrumented. Raw Criterion samples retain outliers.
