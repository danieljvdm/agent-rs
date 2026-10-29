Scripted runtime measurement. Timing gates are not calibrated.

| Workload | Median µs/completed run | Median estimate interval µs |
| --- | ---: | ---: |
| engine/minimal | 9.319 | 9.157–9.468 |
| recovery/restart_to_completion/0 | 9427.206 | 9354.600–9529.037 |
| recovery/restart_to_completion/512 | 28734.764 | 28624.648–29836.978 |
| sqlite/durable_four_tool_rounds | 11995.980 | 11800.522–12311.069 |

Intervals describe Criterion's median estimate, not per-operation tail latency. See metrics.json for raw CPU, latency, first-output, and database samples; allocations.json is separately instrumented. Raw Criterion samples retain outliers.
