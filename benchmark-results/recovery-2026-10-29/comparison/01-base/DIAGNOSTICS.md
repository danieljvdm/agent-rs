Diagnostic intervals include CPU clock reads and concurrency latency bookkeeping; Criterion remains the headline timing source.

| Workload | CPU µs/run | Allocations/run | Requested bytes/run |
| --- | ---: | ---: | ---: |
| durable_four_tool_rounds | 6341.216 | 14580.0 | 2415530.0 |
| minimal | 9.839 | 275.0 | 30320.0 |
| recovery_prior_runs_0 | 3917.169 | 11128.0 | 1865478.0 |
| recovery_prior_runs_512 | 32324.314 | 381175.0 | 109322998.0 |

Startup rows represent one startup rather than one completed run. Raw latency observations, first-output timing, throughput, database counts and sample sizes are in diagnostics.json; small-sample tails are descriptive, not SLO estimates.
