Diagnostic intervals include CPU clock reads and concurrency latency bookkeeping; Criterion remains the headline timing source.

| Workload | CPU µs/run | Allocations/run | Requested bytes/run |
| --- | ---: | ---: | ---: |
| durable_four_tool_rounds | 7060.431 | 10680.0 | 1828183.0 |
| minimal | 16.094 | 275.0 | 30320.0 |
| recovery_prior_runs_0 | 3922.372 | 7981.0 | 1391750.0 |
| recovery_prior_runs_512 | 23686.446 | 181516.0 | 60686451.0 |

Startup rows represent one startup rather than one completed run. Raw latency observations, first-output timing, throughput, database counts and sample sizes are in diagnostics.json; small-sample tails are descriptive, not SLO estimates.
