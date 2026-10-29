Diagnostic intervals include CPU clock reads and concurrency latency bookkeeping; Criterion remains the headline timing source.

| Workload | CPU µs/run | Allocations/run | Requested bytes/run |
| --- | ---: | ---: | ---: |
| durable_four_tool_rounds | 6837.968 | 14576.0 | 2415254.0 |
| minimal | 10.982 | 275.0 | 30320.0 |
| recovery_prior_runs_0 | 4364.985 | 11128.0 | 1865478.0 |
| recovery_prior_runs_512 | 38130.523 | 381175.0 | 109322998.0 |

Startup rows represent one startup rather than one completed run. Raw latency observations, first-output timing, throughput, database counts and sample sizes are in diagnostics.json; small-sample tails are descriptive, not SLO estimates.
