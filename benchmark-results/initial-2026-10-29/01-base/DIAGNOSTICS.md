Diagnostic intervals include CPU clock reads and concurrency latency bookkeeping; Criterion remains the headline timing source.

| Workload | CPU µs/run | Allocations/run | Requested bytes/run |
| --- | ---: | ---: | ---: |
| context_entries_0 | 10.000 | 281.0 | 30752.0 |
| context_entries_1024 | 437.000 | 11546.0 | 2601246.0 |
| conversations_engine_1 | 70.469 | 1201.1 | 138578.5 |
| conversations_engine_16 | 83.719 | 1201.1 | 138582.5 |
| conversations_engine_64 | 90.031 | 1201.1 | 138586.5 |
| conversations_sqlite_1 | 8367.047 | 14647.1 | 2418896.1 |
| conversations_sqlite_16 | 7120.062 | 14620.8 | 2420594.4 |
| conversations_sqlite_64 | 5436.578 | 14616.8 | 2422071.6 |
| durable_four_tool_rounds | 11940.000 | 14679.0 | 2422367.0 |
| four_tool_rounds | 44.000 | 1199.0 | 135164.0 |
| minimal | 20.000 | 281.0 | 30752.0 |
| recovery_prior_runs_0 | 8957.000 | 11150.0 | 1867435.0 |
| recovery_prior_runs_512 | 47800.000 | 381197.0 | 109324953.0 |
| startup_sqlite | 3213.000 | 30.0 | 2419.0 |
| stream_chunks_1 | 12.000 | 284.0 | 40083.0 |
| stream_chunks_1024 | 31.000 | 1307.0 | 113739.0 |
| stream_chunks_1024_observed | 152.000 | 1351.0 | 152195.0 |
| stream_chunks_1_observed | 55.000 | 289.0 | 43731.0 |
| stream_chunks_64 | 14.000 | 347.0 | 44619.0 |
| stream_chunks_64_observed | 50.000 | 360.0 | 51387.0 |
| tools_8_bound_1 | 47.000 | 1396.0 | 161973.0 |
| tools_8_bound_4 | 44.000 | 1355.0 | 157157.0 |
| tools_8_bound_8 | 49.000 | 1349.0 | 156413.0 |

Startup rows represent one startup rather than one completed run. Raw latency observations, first-output timing, throughput, database counts and sample sizes are in diagnostics.json; small-sample tails are descriptive, not SLO estimates.
