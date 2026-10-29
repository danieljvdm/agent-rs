Diagnostic intervals include CPU clock reads and concurrency latency bookkeeping; Criterion remains the headline timing source.

| Workload | CPU µs/run | Allocations/run | Requested bytes/run |
| --- | ---: | ---: | ---: |
| context_entries_0 | 10.000 | 281.0 | 30752.0 |
| context_entries_1024 | 402.000 | 11546.0 | 2601246.0 |
| conversations_engine_1 | 74.828 | 1201.1 | 138578.5 |
| conversations_engine_16 | 99.141 | 1201.1 | 138582.5 |
| conversations_engine_64 | 76.297 | 1201.1 | 138578.5 |
| conversations_sqlite_1 | 4774.422 | 14647.9 | 2418937.8 |
| conversations_sqlite_16 | 4595.578 | 14627.2 | 2421050.4 |
| conversations_sqlite_64 | 4686.016 | 14617.0 | 2422127.1 |
| durable_four_tool_rounds | 6330.000 | 14714.0 | 2424704.0 |
| four_tool_rounds | 44.000 | 1199.0 | 135164.0 |
| minimal | 16.000 | 281.0 | 30752.0 |
| recovery_prior_runs_0 | 8066.000 | 11150.0 | 1867433.0 |
| recovery_prior_runs_512 | 42587.000 | 381197.0 | 109324953.0 |
| startup_sqlite | 2914.000 | 30.0 | 2419.0 |
| stream_chunks_1 | 17.000 | 284.0 | 40083.0 |
| stream_chunks_1024 | 38.000 | 1307.0 | 113739.0 |
| stream_chunks_1024_observed | 140.000 | 1351.0 | 152195.0 |
| stream_chunks_1_observed | 43.000 | 289.0 | 43731.0 |
| stream_chunks_64 | 12.000 | 347.0 | 44619.0 |
| stream_chunks_64_observed | 35.000 | 360.0 | 51387.0 |
| tools_8_bound_1 | 47.000 | 1396.0 | 161973.0 |
| tools_8_bound_4 | 44.000 | 1355.0 | 157157.0 |
| tools_8_bound_8 | 64.000 | 1349.0 | 156413.0 |

Startup rows represent one startup rather than one completed run. Raw latency observations, first-output timing, throughput, database counts and sample sizes are in diagnostics.json; small-sample tails are descriptive, not SLO estimates.
