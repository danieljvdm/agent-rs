Diagnostic intervals include CPU clock reads and concurrency latency bookkeeping; Criterion remains the headline timing source.

| Workload | CPU µs/run | Allocations/run | Requested bytes/run |
| --- | ---: | ---: | ---: |
| context_entries_0 | 17.000 | 281.0 | 30752.0 |
| context_entries_1024 | 429.000 | 11546.0 | 2601246.0 |
| conversations_engine_1 | 75.969 | 1201.1 | 138578.5 |
| conversations_engine_16 | 80.219 | 1201.1 | 138578.5 |
| conversations_engine_64 | 90.266 | 1201.1 | 138586.5 |
| conversations_sqlite_1 | 7087.188 | 14667.7 | 2419968.0 |
| conversations_sqlite_16 | 4913.359 | 14627.2 | 2421059.2 |
| conversations_sqlite_64 | 4842.188 | 14617.3 | 2422222.0 |
| durable_four_tool_rounds | 8111.000 | 14702.0 | 2423877.0 |
| four_tool_rounds | 50.000 | 1199.0 | 135164.0 |
| minimal | 19.000 | 281.0 | 30752.0 |
| recovery_prior_runs_0 | 8493.000 | 11150.0 | 1867435.0 |
| recovery_prior_runs_512 | 46238.000 | 381197.0 | 109324950.0 |
| startup_sqlite | 2513.000 | 30.0 | 2419.0 |
| stream_chunks_1 | 14.000 | 284.0 | 40083.0 |
| stream_chunks_1024 | 31.000 | 1307.0 | 113739.0 |
| stream_chunks_1024_observed | 125.000 | 1344.0 | 144803.0 |
| stream_chunks_1_observed | 50.000 | 289.0 | 43731.0 |
| stream_chunks_64 | 20.000 | 347.0 | 44619.0 |
| stream_chunks_64_observed | 48.000 | 360.0 | 51387.0 |
| tools_8_bound_1 | 54.000 | 1396.0 | 161973.0 |
| tools_8_bound_4 | 49.000 | 1355.0 | 157157.0 |
| tools_8_bound_8 | 44.000 | 1349.0 | 156413.0 |

Startup rows represent one startup rather than one completed run. Raw latency observations, first-output timing, throughput, database counts and sample sizes are in diagnostics.json; small-sample tails are descriptive, not SLO estimates.
