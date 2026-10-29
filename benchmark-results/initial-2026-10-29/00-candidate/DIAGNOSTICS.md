Diagnostic intervals include CPU clock reads and concurrency latency bookkeeping; Criterion remains the headline timing source.

| Workload | CPU µs/run | Allocations/run | Requested bytes/run |
| --- | ---: | ---: | ---: |
| context_entries_0 | 10.000 | 281.0 | 30752.0 |
| context_entries_1024 | 472.000 | 11546.0 | 2601246.0 |
| conversations_engine_1 | 75.812 | 1201.1 | 138578.5 |
| conversations_engine_16 | 82.531 | 1201.1 | 138578.5 |
| conversations_engine_64 | 64.109 | 1201.1 | 138586.5 |
| conversations_sqlite_1 | 8647.422 | 14645.3 | 2418805.0 |
| conversations_sqlite_16 | 5239.047 | 14626.4 | 2420944.5 |
| conversations_sqlite_64 | 5687.469 | 14618.1 | 2422132.9 |
| durable_four_tool_rounds | 13157.000 | 14730.0 | 2425758.0 |
| four_tool_rounds | 46.000 | 1199.0 | 135164.0 |
| minimal | 16.000 | 281.0 | 30752.0 |
| recovery_prior_runs_0 | 9976.000 | 11150.0 | 1867432.0 |
| recovery_prior_runs_512 | 51043.000 | 381197.0 | 109324953.0 |
| startup_sqlite | 3517.000 | 30.0 | 2419.0 |
| stream_chunks_1 | 11.000 | 284.0 | 40083.0 |
| stream_chunks_1024 | 31.000 | 1307.0 | 113739.0 |
| stream_chunks_1024_observed | 127.000 | 1351.0 | 152195.0 |
| stream_chunks_1_observed | 67.000 | 289.0 | 43731.0 |
| stream_chunks_64 | 16.000 | 347.0 | 44619.0 |
| stream_chunks_64_observed | 29.000 | 360.0 | 51387.0 |
| tools_8_bound_1 | 48.000 | 1396.0 | 161973.0 |
| tools_8_bound_4 | 45.000 | 1355.0 | 157157.0 |
| tools_8_bound_8 | 44.000 | 1349.0 | 156413.0 |

Startup rows represent one startup rather than one completed run. Raw latency observations, first-output timing, throughput, database counts and sample sizes are in diagnostics.json; small-sample tails are descriptive, not SLO estimates.
