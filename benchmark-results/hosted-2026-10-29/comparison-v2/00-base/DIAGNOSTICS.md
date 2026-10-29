Diagnostic intervals include CPU clock reads and concurrency latency bookkeeping; Criterion remains the headline timing source.

| Workload | CPU µs/run | Allocations/run | Requested bytes/run |
| --- | ---: | ---: | ---: |
| context_entries_0 | 15.479 | 275.0 | 30320.0 |
| context_entries_1024 | 1103.108 | 11540.0 | 2600814.0 |
| conversations_engine_1 | 197.645 | 1179.1 | 137050.5 |
| conversations_engine_16 | 131.777 | 1179.1 | 137050.5 |
| conversations_engine_64 | 120.227 | 1179.1 | 137050.5 |
| conversations_sqlite_1 | 10534.315 | 14579.3 | 2415354.7 |
| conversations_sqlite_16 | 9717.521 | 14605.6 | 2419309.0 |
| conversations_sqlite_64 | 9805.255 | 14565.2 | 2417862.2 |
| durable_four_tool_rounds | 11916.004 | 14602.0 | 2416948.0 |
| four_tool_rounds | 60.523 | 1177.0 | 133636.0 |
| minimal | 27.336 | 275.0 | 30320.0 |
| recovery_prior_runs_0 | 7116.340 | 11128.0 | 1865478.0 |
| recovery_prior_runs_512 | 54621.899 | 381175.0 | 109322998.0 |
| startup_sqlite | 1118.542 | 28.0 | 1851.0 |
| stream_chunks_1 | 17.728 | 278.0 | 39651.0 |
| stream_chunks_1024 | 85.004 | 1301.0 | 113307.0 |
| stream_chunks_1024_observed | 256.854 | 1331.0 | 139955.0 |
| stream_chunks_1_observed | 81.036 | 280.0 | 43107.0 |
| stream_chunks_64 | 22.572 | 341.0 | 44187.0 |
| stream_chunks_64_observed | 95.152 | 351.0 | 50763.0 |
| tools_8_bound_1 | 76.647 | 1372.0 | 160293.0 |
| tools_8_bound_4 | 72.695 | 1331.0 | 155477.0 |
| tools_8_bound_8 | 69.094 | 1325.0 | 154733.0 |

Startup rows represent one startup rather than one completed run. Raw latency observations, first-output timing, throughput, database counts and sample sizes are in diagnostics.json; small-sample tails are descriptive, not SLO estimates.
