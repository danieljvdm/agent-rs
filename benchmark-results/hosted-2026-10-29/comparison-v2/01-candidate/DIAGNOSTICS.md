Diagnostic intervals include CPU clock reads and concurrency latency bookkeeping; Criterion remains the headline timing source.

| Workload | CPU µs/run | Allocations/run | Requested bytes/run |
| --- | ---: | ---: | ---: |
| context_entries_0 | 15.194 | 275.0 | 30320.0 |
| context_entries_1024 | 1102.958 | 11540.0 | 2600814.0 |
| conversations_engine_1 | 197.538 | 1179.1 | 137050.5 |
| conversations_engine_16 | 130.590 | 1179.1 | 137050.5 |
| conversations_engine_64 | 122.584 | 1179.1 | 137050.5 |
| conversations_sqlite_1 | 10482.712 | 14578.0 | 2415290.2 |
| conversations_sqlite_16 | 9526.704 | 14604.6 | 2419277.2 |
| conversations_sqlite_64 | 9425.825 | 14565.3 | 2417895.1 |
| durable_four_tool_rounds | 12512.284 | 14604.0 | 2417087.0 |
| four_tool_rounds | 59.731 | 1177.0 | 133636.0 |
| minimal | 15.725 | 275.0 | 30320.0 |
| recovery_prior_runs_0 | 7043.509 | 11128.0 | 1865478.0 |
| recovery_prior_runs_512 | 50630.623 | 381175.0 | 109322998.0 |
| startup_sqlite | 1075.101 | 28.0 | 1851.0 |
| stream_chunks_1 | 17.172 | 278.0 | 39651.0 |
| stream_chunks_1024 | 72.621 | 1301.0 | 113307.0 |
| stream_chunks_1024_observed | 254.214 | 1331.0 | 139955.0 |
| stream_chunks_1_observed | 78.647 | 280.0 | 43107.0 |
| stream_chunks_64 | 21.310 | 341.0 | 44187.0 |
| stream_chunks_64_observed | 95.633 | 351.0 | 50763.0 |
| tools_8_bound_1 | 75.486 | 1372.0 | 160293.0 |
| tools_8_bound_4 | 72.234 | 1331.0 | 155477.0 |
| tools_8_bound_8 | 68.933 | 1325.0 | 154733.0 |

Startup rows represent one startup rather than one completed run. Raw latency observations, first-output timing, throughput, database counts and sample sizes are in diagnostics.json; small-sample tails are descriptive, not SLO estimates.
