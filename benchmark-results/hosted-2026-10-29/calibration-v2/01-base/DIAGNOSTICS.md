Diagnostic intervals include CPU clock reads and concurrency latency bookkeeping; Criterion remains the headline timing source.

| Workload | CPU µs/run | Allocations/run | Requested bytes/run |
| --- | ---: | ---: | ---: |
| context_entries_0 | 15.233 | 275.0 | 30320.0 |
| context_entries_1024 | 1101.841 | 11540.0 | 2600814.0 |
| conversations_engine_1 | 197.442 | 1179.1 | 137050.5 |
| conversations_engine_16 | 129.905 | 1179.1 | 137050.5 |
| conversations_engine_64 | 121.968 | 1179.1 | 137050.5 |
| conversations_sqlite_1 | 10092.397 | 14574.3 | 2415095.3 |
| conversations_sqlite_16 | 9285.629 | 14606.6 | 2419361.0 |
| conversations_sqlite_64 | 9093.830 | 14565.2 | 2417861.5 |
| durable_four_tool_rounds | 12312.335 | 14604.0 | 2417086.0 |
| four_tool_rounds | 60.583 | 1177.0 | 133636.0 |
| minimal | 23.689 | 275.0 | 30320.0 |
| recovery_prior_runs_0 | 6918.305 | 11128.0 | 1865480.0 |
| recovery_prior_runs_512 | 50925.423 | 381175.0 | 109322998.0 |
| startup_sqlite | 1060.264 | 28.0 | 1851.0 |
| stream_chunks_1 | 17.222 | 278.0 | 39651.0 |
| stream_chunks_1024 | 85.754 | 1301.0 | 113307.0 |
| stream_chunks_1024_observed | 254.200 | 1331.0 | 139955.0 |
| stream_chunks_1_observed | 78.752 | 280.0 | 43107.0 |
| stream_chunks_64 | 21.365 | 341.0 | 44187.0 |
| stream_chunks_64_observed | 93.254 | 351.0 | 50763.0 |
| tools_8_bound_1 | 75.651 | 1372.0 | 160293.0 |
| tools_8_bound_4 | 72.430 | 1331.0 | 155477.0 |
| tools_8_bound_8 | 70.251 | 1325.0 | 154733.0 |

Startup rows represent one startup rather than one completed run. Raw latency observations, first-output timing, throughput, database counts and sample sizes are in diagnostics.json; small-sample tails are descriptive, not SLO estimates.
