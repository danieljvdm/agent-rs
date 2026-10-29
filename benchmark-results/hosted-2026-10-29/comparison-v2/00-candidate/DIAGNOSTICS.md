Diagnostic intervals include CPU clock reads and concurrency latency bookkeeping; Criterion remains the headline timing source.

| Workload | CPU µs/run | Allocations/run | Requested bytes/run |
| --- | ---: | ---: | ---: |
| context_entries_0 | 15.248 | 275.0 | 30320.0 |
| context_entries_1024 | 1123.932 | 11540.0 | 2600814.0 |
| conversations_engine_1 | 199.744 | 1179.1 | 137050.5 |
| conversations_engine_16 | 127.756 | 1179.1 | 137050.5 |
| conversations_engine_64 | 122.743 | 1179.1 | 137050.5 |
| conversations_sqlite_1 | 10401.062 | 14579.3 | 2415355.9 |
| conversations_sqlite_16 | 9496.158 | 14609.6 | 2419489.3 |
| conversations_sqlite_64 | 9302.809 | 14565.2 | 2417868.9 |
| durable_four_tool_rounds | 11627.201 | 14600.0 | 2416810.0 |
| four_tool_rounds | 59.531 | 1177.0 | 133636.0 |
| minimal | 27.136 | 275.0 | 30320.0 |
| recovery_prior_runs_0 | 7046.690 | 11128.0 | 1865480.0 |
| recovery_prior_runs_512 | 54323.685 | 381175.0 | 109322998.0 |
| startup_sqlite | 1120.455 | 28.0 | 1851.0 |
| stream_chunks_1 | 16.912 | 278.0 | 39651.0 |
| stream_chunks_1024 | 73.257 | 1301.0 | 113307.0 |
| stream_chunks_1024_observed | 254.034 | 1331.0 | 139955.0 |
| stream_chunks_1_observed | 79.368 | 280.0 | 43107.0 |
| stream_chunks_64 | 21.435 | 341.0 | 44187.0 |
| stream_chunks_64_observed | 94.481 | 351.0 | 50763.0 |
| tools_8_bound_1 | 74.654 | 1372.0 | 160293.0 |
| tools_8_bound_4 | 71.499 | 1331.0 | 155477.0 |
| tools_8_bound_8 | 68.719 | 1325.0 | 154733.0 |

Startup rows represent one startup rather than one completed run. Raw latency observations, first-output timing, throughput, database counts and sample sizes are in diagnostics.json; small-sample tails are descriptive, not SLO estimates.
