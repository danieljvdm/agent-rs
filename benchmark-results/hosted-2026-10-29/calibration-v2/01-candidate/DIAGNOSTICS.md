Diagnostic intervals include CPU clock reads and concurrency latency bookkeeping; Criterion remains the headline timing source.

| Workload | CPU µs/run | Allocations/run | Requested bytes/run |
| --- | ---: | ---: | ---: |
| context_entries_0 | 15.268 | 275.0 | 30320.0 |
| context_entries_1024 | 1096.657 | 11540.0 | 2600814.0 |
| conversations_engine_1 | 196.850 | 1179.1 | 137050.5 |
| conversations_engine_16 | 134.064 | 1179.1 | 137050.5 |
| conversations_engine_64 | 122.239 | 1179.1 | 137050.5 |
| conversations_sqlite_1 | 10395.337 | 14575.0 | 2415135.3 |
| conversations_sqlite_16 | 9473.749 | 14604.4 | 2419196.5 |
| conversations_sqlite_64 | 9310.931 | 14565.2 | 2417873.3 |
| durable_four_tool_rounds | 12478.233 | 14600.0 | 2416810.0 |
| four_tool_rounds | 59.956 | 1177.0 | 133636.0 |
| minimal | 26.249 | 275.0 | 30320.0 |
| recovery_prior_runs_0 | 6898.264 | 11128.0 | 1865478.0 |
| recovery_prior_runs_512 | 52609.094 | 381175.0 | 109322998.0 |
| startup_sqlite | 1093.600 | 28.0 | 1851.0 |
| stream_chunks_1 | 17.037 | 278.0 | 39651.0 |
| stream_chunks_1024 | 72.570 | 1301.0 | 113307.0 |
| stream_chunks_1024_observed | 259.942 | 1331.0 | 139955.0 |
| stream_chunks_1_observed | 79.238 | 280.0 | 43107.0 |
| stream_chunks_64 | 21.330 | 341.0 | 44187.0 |
| stream_chunks_64_observed | 96.800 | 351.0 | 50763.0 |
| tools_8_bound_1 | 74.574 | 1372.0 | 160293.0 |
| tools_8_bound_4 | 71.273 | 1331.0 | 155477.0 |
| tools_8_bound_8 | 68.597 | 1325.0 | 154733.0 |

Startup rows represent one startup rather than one completed run. Raw latency observations, first-output timing, throughput, database counts and sample sizes are in diagnostics.json; small-sample tails are descriptive, not SLO estimates.
