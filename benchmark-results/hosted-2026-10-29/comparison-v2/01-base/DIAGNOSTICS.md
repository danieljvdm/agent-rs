Diagnostic intervals include CPU clock reads and concurrency latency bookkeeping; Criterion remains the headline timing source.

| Workload | CPU µs/run | Allocations/run | Requested bytes/run |
| --- | ---: | ---: | ---: |
| context_entries_0 | 15.459 | 275.0 | 30320.0 |
| context_entries_1024 | 1091.582 | 11540.0 | 2600814.0 |
| conversations_engine_1 | 197.545 | 1179.1 | 137050.5 |
| conversations_engine_16 | 134.517 | 1179.1 | 137050.5 |
| conversations_engine_64 | 123.039 | 1179.1 | 137050.5 |
| conversations_sqlite_1 | 10473.102 | 14577.6 | 2415266.9 |
| conversations_sqlite_16 | 9487.249 | 14605.5 | 2419302.6 |
| conversations_sqlite_64 | 9469.523 | 14565.2 | 2417856.8 |
| durable_four_tool_rounds | 11635.706 | 14605.0 | 2417142.0 |
| four_tool_rounds | 60.193 | 1177.0 | 133636.0 |
| minimal | 26.680 | 275.0 | 30320.0 |
| recovery_prior_runs_0 | 6901.150 | 11128.0 | 1865480.0 |
| recovery_prior_runs_512 | 51731.210 | 381175.0 | 109322998.0 |
| startup_sqlite | 1068.854 | 28.0 | 1851.0 |
| stream_chunks_1 | 17.492 | 278.0 | 39651.0 |
| stream_chunks_1024 | 72.916 | 1301.0 | 113307.0 |
| stream_chunks_1024_observed | 258.406 | 1331.0 | 139955.0 |
| stream_chunks_1_observed | 79.559 | 280.0 | 43107.0 |
| stream_chunks_64 | 21.896 | 341.0 | 44187.0 |
| stream_chunks_64_observed | 94.246 | 351.0 | 50763.0 |
| tools_8_bound_1 | 76.888 | 1372.0 | 160293.0 |
| tools_8_bound_4 | 71.594 | 1331.0 | 155477.0 |
| tools_8_bound_8 | 68.534 | 1325.0 | 154733.0 |

Startup rows represent one startup rather than one completed run. Raw latency observations, first-output timing, throughput, database counts and sample sizes are in diagnostics.json; small-sample tails are descriptive, not SLO estimates.
