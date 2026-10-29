Diagnostic intervals include CPU clock reads and concurrency latency bookkeeping; Criterion remains the headline timing source.

| Workload | CPU µs/run | Allocations/run | Requested bytes/run |
| --- | ---: | ---: | ---: |
| context_entries_0 | 15.414 | 275.0 | 30320.0 |
| context_entries_1024 | 1104.476 | 11540.0 | 2600814.0 |
| conversations_engine_1 | 195.169 | 1179.1 | 137050.5 |
| conversations_engine_16 | 131.143 | 1179.1 | 137050.5 |
| conversations_engine_64 | 121.929 | 1179.1 | 137050.5 |
| conversations_sqlite_1 | 10327.436 | 14574.6 | 2415113.7 |
| conversations_sqlite_16 | 9470.511 | 14605.2 | 2419317.5 |
| conversations_sqlite_64 | 9320.022 | 14565.2 | 2417872.5 |
| durable_four_tool_rounds | 11649.821 | 14598.0 | 2416697.0 |
| four_tool_rounds | 61.424 | 1177.0 | 133636.0 |
| minimal | 26.614 | 275.0 | 30320.0 |
| recovery_prior_runs_0 | 7029.538 | 11128.0 | 1865478.0 |
| recovery_prior_runs_512 | 50563.749 | 381175.0 | 109322998.0 |
| startup_sqlite | 1065.062 | 28.0 | 1851.0 |
| stream_chunks_1 | 17.026 | 278.0 | 39651.0 |
| stream_chunks_1024 | 72.796 | 1301.0 | 113307.0 |
| stream_chunks_1024_observed | 256.748 | 1331.0 | 139955.0 |
| stream_chunks_1_observed | 78.827 | 280.0 | 43107.0 |
| stream_chunks_64 | 21.896 | 341.0 | 44187.0 |
| stream_chunks_64_observed | 93.008 | 351.0 | 50763.0 |
| tools_8_bound_1 | 75.851 | 1372.0 | 160293.0 |
| tools_8_bound_4 | 72.154 | 1331.0 | 155477.0 |
| tools_8_bound_8 | 70.897 | 1325.0 | 154733.0 |

Startup rows represent one startup rather than one completed run. Raw latency observations, first-output timing, throughput, database counts and sample sizes are in diagnostics.json; small-sample tails are descriptive, not SLO estimates.
