Diagnostic intervals include CPU clock reads and concurrency latency bookkeeping; Criterion remains the headline timing source.

| Workload | CPU µs/run | Allocations/run | Requested bytes/run |
| --- | ---: | ---: | ---: |
| context_entries_0 | 15.479 | 275.0 | 30320.0 |
| context_entries_1024 | 1084.829 | 11540.0 | 2600814.0 |
| conversations_engine_1 | 194.605 | 1179.1 | 137050.5 |
| conversations_engine_16 | 133.916 | 1179.1 | 137050.5 |
| conversations_engine_64 | 122.827 | 1179.1 | 137050.5 |
| conversations_sqlite_1 | 10069.638 | 14574.6 | 2415112.6 |
| conversations_sqlite_16 | 9244.047 | 14603.6 | 2419146.5 |
| conversations_sqlite_64 | 9161.580 | 14565.2 | 2417871.2 |
| durable_four_tool_rounds | 11506.987 | 14604.0 | 2417086.0 |
| four_tool_rounds | 59.561 | 1177.0 | 133636.0 |
| minimal | 26.144 | 275.0 | 30320.0 |
| recovery_prior_runs_0 | 6822.651 | 11128.0 | 1865478.0 |
| recovery_prior_runs_512 | 51246.871 | 381175.0 | 109322998.0 |
| startup_sqlite | 1051.012 | 28.0 | 1851.0 |
| stream_chunks_1 | 16.852 | 278.0 | 39651.0 |
| stream_chunks_1024 | 75.506 | 1301.0 | 113307.0 |
| stream_chunks_1024_observed | 256.699 | 1331.0 | 139955.0 |
| stream_chunks_1_observed | 79.673 | 280.0 | 43107.0 |
| stream_chunks_64 | 21.230 | 341.0 | 44187.0 |
| stream_chunks_64_observed | 93.654 | 351.0 | 50763.0 |
| tools_8_bound_1 | 75.196 | 1372.0 | 160293.0 |
| tools_8_bound_4 | 70.977 | 1331.0 | 155477.0 |
| tools_8_bound_8 | 68.227 | 1325.0 | 154733.0 |

Startup rows represent one startup rather than one completed run. Raw latency observations, first-output timing, throughput, database counts and sample sizes are in diagnostics.json; small-sample tails are descriptive, not SLO estimates.
