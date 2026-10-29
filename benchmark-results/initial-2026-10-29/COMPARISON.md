Repeated AB/BA comparison. Identical-source runs calibrate variation; they are not improvement evidence.

| Workload | Base median µs/run | Candidate median µs/run | Change |
| --- | ---: | ---: | ---: |
| conversations/engine_workers_4/1 | 102.210 | 124.847 | same source |
| conversations/engine_workers_4/16 | 42.978 | 53.945 | same source |
| conversations/engine_workers_4/64 | 35.376 | 48.528 | same source |
| engine/context_entries_0 | 9.106 | 11.103 | same source |
| engine/context_entries_1024 | 440.415 | 469.161 | same source |
| engine/four_tool_rounds | 38.978 | 41.788 | same source |
| engine/minimal | 9.076 | 9.727 | same source |
| engine/stream_chunks_1 | 9.238 | 10.629 | same source |
| engine/stream_chunks_1024 | 30.570 | 34.612 | same source |
| engine/stream_chunks_64 | 10.692 | 15.289 | same source |
| engine/tools_8_bound_1 | 44.495 | 44.939 | same source |
| engine/tools_8_bound_4 | 40.004 | 40.100 | same source |
| engine/tools_8_bound_8 | 40.173 | 87.341 | same source |
| recovery/restart_to_completion/0 | 31568.646 | 32679.459 | same source |
| recovery/restart_to_completion/512 | 109183.375 | 130657.406 | same source |
| sqlite/durable_four_tool_rounds | 35886.406 | 41277.604 | same source |
| sqlite_conversations/host_workers_4/1 | 31656.422 | 24652.805 | same source |
| sqlite_conversations/host_workers_4/16 | 17386.642 | 10829.843 | same source |
| sqlite_conversations/host_workers_4/64 | 18424.575 | 14031.047 | same source |
