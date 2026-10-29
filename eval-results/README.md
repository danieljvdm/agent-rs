# Personal workflow evidence

The live and scripted personal-agent evaluator writes one new directory per invocation. Reports contain synthetic calendar/reminder data, model outputs, counts, and grading results. Credentials are never included. See [the workflow guide](../docs/personal-workflow-eval.md) for commands and interpretation.

Keep failed attempts when comparing prompts or runtime changes. SQLite files are local ignored artifacts; the JSON reports retain the inspectable run state and effect records.

The [2026-10-29 v2 live evaluation](live-2026-10-29/SUMMARY.md) passed all nine cases (three scenarios, three trials). It used 36 model calls, 33 tool calls, 24,782 input tokens, and 1,593 output tokens. Six approved reminder files were created and all three denied cases created none. The earlier [v1 smoke report](live-smoke-2026-10-29/report.json) is retained with its exact source, before the grader was strengthened to require successful source reads in the model context.
