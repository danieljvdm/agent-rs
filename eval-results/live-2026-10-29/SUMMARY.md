# Live personal-agent workflow results

Model: `gpt-4.1-2025-04-14`. Fixture: `personal-reminder-v2`. All 9 attempts passed. No failed attempts were discarded.

| Case | Trial | Result | Model calls | Tool calls | Input/output tokens | Host restarts |
| --- | ---: | --- | ---: | ---: | --- | ---: |
| dentist | 1 | PASS | 4 | 4 | 2733 / 191 | 2 |
| passport_unsorted | 1 | PASS | 4 | 4 | 2827 / 186 | 2 |
| approval_denied | 1 | PASS | 4 | 3 | 2690 / 150 | 2 |
| dentist | 2 | PASS | 4 | 4 | 2736 / 192 | 2 |
| passport_unsorted | 2 | PASS | 4 | 4 | 2837 / 189 | 2 |
| approval_denied | 2 | PASS | 4 | 3 | 2690 / 150 | 2 |
| dentist | 3 | PASS | 4 | 4 | 2745 / 196 | 2 |
| passport_unsorted | 3 | PASS | 4 | 4 | 2834 / 189 | 2 |
| approval_denied | 3 | PASS | 4 | 3 | 2690 / 150 | 2 |

Totals: 36 model calls, 33 tool calls, 24782 input tokens and 1593 output tokens. Six reminder files were created in fixture directories; all three denial cases created none.

This is real OpenAI inference over synthetic calendar/preferences and real local file tools. Each attempt restarts the durable host at its input and approval waits. It does not access or change a real personal calendar. `report.json` contains all checks, usage, model contexts, committed state, and effect records. `fixture.rs` and `cli.rs` retain the exact eval source. The earlier smoke run is retained separately and used the v1 grader.
