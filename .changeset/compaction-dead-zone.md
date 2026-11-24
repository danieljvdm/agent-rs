---
default: patch
---

# Eliminate compaction dead zone on multi-run conversations

Align `ContextWindow::compaction_due` and `output_room` so multi-run conversations trigger compaction before `max_tokens` can collapse to `1`.
