---
default: minor
---

# Replace earlier runs' tool results in context

Implement `ContextPolicy::prior_result` to send later runs of a conversation a compact reference, such as IDs and a count, in place of a tool result an earlier run recorded. Stored history and a run's own results are untouched. A custom `DurableStore` should now bound prior context with `Agent::prior_context` and `Agent::new_run_from`.
