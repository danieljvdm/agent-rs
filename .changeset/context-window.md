---
default: minor
---

# Keep conversations inside the model's context window

`AgentBuilder::context_window(ContextWindow::new(tokens))` names the model's context window. Before a turn that would not fit, the engine asks the model for a summary, records it as `HistoryEntry::Compaction`, and from then on sends the summary with the newest entries in place of everything older; a summary the model cannot write is built from the conversation instead. The context is append-only between compactions, so a provider's prompt cache serves every turn but the first after one. Adapters report a request the provider rejects for its size as `Error::ContextOverflow`, which the engine answers once per turn by compacting and making the turn again. `ModelRequest::allow_tool_calls`, `ProgressKind::CompactionStarted`, `ProgressKind::Compacted` and `LlamaModel::context_window` are new, and `ModelRequest::remaining_tokens` is capped at the room a turn's prompt leaves in the window. See `docs/context-compaction.md`.
