---
default: minor
---

# Keep conversations inside the model's context window

Name the model's context window with `AgentBuilder::context_window(ContextWindow::new(tokens))`. Before a turn that would not fit, the engine replaces the oldest part of the context with a summary the model writes, recorded as `HistoryEntry::Compaction`, and it refuses a tool result that one turn has no room for with an error the model can act on. Adapters report a request the provider rejects for its size as `Error::ContextOverflow`, which the engine answers once per turn by compacting and making the turn again. `ModelRequest::allow_tool_calls`, `ProgressKind::CompactionStarted`, `ProgressKind::Compacted` and `LlamaModel::context_window` are new. See `docs/context-compaction.md`.
