# Changelog

## 0.2.3 (2026-11-20)

### Features

#### Add `CodeMode::description`

`CodeMode::description(text)` replaces the parent tool description, so callers that discover action signatures on demand can keep the prompt short while keeping `CodeMode`'s child result mapping.

## 0.2.2 (2026-11-18)

### Fixes

#### A closed SQLite store can be reopened at once

`SqliteStore` now unlocks its `.lock` file when it closes or is dropped, instead of relying on the
file closing. A process the host spawned while the store was open could hold a duplicate of the
descriptor until it exec'd, keeping the lock held after `close()`, so reopening the same path failed
with `Error::StoreBusy`.

## 0.2.1 (2026-11-18)

### Features

#### Help a model recover in code mode

Declare result types made of named structs instead of `unknown`, with a type that several tools share declared once; this changes a code mode tool's description, and so the binding of an agent that has one. Answer a script stopped by its call limits, a delegation the run can no longer afford, and a call without a `code` string with an error that says what to change, the last instead of failing the run.

## 0.2.0 (2026-11-15)

### Breaking Changes

#### Describe tool results with an output schema

Read a tool's result schema from `Tool::output_schema`: `typed_tool` derives it from the output type, which must now implement `JsonSchema`, and an MCP tool's wraps the server's declared output schema. Code mode declares each function's return type from it instead of `unknown`; `codemode::description` now takes the tools and `codemode::declaration` takes the output schema beside the spec.

### Features

#### Keep conversations inside the model's context window

Name the model's context window with `AgentBuilder::context_window(ContextWindow::new(tokens))`. Before a turn that would not fit, the engine replaces the oldest part of the context with a summary the model writes, recorded as `HistoryEntry::Compaction`, and it tells each tool call how much room its result has (`ToolContext::max_output_bytes`), refusing a result that does not fit with an error the model can act on. Adapters report a request the provider rejects for its size as `Error::ContextOverflow`, including from other providers behind OpenAI-compatible endpoints, and the engine answers it once per turn by compacting and making the turn again. `ModelRequest::allow_tool_calls`, `ProgressKind::CompactionStarted`, `ProgressKind::Compacted` and `LlamaModel::context_window` are new. See `docs/context-compaction.md`.

## 0.1.3 (2026-11-12)

### Features

#### Add code mode

With the `codemode` feature, `codemode::CodeMode` builds a subagent the model calls with JavaScript that calls the agent's other tools as async functions and returns one value. Each round of calls is a durable tool batch of the script's child run, so calls keep their authorization, approval, retry policy, budgets and crash recovery; each turn replays the script in a fresh, deterministic QuickJS interpreter against the recorded rounds. See `docs/codemode.md`. `codemode::Subprocess` runs the interpreter in a process of its own per turn, the `agent-rs-quickjs` binary or any binary that calls `codemode::serve`.

#### Replace earlier runs' tool results in context

Implement `ContextPolicy::prior_result` to send later runs of a conversation a compact reference, such as IDs and a count, in place of a tool result an earlier run recorded. It is told how many runs back each result is, so the latest can stay whole. Stored history and a run's own results are untouched. A custom `DurableStore` should now bound prior context with `Agent::prior_context` and `Agent::new_run_from`.

## 0.1.2 (2026-11-12)

### Features

#### Add content-free engine tracing

Expose run, model, and tool timings and outcomes through `tracing`, with safe call identifiers and warnings for timeouts, failed cleanup, recovery, and unknown effects. Keep message content out of telemetry and preserve no-subscriber allocation budgets.

## 0.1.1 (2026-11-11)

### Features

#### Encrypt the SQLite store with a key

`SqliteStore::open_with(path, SqliteOptions::default().with_key(SqliteKey::new(bytes)))` encrypts
the database file and its write-ahead log with a 256-bit key you supply. It needs the new
`sqlcipher` feature, which builds SQLite as SQLCipher. Opening with a key on a build without
encryption fails instead of writing an unencrypted file, and a key that does not open the file
is reported as `Error::Config`. `SqliteStore::open` is unchanged.

#### Run agents on a local llama.cpp server

Use `openai::LlamaModel` to run agents on llama.cpp's `llama-server`: string outputs stream as plain text, other outputs are grammar-constrained, and the server's error messages are reported. A reasoning model's thinking streams as the new `ProgressKind::ReasoningDelta`.

## 0.1.0 (2026-11-11)

Initial release.
