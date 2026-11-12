# Changelog

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
