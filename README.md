# Yielded Agent · Rust

A small, Rust-native agent runtime with a reusable execution engine and an optional SQLite durable host. Built for [YIE-245](https://linear.app/yielded/issue/YIE-245).

The same engine drives in-memory conversations and durable runs. Applications supply model clients, typed tools, authorization policy, and presentation. The runtime owns bounded model/tool execution, checkpoint transitions, and recovery.

```rust
use std::sync::Arc;
use serde_json::json;
use yielded_agent::*;

# async fn example() -> Result<()> {
let agent = Agent::new::<String, String>(
    "assistant-v1", "Answer the request.",
    Arc::new(ScriptedModel::new(vec![ModelResponse::final_output(json!("hello"))])),
    ModelConfig::default(), vec![], Limits::default(),
)?;
let engine = Engine::new(agent);
let mut conversation = Conversation::default();
let output: String = engine.run(&mut conversation, "hello").await?;
assert_eq!(output, "hello");
# Ok(()) }
```

`Agent::new::<Input, Output>` derives schemas and validates deserialization. `typed_tool::<Arguments, Output, _, _>` registers typed handlers behind one dispatch boundary. Use `#[serde(deny_unknown_fields)]` on argument structs when unknown fields should be rejected, and implement semantic validation in custom `Tool::validate` where deserialization is insufficient.

`Conversation::last_run` retains the run state, including waits and uncertain outcomes. Resolve its pending operation and call `Engine::resume` to continue; `run` refuses a new input while that conversation has unfinished work. Advanced callers can use `Agent::new_run` and `Engine::drive` directly.

Default features enable `sqlite` and `openai`. `default-features = false` builds the engine without either adapter. Tokio runs asynchronous work; a dedicated bounded worker keeps SQLite operations off its worker threads.

## Durable execution

```rust
# #[cfg(feature = "sqlite")]
# async fn durable(agent: yielded_agent::Agent) -> yielded_agent::Result<()> {
use std::{sync::Arc, time::Duration};
use yielded_agent::{host::{Host, HostOptions}, sqlite::SqliteStore};
use serde_json::json;
let store = Arc::new(SqliteStore::open("agent.sqlite").await?);
let mut host = Host::start(store.clone(), agent, HostOptions {
    max_concurrent_runs: 16,
    poll_interval: Duration::from_millis(50),
    events: None,
})?;
let run_id = host.submit("conversation-1", "client-request-1", json!("hello"), None).await?;
// RunId is returned only after acceptance commits. Reuse the same key to retry admission.
let state = host.status(run_id).await?;
println!("{:?}", state.phase);
// Shutdown preserves accepted work. Reopening resumes eligible checkpoints.
host.shutdown().await?;
store.close().await?;
# Ok(()) }
```

Waits and uncertain operations are visible in `RunState::operations`. Resolve the exact call ID with `Resolution::Approve`, `Resolution::Input`, or `Resolution::Reconcile`. Waiting releases execution slots. The host rediscovers due work from SQLite on startup and periodically, so progress does not depend on transient notifications.

See [the durability contract](docs/durability.md) for external-effect guarantees and application responsibilities. Run `cargo run --example durable_workflow` for the restart/wait/reconciliation example.

The [personal-agent workflow eval](docs/personal-workflow-eval.md) exercises a model choosing tools, requesting missing information, surviving host restarts, and creating a reminder only after approval. Run `scripts/eval-personal.sh --trials 3 --out eval-results/my-live-run` to use the existing Infisical credentials. Its deterministic counterpart and negative grading cases run in ordinary tests.

[Process recovery tests](docs/process-recovery.md) also terminate and relaunch real subprocesses at acceptance, dispatch, and outcome-commit boundaries. The opt-in restart soak keeps a persisted future deadline across repeated process deaths.

## Providers and limits

`ScriptedModel` is a deterministic in-process provider for tests and benchmarks. `openai::OpenAiModel` implements Chat Completions SSE, buffers and validates a complete response before tools start, and accepts explicit credentials. It does not load credentials from the environment or retry HTTP requests implicitly. Supply the model name through `ModelConfig`.

`Limits` bounds model attempts, tool attempts, tokens, output bytes, context bytes, parallelism, operation time, and cumulative active time. `RunState` carries the original limits and counters through recovery. Context selection keeps complete tool-call/result groups and the current input. Retained history is separate from model context.

Progress events are transient and best effort through a caller-supplied bounded Tokio channel; a full channel drops events. Reconnect using durable status/history, not token deltas. Standard `tracing` emits run spans without prompts or credentials.

## Development

The checked-in `rust-toolchain.toml` pins Rust and the lint/format components. If Homebrew Cargo takes precedence over rustup, use `scripts/rust.sh` in place of `cargo` to select the pinned toolchain explicitly.

```sh
cargo test --all-features --locked
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo fmt --check
cargo check --no-default-features --locked
cargo bench --bench agent_loop --locked
```

[Benchmark instructions and retained results](benchmark-results/README.md) describe fixed workloads, correctness assertions, process CPU and allocation runs, and revision comparisons. Timings are informational until observed variance supports a timing gate.

[Verification evidence](docs/verification.md) records the 45 passing tests, restart workflow, and [initial AB/BA benchmark comparison](benchmark-results/initial-2026-10-29/COMPARISON.md).

The manual [performance workflow](.github/workflows/performance.yml) runs longer comparisons or same-source calibration on one hosted Linux runner. [Calibration notes](docs/benchmark-calibration.md) explain the paired measurements and why timing gates remain informational until enough independent runs establish useful thresholds.

This is a Rust-only initial implementation. There are no distributed leases, Node bindings, Cloudflare adapter, arbitrary active-run input joining, agent graphs, or placeholder subagent/MCP services.
