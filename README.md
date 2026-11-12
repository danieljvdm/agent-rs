# agent-rs

A resumable agent engine for Rust. One engine runs an agent in memory or as a durable run that survives a restart, with typed tools, approvals, waits and hard limits.

```toml
[dependencies]
agent-rs = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
# For typed tools and typed input or output:
schemars = "1"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
```

Requires Rust 1.89 and a Tokio runtime.

## Quick start

```rust,no_run
use std::sync::Arc;

use agent_rs::{Agent, Conversation, ModelConfig, openai::OpenAiModel};

#[tokio::main]
async fn main() -> agent_rs::Result<()> {
    let model = OpenAiModel::new(std::env::var("OPENAI_API_KEY").unwrap_or_default())?;
    let agent = Agent::builder("assistant-v1", Arc::new(model))
        .config(ModelConfig::new("gpt-4.1"))
        .instructions("Answer in one sentence.")
        .input::<String>()
        .output::<String>()
        .build()?;

    let mut conversation = Conversation::default();
    let answer = agent.run(&mut conversation, "Why is the sky blue?").await?;
    println!("{answer}");
    Ok(())
}
```

`input` and `output` set the types `run` takes and returns, and derive the schemas the model sees. A `Conversation` keeps history between runs.

## Tools

A tool is an async function with typed arguments. `ScriptedModel` replays fixed turns, so this example, and your tests, run without a network.

```rust
use std::sync::Arc;

use agent_rs::{
    Agent, Conversation, ModelResponse, ScriptedModel, ToolCall, ToolOutput, ToolSpec, typed_tool,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

#[derive(Deserialize, JsonSchema)]
struct Add {
    a: i64,
    b: i64,
}

#[tokio::main]
async fn main() -> agent_rs::Result<()> {
    let add = typed_tool(
        ToolSpec::new("add", "1").with_description("Add two integers."),
        |args: Add, _context| async move { Ok(ToolOutput::Complete(args.a + args.b)) },
    );

    let model = ScriptedModel::new(vec![
        ModelResponse::tools(vec![ToolCall {
            id: "call-1".into(),
            name: "add".into(),
            arguments: json!({"a": 2, "b": 3}),
        }]),
        ModelResponse::final_output(json!("2 + 3 = 5")),
    ]);

    let agent = Agent::builder("calculator-v1", Arc::new(model))
        .tool(add)
        .input::<String>()
        .output::<String>()
        .build()?;

    let answer = agent.run(&mut Conversation::default(), "What is 2 + 3?").await?;
    assert_eq!(answer, "2 + 3 = 5");
    Ok(())
}
```

A tool can also:

- return `ToolOutput::Failed` to report a failure to the model;
- return `ToolOutput::Wait` to pause the run for a reply or until a time;
- require approval before each call, with `ToolSpec::with_requires_approval(true)`.

Calls in one model turn run in parallel, up to `Limits::max_parallel_tools`.

## Durable runs

A `Host` runs agents over a store. Every step is committed, so a run that is waiting or interrupted continues after the process restarts.

```rust,no_run
use std::sync::Arc;

use agent_rs::{Agent, Host, HostOptions, OperationStatus, Resolution, sqlite::SqliteStore};
use serde_json::json;

async fn serve(agent: Agent) -> agent_rs::Result<()> {
    let store = Arc::new(SqliteStore::open("agent.sqlite").await?);
    let mut host = Host::start(store.clone(), agent, HostOptions::default())?;

    // Returns once the run is committed. Retry a failed submission with the same key.
    let run_id = host.submit("conversation-1", "request-1", json!("hello"), None).await?;

    // Calls that need a person stay in the run's state until they are resolved.
    let state = host.status(run_id).await?;
    for operation in &state.operations {
        if operation.status == OperationStatus::Approval {
            host.resolve(run_id, &operation.call.id, Resolution::Approve(true)).await?;
        }
    }

    // Accepted work survives shutdown. The next host opened on this file resumes it.
    host.shutdown().await?;
    store.close().await
}
```

`MemoryStore` is the same host without a database. Implement `DurableStore` to use another one. The [durability contract](https://github.com/danieljvdm/agent-rs/blob/main/docs/durability.md) says what is guaranteed about external effects, and `cargo run --example durable_workflow` walks through approval, input, a timer and a restart.

## Features

| Feature | Default | Adds |
| --- | --- | --- |
| `sqlite` | yes | `sqlite::SqliteStore`, a durable store in one file (SQLite is bundled) |
| `sqlcipher` | | Builds SQLite as SQLCipher, so `SqliteStore::open_with` can encrypt the file with a key you supply |
| `openai` | yes | `openai::OpenAiModel` (Chat Completions), `openai::OpenAiResponsesModel` (Responses) and `openai::LlamaModel` (a local llama.cpp `llama-server`) |
| `postgres` | | `postgres::PostgresStore` |
| `memory` | | `memory::MemoryPolicy`: summaries and retrieval over earlier conversation |
| `mcp` | | `mcp::McpClient`: tools from an MCP server over stdio |
| `google-calendar` | | `google_calendar::GoogleCalendar`: calendar reads and reminders as tools |
| `codemode` | | `codemode::CodeMode`: one tool whose JavaScript calls the agent's other tools, run in QuickJS in-process or in a subprocess (`agent-rs-quickjs`) |

With `default-features = false` the crate is the engine, the host and `MemoryStore`.

## More

- **Limits.** `Limits` caps model calls, tool calls, tokens, bytes, parallelism and time. A run that hits one stops with `Error::Stopped`.
- **Errors.** Every call returns one `Error` type. Its variants separate input to fix, work that failed, storage that could not be reached, and a request the store refused.
- **Providers.** Implement `Model` for another provider. See [model adapters](https://github.com/danieljvdm/agent-rs/blob/main/docs/model-adapters.md).
- **Context.** A `ContextPolicy` pins history entries, adds derived context to each model call, and replaces earlier runs' tool results with compact references. [Memory](https://github.com/danieljvdm/agent-rs/blob/main/docs/memory.md) is the built-in policy.
- **Subagents.** `AgentBuilder::subagents` lets the model delegate to child agents as tools. See [attached subagents](https://github.com/danieljvdm/agent-rs/blob/main/docs/attached-subagents.md).
- **Code mode.** `codemode::CodeMode` gives the model one tool that runs a script calling other tools, each call durable as it happens. See [code mode](https://github.com/danieljvdm/agent-rs/blob/main/docs/codemode.md).
- **Steering.** `Host::steer` adds an instruction to a run that is already going. See [steering](https://github.com/danieljvdm/agent-rs/blob/main/docs/steering.md).
- **Automations.** `Host::fire_automations` starts runs on an interval. See [automations](https://github.com/danieljvdm/agent-rs/blob/main/docs/automation.md).
- **Progress.** `HostOptions::with_events` streams best-effort `ProgressEvent`s; a full channel drops events and never blocks a run.
- **Telemetry.** Content-free model, tool, and run spans use `tracing`. See [telemetry](https://github.com/danieljvdm/agent-rs/blob/main/docs/telemetry.md).
- **Other guides.** [PostgreSQL](https://github.com/danieljvdm/agent-rs/blob/main/docs/postgres.md), [MCP](https://github.com/danieljvdm/agent-rs/blob/main/docs/mcp.md), [Google Calendar](https://github.com/danieljvdm/agent-rs/blob/main/docs/calendar-integration.md), [process recovery](https://github.com/danieljvdm/agent-rs/blob/main/docs/process-recovery.md).

The repository also holds a [personal-agent application](https://github.com/danieljvdm/agent-rs/blob/main/docs/personal-agent.md) built on the crate: a local API and an inbox for approvals.

## Not included

Distributed workers, bindings for other languages, in-flight preemption, agent graphs, nested or detached subagents, MCP transports other than stdio, and code mode scripts that call subagents.

## Development

```sh
cargo test --all-features --locked
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo fmt --check
```

`rust-toolchain.toml` pins the toolchain CI uses. [Benchmarks](https://github.com/danieljvdm/agent-rs/blob/main/docs/benchmarks.md) and [verification](https://github.com/danieljvdm/agent-rs/blob/main/docs/verification.md) describe how cost and behaviour are measured. [Releasing](https://github.com/danieljvdm/agent-rs/blob/main/docs/releasing.md) covers change files and how a version is published.

## License

MIT
