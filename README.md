# Yielded Agent · Rust

A small, Rust-native agent runtime with a reusable execution engine and an optional SQLite durable host.

The same engine drives in-memory conversations and durable runs. Applications supply model clients, typed tools, authorization policy, and presentation. The runtime owns bounded model/tool execution, checkpoint transitions, and recovery.

```rust
use std::sync::Arc;
use serde_json::json;
use yielded_agent::*;

# async fn example() -> Result<()> {
let model = ScriptedModel::new(vec![ModelResponse::final_output(json!("hello"))]);
let agent = Agent::builder("assistant-v1", Arc::new(model))
    .instructions("Answer the request.")
    .input::<String>()
    .output::<String>()
    .build()?;
let mut conversation = Conversation::default();
let output: String = agent.run(&mut conversation, "hello").await?;
assert_eq!(output, "hello");
# Ok(()) }
```

`Agent::builder(id, model)` takes the two required parts; instructions, model configuration, tools, limits, an authorizer and a context policy each have a default and their own method, and `build` checks the whole definition once. `input::<Input>()` and `output::<Output>()` derive schemas and validate deserialization, and make `Agent<Input, Output>::run` take an `Input` and return an `Output`, so a mismatch does not compile. The plain `Agent` (both types `serde_json::Value`) is the erased form that hosts and stores take; `Agent::erased` converts a typed agent. Cloning an agent shares its definition, and its durable binding is composed once at `build`. `typed_tool::<Arguments, Output, _, _>` registers typed handlers behind one dispatch boundary. Use `#[serde(deny_unknown_fields)]` on argument structs when unknown fields should be rejected, and implement semantic validation in custom `Tool::validate` where deserialization is insufficient.

`Conversation::last_run` retains the run state, including waits and uncertain outcomes. A run that does not complete returns an error naming how it ended: `Error::Waiting`, `Error::Stopped(reason)`, `Error::Failed(message)` or `Error::Cancelled`. Resolve a waiting run's pending operation and call `Agent::resume` to continue; `run` refuses a new input while that conversation has unfinished work. Advanced callers can use `Agent::new_run` and `Agent::drive` directly.

Every fallible call returns the one `Error` type. Its variants say what a caller would do about a failure: configuration or input to fix, a run's own work that went wrong, storage that could not be reached, or a store refusing a request. Errors that wrap another keep it as `source()`. While driving a run, `Error::fails_run` decides between settling the run as failed and returning the error with the run left resumable.

Everything except the optional features is exported from the crate root, so `use yielded_agent::*;` brings in the engine, the durable host and the storage contract. Each optional feature lives in its own module: the adapters `yielded_agent::sqlite`, `yielded_agent::postgres` (opt-in) and `yielded_agent::openai`, and the conversation-memory context policy `yielded_agent::memory`. Default features enable `sqlite` and `openai`. `default-features = false` builds the engine and the durable host without either adapter; the host then runs over the in-process `MemoryStore` or any other `DurableStore`. Tokio runs asynchronous work; a dedicated bounded worker keeps SQLite operations off its worker threads.

The optional `mcp` feature, in `yielded_agent::mcp`, starts an application-owned MCP server over stdio and registers the tools the application selects as ordinary `Tool`s, under its own aliases and policy. See [MCP tools over stdio](https://github.com/danieljvdm/agent-rs/blob/main/docs/mcp.md).

## Durable execution

```rust
# #[cfg(feature = "sqlite")]
# async fn durable(agent: yielded_agent::Agent) -> yielded_agent::Result<()> {
use std::{sync::Arc, time::Duration};
use yielded_agent::{Host, HostOptions, sqlite::SqliteStore};
use serde_json::json;
let store = Arc::new(SqliteStore::open("agent.sqlite").await?);
let options = HostOptions::default().with_poll_interval(Duration::from_millis(50));
let mut host = Host::start(store.clone(), agent, options)?;
let run_id = host.submit("conversation-1", "client-request-1", json!("hello"), None).await?;
// RunId is returned only after acceptance commits. Reuse the same key to retry admission.
let state = host.status(run_id).await?;
println!("{:?}", state.phase);
// Shutdown preserves accepted work. Reopening resumes eligible checkpoints.
host.shutdown().await?;
store.close().await?;
# Ok(()) }
```

Waits and uncertain operations are visible in `RunState::operations`. Resolve the exact call ID with `Resolution::Approve`, `Resolution::Input`, or `Resolution::Reconcile`. Waiting releases execution slots. The host rediscovers due work from its store on startup and periodically, so progress does not depend on transient notifications.

Use `Host::steer(run_id, key, instruction)` to durably queue new instructions for an active run without an input wait, on a store that implements `SteeringStore` (`MemoryStore` and `SqliteStore` do). Receipts are idempotent per run. Instructions enter the run's history at its next model dispatch and stay pinned in its context; in-flight work and explicit waits continue normally. See [durable steering](https://github.com/danieljvdm/agent-rs/blob/main/docs/steering.md) for the completion race, bounds, and receipt status contract.

See [the durability contract](https://github.com/danieljvdm/agent-rs/blob/main/docs/durability.md) for external-effect guarantees and application responsibilities. Run `cargo run --example durable_workflow` for the restart/wait/reconciliation example.

The optional [`postgres` feature](https://github.com/danieljvdm/agent-rs/blob/main/docs/postgres.md) adds `yielded_agent::postgres::PostgresStore`, which runs the same host over a PostgreSQL schema owned by one dedicated session, so a service can restart on another machine without sharing a SQLite file. `scripts/test-postgres.sh` runs the shared durability contract, the PostgreSQL-only tests and the workflow example against an isolated local database. [Deployment targets](https://github.com/danieljvdm/agent-rs/blob/main/docs/deployment-targets.md) records the next independent slices (distributed workers, Node/Bun bindings, Cloudflare); none of them is implemented.

The [personal-agent workflow eval](https://github.com/danieljvdm/agent-rs/blob/main/docs/personal-workflow-eval.md) exercises a model choosing tools, requesting missing information, surviving host restarts, and creating a reminder only after approval. Run `scripts/eval-personal.sh --trials 3 --out eval-results/my-live-run` for a live run; the script loads credentials from the maintainers' Infisical project. Its deterministic counterpart and negative grading cases run in ordinary tests.

The optional [`google-calendar` integration](https://github.com/danieljvdm/agent-rs/blob/main/docs/calendar-integration.md) connects those typed tools to real calendar reads and private popup reminders, with application-supplied credentials, approval and read-only reconciliation of uncertain writes. The same eval supports stateful HTTP fixtures and explicitly opted-in live service tests in isolated calendars.

[Process recovery tests](https://github.com/danieljvdm/agent-rs/blob/main/docs/process-recovery.md) also terminate and relaunch real subprocesses at acceptance, dispatch, and outcome-commit boundaries. The opt-in restart soak keeps a persisted future deadline across repeated process deaths.

[Interval automations](https://github.com/danieljvdm/agent-rs/blob/main/docs/automation.md) create new runs on a fixed schedule. A store implementing `AutomationStore` (`MemoryStore` and `SqliteStore` do) keeps each automation with the agent that fires it, and admits each occurrence's run atomically with its identity and the next due time; the application calls `Host::fire_automations` from its own poll loop and manages schedules through the store. Run `cargo run --example recurring_personal_agent` for a recurring inbox review across a restart. Calendar/cron schedules and authenticated event subscriptions are later work.

## Attached subagents

`AgentBuilder::subagents(max_parallel, [Subagent::new(name, description, child)])` lets the model delegate to child agents as if they were tools, with the child's input type as the arguments and a typed `ChildResult` as the result. A child runs inside its parent's run: its admission, every commit it makes, and its result are commits of the parent, its limits are an allocation reserved from the parent's budget, and a child's question surfaces on the parent until `Host::resolve_child` answers it. See [attached subagents](https://github.com/danieljvdm/agent-rs/blob/main/docs/attached-subagents.md); `cargo run --example delegated_workflow` delegates to two children and reopens SQLite while one of them waits.

## Providers and limits

`ScriptedModel` is a deterministic in-process provider for tests and benchmarks. `openai::OpenAiModel` implements Chat Completions SSE and `openai::OpenAiResponsesModel` implements stateless Responses SSE, both for JSON output and application function tools. Each buffers and validates a complete response before tools start, accepts explicit credentials, and reports usage that the engine charges even when the response is then rejected. Neither loads credentials from the environment, retries HTTP requests, falls back to another API or substitutes a model name. Choose the adapter in code and the exact model through `ModelConfig`; the adapter's API and endpoint become part of the agent's durable binding. The [adapter contract](https://github.com/danieljvdm/agent-rs/blob/main/docs/model-adapters.md) covers supported capabilities, durable identity, usage and retries.

`Limits` bounds model attempts, tool attempts, tokens, output bytes, context bytes, parallelism, operation time, and cumulative active time. `RunState` carries the original limits and counters through recovery. Context selection keeps complete tool-call/result groups, the current input and the latest exchange. An optional `ContextPolicy`, set with `AgentBuilder::context_policy`, can pin further entries of the run's history and add bounded derived context prepared inside the model operation; its rustdoc states the contract. Retained history is separate from model context.

The optional [`memory` feature](https://github.com/danieljvdm/agent-rs/blob/main/docs/memory.md) is one such policy: extractive summaries and semantic retrieval over caller-supplied local embeddings, from source-versioned stores (in process, or in the SQLite database). Source edits and deletions invalidate their derived records, and derived references stay separate from authoritative history and within the context cap. Memory is scoped to the conversation ID: `Conversation::new(id)` names one, and a default conversation gets a unique ID. It is unrelated to the root's `MemoryStore` and `MemoryCheckpoint`, which keep runs in process.

Progress events are transient and best effort through a caller-supplied bounded Tokio channel; a full channel drops events and never blocks a run. Each `ProgressEvent` carries its `run_id`, a child's `parent_run_id`, and a `ProgressKind` (model started, text delta, tools started, committed, settled), so one channel, such as `HostOptions::events`, can serve many concurrent runs. Events are neither persisted nor counted in run state, and both types are `#[non_exhaustive]`: new kinds and fields may be added. Reconnect using durable status/history, not token deltas. Standard `tracing` emits run spans without prompts or credentials.

## Personal agent application

The separate [`yielded-personal-agent` application](https://github.com/danieljvdm/agent-rs/blob/main/docs/personal-agent.md) in `apps/personal-agent` provides a local authenticated API, conversation UI, and approval/input/reconciliation inbox over a `Host<SqliteStore>`. It restores from durable state after reconnect or restart, and maps runtime errors to HTTP statuses by `Error` variant. Product policy, credentials, and presentation stay outside the runtime crate.

With `PERSONAL_AGENT_TOKEN` set to a randomly generated token of at least 32 characters, run `scripts/rust.sh run -p yielded-personal-agent -- --demo` and open `http://127.0.0.1:3000`. The offline notebook workflow asks for details and approval before saving a local note. Omit `--demo` and supply `OPENAI_API_KEY` and `OPENAI_MODEL` to use the existing live adapter. The repository is a Cargo workspace; add `-p yielded-personal-agent` (or `--workspace`) to the commands below to check the application.

## Development

The checked-in `rust-toolchain.toml` pins Rust and the lint/format components. If Homebrew Cargo takes precedence over rustup, use `scripts/rust.sh` in place of `cargo` to select the pinned toolchain explicitly.

```sh
cargo test --all-features --locked
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo fmt --check
cargo check --no-default-features --locked
cargo bench --bench agent_loop --locked
```

[Benchmark instructions](https://github.com/danieljvdm/agent-rs/blob/main/docs/benchmarks.md) and [recorded results](https://github.com/danieljvdm/agent-rs/blob/main/docs/benchmark-results.md) describe fixed workloads, correctness assertions, process CPU and allocation runs, and revision comparisons. Timings are informational until observed variance supports a timing gate.

[Verification evidence](https://github.com/danieljvdm/agent-rs/blob/main/docs/verification.md) records the automated checks, restart workflow, [live personal-agent evaluation](https://github.com/danieljvdm/agent-rs/blob/main/docs/personal-workflow-eval.md#recorded-results), and [full hosted benchmark comparison and calibration](https://github.com/danieljvdm/agent-rs/blob/main/docs/benchmark-results.md#full-hosted-comparison-and-calibration).

The manual [performance workflow](https://github.com/danieljvdm/agent-rs/blob/main/.github/workflows/performance.yml) runs longer comparisons or same-source calibration on one hosted Linux runner. [Calibration notes](https://github.com/danieljvdm/agent-rs/blob/main/docs/benchmark-calibration.md) explain the paired measurements and why timing gates remain informational until enough independent runs establish useful thresholds.

[Recovery profiling](https://github.com/danieljvdm/agent-rs/blob/main/docs/recovery-profiling.md) describes the allocation-stack capture and the focused recovery comparison suite.

This is a Rust-only initial implementation. There are no distributed leases, Node bindings, Cloudflare adapter, in-flight preemption, active-run input joining, agent graphs, or nested or detached subagents. MCP support is an optional stdio adapter; other transports are separate work.
