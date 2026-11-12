# MCP tools over stdio

The optional `mcp` feature starts an application-owned [MCP](https://modelcontextprotocol.io)
server over stdio, discovers its tools, and registers the ones the application selects as
ordinary `Tool`s. They go into `Agent::builder(..).tool(..)` beside local tools and get the same
validation, approval, authorization, limits, checkpoints and recovery. Nothing in the engine or
host knows about MCP, and nothing starts a server but the application. Default features and
`default-features = false` build neither the adapter nor its dependencies.

The first concrete integration is
[`@modelcontextprotocol/server-filesystem`](https://github.com/modelcontextprotocol/servers/tree/main/src/filesystem).
The adapter uses the [`rmcp`](https://docs.rs/rmcp) SDK, pinned to an exact version because
its `Tool` type is re-exported as `mcp::ToolDefinition`, behind its own bounded stdio framing.
It accepts the `initialize` protocol versions `2024-11-05`, `2025-03-26`, `2025-06-18` and
`2025-11-25`, and advertises no sampling, roots or elicitation capabilities.

```rust,no_run
use std::sync::Arc;
use tokio::process::Command;
use tokio_util::sync::CancellationToken;
use agent_rs::mcp::{McpClient, McpOptions, ServerIdentity, ToolPolicy};
use agent_rs::{Agent, Execution, Model, Result, RetryPolicy};

async fn example(model: Arc<dyn Model>) -> Result<()> {
    let mut command = Command::new("node");
    command.arg("/opt/mcp/server-filesystem/dist/index.js").arg("/data/workspace");
    let client = McpClient::connect(
        ServerIdentity::new("workspace-files", "filesystem-2.1.0-workspace-v1"),
        command,
        McpOptions::default(),
        CancellationToken::new(),
    )
    .await?;
    let read = client.tool(
        "read_text_file",
        "files_read",
        ToolPolicy::default()
            .with_retry(RetryPolicy::ReadOnly)
            .with_execution(Execution::Parallel)
            .with_requires_approval(false),
    )?;
    let write = client.tool("write_file", "files_write", ToolPolicy::default())?;
    let agent = Agent::builder("files", model).tool(read).tool(write).build()?;
    // Run or host the agent. Stop it before closing the client.
    client.close().await
}
```

Keep the client alive while its tools are in use. `tools()` is the bounded catalog snapshot
taken at connection; registration needs an explicit remote name, a model-facing alias and an
application policy. The default `ToolPolicy` requires approval, runs sequentially and never
retries. A server's `readOnlyHint` and `idempotentHint` annotations grant nothing. An alias must
be unique within the agent, tools from other servers included; `build` rejects duplicates.

Arguments are checked against the tool's discovered JSON Schema in `Tool::validate`, at the
engine's whole-response validation boundary, so no call of a malformed batch runs and the
rejection fails the run as `Error::ModelOutput`. Bundled `$ref`s are supported; external
HTTP and file retrieval is disabled, and a schema needing it is rejected at registration. A
declared output schema validates successful `structuredContent`. Results keep MCP content
blocks, structured content and `_meta`; resource links are data and are not fetched.
`Tool::output_schema` describes that whole result: `content`, and `structuredContent` as the
declared output schema when there is one. Code mode declares the tool's return type from it.

## Outcomes

Each failure path maps to the `Tool` contract by whether its outcome is known:

| Path | Result | Run with a `Never` tool |
| --- | --- | --- |
| Server result | `Complete` | continues |
| Server result with `isError` | `Failed` with the whole result | continues, model sees it |
| JSON-RPC error reply to `tools/call` | `Failed` with `{"error": {code, message, data}}` | continues |
| Not sent: connection already closed, catalog changed, re-listing failed | `Failed` with `{"error": {message}}` | continues |
| After sending: transport died, request timed out, malformed, truncated, incomplete or oversized reply | `Err(Error::Tool)` | `Unknown` |
| Success whose `structuredContent` violates the output schema | `Err(Error::Tool)` | `Unknown` |
| Engine timeout, cancellation or shutdown (the future is dropped) | the engine's own error | `Unknown` (`ReadOnly` reruns after shutdown) |
| Output above `Limits::max_tool_output_bytes` | handled by the engine like `Err` | `Unknown` |

A failure the server reports is its own account, not proof that nothing changed. A
`ReadOnly` tool records every `Err` as an error result for the model instead of `Unknown`.

## Durable contracts and catalog changes

Each registered tool's `ToolSpec::version` is a deterministic JSON contract holding the adapter
version, the application's `ServerIdentity`, what the server reported when initialized
(`serverInfo`, protocol version, capabilities, instructions) and the tool's complete
definition. The spec is part of the agent's binding and of every recorded operation, so a stored
run resumes only against the same deployment serving the same tool: a changed schema, server
version or identity fails with `Error::Incompatible`, and `tests/mcp.rs` pins each case. Tools
the agent did not register are not part of any contract, so a server that adds an unrelated tool
leaves stored runs resumable. Commands and credentials are never recorded.

Change `ServerIdentity::revision` whenever a deployment's command, configuration,
authorization scope or backing service changes, and give different servers different IDs. A
server's self-reported name and version do not identify a deployment. Keep the original
deployment available for compatible recovery, or reconcile and migrate retained work
explicitly.

Within a connection, every call first lists the whole catalog again, without the SDK's cache,
and compares it with the snapshot. A change, or a `notifications/tools/list_changed`, closes the
connection instead of silently replacing registered tools; the call was not sent, so it is a
`Failed` result the model sees. MCP has no conditional `tools/call`, so a change between the
check and the call, or one a server does not disclose, still needs a pinned, trusted deployment.

## Connection ownership and cleanup

A request that times out, or that the engine drops on a timeout, cancellation or shutdown,
closes the **whole connection**, other calls on it included, because the server may still be
acting on it. The task that owns the server then sends best-effort cancellation notifications
for open requests (within `McpOptions::cancellation_grace`), kills the server's process group,
reaps the server and closes the protocol. `Tool::cleanup` of every call that saw the connection
close waits for that, so when the engine finishes a batch no server process is left running.
Killing a process cannot undo an external effect; other mutations on the same connection may
also need reconciliation. Use separate clients where independent interruption domains are
needed.

On Unix the server is spawned with `process_group(0)` and leads its own group. Cleanup sends
`SIGKILL` to the whole group through the safe [`rustix`](https://docs.rs/rustix) wrapper, so
the crate keeps `#![forbid(unsafe_code)]`. rustix is smaller than `nix` for this one call and
was already locked as a dependency of `tempfile`. Cleanup then kills the server directly in case it left the group, and reaps it. The
group is signalled only before the leader is reaped, so its ID cannot have been reused. If the
owning task is dropped without running, for example by a runtime shutting down, a guard kills
the group from `Drop`. Descendants that create their own session or group escape this, as with
any process-group approach. Other platforms kill the direct child only.

`close()` is idempotent and cancel-safe: awaiting it again finishes the same cleanup.
Dropping the client starts cleanup even while tool handles survive, and those tools then report
`Failed` without sending anything; await `close()` before the Tokio runtime shuts down to be sure
the server was reaped. The token passed to `connect()` cancels initialization and discovery and
returns `Error::Interrupted` after reaping the server; after connection, use `close()`. Reconnect
explicitly after a disconnect. Interrupted `ReadOnly` work may rerun on a new, compatible
client; `Never` work does not. `RetryPolicy::Idempotent` is rejected because MCP has no general
deduplication guarantee; the operation's `idempotency_key` remains available to application
reconciliation.

All work stays off the run's task except brief, bounded steps: process spawn happens in
`connect`, and the protocol, framing and decoding run on the connection's own task.
`McpOptions` bounds each message's bytes (frames are capped before decoding), the catalog's
total bytes, its tools and pages, each request's duration and the calls in flight across runs.
The engine's parallelism, deadlines and output limits apply independently. No hidden
reconnect, automatic tool retry or model turn is introduced.

## Costs

`tests/cost_budget.rs` holds deterministic budgets for the hot path, behind the feature:
requests per call (two with a single-page catalog, five with four pages, because of the
re-listing), allocations per call on the run's task, and no allocation for `validate`.

Timing is not gated. A hosted measurement of the original slice (2026-10-29, AMD EPYC 7763,
two CPUs, Python 3.12 fixture server) found one warm stdio call adds about 0.53 ms over the
same contract served in process with a three-tool catalog, and about 3.0 ms with a 32-tool
catalog in four pages, almost all of it the per-call re-listing; eight calls added 2.8 ms and
19.8 ms. A cold connection, including spawn, handshake, discovery and registration, took about
25 ms and 28 ms. Existing engine timings were unchanged within noise. Those figures describe
that runner and fixture, not MCP servers in general. Run `cargo bench --features mcp --bench
mcp` for the warm and cold Criterion cases; see [the benchmark guide](benchmarks.md).

## Reproduce the concrete integration

Install the pinned server into a dedicated directory, then run the example. It creates and reads
only its own temporary file and closes the process afterwards.

```sh
npm install --prefix "$TMPDIR/agent-mcp-example" --ignore-scripts --no-audit --no-fund \
  @modelcontextprotocol/server-filesystem@2.1.0
cargo run --features mcp --example mcp_filesystem -- \
  "$TMPDIR/agent-mcp-example/node_modules/@modelcontextprotocol/server-filesystem/dist/index.js"
```

`tests/mcp.rs` uses an independent Python stdio server (`tests/support/mcp_server.py`) and
temporary files and SQLite databases to cover discovery, validation, approval and denial,
parallel bounds, outcome mapping, interruption, process-group cleanup, catalog drift and
durable recovery. The tests need Python 3; they need no Node, npm, model or network access.

Additional transports (including Streamable HTTP and OAuth), protocol `2026-07-28`,
server-driven input and tasks, resource and prompt APIs, and server-specific idempotent retry
integrations are not part of this adapter.
