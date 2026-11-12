# Code mode

Code mode gives the model one tool, `code` by default, whose input is JavaScript. The code calls the agent's other tools as async functions and returns one value. A model that would take a turn per call, or per batch, writes a script instead, and the parent sees one result.

```rust
use agent_rs::codemode::CodeMode;
use agent_rs::{Agent, Result, ScriptedModel, Tool};
use std::sync::Arc;

fn planner(model: Arc<ScriptedModel>, search: Arc<dyn Tool>, read: Arc<dyn Tool>) -> Result<Agent> {
    let code = CodeMode::new("code")
        .tools([search, read]) // only these are callable
        .build()?; // a Subagent
    Agent::builder("planner", model).subagents(2, [code]).build()
}
```

It is behind the `codemode` feature, which adds `rquickjs` and builds QuickJS from source.

## The contract

The input is `{code: string}`, the body of an async function. The model is told, in the tool's description:

- `code` uses `await` and `return`s the result, which must be JSON. `undefined` is `null`.
- Every call should be awaited. `Promise.all` runs calls in parallel; calls awaited one after another run one after another. A call that is not awaited still runs, in the round of the next call the code waits on, but its failure is lost; a call not yet run when the code returns is never made, and the script fails with `N calls were not awaited`.
- A failed call throws a `ToolError` whose `value` is the tool's error result and whose `tool` is its name. The effects of calls that already ran are not undone.
- There is no clock, no timers, no randomness, and no network or file access other than through the declared functions.

Each allowed tool is declared as a TypeScript function after the contract, rendered from its input schema and its output schema: `declare function search(input: { q: string }): Promise<{ hits: string[] }>;`. The output schema is `Tool::output_schema`. `typed_tool` derives it from the handler's output type, and an MCP tool's is the MCP result around the server's declared output schema, so its data is under `structuredContent`. A tool without one resolves to `unknown`; a tool that wraps another should return the inner tool's. The schema describes a successful result and nothing checks a result against it; a failed call throws instead. A schema that cannot be rendered, or whose type is over 4 KiB, is declared `unknown`. A tool name that is not an identifier is mangled (`read-file` becomes `read_file`); two tools that mangle to the same name are a `build` error. The description is rendered once, in `build`, and is byte-stable for a given list of tools, because it is part of the parent's binding.

A function takes at most one argument. No argument sends `{}`; an argument that is not JSON, or a second argument, throws a `TypeError` before anything is called. A call whose tool would reject its arguments (`Tool::validate`) is answered as a `ToolError` and is never dispatched.

The parent's model receives:

- `{result, logs}` when the script returns, as a successful result.
- `{error, logs, calls}` when it throws, as an error result. `calls` lists the first 100 calls it made (`calls_omitted` counts the rest), in the order it made them, with its `id` if dispatched and its `status`: `succeeded`, `failed`, `rejected` (refused before dispatch) or `not_dispatched` (issued but not awaited before the script ended).
- `{error}` naming how the child ended otherwise: `stopped: <limit>`, `failed: <message>` or `cancelled`.

`logs` holds each `console.log`, `info`, `warn`, `error` or `debug` line, strings as they are and other values as JSON, joined by spaces.

## How it runs

The script runs as an attached child run, an ordinary [attached subagent](attached-subagents.md) whose `Model` is a `ScriptModel`. Its history is its input `{code}`, then one exchange per round of calls. On each model turn the `ScriptModel`:

1. starts the script from the top in a fresh interpreter;
2. runs it until every task waits on a call;
3. while the history holds a recorded round, checks that the waiting calls match it by count, tool and arguments, answers them with the recorded results, and goes on;
4. when the history runs out, returns the waiting calls as the turn's tool calls, or the script's end as its final output.

Call IDs are `c0`, `c1`, … in the order the script issues them, so they are the same on every replay. The engine then runs the round through the child's tool window and records the results, which the next turn replays. A script whose calls differ from the recorded ones fails the child with "script diverged from its recorded calls"; a history whose call IDs have a gap is refused the same way.

Every call is therefore a normal operation of the child: it is authorized by `CodeMode::authorizer` (default `AllowAll`), waits for approval if its tool requires it, follows its tool's `RetryPolicy`, counts against the child's limits, and is durable before the script sees its result.

## Determinism

Replay depends on the interpreter doing the same thing every time. `QuickJs` runs each turn in a fresh QuickJS runtime with only deterministic built-ins: the base objects, `eval`, regular expressions, JSON, `Map` and `Set`, promises and `Proxy`. There is no `Date`, `Math.random`, `WeakRef`, `FinalizationRegistry`, `performance`, typed arrays or shared memory, and a bare context has no timers or I/O. A replacement `ScriptRuntime` must keep the same property: the same code and the same answers must issue the same calls in the same order.

Two failures depend on the build rather than the script: where a stack overflow (`RangeError: Maximum call stack size exceeded`) or running out of memory happens can change with the interpreter's build or the host's compiler. Both are catchable. A script that catches one and goes on can therefore make different calls after the host is rebuilt, and its replay then fails the child cleanly with "script diverged from its recorded calls"; it never runs a call twice or out of order.

Strings reach the host as Unicode: a lone surrogate, as left by `"😀".slice(0, 1)`, in an argument, the result, a log line or an error becomes U+FFFD.

The tool functions and `console` are non-writable, non-configurable globals, and the bridge between the script and the host captures every built-in it uses before the script runs, so a script that replaces `JSON`, `Map.prototype` or `Array.prototype` methods cannot change which calls are reported. The script cannot reach the bridge. Built-in prototypes are not frozen: that would break ordinary code such as `this.name = "MyError"` in an `Error` subclass.

## Isolation

Two runtimes are included, and `tests/codemode.rs` runs the same conformance suite against both.

- `QuickJs`, the default, runs the interpreter in the host's process, within a memory limit and an interrupt. A defect in the interpreter is a defect of the host.
- `Subprocess` runs the same interpreter in a process of its own for each turn, so a crash or corruption of the interpreter ends that process and fails the turn, and cannot reach the host's memory. The process is killed when the turn's deadline passes, the run is cancelled, or the turn ends. Its program is the `agent-rs-quickjs` binary (`cargo install agent-rs --features codemode --bin agent-rs-quickjs`) or any binary of yours that calls `codemode::serve`. It must be built from the same version of the crate: a process that reports a different interpreter fails the turn. A bare name is looked up on the host's `PATH`; the process itself gets none of the host's environment unless passed with `Subprocess::env`. The program's path is not part of the binding.

```rust
let code = CodeMode::new("code")
    .tools(tools)
    .runtime(Arc::new(Subprocess::new("/usr/local/bin/agent-rs-quickjs")))
    .build()?;
```

A process start costs about 3 ms per turn in `benches/attached_subagents.rs` (`code-mode-rounds-v1/subprocess`), against well under a millisecond in-process. The runtime is part of the binding, so a stored run resumes only under the runtime it started with. The process gets no limits from the operating system beyond the interpreter's memory limit; run it under a sandbox or resource limits of your platform to bound more.

## Limits and budgets

The child runs within `CodeMode::limits`, by default `codemode::default_limits()`: 16 model calls, 64 tool calls, one token and 60 seconds of active time, the rest as `Limits::default`. Like any child's, these are reserved from the parent when the script starts and replaced with what it used when it ends.

- **Rounds are model calls.** Each round is one model turn of the child, plus one turn to finish, so a script of ten sequential calls spends eleven of the parent's `max_model_calls`. Calls in one `Promise.all` are one round. Raise `max_model_calls` for long scripts, and the parent's to fit.
- **Tokens.** A script reports no tokens. The child needs `max_tokens` of at least one, because a run stops once it has used all of them.
- **Tool calls.** Every dispatched call counts against the child's `max_tool_calls`. A round that would exceed it stops the script with `StopReason::ToolCalls`; a single round larger than the whole limit fails it.
- **Time.** Each turn, replay included, is one model operation and must finish within `operation_timeout_ms`. A script that runs too long, such as an infinite loop, is interrupted when the operation's deadline passes and the child fails with "model operation timed out". The model's cleanup waits for the interpreter to stop, within `cleanup_timeout_ms`.
- **Context.** The child pins its whole history (`PinAll`), because replay needs every round. A history that grows past `max_context_bytes` fails the child with `Error::Context` instead of dropping rounds.
- **Refused calls.** Calls refused before dispatch spend no budget, so a script that makes more than 1,000 of them fails.
- **The output.** A final output larger than the child's `max_response_bytes` becomes an error naming its size and keeping the start of the script's error.
- **The script.** `CodeMode::script_limits` sets `ScriptLimits`: the interpreter's memory (64 MiB), log bytes kept (16 KiB; later lines are dropped and counted in a last line), and the result's encoded bytes (128 KiB; a larger result is a failure).

Replay work grows with the square of the rounds, since turn *n* replays *n − 1* rounds, but for short scripts the fixed cost of a fresh interpreter per turn dominates: `benches/attached_subagents.rs` measures a script of 1, 8 and 32 sequential calls. Storage grows linearly, since each round is one exchange of the child's history; `tests/cost_budget.rs` pins both the SQLite transactions, six per round as for any child tool round, and the record bytes.

## Approvals and waits

An inner call whose tool requires approval waits in the child's `OperationStatus::Approval`, which stops the parent's batch like any blocked child. Resolve it with `Host::resolve_child(parent, child, "c3", Resolution::Approve(true))`, or `RunState::resolve_child` in memory. The same holds for a tool that waits for input or a timer, and for a `RetryPolicy::Never` call whose outcome is unknown, which waits for `Resolution::Reconcile`. A denied approval is answered to the script as a `ToolError`.

## Recovery

Nothing about a script is stored except its child's history. After a crash, a shutdown, or a failed commit, the child is resumed like any attached child: the next turn replays every recorded round and goes on from the first call without a result. A call that was dispatched but not recorded is recovered by its tool's retry policy, as any tool call is. Calls already recorded are never made again.

Cancellation settles the child as cancelled and the parent sees `{error: "cancelled"}`. Shutdown interrupts the turn, and the child resumes from its last durable round.

## Not supported

- Scripts calling subagents: delegation stays one level deep, and a code mode child cannot have subagents.
