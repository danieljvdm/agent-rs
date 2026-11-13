# Engine telemetry

The engine emits content-free `tracing` spans and events. The application chooses
its subscriber and exporters; the library does not install one or depend on
OpenTelemetry. Filter on the `agent_rs` target prefix and the names below. These
names, levels, and fields are the telemetry contract.

The `gen_ai.*` names follow the current OpenTelemetry GenAI conventions for
[agents](https://github.com/open-telemetry/semantic-conventions-genai/blob/main/docs/gen-ai/gen-ai-agent-spans.md),
[inference](https://github.com/open-telemetry/semantic-conventions-genai/blob/main/docs/gen-ai/client-inference.md),
and [tools](https://github.com/open-telemetry/semantic-conventions-genai/blob/main/docs/gen-ai/execute-tool.md)
(checked November 2026; those conventions are still in development).
`otel.name` supplies the display name for subscribers such as
`tracing-opentelemetry`; the static tracing span name remains suitable for filters.
Other fields below are engine-specific.

## Spans

| Name | Level | Fields |
| --- | --- | --- |
| `invoke_agent` | INFO | `otel.name` = `invoke_agent {agent id}`, `otel.kind` = `internal`, `gen_ai.operation.name` = `invoke_agent`, `gen_ai.agent.id`, `run_id`, optional `parent_run_id`, optional `gen_ai.conversation.id`, `outcome`, optional `error.type` |
| `chat` | INFO | `otel.name` = `chat {model}`, `otel.kind` = `client`, `gen_ai.operation.name` = `chat`, `gen_ai.request.model`, `turn`, `gen_ai.usage.input_tokens`, `gen_ai.usage.output_tokens`, `response_bytes`, optional `time_to_first_delta_ms`, `outcome`, optional `error.type` |
| `compact` | INFO | A [compaction](context-compaction.md)'s summary call, in place of `chat`: the same fields, with `otel.name` = `compact {model}`, `forced` (whether the provider rejected the turn first) and `summarized` (whether the model's summary was used). Its events name their operation `chat`. |
| `execute_tool` | INFO | `otel.name` = `execute_tool {tool name}`, `otel.kind` = `internal`, `gen_ai.operation.name` = `execute_tool`, `gen_ai.tool.name`, `run_id`, `turn`, `tool_call_index`, `operation.status`, optional `output_bytes` |
| `tool_window` | DEBUG | None |
| `commit` | DEBUG | `revision` after the checkpoint returns successfully |
| `assemble_context` | DEBUG | None |
| `model_cleanup` | DEBUG | `outcome`, optional `error.type` |
| `context_cleanup` | DEBUG | None; present when a context policy is configured |
| `tool_cleanup` | DEBUG | `outcome`, optional `error.type` |
| `settle` | DEBUG | None |

An invocation covers one drive, including its commits and settlement. A waiting
or interrupted run gets another invocation when driven again. Conversation IDs
are recorded for root runs; attached children have a parent run ID instead of a
synthetic conversation ID. Hosted runs start root spans; an application span around
`Host::start` is not their parent.

`chat` covers dispatch, context assembly, streaming, cleanup, validation and the
resulting commit. `turn` is one-based and counts this run's attempts across
restarts, excluding its children's turns. Token counts are the latest cumulative
usage reported for this attempt, including usage from rejected or interrupted
attempts. `response_bytes` is the greater of the complete response's compact JSON
size and the total UTF-8 text/reasoning delta bytes received. With no complete
response it is the received delta bytes. `time_to_first_delta_ms` is measured from
just after the dispatch commit, includes context preparation, and is absent if no
text or reasoning delta arrives. It uses monotonic time and truncates to whole
milliseconds. There are no per-delta spans or events.

`execute_tool` covers one dispatched attempt through cleanup and recording its
operation status. `output_bytes` is the compact JSON size of the recorded result
value or wait request; it is absent for an unknown or interrupted outcome with no
recorded output. `operation.status` is `complete`, `waiting`, `unknown`,
`dispatched`, or `child` (a delegation still awaiting its child). A complete result
may represent a reported tool failure. Awaiting approval and undispatched calls
do not have execution spans.

Tool identity is the tuple (`run_id`, `turn`, `tool_call_index`), where the index
is the call's zero-based position in the model response. It stays the same when
that call is retried. Model-supplied call IDs are never recorded, including in
warnings, because they may contain message text. `gen_ai.tool.name` comes from
the registered tool specification.

An attached child's `invoke_agent` is nested under its parent's `execute_tool`.
Its `chat` and other spans nest normally. Concurrent tools are sibling spans under
`tool_window`, with separate cleanup children. `model_cleanup` includes model
cleanup and, if configured, concurrent context-policy cleanup (`context_cleanup`).
Span lifetimes provide durations; these timings are not stored in `RunState`.

Invocation outcomes are `completed`, `cancelled`, `stopped`, `failed`, `waiting`,
or the error kind returned by a drive (including `interrupted`). Chat outcomes
are `done`, `timed_out`, `interrupted`, or `rejected`; rejection includes provider,
context, validation, cleanup and checkpoint errors. Cleanup outcomes are `done`,
`failed`, or `timed_out`. End fields are initially empty and recorded when known;
a dropped drive can leave them empty.

## Warning events

| Name | Level | Fields and trigger |
| --- | --- | --- |
| `operation_timed_out` | WARN | `operation`: `chat`, `execute_tool`, or `authorize_tool`. The work deadline elapsed. |
| `cleanup_timed_out` | WARN | `operation`: `chat` or `execute_tool`, `timeout_ms`. Cleanup exceeded its allowance. |
| `cleanup_failed` | WARN | `operation`: `chat` or `execute_tool`, `error.type`. Cleanup returned an error. |
| `operation_unknown` | WARN | A dispatched operation became unknown, including recovery and settlement. |
| `work_recovered` | WARN | `operation`: `chat` or `execute_tool`. An in-flight model attempt was recovered, an unretryable call became unknown, or a retryable dispatched call actually restarted. |

Every warning includes `run_id` and `turn`. Warnings about tools additionally
include `gen_ai.tool.name` and `tool_call_index`; they are identifiable even with
WARN-only filtering and without printed span context. Retryable operations that
are blocked by another unresolved operation do not emit recovery warnings until
they restart. Recovering an attached child does not double-report its retries.

`error.type` is a fixed classification, never an error's display/debug text:
`config`, `input`, `model_output`, `provider`, `tool`, `authorization`, `context`,
`storage`, `idempotency_conflict`, `conversation_busy`, `store_busy`, `stale`,
`incompatible`, `not_found`, `not_waiting`, `run_not_active`, `interrupted`,
`waiting`, `stopped`, `failed`, `cancelled`, or `timeout`.

## Privacy and overhead

No messages, instructions, arguments, output values, schemas, or error text are
recorded, even on failures. Model-supplied tool call IDs are also excluded.
Application-owned run/conversation/agent IDs and configured model/tool names are
recorded as supplied: applications should keep content out of those identifiers.
The application or provider may emit its own telemetry separately.

Background host, PostgreSQL worker, and MCP transport tasks retain an installed
subscriber without retaining the span of the call that started them. If started
without a subscriber, they use the ambient default: installing a global subscriber
later enables tracing for subsequent work. Existing spans are not recreated.
Install tracing before startup to observe startup itself. Scoped subscribers must
be installed when the background task starts if it should keep using that scope.

Hosted runs have root spans. Uninstrumented PostgreSQL connection tasks and
SQLite/PostgreSQL jobs do not capture spans or subscribers. The SQLite thread
join does no run work and needs no run span.

End fields are recorded through their owning span handles. Disabling a span
cannot redirect its fields into an enabled ancestor or application span. Warning
events use the intended parent when enabled and otherwise fall back to the
current context, so filtering out a cleanup span does not detach its warning.

Tool execution has one generic protocol with a zero-sized observer when tracing
has no subscriber. When any subscriber is installed, the engine conservatively
uses the traced path even if that subscriber currently filters out every event.
The public tracing API does not expose exact macro callsites for probing, and
this policy avoids silently dropping diagnostics selected by name or field.
A fully uninterested subscriber therefore has some boxing overhead; the existing
no-subscriber allocation budgets are unchanged.
`tests/cost_budget.rs` also pins deterministic costs with a minimal enabled
subscriber; a real subscriber or exporter can allocate additional memory.
