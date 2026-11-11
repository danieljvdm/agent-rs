# Model adapters and explicit selection

The `Model` boundary stays provider-neutral. With the `openai` feature,
`openai::OpenAiModel` calls Chat Completions, `openai::OpenAiResponsesModel`
calls Responses, and `openai::LlamaModel` calls a local llama.cpp
`llama-server` through its Chat Completions endpoint. The application chooses
the adapter and the exact `ModelConfig`;
the engine never infers an endpoint from a model name, and an adapter never
substitutes one.

```rust
use std::sync::Arc;
use agent_rs::{Agent, Limits, ModelConfig, openai::OpenAiResponsesModel};

fn assistant(key: String) -> agent_rs::Result<Agent<String, String>> {
    Agent::builder("personal-assistant-v1", Arc::new(OpenAiResponsesModel::new(key)?))
        .instructions("Answer the request using the available tools.")
        .config(ModelConfig::new("gpt-4.1-2025-04-14").with_temperature(0.0))
        .limits(Limits::default().with_max_tokens(20_000))
        .input::<String>()
        .output::<String>()
        .build()
}
```

Swapping `OpenAiResponsesModel` for `OpenAiModel` selects Chat Completions with
nothing else changed. The Responses adapter covers text/JSON and
ordinary application function tools, with explicit adapter selection. Dynamic
per-turn routing, automatic fallback, other providers, and provider-specific
reasoning or hosted-tool replay are later slices, driven by consumer needs.

## Supported Responses contract

Requests use `/responses` with `stream: true`, `store: false`,
`background: false` and `truncation: disabled`. Every request replays the
engine's bounded local history; no provider conversation or response ID is kept,
and no key is persisted. Answers use the same `{"value": O}` envelope as Chat
Completions, so scalar and nested Rust outputs work. Tool calls become
`function_call` items and results become `function_call_output` items with the
original `call_id`; a failed tool is sent as `{"error": result}`.

The output schema goes in `text.format` and tools use the Responses function
shape. Schemas are non-strict because Rust schemas can contain keywords outside
the provider's strict subset; the engine's typed validation stays authoritative.
Derived context from a `ContextPolicy` is rendered exactly as for Chat
Completions: after the instructions, a developer notice that it is untrusted
reference data, then the items as one user-role JSON document, ahead of history.
[Steering](steering.md) instructions are rendered the same way in both APIs
too: a user message with the instruction as written, in its place in history.

The parser requires `response.completed`, final usage, completed output items,
and agreement between streamed deltas and the final text and arguments. It
rejects incomplete or failed responses, refusals, malformed JSON, missing or
changing identities, duplicate call IDs, and unknown items or events. Tool-turn
text is transient, and no tool runs until the whole response and every call
validate.

Reasoning items (including encrypted reasoning), hosted tools, images and audio,
background responses and remote conversation state are **not supported** in this
slice: they need replayable provider context that the neutral history does not
carry. Receiving one fails the turn; the adapter neither drops required context
nor switches models. Selecting a reasoning-capable model does not make its
default reasoning output usable here.

Both adapters share one HTTP transport and bounded SSE framing. Logical payload,
wire bytes, single lines and events, event counts, and Responses item and part
counts are bounded. Cancellation drops the request and response stream, including
buffered output; there is no detached reader or provider-side background job.

## llama.cpp's llama-server

`LlamaModel` speaks Chat Completions, sharing `OpenAiModel`'s transport, stream
parser and tool handling, but writes requests for a local chat model:

| | `OpenAiModel` | `LlamaModel` |
| --- | --- | --- |
| A `String` output | JSON in the `{"value": O}` envelope | Plain text, no `response_format` |
| Any other output | `json_schema`, non-strict, in the envelope | llama-server's JSON-schema grammar, in the envelope |
| Instructions | `developer` messages | One `system` message, with the derived-context notice appended |
| Inputs and string tool results | JSON (`"hello"` arrives quoted) | As written; other values as JSON |
| Consecutive user messages | Separate | Joined, for chat templates that require alternating roles |
| Reasoning (`reasoning_content`) | Ignored | Streamed as `ModelEvent::ReasoningDelta` |
| Failed status or stream error | Status or kind only | Also the server's `error.message`, bounded |
| API key | Required | Optional (`with_api_key`) |
| Token cap | `max_completion_tokens` | `max_tokens` |

An output is plain text when its schema accepts any string: `{"type": "string"}`
with only annotations (`title`, `description`, `$schema`, `$comment`), `{}` or
`true`. A string with constraints, such as an `enum`, uses the grammar.

Typed outputs keep the envelope because a grammar whose root is a bare scalar
stops a local model after its first token. Against Gemma 4 26B-A4B, asking
"What is 6 times 7?" with an `integer` root schema answered `6` every time
(the model began "6 times 7…" and the grammar ended the number), while the
envelope answered `{"value": 42}`.

Reasoning is streamed, never retained, never replayed on later turns, and
counts against `Limits::max_response_bytes` with the answer. The engine passes
it on as `ProgressKind::ReasoningDelta`. A reasoning model can spend its whole
token allowance thinking; the finish reason `length` fails the turn with
"output reached the token limit before it was complete".

`LlamaModel::new` builds a client with no proxy (local traffic must not go
through a proxy from the environment), no redirects and no read timeout, since
a model may load for minutes before its first byte. The engine's
`Limits::operation_timeout_ms` still bounds each turn, so raise it, and the
active budget, for local models. A router-mode server picks the model from
`ModelConfig::model`.

`tests/llama.rs` replays streams recorded from llama-server 0.9.0 serving
Gemma 4 26B-A4B: a reasoning chat, a tool call, a typed answer, a reply that
ran out of tokens, and a context-size error. Its ignored
`live_server_chats_answers_typed_and_calls_a_tool` runs the same cases against
a live server:

```sh
LLAMA_SERVER_URL=http://127.0.0.1:8080/v1 LLAMA_MODEL=<name> \
  cargo test --test llama -- --ignored --nocapture
```

## Errors persisted with a failed run

A failed run stores its error text, so network and protocol errors from every
adapter never contain request URLs or credentials, and the OpenAI adapters
never contain server bodies:

- HTTP failures report the status code only.
- `reqwest` errors are kept as causes only after `without_url()`.
- A typed decode of server data (a Chat chunk, a Responses event, item, part or
  usage) is reported without its cause, since serde's message can quote the
  offending value. Syntax errors from parsing JSON text (model output, tool
  arguments) are kept: they name a position, never the text.
- Provider error events (`error`, `response.failed`) are reported by kind,
  without the provider's message.
- `LlamaModel` is the exception for server messages: a failed status or a
  stream `error` also reports llama-server's `error.message`, such as a request
  that exceeds the context size. At most 8 KiB of the body is read, and the
  message is cut to 512 bytes with control characters replaced. It names what
  to fix, and a local server's messages carry no credentials.

Unit tests in `src/openai.rs`, `src/openai/responses.rs` and
`src/openai/transport.rs` walk every error's message and cause chain for planted
server text, the request path and the key. Custom clients should disable
redirects.

## Durable identity

`Model::binding()` supplies a stable, non-secret adapter identity, and
`AgentBuilder::build` adds it to the agent's binding as `model_adapter`. Both
OpenAI adapters report `{"provider":"openai","api":…,"version":1,"endpoint":…}`,
with `api` either `chat_completions` or `responses`. `LlamaModel` reports
`"provider":"llama.cpp"` with `"api":"chat_completions"`, so a run started
through it cannot resume through `OpenAiModel` at the same endpoint. With the exact model
configuration already in the binding, a stored run cannot silently resume
through another API, endpoint or model; rotating a key keeps it compatible.
Build a new agent to change the route. A model wrapper must forward its inner
binding or include its own immutable routing policy.

The default binding, `null`, adds no key, so the binding of every agent whose
model does not report one is byte-identical to before (pinned in
`tests/context_policy.rs`). Such an application must change `Agent::id` when its
model implementation or routing changes.

Runs persisted before this change through `OpenAiModel` lack the
`model_adapter` key and are rejected as incompatible: drain them with the old
version, or migrate them deliberately. A host reports the incompatibility
through `last_error`; restoring the original route and calling `Host::retry`
repairs a blocked run.

## Usage, retries and recovery

There are **no implicit HTTP retries or fallback attempts**, including for 429,
5xx, protocol failures and timeouts. Such failures settle the run. One
`Model::stream` call is one attempt that the engine reserves and charges. A
cooperative host interruption resumes an unfinished run with the same binding;
a model call left in flight by process death is eligible for a new attempt, and
the earlier attempt's model-call and active-time reservations stay charged.

`ModelEvent::Usage` is a cumulative snapshot for one attempt. Snapshots must not
decrease, and `ModelResponse::usage` repeats the final totals rather than adding
to them. The engine charges the highest snapshot when the attempt ends, however
it ends: a rejected response, cancellation, shutdown, timeout or a failed
cleanup. The next attempt and later tool rounds receive the remaining allowance.
Usage is checkpointed with the turn's outcome or interruption, not with every
event. Tokens never reported, or lost to abrupt process death before a
checkpoint, cannot be inferred; these counters are not a billing ledger.

A future fallback must durably bind its route policy, record and reserve each
attempt, carry usage across failures and recovery, and define replay
compatibility. Retrying inside `Model::stream` would undercount the budget and
is outside this contract.

## Costs and measurements

`tests/cost_budget.rs` holds deterministic budgets for each adapter's hot paths
(`LlamaModel` decoding plain text),
measured through the hidden, unsupported `openai::cost_probe` (no network):
allocations to decode a 1,024-byte answer streamed as 64 deltas, and
allocations and exact wire bytes for a request body with four tools and one
tool exchange. It also pins that usage snapshots and an adapter binding add no
allocation to a run or to the binding check of a stored run.

`cargo bench --bench model_adapters` measures the same encoding and decoding
paths with Criterion, for 1, 64 and 1,024 deltas; use `-- --test` for a
correctness-only pass. See [runtime benchmarks](benchmarks.md) for how to
capture and compare results. These figures exclude HTTP and live inference.

The original pull request also recorded hosted measurements, kept outside the
repository. Over loopback HTTP with a one-scalar fixture, a Chat round trip took
about 316 µs directly and 330 µs through the engine, with no measurable change
from the transport split; Responses took about 346 µs directly and 359 µs
through the engine. Connection setup dominates those figures, which is why the
bench above excludes HTTP. An opt-in live run of the
[personal workflow eval](personal-workflow-eval.md) with `--api responses` and
`gpt-4.1-2025-04-14` passed all three synthetic cases on two revisions. These
are smoke results, not quality or latency claims.
