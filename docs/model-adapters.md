# Model adapters and explicit selection

The `Model` boundary stays provider-neutral. With the `openai` feature,
`openai::OpenAiModel` calls Chat Completions and `openai::OpenAiResponsesModel`
calls Responses. The application chooses the adapter and the exact `ModelConfig`;
the engine never infers an endpoint from a model name, and an adapter never
substitutes one.

```rust
use std::sync::Arc;
use yielded_agent::{Agent, Limits, ModelConfig, openai::OpenAiResponsesModel};

fn assistant(key: String) -> yielded_agent::Result<Agent<String, String>> {
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
nothing else changed. This is the first slice of
[YIE-251](https://linear.app/yielded/issue/YIE-251): Responses for text/JSON and
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

## Errors persisted with a failed run

A failed run stores its error text, so network and protocol errors from either
adapter never contain request URLs, server bodies or credentials:

- HTTP failures report the status code only.
- `reqwest` errors are kept as causes only after `without_url()`.
- A typed decode of server data (a Chat chunk, a Responses event, item, part or
  usage) is reported without its cause, since serde's message can quote the
  offending value. Syntax errors from parsing JSON text (model output, tool
  arguments) are kept: they name a position, never the text.
- Provider error events (`error`, `response.failed`) are reported by kind,
  without the provider's message.

Unit tests in `src/openai.rs`, `src/openai/responses.rs` and
`src/openai/transport.rs` walk every error's message and cause chain for planted
server text, the request path and the key. Custom clients should disable
redirects.

## Durable identity

`Model::binding()` supplies a stable, non-secret adapter identity, and
`AgentBuilder::build` adds it to the agent's binding as `model_adapter`. Both
OpenAI adapters report `{"provider":"openai","api":…,"version":1,"endpoint":…}`,
with `api` either `chat_completions` or `responses`. With the exact model
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

`tests/cost_budget.rs` holds deterministic budgets for each adapter's hot paths,
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
