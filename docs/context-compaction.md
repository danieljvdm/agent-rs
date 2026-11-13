# Context windows and compaction

An agent that names its model's context window keeps every conversation inside it. When the next model turn would not fit, the engine replaces the oldest part of the context with a summary the model writes, keeps the recent part as recorded, and carries on. Stored history is never rewritten. Without a context window the engine never compacts, and an agent behaves as it did before; it still sends a compaction another agent recorded, and still reports a request the provider rejects for its size as `Error::ContextOverflow`.

```rust
use std::sync::Arc;
use agent_rs::{Agent, ContextWindow, ModelConfig, Result, openai::LlamaModel};

# fn example() -> Result<()> {
let model = LlamaModel::new("http://127.0.0.1:8080/v1")?;
let agent = Agent::builder("reader-v1", Arc::new(model))
    .instructions("Answer from the user's messages.")
    .config(ModelConfig::new("qwen3-30b-a3b"))
    .context_window(ContextWindow::new(32_768))
    .input::<String>()
    .output::<String>()
    .build()?;
# Ok(()) }
```

## Why it works this way

The design follows from how a local model spends time. llama-server reuses its cache for the longest token prefix a request shares with the previous one, and evaluates everything after the first difference again. A request that appends to the last one costs only the new tokens; a request that changes anything near the front costs the whole prompt. Dropping the oldest entry on every turn, which is all a byte limit can do, therefore makes every turn a cold prompt exactly when the context is largest.

So the context is append-only between compactions, and every change to earlier content happens in one step that frees a large part of the window at once.

`tests/context_window.rs` holds this as a cost guard on the messages `LlamaModel` sends over one scripted conversation of 24 runs, each a question, a 1,200-byte tool result and an answer, with a 16,384-byte limit or a window of the same size in tokens (5,461):

| | Byte limit only | Context window |
| --- | --- | --- |
| Requests | 48 | 51, three of them summary requests |
| Bytes sent | 598,867 | 338,053 |
| Bytes after the prefix shared with the previous request | 244,387 | 45,854 |
| Requests that reuse under a quarter of the prompt before them | 15 | 3, each the first after a compaction |

The scripted model does no inference, so these are bytes, not seconds. The bytes after the shared prefix are what a prefix cache evaluates again.

Other harnesses were read for this design: pi, Yielded Agent, opencode and Codex. All four keep an append-only log and derive the context from it, anchor their token estimate on the provider's last reported usage, check before every model call, and compact and retry once when the provider rejects a request as too large. opencode and Codex ask for the summary by appending one instruction to the conversation as it stands, which reuses the provider's cached prefix; pi and Yielded Agent send a fresh transcript, which a local model must evaluate from the start. This engine does the former.

## What a turn sends

`HistoryEntry::Compaction { summary, kept }` records one compaction. It is appended to the run's history like any other entry, and it says that the `kept` entries immediately before it are still sent as recorded while everything older is covered by `summary`.

With a compaction in the run's prior context or its own history, a turn sends the latest compaction's summary first, as a user message that says it is a record and not a request. After it come, in their recorded order, the entries the compaction kept, every entry after it, and whatever the run always sends: its input, its steering instructions and what its `ContextPolicy` pins, even where the compaction covered them.

Earlier compactions are never sent: each summary carries forward what the one before it held. `max_context_bytes` still bounds the whole, as [`ContextPolicy`](../src/context.rs) describes, and should be several times the window in bytes so that it never moves the start of the context on its own.

A later run of the conversation is accepted with the same view. Acceptance reads history newest first, and stops after the latest compaction and the entries it kept, so a compaction carries across runs and restarts, whether or not the accepting agent has a context window. `ContextPolicy::prior_result` still replaces the earlier runs' tool results among them. A run's input and instructions are pinned for that run only: if a compaction in the middle of a long run covered its input, the next run sends the summary without it, and that run's first prompt is cold.

## When the engine compacts

Before each model turn the engine estimates the turn's prompt in tokens:

- the prompt and output tokens the provider reported for the run's latest exchange, plus an estimate from the encoded bytes of everything recorded since;
- or, when no exchange since the latest compaction reported usage, an estimate from the encoded bytes of the instructions, tool specs, output schema, derived-context cap and the context itself.

Bytes become tokens at three bytes a token, or fewer when the provider's count for the run's latest prompt shows the context is denser than that. Prose runs near four bytes a token, but tool results are JSON: against Gemma 4 31B, pages of message records came to 2.5. An estimate that assumes prose lets such a turn through to the provider, which rejects it. The same density sizes what a compaction keeps and what its summary request sends.

It compacts when that estimate reaches `tokens - reserve_tokens`, the compaction would cover at least a quarter of the context, and the run has at least two model calls left.

The quarter is there because a compaction is not free: it is a model call, and the prompt after it is evaluated from its start. When the entries that must be kept are most of the context, such as one tool result that fills half a small window, summarizing the rest frees little and the next result puts the run over the threshold again. The turn is then made as it is, inside the reserve, and the engine compacts only if the provider rejects it.

| `ContextWindow` field | Default | Meaning |
| --- | --- | --- |
| `tokens` | required | The model's context window, or less to compact sooner. |
| `reserve_tokens` | 15% of the window, 4,096 to 32,768 | Room kept for the answer, reasoning and estimate error. At least twice `summary_tokens`. |
| `keep_recent_tokens` | 15% of the window, at most 20,000 | The newest entries kept as recorded. |
| `summary_tokens` | 5% of the window, at most 4,096 | The size the summary is asked to stay within. An answer over twice this is not used. |
| `summary_instructions` | built in | Replaces the summary instruction. |

A 32,768-token window compacts at 27,853 tokens, keeps 4,915, and asks for a summary of 1,638, so the context after a compaction is about a fifth of the window and the conversation then appends for the rest.

What a compaction keeps is evaluated again, because the summary before it is new, and it is room the compaction did not free. Keeping 30% of the window made the guard conversation above compact five times instead of three and evaluate 37% more bytes again. Below 15% nothing more was saved there. Raise `keep_recent_tokens` when what the model needs is the recent messages word for word, and expect to pay for it on a slow model. The defaults scale down to a 1,024-token window: fixed thresholds of 16,000 and 20,000 tokens, common elsewhere, leave nothing to cut below a 36,000-token window. `AgentBuilder::build` rejects thresholds that do not fit their window.

Reasoning that a model reports apart from its answer is generated inside the reserve too. Raise `reserve_tokens` for a model that thinks at length, or its summaries and answers will run out of room.

The newest entry and the run's latest exchange are always kept, whatever their size, because the model cannot continue without the results it just asked for.

## What one turn's tools may return

An exchange is sent whole or not at all, so the results of one turn must fit the window on their own. No compaction can make room for them: they are the part it keeps.

With a context window, the engine therefore bounds what one exchange records. The bound is what the window holds below its reserve, less a summary of everything earlier, the instructions and tools, the calls themselves and the entries the run always sends. Results are recorded in the order the model made the calls while they fit. One that does not fit is recorded as a tool error that gives its size, the bound and what was left, and tells the model to ask for less output or make fewer calls at a time. A model that asks for five pages at once gets the first few and asks again for the rest; by then the earlier pages can be compacted.

The tool has still run. Give a tool whose output can be large a way to return less, such as a page size, and say so in its description. `Limits::max_tool_output_bytes` remains the limit on a single output, with or without a window.

The bound is in bytes at the estimated density, so it is as wrong as that estimate is. The reserve absorbs results up to a fifth denser than estimated.

## The summary call

A compaction is one model call, made under the same protocol as any other: its intent is committed first, it is charged to the run's model calls, tokens and active time, and a crash before its result is committed makes it again.

The request is the part of the turn's context the compaction will cover, with the same instructions and tools, followed by one user message asking for the summary. The recent entries the compaction keeps are not in it: they are sent as recorded afterwards, and leaving them out is what lets a compaction work when the kept entries alone fill most of the window. What is sent is the start of the turn's own context, so a provider's prefix cache still serves it. `ModelRequest::allow_tool_calls` is false, which the OpenAI-protocol adapters send as `tool_choice: "none"`: the tools stay in the prompt, so the prefix is unchanged, and the model cannot call them. The answer is plain text; an agent whose output is already text keeps its own output schema, so nothing before the instruction changes. Its deltas are not forwarded as progress; `ProgressKind::CompactionStarted` and `ProgressKind::Compacted` are. If what the compaction covers is larger than the window less twice the summary budget, its oldest entries are left out of the request, and so out of the summary.

The built-in instruction asks for five sections (goal, requirements, progress, open items, key details), tells the model to carry forward an earlier summary where newer messages do not contradict it, and gives a word budget. An application whose conversations have a different shape replaces it with `ContextWindow::with_summary_instructions`.

When the model cannot write a usable summary, the engine writes one. If the model calls a tool, answers with nothing or exceeds twice the summary budget, or the provider rejects the summary request as too large, the engine records a compaction whose summary it builds itself: the earlier summary, if any, and the covered inputs and final answers, newest first, each clipped, within the summary budget. `ProgressKind::Compacted` reports which kind was recorded. A conversation can therefore always continue, at the cost of detail.

Any other failure of the summary call, such as an outage, a rate limit or a timeout, fails the turn as it would any model call. It says nothing about whether the model can summarize, and a lossy record made then would stay in the conversation for good. The next run asks for the summary again.

## When the provider says the request is too large

Estimates can be wrong, and a server can be restarted with a smaller window. `Error::ContextOverflow` is what an adapter returns when the provider rejects a request for its size. The OpenAI-protocol adapters recognise llama-server's `exceed_context_size_error`, OpenAI's `context_length_exceeded`, HTTP 413, and the messages that other providers behind OpenAI-compatible endpoints use, such as "prompt is too long" and "maximum context length". They read the message only to classify it; it is quoted only where the adapter already quotes server messages. An adapter for another provider should map that provider's rejection the same way: an overflow it misses fails the run, and another error taken for one costs one compaction.

With a context window configured, the engine answers the first such error on a turn by compacting with half the usual recent part, and makes the turn again. The estimate that allowed the turn was too low, so the summary request may send only half of what the window would otherwise hold. A second overflow on the same turn fails the run, as does any overflow without a context window or with nothing left to cover.

Every request's output allowance, `ModelRequest::remaining_tokens`, is also capped at the room the estimate leaves in the window, so a provider is not asked for more output than could fit.

## Finding the window

The window is part of the agent's binding, like its limits: a stored run resumes under the window it was accepted with. The application supplies it.

For llama-server, `LlamaModel::context_window(model)` asks the server what one request may use (`/v1/models`, `meta.n_ctx`), and returns `None` while the model is not loaded. An application that starts the server itself should set the size explicitly and pass the same number here.

## Hosted models

Nothing in the engine assumes a local model. What changes with a hosted one is what each decision costs.

- **Usage must count the whole prompt.** The estimate starts from `TokenUsage::input` of the latest exchange. A provider that reports cached tokens apart from the rest, as some do, needs its adapter to add them back, or the estimate is low by what the cache served and the engine compacts late.
- **Keep the prefix stable.** A compaction is the only change to earlier content, so every other turn is served from the provider's prompt cache. OpenAI documents `tool_choice: "none"` as the way to stop tool calls without removing the tools, which keeps that cache. A provider whose cache is keyed on the tool choice should not be sent a different one: its adapter should leave the request as it is and rely on the engine discarding a summary that calls a tool.
- **A smaller `tokens` than the model's window is often right.** A frontier model's window can be a million tokens. A conversation that fills it costs that many input tokens on every turn, cached or not, and models answer less well from a full window. Set `tokens` to the context the conversation is worth.

## Running local models on a Mac

These are llama.cpp's defaults at the time of writing, read from its source; measure before relying on the numbers.

- **The context size is a memory decision.** llama-server allocates its key-value cache for the whole context when the model loads: per token, each layer's key and value heads at 16 bits. That is roughly 96 KiB per token for a 48-layer model with four 128-wide heads, and 256 KiB for a 64-layer model with eight, so 3 and 8 GiB at 32,768 tokens and 24 and 64 GiB at 262,144.
- **Without `--ctx-size` the server picks one.** `--fit`, on by default, shrinks the context until the model fits free memory, to anywhere between 4,096 tokens and the trained size. A large machine gives its memory to context nobody uses, and a small one gets a window nobody chose.
- **Set three flags together:** `--ctx-size` from a memory budget, `--parallel 1` when one run is active at a time, and `--cache-ram`, which otherwise keeps up to 8 GiB of evicted prompts in host memory.
- **A smaller window is also a latency bound.** After a compaction, a model reload or a switch between conversations the prompt is cold, and a model that evaluates a few hundred tokens a second takes about a minute per 15,000 tokens.
- **Other requests evict the cache.** A second conversation, a subagent or a title request on the same server replaces the slot's cache unless the server has a slot to spare.

### Measured

One model on one machine: Gemma 4 31B at four bits a weight, llama-server 0.9.0, an M4 Max. It evaluated about 165 prompt tokens a second and generated about 18. The conversations were a reader agent with five tools over a synthetic message store.

- **Time is tokens evaluated for the first time, and most of those are tool results.** With an append-only context, prompt evaluation was 85% of a conversation's wall time, and 86% of the tokens evaluated were tool results seen for the first time. Nothing in the engine can make that cheaper; the tool can. A page of 20 message records was 4,400 tokens, 27 seconds. The same page without fields at their defaults, without a timestamp in two formats, and with each thread's participants listed once was 1,470 tokens, 9 seconds. Over a 14-turn conversation that was 38% fewer tokens evaluated, less than a page saves because the model read more pages. A model tuned on one format may answer worse in another: measure that too.
- **Memory after use is context checkpoints.** A model with sliding-window attention cannot drop the end of its cache, so the server keeps snapshots to roll back to, 32 a slot by default. One conversation grew the server from 22 GiB to 51 GiB, or to 43 GiB with `--parallel 1 --cache-ram 0`. Adding `--ctx-checkpoints 8` held it to 25 GiB with nothing evaluated twice. `--ctx-checkpoints 2` held it to 21 GiB, but evaluated 70% more tokens.
- **A policy that rewrites earlier results needs those checkpoints.** `ContextPolicy::prior_result` changes the previous run's results when the next run is accepted, so the server rolls back to before them. Eight checkpoints were enough for runs of up to four model calls; two were not, and most runs then started with a cold prompt. How far eight reach depends on the server's other settings: with `--batch-size 64` and a 32,768-token context, the run after one of four model calls started cold, and sixteen were needed to avoid that.
- **`--swa-full` is the other way to roll back,** and costs memory up front instead: 11 GiB more at load for a 16,384-token context.
- **The context itself is cheap** for this model, about 76 KiB a token: 2.5 GiB at 32,768 tokens. `--parallel 1` saved another 2.3 GiB at load.
- **An adapter applied at load slowed prompt evaluation by about 15%.** A larger batch size changed nothing.

## Not built yet

- **Clearing tool results inside a run.** `ContextPolicy::prior_result` replaces earlier runs' results at acceptance. A long run still compacts by summary only; a step that replaces its own old results without a model call would be cheaper. It matters most where the window holds about one result: a run that read page after page at 8,192 tokens made a compaction for each page, 24 summary calls in 14 turns, and one run lost track of which pages it had read.
- **Clipping a result instead of refusing it.** A result that does not fit its turn is replaced by an error, and the model asks again. Sending the part that fits would save that round trip, at the cost of a result the model cannot tell is whole.
- **Derived context ahead of history.** Adapters send derived items before the conversation, so a change in them re-evaluates everything after. They belong beside the newest input.
- **Reusing what a compaction keeps.** The entries a compaction keeps are evaluated again, because the summary before them is new. `--cache-reuse` lets llama-server shift cached tokens to a new position instead, but only when the request deletes from the middle of the previous one, so the summary would have to follow the kept entries. It does not work at all on a sliding-window model without `--swa-full`.
- **Cache measurements.** `prompt_tokens_details.cached_tokens` and llama-server's `timings` are not read. They would let a consumer see a cold prompt.
- **Counting tokens exactly.** llama-server's `/v1/chat/completions/input_tokens` could replace the estimate when it is close to the threshold.
