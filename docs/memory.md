# Optional memory

Enable the `memory` feature for extractive summaries, exact cosine retrieval over embeddings the application computes, and an in-process source store. Add `sqlite` to keep memory across restarts in the same database as durable runs. Memory is one implementor of the engine's `ContextPolicy`; it adds no service, network client or model, and nothing changes for agents that do not use it. Memory here always means derived context for the model; the root's `MemoryStore` and `MemoryCheckpoint` are unrelated, and only keep runs in process.

This first slice provides deterministic lead-sentence summaries and retrieval over caller-supplied local embeddings. Applications ingest sources and build derived records explicitly. Abstractive summarization, hosted embedding clients, synchronization with external sources and large-corpus vector databases remain separate adapter work.

## A consumer

```rust
use std::sync::Arc;
use serde_json::json;
use agent_rs::memory::{
    ExtractiveSummarizer, InMemorySourceStore, MemoryPolicy, SourceStore, SourceTrust,
    Summarizer, index_summary,
};
use agent_rs::{Agent, Conversation, ModelResponse, Result, ScriptedModel};

# async fn example() -> Result<()> {
let store = Arc::new(InMemorySourceStore::default());
let mut conversation = Conversation::new("trip-1");
let source = store
    .upsert_source(conversation.id(), "preferences", "Prefer the train. Leave after 09:00.",
        SourceTrust::Approved)
    .await?;
let summarizer = ExtractiveSummarizer::new(2, 256)?;
index_summary(store.as_ref(), "trip-summary", &[source], &summarizer).await?;

let policy = MemoryPolicy::new(store, 32)?.with_summaries(summarizer.producer())?;
let model = ScriptedModel::new(vec![ModelResponse::final_output(json!("Ready"))]);
let agent = Agent::builder("travel-v1", Arc::new(model))
    .instructions("Help plan the trip using the supplied facts.")
    .input::<String>()
    .output::<String>()
    .context_policy(Arc::new(policy), 1024)
    .build()?;
let answer = agent.run(&mut conversation, "Plan my trip").await?;
assert_eq!(answer, "Ready");
# Ok(()) }
```

The scripted model makes this example runnable without credentials; it does not evaluate answer quality. The [long-conversation consumer](https://github.com/danieljvdm/agent-rs/blob/main/examples/support/memory_workflow.rs) observes actual model requests, checks retained facts, and supports the live OpenAI provider.

A source's scope is the conversation ID it belongs to, and `MemoryPolicy` reads only the run's own conversation. `Conversation::default()` has a fresh, unique ID; use `Conversation::new(id)` when memory outlives the process, and keep the ID with the retained history. `Agent::run` and `Agent::resume` refuse a retained run whose conversation ID differs from the conversation's. Durable `accept` already takes an explicit conversation ID. Choose IDs unique across your authorization boundaries. A child of an [attached subagent](attached-subagents.md) runs in its own conversation, `child:{root}:{call}`, so a `MemoryPolicy` on the child agent reads that scope and not its parent's.

For SQLite, pass `Arc<SqliteStore>` as the `SourceStore`: `SqliteStore` implements the trait, on its existing bounded worker and ownership lock. The memory tables (`memory_meta`, `memory_sources`, `memory_records`, `memory_dependencies`) are created on first use, carry their own schema version, and never touch run history, checkpoints, operation evidence or the run schema's version.

## How it fits the engine

`MemoryPolicy` implements `ContextPolicy::prepare` and nothing else: it pins no history and has no cleanup. Each model turn it reads the conversation's records once, ranks retrieved items for the run's input if a retriever is set, and appends current summaries. It returns each item as the JSON encoding of `DerivedContext`, retrieved items first.

The engine treats derived items as opaque JSON, so the policy enforces what it may send. An item is returned only if it is of `DERIVED_CONTEXT_VERSION`, cites 1–128 sources that are all in the run's conversation, has bounded producer and source fields, and has content that fits the derived-context cap on its own. At most 128 items are returned. The engine then keeps the items whose array fits the cap, in order, and drops the lowest-priority ones if pinned history needs the room, as `ContextPolicy` describes. [Steering](steering.md) instructions of the run's own history are pinned by the engine's default selection, with or without a policy, so `MemoryPolicy` never pins them and derived items give way to them as to any pinned history.

The derived-context cap, given to `AgentBuilder::context_policy`, covers each item's encoding including provenance. The policy's binding (its summary producer, read limit and retriever configuration) and the cap are part of the agent binding, so a durable run is never resumed under a different memory configuration.

Preparation runs inside the model operation, after the dispatch checkpoint and within its deadline and cancellation. `SemanticRetriever` yields every 32 records so a deadline or cancellation can interrupt a long ranking. An error from the store keeps its kind: `Error::Storage` (the store is unavailable or closed) makes `drive` return the error and leaves the run resumable, while other kinds fail the run. A failure never falls back to an unchecked cache. No extra generation calls or tokens are hidden by the built-in policy.

## Source and derived-data contract

`MemorySource` is an application-maintained snapshot. Its `SourceRef` names its scope, source ID and an opaque revision. `DerivedContext` carries a format version, a `Summary` or `Retrieved` kind, its producer and version, the content, and the source revisions used. All derived context remains lower-trust reference data, including content from sources approved for indexing.

The model receives derived items in `ModelRequest::derived_context`, separate from `history` and `instructions`; they are never appended to authoritative history or checkpointed. Both OpenAI adapters, Chat Completions and Responses, send them the same way: a labelled reference-data message with provenance and fixed guidance against following embedded instructions, after the instructions and ahead of history; source text never enters a developer message. Custom providers must preserve this distinction. Memory cannot bypass tool authorization or approval.

Applications decide which committed history or external documents to index. For a history entry, a stable ID such as `run:{run_id}/entry:{ordinal}` can identify its source snapshot. The engine never reads a conversation's whole lifetime history on a model turn.

| Operation | Behavior |
| --- | --- |
| Upsert identical text and trust | Keep the source revision and existing derived records. |
| Edit text or change indexing approval | Assign a new revision and atomically remove all summaries and vectors depending on the old one. |
| Delete source | Atomically remove its derived records and source snapshot. Authoritative conversation history remains intact. |
| Recreate a deleted ID | Assign a fresh revision; an old cache or delayed index writer cannot resurrect deleted material. |
| Publish after a concurrent edit or deletion | Return `Error::Stale`; reread the source and rebuild. No partial replacement is published. |
| Read | Return only records whose provenance still exists at the recorded revisions and remains approved, in record-ID order. |

An application must propagate external edits, deletion or loss of access through `upsert_source` and `remove_source` **before** the next model turn. The store cannot detect a change in a system it does not own. The read in `prepare` is the freshness point: an edit after it affects the next turn and cannot revoke a request already sent. Deleting a source does not redact old raw history still selected by ordinary context selection.

## Summarization and retrieval

`ExtractiveSummarizer` selects complete, distinct lead sentences, round-robin across approved sources, until its sentence and content-byte caps are reached. It keeps Unicode text and only the provenance of selected sentences, and skips sentences that do not fit rather than cutting facts mid-sentence. It does not resolve contradictions, extract every salient fact, or infer new facts. Source order is application policy. A summary with no fitting approved sentence is an error.

`Summarizer` is replaceable. Its producer string identifies algorithm and settings, and `MemoryPolicy` uses only summaries from the producer it was given, so changing the algorithm or its settings requires reindexing.

`index_source` uses the application's `Embedder` to build a versioned vector record. `SemanticRetriever` computes exact cosine similarity over the policy's bounded snapshot, considers only records from the same model with the same dimensions, omits those below the minimum similarity, and returns a bounded ranking with record-ID tie breaks. Attach it with `MemoryPolicy::with_retriever`. Model identity must include model revision and preprocessing and dimension choices; quality depends on the supplied model. `Retriever` is replaceable too, and the policy checks whatever a retriever returns.

`Embedder` is bounded local computation. Remote embedding, model-generated summaries and ingestion belong to an explicit application pipeline with its own budgets, never inside `prepare`.

A store read returns 1–4096 records; each source and record content is capped at 1 MiB and vectors at 4096 finite dimensions. This is a small-corpus adapter, not approximate-nearest-neighbor indexing. If the corpus exceeds the read limit, record-ID order decides the candidate set; partition or prune it, or implement a bounded `SourceStore` or `Retriever` of your own. A `SourceStore` implementation must keep validation and invalidation atomic, not merely persist opaque JSON.

## Persistence and recovery

Derived records persist apart from run checkpoints. Memory schema version 1 and derived format version 1 are independent of the run schema. An unknown memory schema version fails with `Error::Incompatible`; records of another format version are skipped until reindexed. Every model turn, including one after a wait or restart, reads a fresh snapshot, and an earlier turn's derived context is never replayed. Run IDs, fencing, cumulative usage, waits and tool replay stay with the engine and the durable store.

`InMemorySourceStore` does not survive the process. Applications using custom stores own their durability and source synchronization. Source snapshots and derived text are sensitive application data and follow the application's storage and retention policy.

## Evaluation

```sh
cargo test --all-features --locked --test context_policy --test memory --test memory_workflow
scripts/rust.sh run --locked --release --features memory --example memory_workflow_eval -- \
  --mode scripted --trials 3 --history-turns 256 --out eval-results/memory-scripted-local
scripts/eval-memory.sh --trials 3 --history-turns 256 --out eval-results/memory-live-local
```

The live wrapper loads credentials from the same Infisical project as `eval-personal.sh`. All history and source data are synthetic, and the only external calls are model requests through the OpenAI Chat Completions adapter, `OpenAiModel`. The default live model and settings match the personal workflow evaluation: `gpt-4.1-2025-04-14` at temperature 0.

Each case copies the same closed SQLite template for the history-only, summary and retrieval policies. All use a 2048-byte context cap and identical queries; the memory policies get at most 1024 derived bytes within that cap. Order rotates across trials. `--background-records N` adds N unrelated indexed documents before the queries.

| Case | Expected behavior |
| --- | --- |
| Retained fact | Memory recalls an old preference omitted by the bounded raw suffix. |
| Updated fact | Editing the source invalidates old records; only the replacement fact is supplied. |
| Untrusted source | Even a forged index request cannot publish a source lacking indexing approval. |
| Deleted source | The old source disappears from derived context; the model abstains when it is absent from the raw suffix too. |
| Recovery | A run waits, the store closes and reopens, the source changes, and the same run resumes with the replacement and its cumulative usage. |

`report.json` (written under the gitignored `eval-results/`) keeps every sample: outcomes, per-request context bytes and derived items, model and tool counts, tokens, time to first output and completion time. History-only controls are expected to abstain when the old fact is absent: `passed` checks that, and `recalled_current_fact` records the accuracy gap separately. Scripted tokens are synthetic byte-based estimates, not tokenizer or provider measurements. The scripted model reads the actual supplied context, so losing memory changes its answer. These cases demonstrate the bounded contract, not general prompt-injection resistance or embedding quality.

### Recorded results

The 2026-10-29 live evaluation, on the pre-port API, used `gpt-4.1-2025-04-14` at temperature 0 with 256 history turns and 256 unrelated indexed documents, and passed all 30 contract cases over two trials. History-only controls recalled 0 of 8 current facts and abstained as expected; the summary and retrieval policies each recalled 8 of 8. All deleted-source samples abstained, and all recovery samples waited, reopened the store and resumed the same run with cumulative usage intact. In the retained-fact case the median context was 1,943 bytes and 471 input tokens for history only, 2,039 bytes and 595 tokens with summaries, and 1,967 bytes and 581 tokens with retrieval; output was 7–9 tokens. The embedder was the synthetic two-axis fixture, so this validates the pipeline and grounding on this fixture, not a production embedding model. Local timings overlapped other work and support no performance claim.
