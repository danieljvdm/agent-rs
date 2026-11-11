//! Cost guards for fixed workloads. Every hard gate is a deterministic count, never wall-clock
//! time. A change that makes the runtime cheaper proves it by lowering one constant below; a
//! change that makes it more expensive fails here instead of regressing silently.
//!
//! Exact counts are asserted with equality, so an improvement must also update this table.
//! Allocation and record-byte budgets hold the measured value and allow `HEADROOM_PERCENT` above
//! it for standard-library and platform differences. Run with `--nocapture` to print every value.
// The SQLite budgets stay in the one table even when the feature is disabled.
#![cfg_attr(not(feature = "sqlite"), allow(dead_code))]

// ---------------------------------------------------------------------------------------------
// Budget table: today's measured values.
// ---------------------------------------------------------------------------------------------

// SQLite transactions for `SqliteStore::claim` plus `Agent::drive` to settlement (exact).
/// A run whose first model response is final: claim, model intent, settlement.
const SQLITE_TRANSACTIONS_NO_TOOLS: u64 = 3;
/// One round of one tool: six commits per single-tool round (model intent, model outcome, tool
/// reservation, tool intent, tool outcome with the refund, return to the model).
const SQLITE_TRANSACTIONS_ONE_TOOL_ROUND: u64 = 9;
/// Four rounds of one tool each.
const SQLITE_TRANSACTIONS_FOUR_TOOL_ROUNDS: u64 = 27;
/// One round of four parallel tools: one commit per tool outcome.
const SQLITE_TRANSACTIONS_FOUR_PARALLEL_TOOLS: u64 = 12;

// Bytes of SQLite records written (every inserted or updated row), from `accept` to settlement.
/// Full copies of the retained prior context written by one four-round run (exact): the one
/// immutable acceptance record. Checkpoints write progress and new history only.
const SQLITE_CONTEXT_COPIES_FOUR_TOOL_ROUNDS: u64 = 1;
/// Of those, copies written from claim through settlement (exact): bytes written by the run's
/// 27 transactions do not depend on how much context the conversation retains.
const SQLITE_CONTEXT_COPIES_CLAIM_TO_SETTLEMENT: u64 = 0;
/// Record bytes written by the same four-round run with no retained context (with headroom).
const SQLITE_RECORD_BYTES_FOUR_TOOL_ROUNDS: u64 = 26_787;

// Tool scheduling under paused Tokio time (exact virtual milliseconds).
/// Five parallel calls sleeping 300/10/10/10/300 ms with `max_parallel_tools = 4`. The sliding
/// window starts the fifth call when the first 10 ms call frees its slot.
const TOOL_SCHEDULE_VIRTUAL_MS: u64 = 310;

// Heap allocations on the driving thread: (count, requested bytes), with headroom.
/// `Agent::run` of a final-only response in an empty conversation.
const ALLOC_RUN_EMPTY_CONVERSATION: Budget = Budget::new(32, 2_148);
/// The same run in a conversation holding 1,024 prior entries: one copy into the run's prior
/// context and one into the model request.
const ALLOC_RUN_1024_PRIOR_ENTRIES: Budget = Budget::new(6_177, 1_737_272);
/// `Agent::run` of four rounds of one tool each in an empty conversation.
const ALLOC_RUN_FOUR_TOOL_ROUNDS: Budget = Budget::new(430, 52_026);
/// `Agent::drive` of a final response streamed as 64 text deltas of 16 bytes, observed through
/// an attached progress channel that a receiver drains concurrently. Includes the scripted
/// model's copy of each delta, so the engine itself must add no allocation per delta. Bytes
/// include the channel's two 32-slot blocks of 64-byte `ProgressEvent`s, a per-channel cost;
/// naming a child's parent (`parent_run_id`) took the event from 40 bytes to 64.
const ALLOC_STREAM_64_DELTAS_OBSERVED: Budget = Budget::new(97, 12_386);
/// `Agent::run` of a final-only response for an agent with typed input and output, in an empty
/// conversation: includes validating the input and the output against their types.
const ALLOC_RUN_TYPED_EMPTY_CONVERSATION: Budget = Budget::new(57, 7_926);
/// `Agent::clone` with four typed tools (once per spawned durable run and per `accept`).
const ALLOC_AGENT_CLONE: Budget = Budget::new(0, 0);
/// `Agent::drive` of a settled run decoded from storage, with four typed tools: only the
/// compatibility check every `drive` performs against the agent's binding.
const ALLOC_DRIVE_BINDING_CHECK: Budget = Budget::new(0, 0);
/// `Tool::validate` then `Tool::execute` of one typed tool call with a string argument.
const ALLOC_TYPED_TOOL_CALL: Budget = Budget::new(5, 382);
/// `Agent::drive` of a final response after 64 cumulative usage snapshots. Exactly as many
/// allocations as the same run without snapshots: the engine adds none per snapshot.
const ALLOC_STREAM_64_USAGE_SNAPSHOTS: Budget = Budget::new(0, 0);
/// `drive_binding_check` for an agent whose model contributes an adapter binding.
const ALLOC_DRIVE_BINDING_CHECK_MODEL_ADAPTER: Budget = Budget::new(0, 0);

// OpenAI adapters, through the hidden `openai::cost_probe`: no network, so only the adapter's
// own encoding and decoding count. (Allocations as above, with headroom; bytes exact.)
/// Decoding a Chat Completions answer of 1,024 bytes streamed as 64 content deltas, one SSE
/// event per network read, through the final response: two allocations per delta (its parsed
/// chunk and its text) and the growing buffers.
const ALLOC_CHAT_DECODE_64_DELTAS: Budget = Budget::new(153, 47_463);
/// The same answer from Responses, with its item lifecycle events around the 64 deltas. Delta
/// events decode without allocating; only the text they yield and the lifecycle objects do.
const ALLOC_RESPONSES_DECODE_64_DELTAS: Budget = Budget::new(219, 47_374);
/// Encoding the Chat Completions request body for a turn with four typed tools and one tool
/// exchange.
const ALLOC_CHAT_REQUEST_BODY: Budget = Budget::new(347, 47_557);
/// The same turn's Responses request body.
const ALLOC_RESPONSES_REQUEST_BODY: Budget = Budget::new(301, 38_926);
/// Bytes of those request bodies on the wire (exact).
const CHAT_REQUEST_BODY_BYTES: usize = 2_260;
/// See `CHAT_REQUEST_BODY_BYTES`.
const RESPONSES_REQUEST_BODY_BYTES: usize = 2_122;

// Type sizes (exact bytes). `Result<T, Error>` is returned on every hot path, so the error must
// stay small: large payloads belong behind a box.
/// `size_of::<Error>()`.
const ERROR_SIZE_BYTES: usize = 16;

// Steering. Unused, it adds nothing to any budget above.
/// SQLite transactions for `SteeringStore::steer` of one new instruction (exact).
const SQLITE_TRANSACTIONS_STEER: u64 = 1;
/// SQLite transactions for a final-only run steered once before its first dispatch, from claim
/// through settlement plus the acceptance (exact): application rides on the dispatch commit.
const SQLITE_TRANSACTIONS_STEERED_BEFORE_DISPATCH: u64 = 4;
/// The same, steered from inside its first model call (exact): the final response reopens to
/// `Ready`, and a second dispatch applies the instruction. The rolled-back completion attempt
/// that found it queued commits nothing.
const SQLITE_TRANSACTIONS_STEERED_DURING_MODEL: u64 = 6;
/// Record bytes one 64-byte instruction adds to the run steered before dispatch, over the same
/// run unsteered: its queue row, its history row and its applied marker (with headroom).
const SQLITE_RECORD_BYTES_PER_STEERING: u64 = 480;
/// `Agent::drive` of a final-only run whose history holds 16 applied steering instructions,
/// all pinned in context.
const ALLOC_DRIVE_16_STEERING_ENTRIES: Budget = Budget::new(32, 6_522);

/// Allowance above a measured allocation or record-byte budget.
const HEADROOM_PERCENT: u64 = 2;

// ---------------------------------------------------------------------------------------------

use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;
use yielded_agent::*;

#[derive(Clone, Copy, Debug)]
struct Budget {
    count: u64,
    bytes: u64,
}
impl Budget {
    const fn new(count: u64, bytes: u64) -> Self {
        Self { count, bytes }
    }
}
fn with_headroom(measured: u64) -> u64 {
    measured + (measured * HEADROOM_PERCENT).div_ceil(100)
}

/// Counts allocations made by the current thread only while armed. Tests run on a current-thread
/// runtime, so all engine work for one test happens on its own thread; other tests running in
/// parallel and SQLite's worker thread are excluded.
mod counting {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;

    thread_local! {
        static ARMED: Cell<bool> = const { Cell::new(false) };
        static COUNT: Cell<u64> = const { Cell::new(0) };
        static BYTES: Cell<u64> = const { Cell::new(0) };
    }
    fn record(size: usize) {
        let _ = ARMED.try_with(|armed| {
            if armed.get() {
                COUNT.with(|count| count.set(count.get() + 1));
                BYTES.with(|bytes| bytes.set(bytes.get() + size as u64));
            }
        });
    }
    pub struct Counting;
    unsafe impl GlobalAlloc for Counting {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let pointer = unsafe { System.alloc(layout) };
            if !pointer.is_null() {
                record(layout.size());
            }
            pointer
        }
        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            let pointer = unsafe { System.alloc_zeroed(layout) };
            if !pointer.is_null() {
                record(layout.size());
            }
            pointer
        }
        unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
            unsafe { System.dealloc(pointer, layout) }
        }
        unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            let pointer = unsafe { System.realloc(pointer, layout, new_size) };
            if !pointer.is_null() {
                record(new_size);
            }
            pointer
        }
    }
    pub fn arm() {
        COUNT.with(|count| count.set(0));
        BYTES.with(|bytes| bytes.set(0));
        ARMED.with(|armed| armed.set(true));
    }
    pub fn disarm() -> (u64, u64) {
        ARMED.with(|armed| armed.set(false));
        (COUNT.with(Cell::get), BYTES.with(Cell::get))
    }
}
#[global_allocator]
static ALLOCATOR: counting::Counting = counting::Counting;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Allocations {
    count: u64,
    bytes: u64,
}
async fn measure<T>(work: impl std::future::Future<Output = T>) -> (T, Allocations) {
    counting::arm();
    let output = work.await;
    let (count, bytes) = counting::disarm();
    (output, Allocations { count, bytes })
}
/// Warm once, then measure three times; one-time initialisation is excluded and every repeat
/// must be identical, so a nondeterministic workload fails loudly instead of flaking.
async fn stable<F, Fut>(name: &str, budget: Budget, mut work: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    work().await;
    let mut seen = Vec::new();
    for _ in 0..3 {
        let future = work();
        seen.push(measure(future).await.1);
    }
    assert!(
        seen.windows(2).all(|pair| pair[0] == pair[1]),
        "{name}: allocation counts are not deterministic: {seen:?}"
    );
    let measured = seen[0];
    eprintln!(
        "{name}: {} allocations, {} bytes (budget {} / {})",
        measured.count, measured.bytes, budget.count, budget.bytes
    );
    assert!(
        measured.count <= with_headroom(budget.count)
            && measured.bytes <= with_headroom(budget.bytes),
        "{name}: {measured:?} exceeds budget {budget:?} plus {HEADROOM_PERCENT}%"
    );
}

#[derive(Deserialize, JsonSchema)]
struct Echo {
    n: u64,
}
#[derive(Deserialize, JsonSchema)]
struct Lookup {
    key: String,
    limit: Option<u32>,
}
fn spec(name: &str) -> ToolSpec {
    ToolSpec::new(name, "1")
        .with_description(format!("Deterministic {name} tool for cost guards."))
        .with_retry(RetryPolicy::ReadOnly)
}
/// Four typed tools; the scripted model only calls `echo`.
fn tools() -> Vec<Arc<dyn Tool>> {
    let mut tools = vec![typed_tool::<Echo, u64, _, _>(
        spec("echo"),
        |args, _| async move { Ok(ToolOutput::Complete(args.n)) },
    )];
    for name in ["search", "fetch", "summarize"] {
        tools.push(typed_tool::<Lookup, String, _, _>(
            spec(name),
            |args, _| async move {
                Ok(ToolOutput::Complete(format!(
                    "{}:{}",
                    args.key,
                    args.limit.unwrap_or(0)
                )))
            },
        ));
    }
    tools
}
fn responses(rounds: usize, batch: usize) -> Vec<ModelResponse> {
    let mut responses: Vec<_> = (0..rounds)
        .map(|round| {
            ModelResponse::tools(
                (0..batch)
                    .map(|index| ToolCall {
                        id: format!("round-{round}-{index}"),
                        name: "echo".into(),
                        arguments: json!({"n": round * batch + index}),
                    })
                    .collect(),
            )
        })
        .collect();
    responses.push(ModelResponse::final_output(json!("done")));
    responses
}
fn agent(rounds: usize, batch: usize) -> Agent {
    Agent::builder(
        "cost",
        Arc::new(ScriptedModel::new(responses(rounds, batch))),
    )
    .instructions("Answer with the scripted result.")
    .tools(tools())
    .build()
    .unwrap()
}
fn assert_completed(state: &RunState, rounds: usize, batch: usize) {
    assert_eq!(
        state.phase,
        Phase::Settled(Settlement::Completed(json!("done")))
    );
    assert_eq!(state.usage.model_calls as usize, rounds + 1);
    assert_eq!(state.usage.tool_calls as usize, rounds * batch);
    assert_eq!(state.history.len(), rounds + 2);
}
fn prior_entries(count: usize) -> Vec<HistoryEntry> {
    (0..count)
        .map(|n| {
            if n % 2 == 0 {
                HistoryEntry::Input(json!({"request": format!("prior question {n}")}))
            } else {
                HistoryEntry::Exchange {
                    response: ModelResponse::final_output(
                        json!({"answer": format!("prior answer {n}")}),
                    ),
                    results: vec![],
                }
            }
        })
        .collect()
}

#[tokio::test]
async fn engine_allocation_budgets() {
    let simple = agent(0, 0);
    stable(
        "run_empty_conversation",
        ALLOC_RUN_EMPTY_CONVERSATION,
        || async {
            let mut conversation = Conversation::default();
            let output: Value = simple.run(&mut conversation, "go").await.unwrap();
            assert_eq!(output, json!("done"));
        },
    )
    .await;

    let prior = prior_entries(1024);
    let mut prepared = Vec::new();
    for _ in 0..4 {
        prepared.push(Conversation::with_history(prior.clone()));
    }
    stable(
        "run_1024_prior_entries",
        ALLOC_RUN_1024_PRIOR_ENTRIES,
        || {
            let mut conversation = prepared.pop().unwrap();
            let simple = &simple;
            async move {
                let output: Value = simple.run(&mut conversation, "go").await.unwrap();
                assert_eq!(output, json!("done"));
                assert_eq!(conversation.history.len(), 1026);
                let state = conversation.last_run.as_ref().unwrap();
                assert_eq!(state.prior_context.len(), 1024);
            }
        },
    )
    .await;

    let rounds = agent(4, 1);
    stable(
        "run_four_tool_rounds",
        ALLOC_RUN_FOUR_TOOL_ROUNDS,
        || async {
            let mut conversation = Conversation::default();
            let _: Value = rounds.run(&mut conversation, "go").await.unwrap();
            assert_completed(conversation.last_run.as_ref().unwrap(), 4, 1);
        },
    )
    .await;
}

const STREAM_DELTAS: usize = 64;
/// Yields before every event, as a network stream would, so the receiver drains each delta
/// while the run is still streaming.
struct PacedModel(Vec<ModelEvent>);
#[async_trait::async_trait]
impl Model for PacedModel {
    async fn stream(
        &self,
        _: ModelRequest,
        _: tokio_util::sync::CancellationToken,
    ) -> Result<ModelStream> {
        Ok(Box::pin(futures::StreamExt::then(
            futures::stream::iter(self.0.clone()),
            |event| async move {
                tokio::task::yield_now().await;
                Ok(event)
            },
        )))
    }
}
#[tokio::test]
async fn observed_stream_allocation_budget() {
    let agent = Agent::builder(
        "cost",
        Arc::new(PacedModel(
            std::iter::repeat_n(ModelEvent::TextDelta("x".repeat(16)), STREAM_DELTAS)
                .chain([ModelEvent::Response(ModelResponse::final_output(json!(
                    "done"
                )))])
                .collect(),
        )),
    )
    .instructions("Stream the scripted answer.")
    .build()
    .unwrap();
    stable(
        "stream_64_deltas_observed",
        ALLOC_STREAM_64_DELTAS_OBSERVED,
        || async {
            let mut state = agent.new_run("cost", json!("go"), &[], None).unwrap();
            let run_id = state.id;
            // Room for every event, so nothing is dropped and the count stays deterministic.
            let (sender, mut receiver) = tokio::sync::mpsc::channel(2 * STREAM_DELTAS);
            let drive = agent.drive(
                &mut state,
                &MemoryCheckpoint,
                RunControl::default().with_events(sender),
            );
            let drain = async {
                let mut deltas = 0;
                while let Some(event) = receiver.recv().await {
                    assert_eq!(event.run_id, run_id);
                    if let ProgressKind::TextDelta(text) = event.kind {
                        assert_eq!(text.len(), 16);
                        deltas += 1;
                    }
                }
                deltas
            };
            // Without cooperative budgeting the interleaving is fixed, not carried across repeats.
            let (driven, deltas) =
                tokio::task::unconstrained(async { tokio::join!(drive, drain) }).await;
            driven.unwrap();
            assert_eq!(deltas, STREAM_DELTAS);
            assert_eq!(
                state.phase,
                Phase::Settled(Settlement::Completed(json!("done")))
            );
        },
    )
    .await;
}

#[tokio::test]
async fn usage_snapshot_allocation_budget() {
    let agent = |snapshots: usize| {
        Agent::builder(
            "cost",
            Arc::new(PacedModel(
                std::iter::repeat_n(ModelEvent::Usage(TokenUsage::new(9, 4)), snapshots)
                    .chain([ModelEvent::Response(
                        ModelResponse::final_output(json!("done"))
                            .with_usage(TokenUsage::new(9, 4)),
                    )])
                    .collect(),
            )),
        )
        .instructions("Report usage while streaming.")
        .build()
        .unwrap()
    };
    let (without, with) = (agent(0), agent(STREAM_DELTAS));
    let mut measured = Vec::new();
    for agent in [&without, &with] {
        let drive = || async {
            let mut state = agent.new_run("cost", json!("go"), &[], None).unwrap();
            agent
                .drive(&mut state, &MemoryCheckpoint, RunControl::default())
                .await
                .unwrap();
            assert_eq!(
                (state.usage.input_tokens, state.usage.output_tokens),
                (9, 4)
            );
        };
        drive().await;
        measured.push(measure(drive()).await.1);
    }
    // The model's copy of its script is one allocation either way, larger by the snapshots.
    let script = (STREAM_DELTAS * std::mem::size_of::<ModelEvent>()) as u64;
    let extra = Allocations {
        count: measured[1].count - measured[0].count,
        bytes: measured[1].bytes - measured[0].bytes - script,
    };
    eprintln!(
        "stream_64_usage_snapshots: {} extra allocations (budget {})",
        extra.count, ALLOC_STREAM_64_USAGE_SNAPSHOTS.count
    );
    assert_eq!(extra.count, ALLOC_STREAM_64_USAGE_SNAPSHOTS.count);
    assert_eq!(extra.bytes, ALLOC_STREAM_64_USAGE_SNAPSHOTS.bytes);
}

/// A model that contributes an adapter identity to the binding.
struct Routed(ScriptedModel);
#[async_trait::async_trait]
impl Model for Routed {
    fn binding(&self) -> Value {
        json!({"provider": "cost", "api": "responses", "version": 1})
    }
    async fn stream(
        &self,
        request: ModelRequest,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<ModelStream> {
        self.0.stream(request, cancel).await
    }
}

#[tokio::test]
async fn model_adapter_binding_allocation_budget() {
    let agent = Agent::builder("cost", Arc::new(Routed(ScriptedModel::new(vec![]))))
        .instructions("Answer with the scripted result.")
        .tools(tools())
        .build()
        .unwrap();
    assert!(agent.binding().get("model_adapter").is_some());
    let mut settled = agent.new_run("cost", json!("go"), &[], None).unwrap();
    settled.phase = Phase::Settled(Settlement::Cancelled);
    let stored = serde_json::to_string(&settled).unwrap();
    let control = RunControl::default();
    stable(
        "drive_binding_check_model_adapter",
        ALLOC_DRIVE_BINDING_CHECK_MODEL_ADAPTER,
        || {
            let mut state: RunState = serde_json::from_str(&stored).unwrap();
            let control = control.clone();
            let agent = &agent;
            async move {
                agent
                    .drive(&mut state, &MemoryCheckpoint, control)
                    .await
                    .unwrap();
            }
        },
    )
    .await;
}

#[tokio::test]
async fn agent_allocation_budgets() {
    let agent = agent(0, 0);
    stable("agent_clone", ALLOC_AGENT_CLONE, || async {
        drop(std::hint::black_box(agent.clone()));
    })
    .await;

    // A stored run shares nothing with the agent in memory, so this is what a host pays.
    let mut settled = agent.new_run("cost", json!("go"), &[], None).unwrap();
    settled.phase = Phase::Settled(Settlement::Cancelled);
    let stored = serde_json::to_string(&settled).unwrap();
    // Cloned, not created, per drive: a new token's first lock allocates its mutex.
    let control = RunControl::default();
    stable("drive_binding_check", ALLOC_DRIVE_BINDING_CHECK, || {
        let mut state: RunState = serde_json::from_str(&stored).unwrap();
        let control = control.clone();
        let agent = &agent;
        async move {
            agent
                .drive(&mut state, &MemoryCheckpoint, control)
                .await
                .unwrap();
        }
    })
    .await;
}

#[tokio::test]
async fn typed_allocation_budgets() {
    #[derive(serde::Serialize, Deserialize, JsonSchema)]
    struct Question {
        text: String,
    }
    #[derive(serde::Serialize, Deserialize, JsonSchema, Debug, PartialEq)]
    struct Answer {
        text: String,
        confidence: u32,
    }
    let agent = Agent::builder(
        "cost",
        Arc::new(ScriptedModel::new(vec![ModelResponse::final_output(
            json!({"text": "done", "confidence": 9}),
        )])),
    )
    .instructions("Answer with the scripted result.")
    .tools(tools())
    .input::<Question>()
    .output::<Answer>()
    .build()
    .unwrap();
    stable(
        "run_typed_empty_conversation",
        ALLOC_RUN_TYPED_EMPTY_CONVERSATION,
        || async {
            let mut conversation = Conversation::default();
            let output: Answer = agent
                .run(&mut conversation, Question { text: "go".into() })
                .await
                .unwrap();
            assert_eq!(output.confidence, 9);
        },
    )
    .await;

    let tool = tools().remove(1);
    let arguments = json!({"key": "a key long enough to need its own allocation", "limit": 3});
    let context = ToolContext::new(RunId::nil(), "call", "key");
    stable("typed_tool_call", ALLOC_TYPED_TOOL_CALL, || {
        let (arguments, context, tool) = (arguments.clone(), context.clone(), &tool);
        async move {
            tool.validate(&arguments).unwrap();
            let output = tool.execute(arguments, context).await.unwrap();
            assert!(matches!(output, ToolOutput::Complete(Value::String(_))));
        }
    })
    .await;
}

#[cfg(feature = "openai")]
#[path = "support/openai_wire.rs"]
mod openai_wire;
#[cfg(feature = "openai")]
mod openai {
    use super::*;
    use crate::openai_wire::Api as Wire;
    use yielded_agent::openai::cost_probe::{self, Api};

    const ANSWER_BYTES: usize = 1024;

    /// One SSE event per network read: the answer `{"value":"xx…"}` in 64 deltas of 16 bytes.
    fn chunks(api: Api) -> Vec<Vec<u8>> {
        let text = format!("{{\"value\":\"{}\"}}", "x".repeat(ANSWER_BYTES - 12));
        let wire = match api {
            Api::ChatCompletions => Wire::Chat,
            Api::Responses => Wire::Responses,
        };
        wire.streamed_text(&text, ANSWER_BYTES / 64)
    }

    #[test]
    fn decode_allocation_budgets() {
        for (name, api, budget) in [
            (
                "chat_decode_64_deltas",
                Api::ChatCompletions,
                ALLOC_CHAT_DECODE_64_DELTAS,
            ),
            (
                "responses_decode_64_deltas",
                Api::Responses,
                ALLOC_RESPONSES_DECODE_64_DELTAS,
            ),
        ] {
            let chunks = chunks(api);
            let chunks: Vec<&[u8]> = chunks.iter().map(Vec::as_slice).collect();
            futures::executor::block_on(stable(name, budget, || async {
                let (mut deltas, mut responses) = (0, 0);
                cost_probe::decode(api, 4096, &chunks, |event| match event {
                    ModelEvent::TextDelta(_) => deltas += 1,
                    ModelEvent::Response(response) => {
                        assert_eq!(response.usage, TokenUsage::new(9, 4));
                        responses += 1;
                    }
                    _ => {}
                })
                .unwrap();
                assert_eq!((deltas, responses), (64, 1), "{name}");
            }));
        }
    }

    fn request() -> ModelRequest {
        #[derive(Deserialize, JsonSchema)]
        #[allow(dead_code)]
        struct Answer {
            text: String,
            confidence: u32,
        }
        let mut request = ModelRequest::new(
            RunId::nil(),
            vec![
                HistoryEntry::Input(json!("Look up the key, then answer.")),
                HistoryEntry::Exchange {
                    response: ModelResponse::tools(vec![ToolCall {
                        id: "call-0".into(),
                        name: "search".into(),
                        arguments: json!({"key": "a key", "limit": 3}),
                    }]),
                    results: vec![ToolResult {
                        call_id: "call-0".into(),
                        value: json!("a key:3"),
                        is_error: false,
                    }],
                },
            ],
        );
        request.instructions = "Answer with the scripted result.".into();
        request.config = Arc::new(ModelConfig::new("cost-model"));
        request.tools = tools().iter().map(|tool| tool.spec().clone()).collect();
        request.output_schema =
            Arc::new(serde_json::to_value(schemars::schema_for!(Answer)).unwrap());
        request
    }

    #[test]
    fn request_body_budgets() {
        let request = request();
        for (name, api, budget, bytes) in [
            (
                "chat_request_body",
                Api::ChatCompletions,
                ALLOC_CHAT_REQUEST_BODY,
                CHAT_REQUEST_BODY_BYTES,
            ),
            (
                "responses_request_body",
                Api::Responses,
                ALLOC_RESPONSES_REQUEST_BODY,
                RESPONSES_REQUEST_BODY_BYTES,
            ),
        ] {
            let body = cost_probe::request_body(api, request.clone()).unwrap();
            eprintln!("{name}_bytes: {} (budget {bytes})", body.len());
            assert_eq!(body.len(), bytes, "{name}");
            futures::executor::block_on(stable(name, budget, || {
                let request = request.clone();
                async move {
                    let body = cost_probe::request_body(api, request).unwrap();
                    assert_eq!(body.len(), bytes);
                }
            }));
        }
    }
}

#[test]
fn error_size() {
    let size = std::mem::size_of::<Error>();
    eprintln!("error_size: {size} bytes (budget {ERROR_SIZE_BYTES})");
    assert_eq!(size, ERROR_SIZE_BYTES);
}

#[tokio::test(start_paused = true)]
async fn sliding_window_starts_the_next_call_when_a_slot_frees() {
    let tool = typed_tool::<u64, u64, _, _>(spec("sleep"), |ms, _| async move {
        tokio::time::sleep(Duration::from_millis(ms)).await;
        Ok(ToolOutput::Complete(ms))
    });
    let delays = [300, 10, 10, 10, 300];
    let calls = delays
        .iter()
        .enumerate()
        .map(|(index, ms)| ToolCall {
            id: format!("call-{index}"),
            name: "sleep".into(),
            arguments: json!(ms),
        })
        .collect();
    let agent = Agent::builder(
        "cost",
        Arc::new(ScriptedModel::new(vec![
            ModelResponse::tools(calls),
            ModelResponse::final_output(json!("done")),
        ])),
    )
    .instructions("Sleep.")
    .tool(tool)
    .limits(Limits::default().with_max_parallel_tools(4))
    .build()
    .unwrap();
    let mut conversation = Conversation::default();
    let started = tokio::time::Instant::now();
    let _: Value = agent.run(&mut conversation, "go").await.unwrap();
    let elapsed = started.elapsed();
    eprintln!("tool_schedule_virtual: {elapsed:?}");
    let HistoryEntry::Exchange { results, .. } = &conversation.history[1] else {
        panic!("missing tool exchange");
    };
    assert_eq!(
        results.iter().map(|r| r.value.clone()).collect::<Vec<_>>(),
        delays.map(|ms| json!(ms)).to_vec(),
        "results must stay in declaration order"
    );
    assert_eq!(elapsed, Duration::from_millis(TOOL_SCHEDULE_VIRTUAL_MS));
}

#[cfg(feature = "sqlite")]
mod sqlite {
    use super::*;
    use std::path::Path;
    use yielded_agent::sqlite::SqliteStore;

    const CONVERSATION: &str = "conversation";
    const SEED_BYTES: usize = 32 * 1024;

    /// Sums the bytes of every record SQLite inserts or rewrites, through triggers created on a
    /// separate connection. SQLite stores whole records, so an update writes the full new row.
    /// Triggers cover every table that exists when installed, including tables added later.
    pub(super) struct RecordMeter(rusqlite::Connection);
    impl RecordMeter {
        pub(super) fn install(path: &Path) -> Self {
            let connection = rusqlite::Connection::open(path).unwrap();
            connection
                .execute_batch(
                    "CREATE TABLE cost_meter(bytes INTEGER NOT NULL); INSERT INTO cost_meter VALUES(0);",
                )
                .unwrap();
            let tables: Vec<String> = connection
                .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name<>'cost_meter'")
                .unwrap()
                .query_map([], |row| row.get(0))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap();
            for table in tables {
                let columns: Vec<String> = connection
                    .prepare("SELECT name FROM pragma_table_info(?1)")
                    .unwrap()
                    .query_map([&table], |row| row.get(0))
                    .unwrap()
                    .collect::<rusqlite::Result<_>>()
                    .unwrap();
                let sum = columns
                    .iter()
                    .map(|column| format!("coalesce(length(CAST(NEW.\"{column}\" AS BLOB)),0)"))
                    .collect::<Vec<_>>()
                    .join("+");
                for event in ["INSERT", "UPDATE"] {
                    connection
                        .execute_batch(&format!(
                            "CREATE TRIGGER \"cost_meter_{table}_{event}\" AFTER {event} ON \"{table}\"
                             BEGIN UPDATE cost_meter SET bytes=bytes+{sum}; END;"
                        ))
                        .unwrap();
                }
            }
            Self(connection)
        }
        pub(super) fn bytes(&self) -> u64 {
            self.0
                .query_row("SELECT bytes FROM cost_meter", [], |row| row.get(0))
                .unwrap()
        }
    }

    struct DurableCost {
        /// Transactions for claim plus drive.
        transactions: u64,
        /// Record bytes written from accept through settlement.
        record_bytes: u64,
        /// Record bytes written from claim through settlement.
        drive_bytes: u64,
        /// Bytes the retained prior context adds to one encoded copy of it.
        context_bytes: u64,
    }
    /// Seeds `seeds` earlier runs of about `SEED_BYTES` each into the conversation, then accepts,
    /// claims and drives a fresh run of `rounds` x `batch` tool calls without a `Host`.
    async fn durable_run(rounds: usize, batch: usize, seeds: usize) -> DurableCost {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("cost.sqlite");
        let store = SqliteStore::open(&path).await.unwrap();
        let seed = Agent::builder(
            "seed",
            Arc::new(ScriptedModel::new(vec![ModelResponse::final_output(
                json!("ok"),
            )])),
        )
        .instructions("Seed retained context.")
        .build()
        .unwrap();
        for index in 0..seeds {
            let input = json!({"retained": format!("{index:04}{}", "x".repeat(SEED_BYTES))});
            let id = store
                .accept(&seed, CONVERSATION, &format!("seed-{index}"), input, None)
                .await
                .unwrap();
            let mut state = store.claim(id).await.unwrap();
            seed.drive(&mut state, &store, RunControl::default())
                .await
                .unwrap();
            assert!(state.is_terminal());
        }
        let meter = RecordMeter::install(&path);
        let agent = agent(rounds, batch);
        let bytes_before = meter.bytes();
        let id = store
            .accept(&agent, CONVERSATION, "measured", json!("go"), None)
            .await
            .unwrap();
        let transactions_before = store.metrics().transactions;
        let accepted_bytes = meter.bytes();
        let mut state = store.claim(id).await.unwrap();
        agent
            .drive(&mut state, &store, RunControl::default())
            .await
            .unwrap();
        let transactions = store.metrics().transactions - transactions_before;
        let record_bytes = meter.bytes() - bytes_before;
        let drive_bytes = meter.bytes() - accepted_bytes;
        assert_completed(&state, rounds, batch);
        assert_eq!(state.prior_context.len(), seeds * 2);
        let context_bytes = if seeds == 0 {
            0
        } else {
            // `"prior_context":[...]` minus the two brackets an empty list also has.
            serde_json::to_vec(&state.prior_context).unwrap().len() as u64 - 2
        };
        drop(meter);
        store.close().await.unwrap();
        DurableCost {
            transactions,
            record_bytes,
            drive_bytes,
            context_bytes,
        }
    }
    /// Whole copies of the retained context in `added` bytes, after rounding away a remainder
    /// of a few digits of wall-clock active time.
    fn context_copies(name: &str, added: u64, context_bytes: u64) -> u64 {
        let copies = (added + context_bytes / 2) / context_bytes;
        let remainder = added.abs_diff(copies * context_bytes);
        eprintln!(
            "{name}: {copies} copies of {context_bytes} context bytes, {added} added bytes, remainder {remainder}"
        );
        assert!(
            remainder < 256,
            "{name}: added bytes are not whole context copies: {remainder}"
        );
        copies
    }

    #[tokio::test]
    async fn sqlite_transactions_per_run() {
        for (name, rounds, batch, budget) in [
            ("no_tools", 0, 0, SQLITE_TRANSACTIONS_NO_TOOLS),
            ("one_tool_round", 1, 1, SQLITE_TRANSACTIONS_ONE_TOOL_ROUND),
            (
                "four_tool_rounds",
                4,
                1,
                SQLITE_TRANSACTIONS_FOUR_TOOL_ROUNDS,
            ),
            (
                "four_parallel_tools",
                1,
                4,
                SQLITE_TRANSACTIONS_FOUR_PARALLEL_TOOLS,
            ),
        ] {
            let cost = durable_run(rounds, batch, 0).await;
            eprintln!("sqlite_transactions_{name}: {}", cost.transactions);
            assert_eq!(cost.transactions, budget, "{name}");
        }
    }

    #[tokio::test]
    async fn sqlite_record_bytes_do_not_grow_with_retained_context_after_acceptance() {
        let empty = durable_run(4, 1, 0).await;
        eprintln!(
            "sqlite_record_bytes_four_tool_rounds: {} (budget {})",
            empty.record_bytes, SQLITE_RECORD_BYTES_FOUR_TOOL_ROUNDS
        );
        assert!(
            empty.record_bytes <= with_headroom(SQLITE_RECORD_BYTES_FOUR_TOOL_ROUNDS),
            "record bytes exceed budget plus {HEADROOM_PERCENT}%"
        );
        for seeds in [2, 4] {
            let retained = durable_run(4, 1, seeds).await;
            assert_eq!(retained.transactions, empty.transactions);
            let copies = context_copies(
                &format!("sqlite_context_copies_four_tool_rounds (seeds {seeds})"),
                retained.record_bytes - empty.record_bytes,
                retained.context_bytes,
            );
            assert_eq!(copies, SQLITE_CONTEXT_COPIES_FOUR_TOOL_ROUNDS);
            let copies = context_copies(
                &format!("sqlite_context_copies_claim_to_settlement (seeds {seeds})"),
                retained.drive_bytes.abs_diff(empty.drive_bytes),
                retained.context_bytes,
            );
            assert_eq!(copies, SQLITE_CONTEXT_COPIES_CLAIM_TO_SETTLEMENT);
        }
    }
}

#[tokio::test]
async fn steering_allocation_budget() {
    let agent = agent(0, 0);
    let mut prepared = agent.new_run("cost", json!("go"), &[], None).unwrap();
    prepared
        .history
        .extend((0..16).map(|n| HistoryEntry::Steering {
            id: RunId::from_u128(n),
            instruction: format!("{n:02} {}", "steer".repeat(12)),
        }));
    let control = RunControl::default();
    stable(
        "drive_16_steering_entries",
        ALLOC_DRIVE_16_STEERING_ENTRIES,
        || {
            let (mut state, control, agent) = (prepared.clone(), control.clone(), &agent);
            async move {
                agent
                    .drive(&mut state, &MemoryCheckpoint, control)
                    .await
                    .unwrap();
                assert!(state.is_terminal());
            }
        },
    )
    .await;
}

#[cfg(feature = "sqlite")]
mod steering {
    use super::*;
    use std::sync::OnceLock;
    use yielded_agent::sqlite::SqliteStore;

    /// 64 bytes, so the per-instruction budget is per a fixed size.
    const INSTRUCTION: &str = "Use metric units for every measurement in the answer, please. OK";

    #[derive(Clone, Copy, PartialEq)]
    enum Steer {
        Never,
        BeforeDispatch,
        DuringModel,
    }
    /// Answers at once, steering its own run from inside the first call when asked to. A second
    /// call must see the instruction.
    struct SteeringModel {
        store: OnceLock<Arc<SqliteStore>>,
        during: bool,
    }
    #[async_trait::async_trait]
    impl Model for SteeringModel {
        async fn stream(
            &self,
            request: ModelRequest,
            _: tokio_util::sync::CancellationToken,
        ) -> Result<ModelStream> {
            let steered = request
                .history
                .iter()
                .any(|entry| matches!(entry, HistoryEntry::Steering { .. }));
            if self.during && request.turn == 1 {
                assert!(!steered);
                let store = self.store.get().unwrap();
                store
                    .steer(request.run_id, "steer", INSTRUCTION.into())
                    .await
                    .unwrap();
            } else if self.during {
                assert!(steered);
            }
            let response = ModelResponse::final_output(json!("done"));
            Ok(Box::pin(futures::stream::once(async {
                Ok(ModelEvent::Response(response))
            })))
        }
    }
    struct SteeredCost {
        transactions: u64,
        record_bytes: u64,
    }
    async fn steered_run(steer: Steer) -> SteeredCost {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("steering.sqlite");
        let store = Arc::new(SqliteStore::open(&path).await.unwrap());
        let model = Arc::new(SteeringModel {
            store: OnceLock::new(),
            during: steer == Steer::DuringModel,
        });
        let _ = model.store.set(store.clone());
        let agent = Agent::builder("cost", model)
            .instructions("Answer with the scripted result.")
            .tools(tools())
            .build()
            .unwrap();
        let meter = sqlite::RecordMeter::install(&path);
        let bytes_before = meter.bytes();
        let id = store
            .accept(&agent, "conversation", "measured", json!("go"), None)
            .await
            .unwrap();
        let transactions_before = store.metrics().transactions;
        if steer == Steer::BeforeDispatch {
            store.steer(id, "steer", INSTRUCTION.into()).await.unwrap();
        }
        let mut state = store.claim(id).await.unwrap();
        agent
            .drive(&mut state, store.as_ref(), RunControl::default())
            .await
            .unwrap();
        assert_eq!(
            state.phase,
            Phase::Settled(Settlement::Completed(json!("done")))
        );
        let steered = usize::from(steer != Steer::Never);
        assert_eq!(
            state.usage.model_calls as usize,
            1 + usize::from(steer == Steer::DuringModel)
        );
        assert_eq!(
            store.steering(id).await.unwrap().len(),
            steered,
            "every instruction applied once"
        );
        let cost = SteeredCost {
            transactions: store.metrics().transactions - transactions_before,
            record_bytes: meter.bytes() - bytes_before,
        };
        drop(meter);
        store.close().await.unwrap();
        cost
    }

    #[tokio::test]
    async fn sqlite_steering_transactions_and_record_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(directory.path().join("steer.sqlite"))
            .await
            .unwrap();
        let agent = agent(0, 0);
        let id = store
            .accept(&agent, "conversation", "run", json!("go"), None)
            .await
            .unwrap();
        let before = store.metrics().transactions;
        store.steer(id, "steer", INSTRUCTION.into()).await.unwrap();
        let steer = store.metrics().transactions - before;
        eprintln!("sqlite_transactions_steer: {steer}");
        assert_eq!(steer, SQLITE_TRANSACTIONS_STEER);
        store.close().await.unwrap();

        let unsteered = steered_run(Steer::Never).await;
        assert_eq!(unsteered.transactions, SQLITE_TRANSACTIONS_NO_TOOLS);
        let before_dispatch = steered_run(Steer::BeforeDispatch).await;
        eprintln!(
            "sqlite_transactions_steered_before_dispatch: {}",
            before_dispatch.transactions
        );
        assert_eq!(
            before_dispatch.transactions,
            SQLITE_TRANSACTIONS_STEERED_BEFORE_DISPATCH
        );
        let during_model = steered_run(Steer::DuringModel).await;
        eprintln!(
            "sqlite_transactions_steered_during_model: {}",
            during_model.transactions
        );
        assert_eq!(
            during_model.transactions,
            SQLITE_TRANSACTIONS_STEERED_DURING_MODEL
        );
        let added = before_dispatch.record_bytes - unsteered.record_bytes;
        eprintln!(
            "sqlite_record_bytes_per_steering: {added} (budget {SQLITE_RECORD_BYTES_PER_STEERING})"
        );
        assert!(
            added <= with_headroom(SQLITE_RECORD_BYTES_PER_STEERING),
            "record bytes per steering exceed budget plus {HEADROOM_PERCENT}%"
        );
    }
}

/// Budgets for the optional `memory` feature, which the core budgets above never exercise.
#[cfg(feature = "memory")]
mod memory {
    use super::*;
    use tokio_util::sync::CancellationToken;
    use yielded_agent::memory::*;

    // ------------------------------------------------------------------------------------------
    // Budget table: today's measured values.
    // ------------------------------------------------------------------------------------------

    // Heap allocations on the driving thread: (count, requested bytes), with headroom.
    /// `MemoryPolicy::prepare` with summaries and retrieval over an empty `InMemorySourceStore`:
    /// what an enabled policy costs a turn when there is nothing to send, including the request's
    /// cancellation token.
    const ALLOC_PREPARE_EMPTY_STORE: Budget = Budget::new(4, 456);
    /// The same over a fixed store: one summary of four sources and 16 retrievable sources with
    /// 8-dimensional embeddings, of which the query matches four and the retriever keeps two.
    /// Mostly the store's copy of its 17-record snapshot and the encoding of the three items sent.
    const ALLOC_PREPARE_FIXED_STORE: Budget = Budget::new(264, 21_585);

    // SQLite transactions per `SourceStore` call on `SqliteStore` once its tables exist (exact).
    /// `upsert_source` of changed text, including its invalidation of dependent records.
    const SQLITE_TRANSACTIONS_UPSERT_SOURCE: u64 = 1;
    /// `upsert_source` of identical text and trust: a read only.
    const SQLITE_TRANSACTIONS_UPSERT_UNCHANGED: u64 = 0;
    /// `index_summary` over four sources: the record and its dependencies in one transaction.
    const SQLITE_TRANSACTIONS_SUMMARY_WRITE: u64 = 1;
    /// Rows that summary write inserts (exact): the record and one dependency per source.
    const SQLITE_WRITES_SUMMARY_OVER_FOUR_SOURCES: u64 = 5;
    /// `remove_source`, deleting the source and the records citing it.
    const SQLITE_TRANSACTIONS_REMOVE_SOURCE: u64 = 1;
    /// `load`, which `MemoryPolicy::prepare` performs once per model turn.
    const SQLITE_TRANSACTIONS_LOAD: u64 = 0;

    // ------------------------------------------------------------------------------------------

    /// One axis per concept: sources mentioning "train" share a direction, the rest another.
    struct Axes;
    impl Embedder for Axes {
        fn identity(&self) -> &str {
            "cost-axes-v1/8"
        }
        fn embed(&self, text: &str) -> Result<Vec<f32>> {
            let mut values = vec![0.0; 8];
            values[usize::from(!text.contains("train"))] = 1.0;
            Ok(values)
        }
    }

    fn summarizer() -> ExtractiveSummarizer {
        ExtractiveSummarizer::new(4, 512).unwrap()
    }
    fn policy(store: Arc<dyn SourceStore>) -> MemoryPolicy {
        MemoryPolicy::new(store, 64)
            .unwrap()
            .with_summaries(summarizer().producer())
            .unwrap()
            .with_retriever(Arc::new(
                SemanticRetriever::new(Arc::new(Axes), 2, 0.5).unwrap(),
            ))
    }

    /// Four preferences summarized together, and 16 indexed notes, four about trains.
    async fn fill(store: &dyn SourceStore) -> Vec<MemorySource> {
        let mut preferences = Vec::new();
        for n in 0..4 {
            let text = format!("Preference {n} is recorded. It was confirmed.");
            let id = format!("preference-{n}");
            preferences.push(
                store
                    .upsert_source("c", &id, &text, SourceTrust::Approved)
                    .await
                    .unwrap(),
            );
        }
        index_summary(store, "summary", &preferences, &summarizer())
            .await
            .unwrap();
        for n in 0..16 {
            let topic = if n % 4 == 0 { "train" } else { "weather" };
            let id = format!("note-{n:02}");
            let text = format!("Note {n} is about the {topic}.");
            let source = store
                .upsert_source("c", &id, &text, SourceTrust::Approved)
                .await
                .unwrap();
            index_source(store, &id, &source, &Axes).await.unwrap();
        }
        preferences
    }

    async fn prepared(policy: &MemoryPolicy, input: &Value) -> Vec<Value> {
        policy
            .prepare(
                ContextRequest::new(RunId::nil(), "c", input, 4096),
                CancellationToken::new(),
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn prepare_allocation_budgets() {
        let input = json!("When does the train leave?");
        let empty = policy(Arc::new(InMemorySourceStore::default()));
        stable("prepare_empty_store", ALLOC_PREPARE_EMPTY_STORE, || async {
            assert!(prepared(&empty, &input).await.is_empty());
        })
        .await;

        let store = Arc::new(InMemorySourceStore::default());
        fill(store.as_ref()).await;
        let fixed = policy(store);
        stable("prepare_fixed_store", ALLOC_PREPARE_FIXED_STORE, || async {
            let items = prepared(&fixed, &input).await;
            let kinds: Vec<_> = items.iter().map(|item| item["kind"].clone()).collect();
            assert_eq!(
                kinds,
                [json!("Retrieved"), json!("Retrieved"), json!("Summary")]
            );
        })
        .await;
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn sqlite_transactions_per_memory_call() {
        use yielded_agent::sqlite::SqliteStore;
        let directory = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(directory.path().join("memory.sqlite"))
            .await
            .unwrap();
        let mut preferences = fill(&store).await;
        let mut last = store.metrics();
        // Transactions and rows written since the previous call.
        let mut delta = |name: &str| {
            let now = store.metrics();
            let (transactions, writes) = (
                now.transactions - last.transactions,
                now.writes - last.writes,
            );
            last = now;
            eprintln!("sqlite_transactions_{name}: {transactions} ({writes} rows)");
            (transactions, writes)
        };
        let text = "Preference 0 is recorded. It changed.";
        preferences[0] = store
            .upsert_source("c", "preference-0", text, SourceTrust::Approved)
            .await
            .unwrap();
        assert_eq!(delta("upsert_source").0, SQLITE_TRANSACTIONS_UPSERT_SOURCE);
        store
            .upsert_source("c", "preference-0", text, SourceTrust::Approved)
            .await
            .unwrap();
        assert_eq!(
            delta("upsert_unchanged").0,
            SQLITE_TRANSACTIONS_UPSERT_UNCHANGED
        );
        index_summary(&store, "summary", &preferences, &summarizer())
            .await
            .unwrap();
        assert_eq!(
            delta("summary_write"),
            (
                SQLITE_TRANSACTIONS_SUMMARY_WRITE,
                SQLITE_WRITES_SUMMARY_OVER_FOUR_SOURCES
            )
        );
        assert_eq!(store.load("c", 64).await.unwrap().len(), 17);
        assert_eq!(delta("load").0, SQLITE_TRANSACTIONS_LOAD);
        store.remove_source("c", "preference-1").await.unwrap();
        assert_eq!(delta("remove_source").0, SQLITE_TRANSACTIONS_REMOVE_SOURCE);
        assert_eq!(store.load("c", 64).await.unwrap().len(), 16);
        store.close().await.unwrap();
    }
}
