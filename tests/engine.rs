use agent_rs::*;
use async_trait::async_trait;
use futures::stream;
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

fn spec(name: &str) -> ToolSpec {
    ToolSpec::new(name, "1")
        .with_description("test")
        .with_retry(RetryPolicy::ReadOnly)
}
fn call(id: &str, name: &str, value: Value) -> ToolCall {
    ToolCall {
        id: id.into(),
        name: name.into(),
        arguments: value,
    }
}
fn agent(
    model: Arc<dyn Model>,
    tools: Vec<Arc<dyn Tool>>,
    limits: Limits,
) -> Agent<String, String> {
    builder(model, tools, limits).build().unwrap()
}
fn builder(
    model: Arc<dyn Model>,
    tools: Vec<Arc<dyn Tool>>,
    limits: Limits,
) -> AgentBuilder<String, String> {
    Agent::builder("test", model)
        .instructions("test")
        .tools(tools)
        .limits(limits)
        .input::<String>()
        .output::<String>()
}
fn state(agent: &Agent<String, String>) -> RunState {
    agent.new_run("test", json!("go"), &[], None).unwrap()
}

/// `build` checks the whole definition once, whatever order the options were set in.
#[test]
fn build_rejects_each_invalid_definition() {
    struct Policy;
    impl ContextPolicy for Policy {
        fn binding(&self) -> Value {
            json!("policy")
        }
    }
    let model: Arc<dyn Model> = Arc::new(ScriptedModel::new(vec![]));
    let tool = |name: &str, version: &str, retry: RetryPolicy| {
        let mut contract = spec(name);
        contract.version = version.into();
        contract.retry = retry;
        typed_tool::<Value, Value, _, _>(contract, |value, _| async move {
            Ok(ToolOutput::Complete(value))
        })
    };
    let reason = |builder: AgentBuilder| match builder.build() {
        Err(Error::Config(reason)) => reason.to_string(),
        Err(error) => panic!("unexpected error {error}"),
        Ok(_) => panic!("invalid definition was built"),
    };
    let ids = "agent ID must be 1..256 bytes";
    assert_eq!(reason(Agent::builder("", model.clone())), ids);
    assert_eq!(reason(Agent::builder("a".repeat(257), model.clone())), ids);
    let limits = "parallelism, byte limits, and timeouts must be positive";
    for invalid in [
        Limits::default().with_max_parallel_tools(0),
        Limits::default().with_max_context_bytes(1),
        Limits::default().with_cleanup_timeout(Duration::from_millis(0)),
    ] {
        assert_eq!(
            reason(Agent::builder("a", model.clone()).limits(invalid)),
            limits
        );
    }
    let names = "tool names must be unique, bounded, and versioned";
    let read_only = || RetryPolicy::ReadOnly;
    assert_eq!(
        reason(
            Agent::builder("a", model.clone())
                .tool(tool("same", "1", read_only()))
                .tool(tool("same", "2", read_only()))
        ),
        names
    );
    assert_eq!(
        reason(Agent::builder("a", model.clone()).tool(tool("", "1", read_only()))),
        names
    );
    assert_eq!(
        reason(Agent::builder("a", model.clone()).tool(tool("unversioned", "", read_only()))),
        names
    );
    assert_eq!(
        reason(Agent::builder("a", model.clone()).tool(tool(
            "effect",
            "1",
            RetryPolicy::Idempotent {
                contract: String::new()
            }
        ))),
        "idempotent tools require a retry contract"
    );
    // The cap is checked against the final limits, set before or after the policy.
    let small = Limits::default().with_max_context_bytes(64);
    assert_eq!(
        reason(
            Agent::builder("a", model.clone())
                .context_policy(Arc::new(Policy), 65)
                .limits(small.clone())
        ),
        "derived context cap must fit the context limit"
    );
    assert!(
        Agent::builder("a", model)
            .limits(small)
            .context_policy(Arc::new(Policy), 64)
            .build()
            .is_ok()
    );
}

#[tokio::test]
async fn typed_mutation_can_report_a_complete_failure_without_becoming_unknown() {
    let mut contract = spec("mutation");
    contract.retry = RetryPolicy::Never;
    let tool = typed_tool::<u32, Value, _, _>(contract, |_, _| async {
        Ok(ToolOutput::Failed(json!({"reason":"rejected"})))
    });
    let agent = agent(
        Arc::new(ScriptedModel::new(vec![
            ModelResponse::tools(vec![call("op", "mutation", json!(1))]),
            ModelResponse::final_output(json!("handled")),
        ])),
        vec![tool],
        Limits::default(),
    );
    let mut state = state(&agent);
    agent
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    assert_eq!(
        state.phase,
        Phase::Settled(Settlement::Completed(json!("handled")))
    );
    let HistoryEntry::Exchange { results, .. } = &state.history[1] else {
        panic!("missing failure")
    };
    assert_eq!(results[0].value, json!({"reason":"rejected"}));
    assert!(results[0].is_error);
    assert_eq!(state.usage.tool_calls, 1);
}

#[tokio::test]
async fn validates_whole_response_before_dispatch_and_preserves_rejected_response() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let tool = typed_tool::<u32, u32, _, _>(spec("echo"), move |arg, _| {
        counter.fetch_add(1, Ordering::SeqCst);
        async move { Ok(ToolOutput::Complete(arg)) }
    });
    let model = Arc::new(ScriptedModel::new(vec![ModelResponse::tools(vec![
        call("a", "echo", json!(1)),
        call("b", "echo", json!("invalid")),
    ])]));
    let agent = agent(model, vec![tool], Limits::default());
    let mut state = state(&agent);
    agent
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(matches!(state.phase, Phase::Settled(Settlement::Failed(_))));
    assert_eq!(state.pending_response.unwrap().tools.len(), 2);
}

#[tokio::test]
async fn duplicate_ids_and_truncated_streams_dispatch_nothing() {
    for events in [
        vec![ModelEvent::Response(ModelResponse::tools(vec![
            call("a", "echo", json!(1)),
            call("a", "echo", json!(2)),
        ]))],
        vec![ModelEvent::TextDelta("unfinished".into())],
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        let tool = typed_tool::<u32, u32, _, _>(spec("echo"), move |arg, _| {
            counter.fetch_add(1, Ordering::SeqCst);
            async move { Ok(ToolOutput::Complete(arg)) }
        });
        let agent = agent(
            Arc::new(ScriptedModel::with_events(vec![events])),
            vec![tool],
            Limits::default(),
        );
        let mut state = state(&agent);
        agent
            .drive(&mut state, &MemoryCheckpoint, RunControl::default())
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(matches!(state.phase, Phase::Settled(Settlement::Failed(_))));
    }
}

struct Probe {
    spec: ToolSpec,
    active: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    cleanup: Arc<AtomicUsize>,
    order: Arc<Mutex<Vec<String>>>,
}
#[async_trait]
impl Tool for Probe {
    fn spec(&self) -> &ToolSpec {
        &self.spec
    }
    fn validate(&self, _: &Value) -> Result<()> {
        Ok(())
    }
    async fn execute(&self, arguments: Value, context: ToolContext) -> Result<ToolOutput<Value>> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        if self.spec.execution == Execution::Sequential {
            assert_eq!(active, 1);
        }
        self.order
            .lock()
            .unwrap()
            .push(format!("start:{}", context.call_id));
        tokio::time::sleep(Duration::from_millis(if context.call_id == "0" {
            8
        } else {
            2
        }))
        .await;
        self.order
            .lock()
            .unwrap()
            .push(format!("end:{}", context.call_id));
        self.active.fetch_sub(1, Ordering::SeqCst);
        Ok(ToolOutput::Complete(arguments))
    }
    async fn cleanup(&self, _: ToolContext) -> Result<()> {
        self.cleanup.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
#[tokio::test]
async fn bounded_parallelism_sequential_barriers_history_order_and_cleanup() {
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let cleanup = Arc::new(AtomicUsize::new(0));
    let order = Arc::new(Mutex::new(vec![]));
    let mut seq = spec("sequential");
    seq.execution = Execution::Sequential;
    let tools: Vec<Arc<dyn Tool>> = [spec("parallel"), seq]
        .into_iter()
        .map(|spec| {
            Arc::new(Probe {
                spec,
                active: active.clone(),
                peak: peak.clone(),
                cleanup: cleanup.clone(),
                order: order.clone(),
            }) as Arc<dyn Tool>
        })
        .collect();
    let calls = (0..8)
        .map(|i| {
            call(
                &i.to_string(),
                if i == 2 { "sequential" } else { "parallel" },
                json!(i),
            )
        })
        .collect();
    let model = Arc::new(ScriptedModel::new(vec![
        ModelResponse::tools(calls),
        ModelResponse::final_output(json!("done")),
    ]));
    let limits = Limits::default().with_max_parallel_tools(2);
    let agent = agent(model.clone(), tools, limits);
    let mut state = state(&agent);
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    agent
        .drive(
            &mut state,
            &MemoryCheckpoint,
            RunControl::default().with_events(tx),
        )
        .await
        .unwrap();
    assert_eq!(
        state.phase,
        Phase::Settled(Settlement::Completed(json!("done")))
    );
    assert_eq!(model.calls(), 2);
    assert_eq!(state.usage.tool_calls, 8);
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(peak.load(Ordering::SeqCst), 2);
    assert_eq!(cleanup.load(Ordering::SeqCst), 8);
    let order = order.lock().unwrap();
    assert!(
        order.iter().position(|v| v == "end:0").unwrap()
            < order.iter().position(|v| v == "start:2").unwrap()
    );
    assert!(
        order.iter().position(|v| v == "end:2").unwrap()
            < order.iter().position(|v| v == "start:3").unwrap()
    );
    let HistoryEntry::Exchange { results, .. } = &state.history[1] else {
        panic!("missing exchange")
    };
    assert_eq!(
        results
            .iter()
            .map(|r| r.call_id.clone())
            .collect::<Vec<_>>(),
        (0..8).map(|i| i.to_string()).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn approval_and_input_waits_do_not_consume_budget_while_waiting() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let mut approved = spec("question");
    approved.requires_approval = true;
    let tool = typed_tool::<u32, Value, _, _>(approved, move |_, _| {
        counter.fetch_add(1, Ordering::SeqCst);
        async {
            Ok(ToolOutput::Wait(WaitRequest::Input {
                prompt: "Where?".into(),
            }))
        }
    });
    let model = Arc::new(ScriptedModel::new(vec![
        ModelResponse::tools(vec![call("question", "question", json!(1))]),
        ModelResponse::final_output(json!("done")),
    ]));
    let agent = agent(model, vec![tool], Limits::default());
    let mut state = state(&agent);
    agent
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    assert!(matches!(
        state.operations[0].status,
        OperationStatus::Approval
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let usage = state.usage.clone();
    agent
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    assert_eq!(usage, state.usage);
    state
        .resolve("question", Resolution::Approve(true))
        .unwrap();
    agent
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    assert!(matches!(
        state.operations[0].status,
        OperationStatus::Waiting(WaitRequest::Input { .. })
    ));
    let usage = state.usage.clone();
    agent
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    assert_eq!(usage, state.usage);
    state
        .resolve("question", Resolution::Input(json!("there")))
        .unwrap();
    agent
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    assert!(matches!(
        state.phase,
        Phase::Settled(Settlement::Completed(_))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn recovered_model_attempt_does_not_replenish_call_budget() {
    let model = Arc::new(ScriptedModel::new(vec![ModelResponse::final_output(
        json!("must not run"),
    )]));
    let agent = agent(
        model.clone(),
        vec![],
        Limits::default().with_max_model_calls(1),
    );
    let mut state = state(&agent);
    state.phase = Phase::ModelInFlight;
    state.usage.model_calls = 1;
    state.usage.active_ms = 123;
    agent
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    assert_eq!(
        state.phase,
        Phase::Settled(Settlement::Stopped(StopReason::ModelCalls))
    );
    assert_eq!(state.usage.active_ms, 123);
    assert_eq!(model.calls(), 0);
}

#[tokio::test]
async fn cancellation_retains_unknown_mutation_and_allows_reconciliation() {
    let mut mutation = spec("mutate");
    mutation.retry = RetryPolicy::Never;
    let tool = typed_tool::<u32, u32, _, _>(mutation, |_, _| async { panic!("must not replay") });
    let agent = agent(
        Arc::new(ScriptedModel::new(vec![])),
        vec![tool.clone()],
        Limits::default(),
    );
    let mut state = state(&agent);
    state.phase = Phase::Tools;
    state.operations.push(Operation::new(
        call("effect", "mutate", json!(1)),
        tool.spec().clone(),
        "stable",
        OperationStatus::Dispatched,
    ));
    state.cancel_requested = true;
    agent
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    assert_eq!(state.phase, Phase::Settled(Settlement::Cancelled));
    assert!(matches!(
        state.operations[0].status,
        OperationStatus::Unknown { .. }
    ));
    state
        .resolve(
            "effect",
            Resolution::Reconcile(ToolResult {
                call_id: "effect".into(),
                value: json!("actually succeeded"),
                is_error: false,
            }),
        )
        .unwrap();
    assert_eq!(state.phase, Phase::Settled(Settlement::Cancelled));
    assert!(matches!(
        state.operations[0].status,
        OperationStatus::Complete(_)
    ));
}

#[test]
fn context_keeps_whole_groups_and_rejects_an_oversized_newest_group() {
    let group = HistoryEntry::Exchange {
        response: ModelResponse::tools(vec![call("a", "echo", json!(1))]),
        results: vec![ToolResult {
            call_id: "a".into(),
            value: json!(2),
            is_error: false,
        }],
    };
    let input = HistoryEntry::Input(json!("old input"));
    let limit = serde_json::to_vec(&vec![group.clone()]).unwrap().len();
    // A run accepts the bounded suffix of its conversation that fits the context limit.
    let accept = |max_context_bytes, conversation: &[HistoryEntry]| {
        let limits = Limits::default().with_max_context_bytes(max_context_bytes);
        agent(Arc::new(ScriptedModel::new(vec![])), vec![], limits).new_run(
            "c",
            json!("go"),
            conversation,
            None,
        )
    };
    let state = accept(limit, &[input, group.clone()]).unwrap();
    assert_eq!(state.prior_context, vec![group.clone()]);
    assert!(matches!(
        accept(limit - 1, &[group]),
        Err(Error::Context(_))
    ));
}

struct EndlessModel;
#[async_trait]
impl Model for EndlessModel {
    async fn stream(&self, _: ModelRequest, _: CancellationToken) -> Result<ModelStream> {
        Ok(Box::pin(stream::repeat_with(|| {
            Ok(ModelEvent::TextDelta(String::new()))
        })))
    }
}
#[tokio::test]
async fn empty_stream_events_have_a_hard_bound() {
    let agent = agent(
        Arc::new(EndlessModel),
        vec![],
        Limits::default().with_max_response_bytes(16),
    );
    let mut state = state(&agent);
    let (tx, mut rx) = tokio::sync::mpsc::channel(1024);
    agent
        .drive(
            &mut state,
            &MemoryCheckpoint,
            RunControl::default().with_events(tx),
        )
        .await
        .unwrap();
    assert!(matches!(state.phase, Phase::Settled(Settlement::Failed(_))));
    let mut deltas = 0;
    while let Some(event) = rx.recv().await {
        deltas += usize::from(matches!(event.kind, ProgressKind::TextDelta(_)));
    }
    assert_eq!(
        deltas, 16,
        "the model event bound is the response byte limit"
    );
}

struct FailBeforeModel;
#[async_trait]
impl Checkpoint for FailBeforeModel {
    async fn commit(&self, _: &mut RunState) -> Result<()> {
        Err(Error::storage("injected"))
    }
}
#[tokio::test]
async fn failed_intent_commit_never_dispatches() {
    let model = Arc::new(ScriptedModel::new(vec![ModelResponse::final_output(
        json!("never"),
    )]));
    let agent = agent(model.clone(), vec![], Limits::default());
    let mut state = state(&agent);
    assert!(matches!(
        agent
            .drive(&mut state, &FailBeforeModel, RunControl::default())
            .await,
        Err(Error::Storage(_))
    ));
    assert_eq!(model.calls(), 0);
}

#[tokio::test]
async fn reused_call_ids_cannot_accept_a_stale_resolution() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let tool = typed_tool::<u32, u32, _, _>(spec("echo"), move |arg, _| {
        counter.fetch_add(1, Ordering::SeqCst);
        async move { Ok(ToolOutput::Complete(arg)) }
    });
    let model = Arc::new(ScriptedModel::new(vec![
        ModelResponse::tools(vec![call("reused", "echo", json!(1))]),
        ModelResponse::tools(vec![call("reused", "echo", json!(2))]),
    ]));
    let agent = agent(model, vec![tool], Limits::default());
    let mut state = state(&agent);
    agent
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(matches!(state.phase, Phase::Settled(Settlement::Failed(_))));
}

struct CancellableEffect {
    spec: ToolSpec,
    cancellation: CancellationToken,
    cleanup: Arc<AtomicUsize>,
}
#[async_trait]
impl Tool for CancellableEffect {
    fn spec(&self) -> &ToolSpec {
        &self.spec
    }
    fn validate(&self, _: &Value) -> Result<()> {
        Ok(())
    }
    async fn execute(&self, _: Value, _: ToolContext) -> Result<ToolOutput<Value>> {
        self.cancellation.cancel();
        std::future::pending().await
    }
    async fn cleanup(&self, _: ToolContext) -> Result<()> {
        self.cleanup.fetch_add(1, Ordering::SeqCst);
        Err(Error::tool("cleanup could not establish outcome"))
    }
}
#[tokio::test]
async fn cancelled_idempotent_effect_is_unknown_even_when_cleanup_fails() {
    let cancellation = CancellationToken::new();
    let cleanup = Arc::new(AtomicUsize::new(0));
    let mut effect_spec = spec("effect");
    effect_spec.retry = RetryPolicy::Idempotent {
        contract: "external-key-v1".into(),
    };
    let tool = Arc::new(CancellableEffect {
        spec: effect_spec,
        cancellation: cancellation.clone(),
        cleanup: cleanup.clone(),
    });
    let model = Arc::new(ScriptedModel::new(vec![ModelResponse::tools(vec![call(
        "effect",
        "effect",
        json!(null),
    )])]));
    let agent = agent(model, vec![tool], Limits::default());
    let mut state = state(&agent);
    agent
        .drive(
            &mut state,
            &MemoryCheckpoint,
            RunControl::default().with_cancel(cancellation),
        )
        .await
        .unwrap();
    assert_eq!(state.phase, Phase::Settled(Settlement::Cancelled));
    assert!(matches!(
        state.operations[0].status,
        OperationStatus::Unknown { .. }
    ));
    assert_eq!(cleanup.load(Ordering::SeqCst), 1);
}

struct FailedModelCleanup;
#[async_trait]
impl Model for FailedModelCleanup {
    async fn stream(&self, _: ModelRequest, _: CancellationToken) -> Result<ModelStream> {
        Ok(Box::pin(stream::iter([Ok(ModelEvent::Response(
            ModelResponse::final_output(json!("done")).with_usage(TokenUsage::new(7, 5)),
        ))])))
    }
    async fn cleanup(&self, _: RunId) -> Result<()> {
        Err(Error::provider("cleanup failed"))
    }
}
#[tokio::test]
async fn model_cleanup_failure_preserves_received_evidence_and_tokens() {
    let agent = agent(Arc::new(FailedModelCleanup), vec![], Limits::default());
    let mut state = state(&agent);
    agent
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    assert!(matches!(state.phase, Phase::Settled(Settlement::Failed(_))));
    assert_eq!(state.usage.input_tokens, 7);
    assert_eq!(state.usage.output_tokens, 5);
    assert_eq!(state.pending_response.unwrap().output, Some(json!("done")));
}

#[tokio::test]
async fn usage_snapshots_survive_invalid_streams_and_are_not_double_charged() {
    let usage = || ModelEvent::Usage(TokenUsage::new(9, 4));
    let response = || {
        ModelEvent::Response(
            ModelResponse::final_output(json!("done")).with_usage(TokenUsage::new(9, 4)),
        )
    };
    for (events, completed) in [
        // The response repeats the snapshot's totals.
        (vec![usage(), usage(), response()], true),
        (
            vec![ModelEvent::Usage(TokenUsage::new(3, 0)), response()],
            true,
        ),
        // A stream that ends without a response, or sends data after it.
        (vec![usage()], false),
        (
            vec![response(), ModelEvent::TextDelta("trailing".into())],
            false,
        ),
        // A decreasing snapshot fails the turn, keeping the highest counts.
        (
            vec![usage(), ModelEvent::Usage(TokenUsage::new(8, 3))],
            false,
        ),
        (
            vec![
                ModelEvent::Usage(TokenUsage::new(9, 5)),
                ModelEvent::Response(
                    ModelResponse::final_output(json!("done")).with_usage(TokenUsage::new(9, 4)),
                ),
            ],
            false,
        ),
    ] {
        let decreased = matches!(&events[0], ModelEvent::Usage(u) if u.output == 5);
        let agent = agent(
            Arc::new(ScriptedModel::with_events(vec![events])),
            vec![],
            Limits::default(),
        );
        let mut state = state(&agent);
        agent
            .drive(&mut state, &MemoryCheckpoint, RunControl::default())
            .await
            .unwrap();
        let expected = if decreased { (9, 5) } else { (9, 4) };
        assert_eq!(
            (state.usage.input_tokens, state.usage.output_tokens),
            expected
        );
        assert_eq!(
            matches!(state.phase, Phase::Settled(Settlement::Completed(_))),
            completed,
            "{:?}",
            state.phase
        );
    }
}

/// Reports usage, then ends the attempt as `end` says.
struct UsageThen {
    end: UsageEnd,
    trigger: CancellationToken,
    requests: Mutex<Vec<(u32, u64)>>,
    cleanups: AtomicUsize,
}
#[derive(Clone, Copy, Debug, PartialEq)]
enum UsageEnd {
    /// Fires `trigger` (shutdown or cancellation), then never finishes.
    Trigger,
    /// Never finishes, so the operation times out.
    Hang,
    /// Completes, then its cleanup fails.
    CleanupFails,
}
impl UsageThen {
    fn new(end: UsageEnd) -> Arc<Self> {
        Arc::new(Self {
            end,
            trigger: CancellationToken::new(),
            requests: Mutex::new(vec![]),
            cleanups: AtomicUsize::new(0),
        })
    }
}
#[async_trait]
impl Model for UsageThen {
    async fn stream(&self, request: ModelRequest, _: CancellationToken) -> Result<ModelStream> {
        use futures::StreamExt;
        self.requests
            .lock()
            .unwrap()
            .push((request.turn, request.remaining_tokens));
        if request.turn > 1 {
            return Ok(Box::pin(stream::iter([Ok(ModelEvent::Response(
                ModelResponse::final_output(json!("done")).with_usage(TokenUsage::new(2, 3)),
            ))])));
        }
        let usage = stream::iter([Ok(ModelEvent::Usage(TokenUsage::new(9, 4)))]);
        Ok(match self.end {
            UsageEnd::Trigger => {
                let trigger = self.trigger.clone();
                Box::pin(usage.chain(stream::once(async move {
                    trigger.cancel();
                    std::future::pending().await
                })))
            }
            UsageEnd::Hang => Box::pin(usage.chain(stream::pending())),
            UsageEnd::CleanupFails => {
                Box::pin(usage.chain(stream::iter([Ok(ModelEvent::Response(
                    ModelResponse::final_output(json!("done")).with_usage(TokenUsage::new(9, 4)),
                ))])))
            }
        })
    }
    async fn cleanup(&self, _: RunId) -> Result<()> {
        self.cleanups.fetch_add(1, Ordering::SeqCst);
        if self.end == UsageEnd::CleanupFails {
            return Err(Error::provider("cleanup failed"));
        }
        Ok(())
    }
}

#[tokio::test]
async fn reported_usage_survives_interruption_recovery_and_caps_the_next_attempt() {
    let model = UsageThen::new(UsageEnd::Trigger);
    let agent = agent(
        model.clone(),
        vec![],
        Limits::default()
            .with_max_model_calls(2)
            .with_max_tokens(100),
    );
    let mut state = state(&agent);
    let control = RunControl::default().with_shutdown(model.trigger.clone());
    assert!(matches!(
        agent.drive(&mut state, &MemoryCheckpoint, control).await,
        Err(Error::Interrupted)
    ));
    assert_eq!(state.phase, Phase::Ready);
    assert_eq!(
        (
            state.usage.model_calls,
            state.usage.input_tokens,
            state.usage.output_tokens
        ),
        (1, 9, 4)
    );
    // The interruption checkpoint carries the charge through recovery.
    let mut restored: RunState =
        serde_json::from_slice(&serde_json::to_vec(&state).unwrap()).unwrap();
    agent
        .drive(&mut restored, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    assert!(matches!(
        restored.phase,
        Phase::Settled(Settlement::Completed(_))
    ));
    assert_eq!(
        (
            restored.usage.model_calls,
            restored.usage.input_tokens,
            restored.usage.output_tokens
        ),
        (2, 11, 7)
    );
    assert_eq!(*model.requests.lock().unwrap(), [(1, 100), (2, 87)]);
    assert_eq!(model.cleanups.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn reported_usage_survives_cancellation_timeout_and_cleanup_failure() {
    for end in [UsageEnd::Trigger, UsageEnd::Hang, UsageEnd::CleanupFails] {
        let model = UsageThen::new(end);
        let agent = agent(
            model.clone(),
            vec![],
            Limits::default().with_operation_timeout(Duration::from_millis(20)),
        );
        let mut state = state(&agent);
        let control = RunControl::default().with_cancel(model.trigger.clone());
        agent
            .drive(&mut state, &MemoryCheckpoint, control)
            .await
            .unwrap();
        match end {
            UsageEnd::Trigger => assert_eq!(state.phase, Phase::Settled(Settlement::Cancelled)),
            UsageEnd::Hang => assert!(
                matches!(&state.phase, Phase::Settled(Settlement::Failed(reason)) if reason.contains("timed out")),
                "{:?}",
                state.phase
            ),
            UsageEnd::CleanupFails => {
                assert!(
                    matches!(&state.phase, Phase::Settled(Settlement::Failed(reason)) if reason.contains("cleanup failed")),
                    "{:?}",
                    state.phase
                );
                assert!(state.pending_response.is_some());
            }
        }
        assert_eq!(
            (state.usage.input_tokens, state.usage.output_tokens),
            (9, 4),
            "{end:?}"
        );
        assert_eq!(model.cleanups.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn convenience_api_retains_wait_and_can_resume_without_duplicate_history() {
    let tool = typed_tool::<u32, Value, _, _>(spec("question"), |_, _| async {
        Ok(ToolOutput::Wait(WaitRequest::Input {
            prompt: "Where?".into(),
        }))
    });
    let model = Arc::new(ScriptedModel::new(vec![
        ModelResponse::tools(vec![call("question", "question", json!(1))]),
        ModelResponse::final_output(json!("done")),
    ]));
    let agent = agent(model, vec![tool], Limits::default());
    let mut conversation = Conversation::default();
    assert!(agent.run(&mut conversation, "go").await.is_err());
    assert_eq!(conversation.history.len(), 1);
    assert!(matches!(
        agent.run(&mut conversation, "other").await,
        Err(Error::ConversationBusy)
    ));
    conversation
        .last_run
        .as_mut()
        .unwrap()
        .resolve("question", Resolution::Input(json!("here")))
        .unwrap();
    let result: String = agent
        .resume(&mut conversation, RunControl::default())
        .await
        .unwrap();
    assert_eq!(result, "done");
    assert_eq!(conversation.history.len(), 3);
    assert!(conversation.last_run.as_ref().unwrap().is_terminal());
}

struct FailAfterFirstTool;
#[async_trait]
impl Checkpoint for FailAfterFirstTool {
    async fn commit(&self, state: &mut RunState) -> Result<()> {
        if state
            .operations
            .iter()
            .any(|op| matches!(op.status, OperationStatus::Complete(_)))
        {
            return Err(Error::storage("injected outcome commit failure"));
        }
        state.revision += 1;
        Ok(())
    }
}
#[tokio::test]
async fn outcome_commit_failure_still_joins_and_cleans_sibling_tools() {
    let active = Arc::new(AtomicUsize::new(0));
    let cleanup = Arc::new(AtomicUsize::new(0));
    let tool = Arc::new(Probe {
        spec: spec("probe"),
        active: active.clone(),
        peak: Arc::new(AtomicUsize::new(0)),
        cleanup: cleanup.clone(),
        order: Arc::new(Mutex::new(vec![])),
    });
    let model = Arc::new(ScriptedModel::new(vec![ModelResponse::tools(vec![
        call("0", "probe", json!(1)),
        call("1", "probe", json!(2)),
    ])]));
    let agent = agent(model, vec![tool], Limits::default());
    let mut state = state(&agent);
    assert!(matches!(
        agent
            .drive(&mut state, &FailAfterFirstTool, RunControl::default())
            .await,
        Err(Error::Storage(_))
    ));
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(cleanup.load(Ordering::SeqCst), 2);
}

struct SlowModel {
    cleanup: Arc<AtomicUsize>,
}
#[async_trait]
impl Model for SlowModel {
    async fn stream(&self, _: ModelRequest, _: CancellationToken) -> Result<ModelStream> {
        tokio::time::sleep(Duration::from_secs(10)).await;
        Ok(Box::pin(stream::iter([Ok(ModelEvent::Response(
            ModelResponse::final_output(json!("late")),
        ))])))
    }
    async fn cleanup(&self, _: RunId) -> Result<()> {
        self.cleanup.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
#[tokio::test]
async fn wall_expiry_interrupts_model_and_keeps_its_cleanup() {
    let cleanup = Arc::new(AtomicUsize::new(0));
    let agent = agent(
        Arc::new(SlowModel {
            cleanup: cleanup.clone(),
        }),
        vec![],
        Limits::default(),
    );
    let mut state = agent
        .new_run("expiry", json!("go"), &[], Some(now_ms() + 30))
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(2),
        agent.drive(&mut state, &MemoryCheckpoint, RunControl::default()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        state.phase,
        Phase::Settled(Settlement::Stopped(StopReason::Expired))
    );
    assert_eq!(cleanup.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn wall_expiry_interrupts_mutation_and_retains_uncertainty() {
    let started = Arc::new(AtomicUsize::new(0));
    let counter = started.clone();
    let mut mutation = spec("slow");
    mutation.retry = RetryPolicy::Never;
    let tool = typed_tool::<Value, Value, _, _>(mutation, move |_, _| {
        counter.fetch_add(1, Ordering::SeqCst);
        async {
            tokio::time::sleep(Duration::from_secs(10)).await;
            Ok(ToolOutput::Complete(json!("late")))
        }
    });
    let model = Arc::new(ScriptedModel::new(vec![ModelResponse::tools(vec![call(
        "slow",
        "slow",
        json!(null),
    )])]));
    let agent = agent(model, vec![tool], Limits::default());
    let mut state = agent
        .new_run("expiry", json!("go"), &[], Some(now_ms() + 50))
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(2),
        agent.drive(&mut state, &MemoryCheckpoint, RunControl::default()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(started.load(Ordering::SeqCst), 1);
    assert_eq!(
        state.phase,
        Phase::Settled(Settlement::Stopped(StopReason::Expired))
    );
    assert!(matches!(
        state.operations[0].status,
        OperationStatus::Unknown { .. }
    ));
}

struct FailSettlement;
#[async_trait]
impl Checkpoint for FailSettlement {
    async fn commit(&self, state: &mut RunState) -> Result<()> {
        if state.is_terminal() {
            return Err(Error::storage("settlement unavailable"));
        }
        state.revision += 1;
        Ok(())
    }
}
#[tokio::test]
async fn settlement_event_is_emitted_only_after_a_successful_commit() {
    let model = Arc::new(ScriptedModel::new(vec![ModelResponse::final_output(
        json!("done"),
    )]));
    let agent = agent(model, vec![], Limits::default());
    let mut state = state(&agent);
    let (tx, mut rx) = tokio::sync::mpsc::channel(20);
    assert!(matches!(
        agent
            .drive(
                &mut state,
                &FailSettlement,
                RunControl::default().with_events(tx)
            )
            .await,
        Err(Error::Storage(_))
    ));
    while let Some(event) = rx.recv().await {
        assert!(!matches!(event.kind, ProgressKind::Settled));
    }
}

struct Deny {
    calls: Arc<AtomicUsize>,
}
#[async_trait]
impl Authorizer for Deny {
    async fn authorize(&self, _: &RunState, _: &ToolCall) -> Result<bool> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(false)
    }
}
#[tokio::test]
async fn approval_grant_still_requires_current_authorization() {
    let authorizations = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let mut approved = spec("effect");
    approved.requires_approval = true;
    let tool = typed_tool::<u32, u32, _, _>(approved, move |arg, _| {
        counter.fetch_add(1, Ordering::SeqCst);
        async move { Ok(ToolOutput::Complete(arg)) }
    });
    let model = Arc::new(ScriptedModel::new(vec![ModelResponse::tools(vec![call(
        "effect",
        "effect",
        json!(1),
    )])]));
    let agent = builder(model, vec![tool], Limits::default())
        .authorizer(Arc::new(Deny {
            calls: authorizations.clone(),
        }))
        .build()
        .unwrap();
    let mut state = state(&agent);
    agent
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    assert!(matches!(
        state.operations[0].status,
        OperationStatus::Approval
    ));
    assert_eq!(authorizations.load(Ordering::SeqCst), 0);
    state.resolve("effect", Resolution::Approve(true)).unwrap();
    agent
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    assert_eq!(authorizations.load(Ordering::SeqCst), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(matches!(state.phase, Phase::Settled(Settlement::Failed(_))));
}

#[tokio::test]
async fn convenience_api_requires_reconciliation_before_replacing_cancelled_run() {
    let mut mutation = spec("mutate");
    mutation.retry = RetryPolicy::Never;
    let tool = typed_tool::<u32, u32, _, _>(mutation, |_, _| async { panic!("must not replay") });
    let agent = agent(
        Arc::new(ScriptedModel::new(vec![ModelResponse::final_output(
            json!("new run"),
        )])),
        vec![tool.clone()],
        Limits::default(),
    );
    let mut state = state(&agent);
    let original_id = state.id;
    state.phase = Phase::Tools;
    state.operations.push(Operation::new(
        call("effect", "mutate", json!(1)),
        tool.spec().clone(),
        "stable",
        OperationStatus::Dispatched,
    ));
    state.cancel_requested = true;
    agent
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    let mut conversation = Conversation::new(state.conversation_id.clone());
    conversation.history = state.history.clone();
    conversation.last_run = Some(state);
    assert!(matches!(
        agent.run(&mut conversation, "next").await,
        Err(Error::ConversationBusy)
    ));
    let retained = conversation.last_run.as_mut().unwrap();
    assert_eq!(retained.id, original_id);
    assert_eq!(retained.phase, Phase::Settled(Settlement::Cancelled));
    assert!(matches!(
        retained.operations[0].status,
        OperationStatus::Unknown { .. }
    ));
    retained
        .resolve(
            "effect",
            Resolution::Reconcile(ToolResult {
                call_id: "effect".into(),
                value: json!("effect accounted for"),
                is_error: false,
            }),
        )
        .unwrap();
    let result: String = agent.run(&mut conversation, "next").await.unwrap();
    assert_eq!(result, "new run");
    assert_ne!(conversation.last_run.as_ref().unwrap().id, original_id);
}

#[tokio::test]
async fn recovered_dispatched_work_becomes_resolvable_when_budgets_or_expiry_stop_it() {
    for expired in [false, true] {
        let mut effect = spec("idempotent");
        effect.retry = RetryPolicy::Idempotent {
            contract: "external-key-v1".into(),
        };
        let tool = typed_tool::<u32, u32, _, _>(effect, |_, _| async {
            panic!("must not retry after a limit")
        });
        let agent = agent(
            Arc::new(ScriptedModel::new(vec![])),
            vec![tool.clone()],
            Limits::default(),
        );
        let mut state = state(&agent);
        state.phase = Phase::Tools;
        state.operations.push(Operation::new(
            call("effect", "idempotent", json!(1)),
            tool.spec().clone(),
            "same-external-key",
            OperationStatus::Dispatched,
        ));
        if expired {
            state.expires_at_ms = Some(now_ms().saturating_sub(1));
        } else {
            state.usage.active_ms = state.limits.active_budget_ms;
        }
        agent
            .drive(&mut state, &MemoryCheckpoint, RunControl::default())
            .await
            .unwrap();
        assert_eq!(
            state.phase,
            Phase::Settled(Settlement::Stopped(if expired {
                StopReason::Expired
            } else {
                StopReason::ActiveBudget
            }))
        );
        assert!(matches!(
            state.operations[0].status,
            OperationStatus::Unknown { .. }
        ));
        state
            .resolve(
                "effect",
                Resolution::Reconcile(ToolResult {
                    call_id: "effect".into(),
                    value: json!("accounted for"),
                    is_error: false,
                }),
            )
            .unwrap();
    }
}

#[tokio::test]
async fn idempotent_timeout_keeps_mutation_uncertainty() {
    let mut effect = spec("idempotent");
    effect.retry = RetryPolicy::Idempotent {
        contract: "external-key-v1".into(),
    };
    let tool = typed_tool::<Value, Value, _, _>(effect, |_, _| async {
        tokio::time::sleep(Duration::from_secs(10)).await;
        Ok(ToolOutput::Complete(json!("late")))
    });
    let model = Arc::new(ScriptedModel::new(vec![ModelResponse::tools(vec![call(
        "effect",
        "idempotent",
        json!(null),
    )])]));
    let agent = agent(
        model,
        vec![tool],
        Limits::default().with_operation_timeout(Duration::from_millis(30)),
    );
    let mut state = state(&agent);
    agent
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    assert!(matches!(
        state.operations[0].status,
        OperationStatus::Unknown { .. }
    ));
    state.cancel_requested = true;
    agent
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    assert_eq!(state.phase, Phase::Settled(Settlement::Cancelled));
    assert!(matches!(
        state.operations[0].status,
        OperationStatus::Unknown { .. }
    ));
}

#[tokio::test(start_paused = true)]
async fn wait_or_unknown_outcome_stops_further_dispatch_until_resolved() {
    for blocking in ["wait", "unknown"] {
        let slow_runs = Arc::new(AtomicUsize::new(0));
        let counter = slow_runs.clone();
        // Siblings outlast the blocking outcome, so the barrier holds for any batch scheduler.
        let slow = typed_tool::<u32, u32, _, _>(spec("slow"), move |arg, _| {
            counter.fetch_add(1, Ordering::SeqCst);
            async move {
                tokio::time::sleep(Duration::from_millis(10)).await;
                Ok(ToolOutput::Complete(arg))
            }
        });
        let wait = typed_tool::<u32, Value, _, _>(spec("wait"), |_, _| async {
            Ok(ToolOutput::Wait(WaitRequest::Input { prompt: "?".into() }))
        });
        let mut mutation = spec("fail");
        mutation.retry = RetryPolicy::Never;
        let fail = typed_tool::<u32, u32, _, _>(mutation, |_, _| async {
            Err(Error::tool("outcome unknown"))
        });
        let first = if blocking == "wait" { "wait" } else { "fail" };
        let model = Arc::new(ScriptedModel::new(vec![
            ModelResponse::tools(vec![
                call("block", first, json!(0)),
                call("s1", "slow", json!(1)),
                call("s2", "slow", json!(2)),
                call("s3", "slow", json!(3)),
            ]),
            ModelResponse::final_output(json!("done")),
        ]));
        let agent = agent(
            model,
            vec![slow, wait, fail],
            Limits::default().with_max_parallel_tools(2),
        );
        let mut state = state(&agent);
        for _ in 0..2 {
            agent
                .drive(&mut state, &MemoryCheckpoint, RunControl::default())
                .await
                .unwrap();
            assert_eq!(state.phase, Phase::Tools, "{blocking}");
            assert_eq!(slow_runs.load(Ordering::SeqCst), 1, "{blocking}");
            assert!(matches!(
                (blocking, &state.operations[0].status),
                ("wait", OperationStatus::Waiting(_))
                    | ("unknown", OperationStatus::Unknown { .. })
            ));
            assert_eq!(state.operations[2].status, OperationStatus::Pending);
            assert_eq!(state.operations[3].status, OperationStatus::Pending);
        }
        let resolution = if blocking == "wait" {
            Resolution::Input(json!(0))
        } else {
            Resolution::Reconcile(ToolResult {
                call_id: "block".into(),
                value: json!(0),
                is_error: false,
            })
        };
        state.resolve("block", resolution).unwrap();
        agent
            .drive(&mut state, &MemoryCheckpoint, RunControl::default())
            .await
            .unwrap();
        assert_eq!(
            state.phase,
            Phase::Settled(Settlement::Completed(json!("done")))
        );
        assert_eq!(slow_runs.load(Ordering::SeqCst), 3);
        let HistoryEntry::Exchange { results, .. } = &state.history[1] else {
            panic!("missing exchange")
        };
        assert_eq!(
            results
                .iter()
                .map(|r| r.call_id.as_str())
                .collect::<Vec<_>>(),
            ["block", "s1", "s2", "s3"]
        );
    }
}

#[tokio::test]
async fn progress_events_for_one_tool_round_are_exact() {
    let tool = typed_tool::<u32, u32, _, _>(spec("echo"), |arg, _| async move {
        Ok(ToolOutput::Complete(arg))
    });
    let model = Arc::new(ScriptedModel::with_events(vec![
        vec![
            ModelEvent::TextDelta("plan".into()),
            ModelEvent::Response(ModelResponse::tools(vec![call("a", "echo", json!(1))])),
        ],
        vec![
            ModelEvent::TextDelta("do".into()),
            ModelEvent::TextDelta("ne".into()),
            ModelEvent::Response(ModelResponse::final_output(json!("done"))),
        ],
    ]));
    let agent = agent(model, vec![tool], Limits::default());
    let mut state = state(&agent);
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    agent
        .drive(
            &mut state,
            &MemoryCheckpoint,
            RunControl::default().with_events(tx),
        )
        .await
        .unwrap();
    let mut events = Vec::new();
    while let Some(ProgressEvent { run_id, kind, .. }) = rx.recv().await {
        // Every event, text deltas and tool starts included, identifies its run.
        assert_eq!(run_id, state.id, "{kind:?} is not attributed to its run");
        events.push(match kind {
            ProgressKind::ModelStarted => "model".to_owned(),
            ProgressKind::TextDelta(text) => format!("delta:{text}"),
            ProgressKind::ToolsStarted { count, .. } => format!("tools:{count}"),
            ProgressKind::Committed { revision, .. } => format!("commit:{revision}"),
            ProgressKind::Settled => "settled".to_owned(),
            other => panic!("unexpected event: {other:?}"),
        });
    }
    assert_eq!(
        events,
        [
            "model",
            "commit:1",
            "delta:plan",
            "commit:2",
            "commit:3",
            "tools:1",
            "commit:4",
            // The tool outcome carries the active-time refund.
            "commit:5",
            "commit:6",
            "model",
            "commit:7",
            "delta:do",
            "delta:ne",
            "commit:8",
            "settled",
        ]
    );
    assert_eq!(state.revision, 8);
}

#[tokio::test]
async fn reasoning_streams_as_progress_and_shares_the_response_byte_limit() {
    let turn = || {
        vec![
            ModelEvent::ReasoningDelta("think".into()),
            ModelEvent::TextDelta("done".into()),
            ModelEvent::Response(ModelResponse::final_output(json!("done"))),
        ]
    };
    let agent_with = |limits| {
        agent(
            Arc::new(ScriptedModel::with_events(vec![turn()])),
            vec![],
            limits,
        )
    };
    let agent = agent_with(Limits::default());
    let mut run = state(&agent);
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    agent
        .drive(
            &mut run,
            &MemoryCheckpoint,
            RunControl::default().with_events(tx),
        )
        .await
        .unwrap();
    let mut streamed = Vec::new();
    while let Some(event) = rx.recv().await {
        match event.kind {
            ProgressKind::ReasoningDelta(text) => streamed.push(format!("reasoning:{text}")),
            ProgressKind::TextDelta(text) => streamed.push(format!("delta:{text}")),
            _ => {}
        }
    }
    assert_eq!(streamed, ["reasoning:think", "delta:done"]);
    assert_eq!(
        run.phase,
        Phase::Settled(Settlement::Completed(json!("done")))
    );
    // Reasoning is never retained: the exchange holds only the response.
    let retained = serde_json::to_string(&run.history).unwrap();
    assert!(!retained.contains("think"), "{retained}");

    // Five reasoning bytes and four answer bytes exceed an eight-byte limit together.
    let agent = agent_with(Limits::default().with_max_response_bytes(8));
    let mut run = state(&agent);
    agent
        .drive(&mut run, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    let Phase::Settled(Settlement::Failed(error)) = &run.phase else {
        panic!("{:?}", run.phase)
    };
    assert!(
        error.contains("stream exceeds response byte limit"),
        "{error}"
    );
}

/// Streams its own run ID as every text delta, yielding between deltas so concurrent runs
/// interleave on a shared channel.
struct RunIdEcho;
#[async_trait]
impl Model for RunIdEcho {
    async fn stream(&self, request: ModelRequest, _: CancellationToken) -> Result<ModelStream> {
        let run_id = request.run_id.to_string();
        Ok(Box::pin(stream::unfold(0, move |n| {
            let run_id = run_id.clone();
            async move {
                tokio::task::yield_now().await;
                let event = match n {
                    0..3 => ModelEvent::TextDelta(run_id),
                    3 => ModelEvent::Response(ModelResponse::final_output(json!("done"))),
                    _ => return None,
                };
                Some((Ok(event), n + 1))
            }
        })))
    }
}
#[tokio::test]
async fn concurrent_runs_sharing_a_channel_attribute_every_event() {
    let agent = agent(Arc::new(RunIdEcho), vec![], Limits::default());
    let (mut first, mut second) = (state(&agent), state(&agent));
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let control = || RunControl::default().with_events(tx.clone());
    let (a, b) = tokio::join!(
        agent.drive(&mut first, &MemoryCheckpoint, control()),
        agent.drive(&mut second, &MemoryCheckpoint, control()),
    );
    a.unwrap();
    b.unwrap();
    drop(tx);
    let mut deltas = std::collections::HashMap::new();
    let mut previous = None;
    let mut interleaved = false;
    while let Some(event) = rx.recv().await {
        assert!([first.id, second.id].contains(&event.run_id));
        interleaved |= previous.is_some_and(|run| run != event.run_id);
        previous = Some(event.run_id);
        if let ProgressKind::TextDelta(text) = event.kind {
            assert_eq!(
                text,
                event.run_id.to_string(),
                "delta attributed to another run"
            );
            *deltas.entry(event.run_id).or_insert(0) += 1;
        }
    }
    assert!(
        interleaved,
        "the runs did not share the channel concurrently"
    );
    assert_eq!(deltas[&first.id], 3);
    assert_eq!(deltas[&second.id], 3);
}

struct RecordingModel {
    inner: ScriptedModel,
    seen: Mutex<Vec<Vec<HistoryEntry>>>,
}
#[async_trait]
impl Model for RecordingModel {
    async fn stream(
        &self,
        request: ModelRequest,
        cancel: CancellationToken,
    ) -> Result<ModelStream> {
        self.seen.lock().unwrap().push(request.history.clone());
        self.inner.stream(request, cancel).await
    }
}
#[tokio::test]
async fn model_context_fits_truncates_suffix_and_repins_current_input() {
    let blob = |id: &str, n: usize| HistoryEntry::Exchange {
        response: ModelResponse::tools(vec![call(id, "blob", json!(n))]),
        results: vec![ToolResult {
            call_id: id.into(),
            value: json!("x".repeat(n)),
            is_error: false,
        }],
    };
    let len = |entry: &HistoryEntry| serde_json::to_vec(entry).unwrap().len();
    let prior = HistoryEntry::Input(json!(format!("earlier {}", "p".repeat(200))));
    let input = HistoryEntry::Input(json!("go"));
    let (big, small) = (blob("big", 1_000), blob("small", 1));
    // The newest two exchanges fill the window exactly; the sizes below make each turn's
    // selection differ.
    let max = 2 + len(&small) + 1 + len(&big);
    assert!(len(&input) + len(&prior) <= len(&big) + len(&small));
    assert!(len(&input) <= len(&small) && len(&small) <= len(&input) + len(&prior));
    let tool = typed_tool::<usize, String, _, _>(spec("blob"), |n, _| async move {
        Ok(ToolOutput::Complete("x".repeat(n)))
    });
    let model = Arc::new(RecordingModel {
        inner: ScriptedModel::new(vec![
            ModelResponse::tools(vec![call("big", "blob", json!(1_000))]),
            ModelResponse::tools(vec![call("small", "blob", json!(1))]),
            ModelResponse::final_output(json!("done")),
        ]),
        seen: Mutex::new(vec![]),
    });
    let agent = agent(
        model.clone(),
        vec![tool],
        Limits::default().with_max_context_bytes(max),
    );
    let mut state = agent
        .new_run("test", json!("go"), std::slice::from_ref(&prior), None)
        .unwrap();
    agent
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    assert_eq!(
        state.phase,
        Phase::Settled(Settlement::Completed(json!("done")))
    );
    assert_eq!(
        *model.seen.lock().unwrap(),
        [
            // Everything fits.
            vec![prior, input.clone()],
            // The oldest prior entry falls out of the bounded suffix.
            vec![input.clone(), big],
            // The current input would fall out: it is re-pinned ahead of the newest exchange
            // that still fits beside it, so the older exchange is skipped.
            vec![input, small],
        ]
    );
}

#[tokio::test]
async fn input_and_latest_exchange_that_cannot_fit_fail_inside_the_model_operation() {
    let tool = typed_tool::<usize, String, _, _>(spec("blob"), |n, _| async move {
        Ok(ToolOutput::Complete("x".repeat(n)))
    });
    let model = Arc::new(ScriptedModel::new(vec![
        ModelResponse::tools(vec![call("big", "blob", json!(1_000))]),
        ModelResponse::final_output(json!("done")),
    ]));
    let agent = agent(
        model.clone(),
        vec![tool],
        Limits::default().with_max_context_bytes(512),
    );
    let mut state = state(&agent);
    agent
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    assert!(
        matches!(&state.phase, Phase::Settled(Settlement::Failed(reason)) if reason.contains("exceed context limit")),
        "{:?}",
        state.phase
    );
    // Context is assembled after the dispatch checkpoint, so the failed attempt is counted but
    // the model is never called with an incomplete context.
    assert_eq!(model.calls(), 1);
    assert_eq!(state.usage.model_calls, 2);
}

/// The current UTC Unix time in milliseconds, the unit of run expiry and timer waits.
fn now_ms() -> u64 {
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH);
    u64::try_from(now.expect("clock after 1970").as_millis()).expect("milliseconds fit u64")
}
