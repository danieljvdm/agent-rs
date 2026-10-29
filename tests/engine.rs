use async_trait::async_trait;
use futures::stream;
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use yielded_agent::*;

fn spec(name: &str) -> ToolSpec {
    ToolSpec {
        name: name.into(),
        description: "test".into(),
        input_schema: json!({}),
        version: "1".into(),
        retry: RetryPolicy::ReadOnly,
        execution: Execution::Parallel,
        requires_approval: false,
    }
}
fn call(id: &str, name: &str, value: Value) -> ToolCall {
    ToolCall {
        id: id.into(),
        name: name.into(),
        arguments: value,
    }
}
fn agent(model: Arc<dyn Model>, tools: Vec<Arc<dyn Tool>>, limits: Limits) -> Agent {
    Agent::new::<String, String>("test", "test", model, ModelConfig::default(), tools, limits)
        .unwrap()
}
fn state(agent: &Agent) -> RunState {
    agent.new_run("test", json!("go"), vec![], None).unwrap()
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
    Engine::new(agent)
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
        Engine::new(agent)
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
    let limits = Limits {
        max_parallel_tools: 2,
        ..Limits::default()
    };
    let agent = agent(model.clone(), tools, limits);
    let mut state = state(&agent);
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    Engine::new(agent)
        .drive(
            &mut state,
            &MemoryCheckpoint,
            RunControl {
                events: Some(tx),
                ..RunControl::default()
            },
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
    let engine = Engine::new(agent);
    engine
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    assert!(matches!(
        state.operations[0].status,
        OperationStatus::Approval
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let usage = state.usage.clone();
    engine
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    assert_eq!(usage, state.usage);
    resolve(&mut state, "question", Resolution::Approve(true)).unwrap();
    engine
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    assert!(matches!(
        state.operations[0].status,
        OperationStatus::Waiting(WaitRequest::Input { .. })
    ));
    let usage = state.usage.clone();
    engine
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    assert_eq!(usage, state.usage);
    resolve(&mut state, "question", Resolution::Input(json!("there"))).unwrap();
    engine
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
        Limits {
            max_model_calls: 1,
            ..Limits::default()
        },
    );
    let mut state = state(&agent);
    state.phase = Phase::ModelInFlight;
    state.usage.model_calls = 1;
    state.usage.active_ms = 123;
    Engine::new(agent)
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
    state.operations.push(Operation {
        call: call("effect", "mutate", json!(1)),
        spec: tool.spec().clone(),
        idempotency_key: "stable".into(),
        status: OperationStatus::Dispatched,
        approved: false,
    });
    state.cancel_requested = true;
    Engine::new(agent)
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    assert_eq!(state.phase, Phase::Settled(Settlement::Cancelled));
    assert!(matches!(
        state.operations[0].status,
        OperationStatus::Unknown { .. }
    ));
    resolve(
        &mut state,
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
    assert_eq!(
        select_context(&[input, group.clone()], limit).unwrap(),
        vec![group.clone()]
    );
    assert!(select_context(&[group], limit - 1).is_err());
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
        Limits {
            max_response_bytes: 16,
            ..Limits::default()
        },
    );
    let mut state = state(&agent);
    Engine::new(agent)
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    assert!(matches!(state.phase, Phase::Settled(Settlement::Failed(_))));
    assert!(state.usage.events < 30);
}

struct FailBeforeModel;
#[async_trait]
impl Checkpoint for FailBeforeModel {
    async fn commit(&self, _: &mut RunState) -> Result<()> {
        Err(Error::Storage("injected".into()))
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
        Engine::new(agent)
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
    Engine::new(agent)
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
        Err(Error::Tool("cleanup could not establish outcome".into()))
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
    Engine::new(agent)
        .drive(
            &mut state,
            &MemoryCheckpoint,
            RunControl {
                cancel: cancellation,
                ..RunControl::default()
            },
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
            ModelResponse {
                output: Some(json!("done")),
                tools: vec![],
                usage: TokenUsage {
                    input: 7,
                    output: 5,
                },
            },
        ))])))
    }
    async fn cleanup(&self, _: RunId) -> Result<()> {
        Err(Error::Provider("cleanup failed".into()))
    }
}
#[tokio::test]
async fn model_cleanup_failure_preserves_received_evidence_and_tokens() {
    let agent = agent(Arc::new(FailedModelCleanup), vec![], Limits::default());
    let mut state = state(&agent);
    Engine::new(agent)
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    assert!(matches!(state.phase, Phase::Settled(Settlement::Failed(_))));
    assert_eq!(state.usage.input_tokens, 7);
    assert_eq!(state.usage.output_tokens, 5);
    assert_eq!(state.pending_response.unwrap().output, Some(json!("done")));
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
    let engine = Engine::new(agent(model, vec![tool], Limits::default()));
    let mut conversation = Conversation::default();
    assert!(
        engine
            .run::<_, String>(&mut conversation, "go")
            .await
            .is_err()
    );
    assert_eq!(conversation.history.len(), 1);
    assert!(matches!(
        engine.run::<_, String>(&mut conversation, "other").await,
        Err(Error::ConversationBusy)
    ));
    resolve(
        conversation.last_run.as_mut().unwrap(),
        "question",
        Resolution::Input(json!("here")),
    )
    .unwrap();
    let result: String = engine
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
            return Err(Error::Storage("injected outcome commit failure".into()));
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
        Engine::new(agent)
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
        .new_run("expiry", json!("go"), vec![], Some(now_ms() + 30))
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(2),
        Engine::new(agent).drive(&mut state, &MemoryCheckpoint, RunControl::default()),
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
        .new_run("expiry", json!("go"), vec![], Some(now_ms() + 50))
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(2),
        Engine::new(agent).drive(&mut state, &MemoryCheckpoint, RunControl::default()),
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
            return Err(Error::Storage("settlement unavailable".into()));
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
        Engine::new(agent)
            .drive(
                &mut state,
                &FailSettlement,
                RunControl {
                    events: Some(tx),
                    ..RunControl::default()
                }
            )
            .await,
        Err(Error::Storage(_))
    ));
    while let Some(event) = rx.recv().await {
        assert!(!matches!(event, ProgressEvent::Settled { .. }));
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
    let agent = agent(model, vec![tool], Limits::default()).with_authorizer(Arc::new(Deny {
        calls: authorizations.clone(),
    }));
    let mut state = state(&agent);
    let engine = Engine::new(agent);
    engine
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    assert!(matches!(
        state.operations[0].status,
        OperationStatus::Approval
    ));
    assert_eq!(authorizations.load(Ordering::SeqCst), 0);
    resolve(&mut state, "effect", Resolution::Approve(true)).unwrap();
    engine
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
    state.operations.push(Operation {
        call: call("effect", "mutate", json!(1)),
        spec: tool.spec().clone(),
        idempotency_key: "stable".into(),
        status: OperationStatus::Dispatched,
        approved: false,
    });
    state.cancel_requested = true;
    let engine = Engine::new(agent);
    engine
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    let mut conversation = Conversation {
        history: state.history.clone(),
        last_run: Some(state),
    };
    assert!(matches!(
        engine.run::<_, String>(&mut conversation, "next").await,
        Err(Error::ConversationBusy)
    ));
    let retained = conversation.last_run.as_mut().unwrap();
    assert_eq!(retained.id, original_id);
    assert_eq!(retained.phase, Phase::Settled(Settlement::Cancelled));
    assert!(matches!(
        retained.operations[0].status,
        OperationStatus::Unknown { .. }
    ));
    resolve(
        retained,
        "effect",
        Resolution::Reconcile(ToolResult {
            call_id: "effect".into(),
            value: json!("effect accounted for"),
            is_error: false,
        }),
    )
    .unwrap();
    let result: String = engine.run(&mut conversation, "next").await.unwrap();
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
        state.operations.push(Operation {
            call: call("effect", "idempotent", json!(1)),
            spec: tool.spec().clone(),
            idempotency_key: "same-external-key".into(),
            status: OperationStatus::Dispatched,
            approved: false,
        });
        if expired {
            state.expires_at_ms = Some(now_ms().saturating_sub(1));
        } else {
            state.usage.active_ms = state.limits.active_budget_ms;
        }
        Engine::new(agent)
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
        resolve(
            &mut state,
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
        Limits {
            operation_timeout_ms: 30,
            ..Limits::default()
        },
    );
    let mut state = state(&agent);
    let engine = Engine::new(agent);
    engine
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    assert!(matches!(
        state.operations[0].status,
        OperationStatus::Unknown { .. }
    ));
    state.cancel_requested = true;
    engine
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    assert_eq!(state.phase, Phase::Settled(Settlement::Cancelled));
    assert!(matches!(
        state.operations[0].status,
        OperationStatus::Unknown { .. }
    ));
}
