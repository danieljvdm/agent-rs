//! Attached subagents: delegation results, budgets, waits, cancellation, and the tool window's
//! invariants as they apply to child work.
use agent_rs::*;
use async_trait::async_trait;
use futures::stream;
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::sync::{Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

fn child_limits() -> Limits {
    Limits::default()
        .with_max_model_calls(3)
        .with_max_tool_calls(2)
        .with_max_tokens(100)
        .with_operation_timeout(Duration::from_millis(1_000))
        .with_cleanup_timeout(Duration::from_millis(100))
        .with_active_budget(Duration::from_millis(5_000))
}
fn child_builder(id: &str, model: Arc<dyn Model>) -> AgentBuilder<String, String> {
    Agent::builder(id, model)
        .instructions("Use only the supplied input.")
        .limits(child_limits())
        .input::<String>()
        .output::<String>()
}
fn child_agent(model: Arc<dyn Model>) -> Agent<String, String> {
    child_builder("child-v1", model).build().unwrap()
}
fn call(id: usize) -> ToolCall {
    ToolCall {
        id: format!("child-{id}"),
        name: "delegate".into(),
        arguments: json!(format!("task-{id}")),
    }
}
fn parent_builder(
    child: Agent<String, String>,
    calls: Vec<ToolCall>,
    max_parallel: usize,
    limits: Limits,
) -> AgentBuilder<String, String> {
    Agent::builder(
        "parent-v1",
        Arc::new(ScriptedModel::new(vec![
            ModelResponse::tools(calls),
            ModelResponse::final_output(json!("done")),
        ])),
    )
    .instructions("Delegate and collect results.")
    .limits(limits)
    .input::<String>()
    .output::<String>()
    .subagents(
        max_parallel,
        [Subagent::new("delegate", "Perform one task.", child)],
    )
}
fn parent(
    child: Agent<String, String>,
    count: usize,
    max_parallel: usize,
    limits: Limits,
) -> Agent<String, String> {
    parent_builder(child, (0..count).map(call).collect(), max_parallel, limits)
        .build()
        .unwrap()
}
fn new_run(agent: &Agent<String, String>) -> RunState {
    agent
        .new_run(
            "parent-conversation",
            json!("parent-only context"),
            &[],
            None,
        )
        .unwrap()
}
fn results(state: &RunState) -> &[ToolResult] {
    match &state.history[1] {
        HistoryEntry::Exchange { results, .. } => results,
        other => panic!("expected the delegated exchange: {other:?}"),
    }
}
fn outcome(result: &ToolResult) -> ChildOutcome<String> {
    serde_json::from_value::<ChildResult<String>>(result.value.clone())
        .unwrap()
        .outcome
}

/// Echoes the child's input once a permit is available, recording concurrency and cleanup.
struct GatedModel {
    started: mpsc::UnboundedSender<RunId>,
    permits: Arc<Semaphore>,
    active: AtomicUsize,
    peak: AtomicUsize,
    cleanup: AtomicUsize,
}
impl GatedModel {
    fn new(permits: usize) -> (Arc<Self>, mpsc::UnboundedReceiver<RunId>) {
        let (started, starts) = mpsc::unbounded_channel();
        let model = Arc::new(Self {
            started,
            permits: Arc::new(Semaphore::new(permits)),
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            cleanup: AtomicUsize::new(0),
        });
        (model, starts)
    }
}
#[async_trait]
impl Model for GatedModel {
    async fn stream(&self, request: ModelRequest, _: CancellationToken) -> Result<ModelStream> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        self.started.send(request.run_id).unwrap();
        self.permits.acquire().await.unwrap().forget();
        assert_eq!(
            request.history.len(),
            1,
            "parent history must not leak to a child"
        );
        let HistoryEntry::Input(input) = &request.history[0] else {
            panic!("child input missing")
        };
        let response = ModelResponse::final_output(input.clone()).with_usage(TokenUsage::new(2, 3));
        Ok(Box::pin(stream::iter([Ok(ModelEvent::Response(response))])))
    }
    async fn cleanup(&self, _: RunId) -> Result<()> {
        self.active.fetch_sub(1, Ordering::SeqCst);
        self.cleanup.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
async fn next_start(starts: &mut mpsc::UnboundedReceiver<RunId>) -> RunId {
    tokio::time::timeout(Duration::from_secs(5), starts.recv())
        .await
        .expect("a child model call")
        .unwrap()
}

#[tokio::test]
async fn parallel_children_have_typed_ordered_results_isolated_inputs_and_bounded_concurrency() {
    let (model, mut starts) = GatedModel::new(0);
    let agent = parent(child_agent(model.clone()), 5, 2, Limits::default());
    let mut state = new_run(&agent);
    let run = tokio::spawn(async move {
        agent
            .drive(&mut state, &MemoryCheckpoint, RunControl::default())
            .await
            .unwrap();
        state
    });
    let mut ids = Vec::new();
    for batch in [2, 2, 1] {
        for _ in 0..batch {
            ids.push(next_start(&mut starts).await);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(starts.try_recv().is_err(), "more than two children ran");
        model.permits.add_permits(batch);
    }
    let state = run.await.unwrap();
    assert_eq!(
        state.phase,
        Phase::Settled(Settlement::Completed(json!("done")))
    );
    assert_eq!(model.peak.load(Ordering::SeqCst), 2);
    assert_eq!(model.active.load(Ordering::SeqCst), 0);
    assert_eq!(model.cleanup.load(Ordering::SeqCst), 5);
    for (index, result) in results(&state).iter().enumerate() {
        let result: ChildResult<String> = serde_json::from_value(result.value.clone()).unwrap();
        assert_eq!(result.parent_run_id, state.id);
        assert_eq!(
            result.outcome,
            ChildOutcome::Completed(format!("task-{index}"))
        );
        assert!(ids.contains(&result.child_run_id));
        let child = state.child(result.child_run_id).unwrap();
        assert_eq!(
            child.state.parent,
            Some(ParentRun {
                run_id: state.id,
                call_id: format!("child-{index}")
            })
        );
        assert_eq!(child.result::<String>().unwrap(), result);
    }
    // Two parent turns and five children's; five delegations and no child tools.
    assert_eq!(state.usage.model_calls, 7);
    assert_eq!(state.usage.tool_calls, 5);
    assert_eq!(state.child_usage.model_calls, 5);
    assert_eq!(state.usage.input_tokens, 10);
    assert_eq!(state.usage.output_tokens, 15);
}

#[tokio::test]
async fn child_failure_is_a_parent_tool_result_and_does_not_discard_siblings() {
    let good = child_builder(
        "good",
        Arc::new(ScriptedModel::new(vec![ModelResponse::final_output(
            json!("ok"),
        )])),
    )
    .build()
    .unwrap();
    // A number is not the child's `String` output.
    let bad = child_builder(
        "bad",
        Arc::new(ScriptedModel::new(vec![ModelResponse::final_output(
            json!(123),
        )])),
    )
    .build()
    .unwrap();
    let call = |id: &str| ToolCall {
        id: id.into(),
        name: id.into(),
        arguments: json!("task"),
    };
    let agent = Agent::builder(
        "parent",
        Arc::new(ScriptedModel::new(vec![
            ModelResponse::tools(vec![call("bad"), call("good")]),
            ModelResponse::final_output(json!("handled")),
        ])),
    )
    .input::<String>()
    .output::<String>()
    .subagents(
        2,
        [
            Subagent::new("bad", "bad", bad),
            Subagent::new("good", "good", good),
        ],
    )
    .build()
    .unwrap();
    let mut state = new_run(&agent);
    agent
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    assert_eq!(
        state.phase,
        Phase::Settled(Settlement::Completed(json!("handled")))
    );
    let results = results(&state);
    assert!(results[0].is_error);
    assert!(matches!(outcome(&results[0]), ChildOutcome::Failed(_)));
    assert!(!results[1].is_error);
    assert_eq!(outcome(&results[1]), ChildOutcome::Completed("ok".into()));
    assert_eq!(state.usage.model_calls, 4);
}

/// The tool result of an ordinary delegation, byte for byte: the whole `ChildResult` envelope,
/// an error unless the child completed. Code mode replaces it with its own result; ordinary
/// subagents must not change.
#[tokio::test]
async fn a_delegation_result_is_the_whole_child_result_envelope() {
    let completing = child_agent(Arc::new(ScriptedModel::new(vec![
        ModelResponse::final_output(json!("ok")).with_usage(TokenUsage::new(2, 3)),
    ])));
    let failing = child_agent(Arc::new(ScriptedModel::new(vec![])));
    for (child, completes) in [(completing, true), (failing, false)] {
        let agent = parent(child, 1, 1, Limits::default());
        let mut state = new_run(&agent);
        agent
            .drive(&mut state, &MemoryCheckpoint, RunControl::default())
            .await
            .unwrap();
        let child = &state.children[0].state;
        let outcome = match &child.phase {
            Phase::Settled(Settlement::Completed(_)) => json!({"Completed": "ok"}),
            Phase::Settled(Settlement::Failed(message)) => json!({ "Failed": message }),
            other => panic!("{other:?}"),
        };
        let (input, output) = if completes { (2, 3) } else { (0, 0) };
        let expected = json!({
            "parent_run_id": state.id,
            "child_run_id": child.id,
            "outcome": outcome,
            "usage": {
                "model_calls": 1,
                "tool_calls": 0,
                "input_tokens": input,
                "output_tokens": output,
                "active_ms": child.usage.active_ms,
            },
        });
        let result = &results(&state)[0];
        assert_eq!(
            serde_json::to_string(&result.value).unwrap(),
            serde_json::to_string(&expected).unwrap()
        );
        assert_eq!(result.is_error, !completes);
        assert_eq!(result.call_id, "child-0");
    }
}

#[tokio::test]
async fn child_events_name_their_parent_and_never_pose_as_its_own() {
    let child = child_agent(Arc::new(ScriptedModel::with_events(vec![vec![
        ModelEvent::TextDelta("child-only".into()),
        ModelEvent::Response(ModelResponse::final_output(json!("ok"))),
    ]])));
    let agent = parent(child, 1, 1, Limits::default());
    let mut state = new_run(&agent);
    let (events, mut received) = mpsc::channel(256);
    agent
        .drive(
            &mut state,
            &MemoryCheckpoint,
            RunControl::default().with_events(events),
        )
        .await
        .unwrap();
    let child_id = state.children[0].state.id;
    let (mut child_text, mut child_settled, mut parent_settled) = (String::new(), 0, 0);
    while let Ok(event) = received.try_recv() {
        match event.parent_run_id {
            Some(parent) => {
                assert_eq!(parent, state.id);
                assert_eq!(event.run_id, child_id);
                match event.kind {
                    ProgressKind::TextDelta(text) => child_text.push_str(&text),
                    ProgressKind::Settled => child_settled += 1,
                    _ => {}
                }
            }
            None => {
                assert_eq!(event.run_id, state.id);
                match event.kind {
                    ProgressKind::TextDelta(_) => panic!("child text attributed to its parent"),
                    ProgressKind::Settled => parent_settled += 1,
                    _ => {}
                }
            }
        }
    }
    assert_eq!(child_text, "child-only");
    assert_eq!((child_settled, parent_settled), (1, 1));
}

#[tokio::test]
async fn delegation_cannot_replenish_model_or_token_budgets() {
    for tokens in [false, true] {
        let response = ModelResponse::final_output(json!("ok"))
            .with_usage(TokenUsage::new(0, if tokens { 10 } else { 0 }));
        let child = child_builder("child", Arc::new(ScriptedModel::new(vec![response])))
            .limits(
                child_limits()
                    .with_max_model_calls(1)
                    .with_max_tool_calls(0)
                    .with_max_tokens(10),
            )
            .build()
            .unwrap();
        let limits = if tokens {
            Limits::default().with_max_tokens(50)
        } else {
            Limits::default().with_max_model_calls(6)
        };
        let agent = parent(child, 5, 2, limits);
        let mut state = new_run(&agent);
        agent
            .drive(&mut state, &MemoryCheckpoint, RunControl::default())
            .await
            .unwrap();
        let reason = if tokens {
            StopReason::Tokens
        } else {
            StopReason::ModelCalls
        };
        assert_eq!(state.phase, Phase::Settled(Settlement::Stopped(reason)));
        assert_eq!(state.children.len(), 5);
        assert_eq!(state.usage.model_calls, 6);
        assert_eq!(state.child_usage.model_calls, 5);
        assert_eq!(state.usage.output_tokens, if tokens { 50 } else { 0 });
    }
}

#[tokio::test]
async fn an_allocation_that_does_not_fit_or_invalid_input_never_admits_a_child() {
    let model = Arc::new(ScriptedModel::new(vec![ModelResponse::final_output(
        json!("ok"),
    )]));
    let agent = parent(
        child_agent(model.clone()),
        1,
        1,
        Limits::default().with_max_tokens(50),
    );
    let mut state = new_run(&agent);
    agent
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    assert_eq!(
        state.phase,
        Phase::Settled(Settlement::Completed(json!("done")))
    );
    assert!(state.children.is_empty());
    assert!(results(&state)[0].is_error);
    assert_eq!(state.usage.tool_calls, 0, "nothing was dispatched");
    assert_eq!(model.calls(), 0);

    let child = Agent::builder("number-child", model.clone())
        .limits(child_limits())
        .input::<u32>()
        .output::<String>()
        .build()
        .unwrap();
    let agent = Agent::builder(
        "parent",
        Arc::new(ScriptedModel::new(vec![ModelResponse::tools(vec![call(
            0,
        )])])),
    )
    .subagents(1, [Subagent::new("delegate", "number", child)])
    .build()
    .unwrap();
    let mut state = agent.new_run("c", json!("go"), &[], None).unwrap();
    agent
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    assert!(matches!(state.phase, Phase::Settled(Settlement::Failed(_))));
    assert!(state.children.is_empty());
    assert_eq!(model.calls(), 0);
}

#[tokio::test]
async fn cancellation_settles_children_and_shutdown_keeps_them_resumable() {
    for cancel in [false, true] {
        let (model, mut starts) = GatedModel::new(0);
        let agent = parent(child_agent(model.clone()), 2, 2, Limits::default());
        let mut state = new_run(&agent);
        let control = RunControl::default();
        let signal = if cancel {
            control.cancel.clone()
        } else {
            control.shutdown.clone()
        };
        let run = tokio::spawn(async move {
            let result = agent.drive(&mut state, &MemoryCheckpoint, control).await;
            (state, result)
        });
        for _ in 0..2 {
            next_start(&mut starts).await;
        }
        signal.cancel();
        let (state, result) = run.await.unwrap();
        assert_eq!(model.active.load(Ordering::SeqCst), 0);
        assert_eq!(model.cleanup.load(Ordering::SeqCst), 2);
        if cancel {
            result.unwrap();
            assert_eq!(state.phase, Phase::Settled(Settlement::Cancelled));
            for child in &state.children {
                assert_eq!(child.state.phase, Phase::Settled(Settlement::Cancelled));
                assert_eq!(
                    child.result::<String>().unwrap().outcome,
                    ChildOutcome::Cancelled
                );
            }
            assert_eq!(state.usage.model_calls, 3);
        } else {
            assert!(matches!(result, Err(Error::Interrupted)), "{result:?}");
            assert!(!state.cancel_requested);
            assert!(
                state
                    .children
                    .iter()
                    .all(|child| !child.state.is_terminal())
            );
            assert_eq!(
                state.child_usage.model_calls, 6,
                "unsettled children keep their allocations"
            );
        }
    }
}

#[tokio::test]
async fn a_blocked_child_stops_new_dispatch_and_resolving_it_resumes_the_batch() {
    let tool = typed_tool::<Value, Value, _, _>(spec("ask"), |_, _| async {
        Ok(ToolOutput::Wait(WaitRequest::Input {
            prompt: "which?".into(),
        }))
    });
    let model = Arc::new(ScriptedModel::new(vec![
        ModelResponse::tools(vec![ToolCall {
            id: "ask".into(),
            name: "ask".into(),
            arguments: json!(null),
        }]),
        ModelResponse::final_output(json!("answered")),
        ModelResponse::final_output(json!("second")),
    ]));
    let child = child_builder("asking-child", model.clone())
        .tool(tool)
        .build()
        .unwrap();
    // Two delegations, one child at a time: the second waits behind the first's question.
    let agent = parent(child, 2, 1, Limits::default());
    let mut conversation = Conversation::default();
    assert!(matches!(
        agent.run(&mut conversation, "go").await,
        Err(Error::Waiting)
    ));
    let run = conversation.last_run.as_mut().unwrap();
    assert_eq!(run.children.len(), 1, "no dispatch behind a blocked child");
    assert_eq!(run.operations[1].status, OperationStatus::Pending);
    assert_eq!(run.wake_at(), None);
    let first = run.children[0].state.id;
    assert!(matches!(
        run.resolve_child(RunId::new_v4(), "ask", Resolution::Input(json!(1))),
        Err(Error::NotFound)
    ));
    run.resolve_child(first, "ask", Resolution::Input(json!("this one")))
        .unwrap();
    assert_eq!(run.wake_at(), Some(0));
    // The first child completes, and only then is the second admitted. It asks too.
    assert!(matches!(
        agent.resume(&mut conversation, RunControl::default()).await,
        Err(Error::Waiting)
    ));
    let run = conversation.last_run.as_mut().unwrap();
    assert!(matches!(
        run.operations[0].status,
        OperationStatus::Complete(_)
    ));
    assert_eq!(run.children.len(), 2);
    let second = run.children[1].state.id;
    run.resolve_child(second, "ask", Resolution::Input(json!("that one")))
        .unwrap();
    let output = agent
        .resume(&mut conversation, RunControl::default())
        .await
        .unwrap();
    assert_eq!(output, "done");
    let state = conversation.last_run.as_ref().unwrap();
    for result in results(state) {
        assert_eq!(outcome(result), ChildOutcome::Completed("answered".into()));
    }
    assert_eq!(model.calls(), 4);
    // Each child was charged one delegation, and resuming charged nothing more.
    assert_eq!(state.usage.tool_calls, 2 + 2);
}

#[tokio::test]
async fn a_delegation_that_requires_approval_admits_its_child_only_once_approved() {
    for approve in [false, true] {
        let model = Arc::new(ScriptedModel::new(vec![ModelResponse::final_output(
            json!("ok"),
        )]));
        let child = child_agent(model.clone());
        let agent = Agent::builder(
            "parent",
            Arc::new(ScriptedModel::new(vec![
                ModelResponse::tools(vec![call(0)]),
                ModelResponse::final_output(json!("done")),
            ])),
        )
        .input::<String>()
        .output::<String>()
        .subagents(
            1,
            [Subagent::new("delegate", "Ask first.", child).with_requires_approval(true)],
        )
        .build()
        .unwrap();
        let mut conversation = Conversation::default();
        assert!(matches!(
            agent.run(&mut conversation, "go").await,
            Err(Error::Waiting)
        ));
        let run = conversation.last_run.as_mut().unwrap();
        assert_eq!(run.operations[0].status, OperationStatus::Approval);
        assert!(run.children.is_empty());
        run.resolve("child-0", Resolution::Approve(approve))
            .unwrap();
        let output = agent
            .resume(&mut conversation, RunControl::default())
            .await
            .unwrap();
        assert_eq!(output, "done");
        let state = conversation.last_run.as_ref().unwrap();
        assert_eq!(state.children.len(), usize::from(approve));
        assert_eq!(results(state)[0].is_error, !approve);
        assert_eq!(model.calls(), usize::from(approve));
    }
}

#[test]
fn build_checks_names_parallelism_and_nesting() {
    let model = || -> Arc<dyn Model> { Arc::new(ScriptedModel::new(vec![])) };
    let child = || child_agent(model());
    let config = |result: Result<Agent>| matches!(result, Err(Error::Config(_)));
    let tool = typed_tool::<Value, Value, _, _>(spec("delegate"), |_, _| async {
        Ok(ToolOutput::Complete(json!(null)))
    });
    assert!(config(
        Agent::builder("p", model())
            .tool(tool)
            .subagents(1, [Subagent::new("delegate", "", child())])
            .build()
    ));
    assert!(config(
        Agent::builder("p", model())
            .subagents(
                1,
                [
                    Subagent::new("same", "", child()),
                    Subagent::new("same", "", child())
                ]
            )
            .build()
    ));
    for max_parallel in [0, 1025] {
        assert!(config(
            Agent::builder("p", model())
                .subagents(max_parallel, [Subagent::new("d", "", child())])
                .build()
        ));
    }
    assert!(config(
        Agent::builder("p", model())
            .subagents(1, [Subagent::new("", "", child())])
            .build()
    ));
    let nested = Agent::builder("nested", model())
        .subagents(1, [Subagent::new("d", "", child())])
        .build()
        .unwrap();
    assert!(config(
        Agent::builder("p", model())
            .subagents(1, [Subagent::new("n", "", nested)])
            .build()
    ));
    // No subagents leaves the definition, and so its binding, as it was.
    let plain = Agent::builder("p", model()).build().unwrap();
    let empty = Agent::builder("p", model())
        .subagents(1, [])
        .build()
        .unwrap();
    assert_eq!(plain.binding(), empty.binding());
    assert!(plain.binding().get("subagents").is_none());
    // The model is offered the child's input schema.
    let agent = parent(child(), 1, 1, Limits::default());
    let delegate = Subagent::new("delegate", "Perform one task.", child());
    assert_eq!(
        delegate.spec().input_schema,
        json!(schemars::schema_for!(String))
    );
    assert!(
        agent.binding()["tools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|tool| tool["name"] == "delegate")
    );
    assert_eq!(agent.binding()["subagents"]["max_parallel"], json!(1));
}

fn spec(name: &str) -> ToolSpec {
    ToolSpec::new(name, "1")
        .with_description(name)
        .with_input_schema(Value::Null)
        .with_retry(RetryPolicy::ReadOnly)
}

/// What one commit made durable: each child's admission and how far it got.
#[derive(Clone, Debug)]
struct Snapshot {
    /// Delegations whose child was admitted, by call ID.
    admitted: Vec<String>,
    /// Revisions of each child in the checkpoint.
    children: Vec<(RunId, u64, usize)>,
}
#[derive(Clone, Default)]
struct Journal {
    snapshots: Arc<Mutex<Vec<Snapshot>>>,
    /// Fail every commit from the one in which a child first has history beyond its input.
    fail_after_child_progress: bool,
}
impl Journal {
    fn last(&self) -> Snapshot {
        self.snapshots.lock().unwrap().last().cloned().unwrap()
    }
    fn snapshots(&self) -> Vec<Snapshot> {
        self.snapshots.lock().unwrap().clone()
    }
}
#[async_trait]
impl Checkpoint for Journal {
    async fn commit(&self, state: &mut RunState) -> Result<()> {
        if self.fail_after_child_progress
            && state.children.iter().any(|c| c.state.history.len() > 1)
        {
            return Err(Error::storage("injected commit failure"));
        }
        let admitted = (state.operations.iter())
            .filter(|op| matches!(op.status, OperationStatus::Child { .. }))
            .map(|op| op.call.id.clone())
            .collect();
        let children = (state.children.iter())
            .map(|c| (c.state.id, c.state.revision, c.state.history.len()))
            .collect();
        self.snapshots
            .lock()
            .unwrap()
            .push(Snapshot { admitted, children });
        state.revision += 1;
        Ok(())
    }
}
/// Records, at each child model call, what its parent's last checkpoint held.
struct Observing {
    journal: Journal,
    seen: Mutex<Vec<(RunId, Snapshot)>>,
}
#[async_trait]
impl Model for Observing {
    async fn stream(&self, request: ModelRequest, _: CancellationToken) -> Result<ModelStream> {
        self.seen
            .lock()
            .unwrap()
            .push((request.run_id, self.journal.last()));
        let HistoryEntry::Input(input) = &request.history[0] else {
            panic!("child input missing")
        };
        let response = ModelResponse::final_output(input.clone());
        Ok(Box::pin(stream::iter([Ok(ModelEvent::Response(response))])))
    }
}

#[tokio::test]
async fn admission_and_each_child_commit_are_durable_before_the_child_goes_on() {
    let journal = Journal::default();
    let model = Arc::new(Observing {
        journal: journal.clone(),
        seen: Mutex::default(),
    });
    let agent = parent(child_agent(model.clone()), 3, 2, Limits::default());
    let mut state = new_run(&agent);
    agent
        .drive(&mut state, &journal, RunControl::default())
        .await
        .unwrap();
    assert_eq!(
        state.phase,
        Phase::Settled(Settlement::Completed(json!("done")))
    );
    let seen = model.seen.lock().unwrap();
    assert_eq!(seen.len(), 3);
    for (child, committed) in seen.iter() {
        // The child's admission and its model intent (its first commit) are in the parent's
        // checkpoint before its model is called.
        let (_, revision, _) = committed
            .children
            .iter()
            .find(|(id, ..)| id == child)
            .expect("admitted before it ran");
        assert_eq!(*revision, 1, "the child's model intent is committed");
    }
    // Every child admitted was admitted once, and never more than two at a time.
    let snapshots = journal.snapshots();
    assert!(snapshots.iter().all(|s| s.admitted.len() <= 2));
    assert_eq!(state.children.len(), 3);
    assert_eq!(state.usage.tool_calls, 3);
}

#[tokio::test]
async fn a_failed_commit_stops_children_and_admits_nothing_more() {
    let journal = Journal {
        fail_after_child_progress: true,
        ..Journal::default()
    };
    let (model, mut starts) = GatedModel::new(1);
    let agent = parent(child_agent(model.clone()), 2, 1, Limits::default());
    let mut state = new_run(&agent);
    let result = agent
        .drive(&mut state, &journal, RunControl::default())
        .await;
    assert!(matches!(result, Err(Error::Storage(_))), "{result:?}");
    next_start(&mut starts).await;
    assert!(starts.try_recv().is_err(), "the second child never started");
    assert!(
        journal.snapshots().iter().all(|s| s.children.len() <= 1),
        "the second child was never admitted"
    );
    assert_eq!(model.cleanup.load(Ordering::SeqCst), 1);
    // In memory, the run is as of its last durable checkpoint.
    let last = journal.last();
    assert_eq!(state.children.len(), last.children.len());
    assert_eq!(state.children[0].state.revision, last.children[0].1);
}

#[tokio::test(start_paused = true)]
async fn tools_and_children_share_the_window_and_a_sequential_call_waits_for_both() {
    let started = Arc::new(Mutex::new(Vec::new()));
    let sleeper = |name: &'static str, execution: Execution| {
        let started = started.clone();
        typed_tool::<u64, u64, _, _>(spec(name).with_execution(execution), move |ms, context| {
            let started = started.clone();
            async move {
                let now = tokio::time::Instant::now();
                started.lock().unwrap().push((context.call_id, now));
                tokio::time::sleep(Duration::from_millis(ms)).await;
                Ok(ToolOutput::Complete(ms))
            }
        })
    };
    // The child's own tool sleeps 100 ms.
    let child = child_builder(
        "sleeping-child",
        Arc::new(ScriptedModel::new(vec![
            ModelResponse::tools(vec![ToolCall {
                id: "nap".into(),
                name: "sleep".into(),
                arguments: json!(100),
            }]),
            ModelResponse::final_output(json!("rested")),
        ])),
    )
    .tool(sleeper("sleep", Execution::Parallel))
    .build()
    .unwrap();
    let calls = vec![
        call(0),
        ToolCall {
            id: "beside".into(),
            name: "sleep".into(),
            arguments: json!(30),
        },
        ToolCall {
            id: "after".into(),
            name: "alone".into(),
            arguments: json!(10),
        },
    ];
    let agent = parent_builder(child, calls, 1, Limits::default())
        .tool(sleeper("sleep", Execution::Parallel))
        .tool(sleeper("alone", Execution::Sequential))
        .build()
        .unwrap();
    let epoch = tokio::time::Instant::now();
    let mut state = new_run(&agent);
    agent
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    assert_eq!(
        state.phase,
        Phase::Settled(Settlement::Completed(json!("done")))
    );
    let mut started: Vec<(String, u64)> = (started.lock().unwrap().iter())
        .map(|(id, at)| (id.clone(), at.duration_since(epoch).as_millis() as u64))
        .collect();
    started.sort_by_key(|(id, at)| (*at, id != "nap"));
    // The parent's tool runs beside the child; the sequential call waits for both.
    assert_eq!(
        started,
        [
            ("nap".to_owned(), 0),
            ("beside".to_owned(), 0),
            ("after".to_owned(), 100)
        ]
    );
    let child = &state.children[0].state;
    assert_eq!(child.usage.active_ms, 100);
    // The parent's windows charge the 30 ms its tool overlapped the child and the 10 ms of the
    // sequential call, not the 70 ms only the child ran; the child charges its own 100 ms.
    assert_eq!(state.child_usage.active_ms, 100);
    assert_eq!(state.usage.active_ms, 100 + 30 + 10);
}

#[cfg(feature = "sqlite")]
mod durable {
    use super::*;
    use agent_rs::sqlite::SqliteStore;
    use std::sync::atomic::AtomicBool;

    fn options() -> HostOptions {
        HostOptions::default()
            .with_max_concurrent_runs(1)
            .with_poll_interval(Duration::from_millis(2))
    }
    async fn observe(
        host: &Host<SqliteStore>,
        id: RunId,
        ready: impl Fn(&RunState) -> bool,
    ) -> RunState {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                host.health().unwrap();
                let state = host.status(id).await.unwrap();
                if ready(&state) {
                    return state;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("host did not reach the expected state")
    }
    fn work() -> ToolSpec {
        ToolSpec::new("work", "1")
            .with_description("child work")
            .with_input_schema(Value::Null)
    }
    fn child_with_tool(
        tool: Arc<dyn Tool>,
        responses: Vec<ModelResponse>,
    ) -> Agent<String, String> {
        child_builder("child-with-tool", Arc::new(ScriptedModel::new(responses)))
            .tool(tool)
            .build()
            .unwrap()
    }
    fn work_then_done() -> Vec<ModelResponse> {
        vec![
            ModelResponse::tools(vec![ToolCall {
                id: "work".into(),
                name: "work".into(),
                arguments: Value::Null,
            }]),
            ModelResponse::final_output(json!("child done")),
        ]
    }

    #[tokio::test]
    async fn waiting_children_release_the_host_and_resolve_independently_across_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("children.sqlite");
        let tool = typed_tool::<Value, Value, _, _>(work(), |_, _| async {
            Ok(ToolOutput::Wait(WaitRequest::Input {
                prompt: "answer".into(),
            }))
        });
        let agent = parent(
            child_with_tool(tool, work_then_done()),
            2,
            2,
            Limits::default(),
        );
        let store = SqliteStore::open(&path).await.unwrap();
        let id = store
            .accept(&agent.erased(), "conversation", "key", json!("go"), None)
            .await
            .unwrap();
        let mut state = store.claim(id).await.unwrap();
        agent
            .drive(&mut state, &store, RunControl::default())
            .await
            .unwrap();
        assert_eq!(state.children.len(), 2);
        assert_eq!(state.wake_at(), None);
        assert!(store.due(now(), 10).await.unwrap().is_empty());
        // What the store holds is what the run holds.
        let stored = store.status(id).await.unwrap();
        assert_eq!(
            serde_json::to_value(&stored.children).unwrap(),
            serde_json::to_value(&state.children).unwrap()
        );
        // Including the refund of the window that admitted them.
        assert_eq!(stored.usage, state.usage);
        assert_eq!(stored.revision, state.revision);
        let first = state.children[0].state.id;
        let second = state.children[1].state.id;
        let reserved = state.child_usage.clone();
        store.close().await.unwrap();

        let store = Arc::new(SqliteStore::open(&path).await.unwrap());
        let mut host = Host::start(store.clone(), agent.clone(), options()).unwrap();
        host.resolve_child(id, first, "work", Resolution::Input(json!("first")))
            .await
            .unwrap();
        observe(&host, id, |root| {
            root.child(first).unwrap().state.is_terminal()
        })
        .await;
        host.shutdown().await.unwrap();
        let state = store.status(id).await.unwrap();
        assert!(!state.is_terminal());
        assert!(!state.child(second).unwrap().state.is_terminal());
        assert!(state.child_usage.model_calls < reserved.model_calls);
        assert!(store.due(now(), 10).await.unwrap().is_empty());
        store.close().await.unwrap();

        let store = Arc::new(SqliteStore::open(&path).await.unwrap());
        let mut host = Host::start(store.clone(), agent, options()).unwrap();
        host.resolve_child(id, second, "work", Resolution::Input(json!("second")))
            .await
            .unwrap();
        let completed = observe(&host, id, RunState::is_terminal).await;
        assert_eq!(
            completed.phase,
            Phase::Settled(Settlement::Completed(json!("done")))
        );
        assert_eq!(completed.usage.model_calls, 6);
        assert_eq!(completed.usage.tool_calls, 4);
        assert_eq!(completed.child_usage.model_calls, 4);
        assert_eq!(completed.child_usage.tool_calls, 2);
        // A child's operations and history are retained like a run's.
        let operations = store.operations(first).await.unwrap();
        assert_eq!(operations.len(), 1);
        let conversation = &completed.child(first).unwrap().state.conversation_id;
        assert_eq!(store.history(conversation).await.unwrap().len(), 3);
        host.shutdown().await.unwrap();
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn startup_and_explicit_retry_find_abandoned_child_dispatch_behind_an_input_wait() {
        for repair in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let store = Arc::new(
                SqliteStore::open(directory.path().join("nested-recovery.sqlite"))
                    .await
                    .unwrap(),
            );
            let calls = Arc::new(AtomicUsize::new(0));
            let counter = calls.clone();
            let tool = typed_tool::<Value, Value, _, _>(work(), move |_, _| {
                counter.fetch_add(1, Ordering::SeqCst);
                async {
                    Ok(ToolOutput::Wait(WaitRequest::Input {
                        prompt: "answer".into(),
                    }))
                }
            });
            let both = ["a", "b"].map(|id| ToolCall {
                id: id.into(),
                name: "work".into(),
                arguments: Value::Null,
            });
            let child = child_with_tool(tool, vec![ModelResponse::tools(both.to_vec())]);
            let agent = parent(child, 1, 1, Limits::default());
            let id = store
                .accept(&agent.erased(), "conversation", "key", json!("go"), None)
                .await
                .unwrap();
            let mut state = store.claim(id).await.unwrap();
            agent
                .drive(&mut state, store.as_ref(), RunControl::default())
                .await
                .unwrap();
            // The crash image in which one dispatched outcome was lost while its sibling's
            // input wait reached the same checkpoint. A changed child advances its revision.
            let child = &mut state.children[0].state;
            child.operations[0].status = OperationStatus::Dispatched;
            child.revision += 1;
            store.commit(&mut state).await.unwrap();
            assert!(store.due(now(), 10).await.unwrap().is_empty());
            assert!(store.status(id).await.unwrap().needs_recovery());
            if repair {
                store
                    .record_error(id, (&state).into(), "binding unavailable".into())
                    .await
                    .unwrap();
                store.retry(id).await.unwrap();
                assert_eq!(store.due(now(), 10).await.unwrap(), vec![id]);
            }
            let mut host = Host::start(store.clone(), agent, options()).unwrap();
            let recovered = observe(&host, id, |state| {
                matches!(
                    state.children[0].state.operations[0].status,
                    OperationStatus::Unknown { .. }
                )
            })
            .await;
            assert!(!recovered.is_terminal());
            assert!(matches!(
                recovered.children[0].state.operations[1].status,
                OperationStatus::Waiting(WaitRequest::Input { .. })
            ));
            host.shutdown().await.unwrap();
            assert_eq!(
                calls.load(Ordering::SeqCst),
                2,
                "an uncertain child effect is never replayed"
            );
            assert!(store.due(now(), 10).await.unwrap().is_empty());
            store.close().await.unwrap();
        }
    }

    struct CancelAdmission<'a> {
        store: &'a SqliteStore,
        cancelled: AtomicBool,
    }
    #[async_trait]
    impl Checkpoint for CancelAdmission<'_> {
        async fn commit(&self, state: &mut RunState) -> Result<()> {
            if !state.children.is_empty() && !self.cancelled.swap(true, Ordering::SeqCst) {
                self.store.request_cancel(state.id).await?;
            }
            self.store.commit(state).await
        }
    }

    #[tokio::test]
    async fn cancellation_fences_child_admission_before_any_child_dispatch() {
        let directory = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(directory.path().join("fence.sqlite"))
            .await
            .unwrap();
        let model = Arc::new(ScriptedModel::new(vec![ModelResponse::final_output(
            json!("ok"),
        )]));
        let agent = parent(child_agent(model.clone()), 1, 1, Limits::default());
        let id = store
            .accept(&agent.erased(), "conversation", "key", json!("go"), None)
            .await
            .unwrap();
        let mut state = store.claim(id).await.unwrap();
        let checkpoint = CancelAdmission {
            store: &store,
            cancelled: AtomicBool::new(false),
        };
        let result = agent
            .drive(&mut state, &checkpoint, RunControl::default())
            .await;
        assert!(matches!(result, Err(Error::Stale)), "{result:?}");
        assert!(
            state.children.is_empty(),
            "an uncommitted admission is undone"
        );
        let mut state = store.claim(id).await.unwrap();
        assert!(
            state.children.is_empty(),
            "admission and dependency both rolled back"
        );
        agent
            .drive(&mut state, &store, RunControl::default())
            .await
            .unwrap();
        assert_eq!(state.phase, Phase::Settled(Settlement::Cancelled));
        assert_eq!(model.calls(), 0);
        store.close().await.unwrap();
    }

    struct PendingEffect {
        spec: ToolSpec,
        started: mpsc::UnboundedSender<RunId>,
        effects: AtomicUsize,
        cleanup: AtomicUsize,
    }
    #[async_trait]
    impl Tool for PendingEffect {
        fn spec(&self) -> &ToolSpec {
            &self.spec
        }
        fn validate(&self, _: &Value) -> Result<()> {
            Ok(())
        }
        async fn execute(&self, _: Value, context: ToolContext) -> Result<ToolOutput<Value>> {
            self.effects.fetch_add(1, Ordering::SeqCst);
            self.started.send(context.run_id).unwrap();
            std::future::pending().await
        }
        async fn cleanup(&self, context: ToolContext) -> Result<()> {
            assert!(context.cancel.is_cancelled());
            self.cleanup.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn durable_parent_cancel_settles_children_and_keeps_dispatched_effects_reconcilable() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteStore::open(directory.path().join("cancel.sqlite"))
                .await
                .unwrap(),
        );
        let (started, mut starts) = mpsc::unbounded_channel();
        let tool = Arc::new(PendingEffect {
            spec: work(),
            started,
            effects: AtomicUsize::new(0),
            cleanup: AtomicUsize::new(0),
        });
        let agent = parent(
            child_with_tool(tool.clone(), work_then_done()),
            2,
            2,
            Limits::default(),
        );
        let mut host = Host::start(store.clone(), agent, options()).unwrap();
        let id = host
            .submit("conversation", "key", json!("go"), None)
            .await
            .unwrap();
        for _ in 0..2 {
            next_start(&mut starts).await;
        }
        host.cancel(id).await.unwrap();
        let state = observe(&host, id, RunState::is_terminal).await;
        assert_eq!(state.phase, Phase::Settled(Settlement::Cancelled));
        assert_eq!(tool.cleanup.load(Ordering::SeqCst), 2);
        assert_eq!(tool.effects.load(Ordering::SeqCst), 2);
        assert!(state.has_uncertainty());
        for child in &state.children {
            assert_eq!(child.state.phase, Phase::Settled(Settlement::Cancelled));
            assert!(matches!(
                child.state.operations[0].status,
                OperationStatus::Unknown { .. }
            ));
            host.resolve_child(
                id,
                child.state.id,
                "work",
                Resolution::Reconcile(ToolResult {
                    call_id: "work".into(),
                    value: json!({"receipt": "confirmed"}),
                    is_error: false,
                }),
            )
            .await
            .unwrap();
        }
        assert!(!host.status(id).await.unwrap().has_uncertainty());
        host.shutdown().await.unwrap();
        store.close().await.unwrap();
    }

    fn now() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }
}

// ---------------------------------------------------------------------------------------------
// Delegation beside the layers below it: the commit contract, reported usage, memory scope and
// model adapter bindings.
// ---------------------------------------------------------------------------------------------

/// Commits like `MemoryCheckpoint`, but appends an instruction to a child's history, which no
/// commit may do.
struct SteersAChild;
#[async_trait]
impl Checkpoint for SteersAChild {
    async fn commit(&self, state: &mut RunState) -> Result<()> {
        state.revision += 1;
        if let Some(child) = state.children.first_mut() {
            child.state.history.push(HistoryEntry::Steering {
                id: SteeringId::new_v4(),
                instruction: "injected".into(),
            });
        }
        Ok(())
    }
}
#[tokio::test]
async fn a_commit_that_changes_a_child_is_rejected() {
    let model = Arc::new(ScriptedModel::new(vec![ModelResponse::final_output(
        json!("ok"),
    )]));
    let agent = parent(child_agent(model.clone()), 1, 1, Limits::default());
    let mut state = new_run(&agent);
    let error = agent
        .drive(&mut state, &SteersAChild, RunControl::default())
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Storage(_)), "{error:?}");
    assert!(!state.is_terminal());
    assert_eq!(model.calls(), 0, "the child never started");
}

/// Reports usage on its first call and then triggers `shutdown` while the call hangs; answers the
/// second call.
struct UsageThenShutdown {
    calls: AtomicUsize,
    shutdown: CancellationToken,
}
#[async_trait]
impl Model for UsageThenShutdown {
    async fn stream(&self, _: ModelRequest, _: CancellationToken) -> Result<ModelStream> {
        use futures::StreamExt;
        if self.calls.fetch_add(1, Ordering::SeqCst) > 0 {
            let response =
                ModelResponse::final_output(json!("child done")).with_usage(TokenUsage::new(2, 3));
            return Ok(Box::pin(stream::iter([Ok(ModelEvent::Response(response))])));
        }
        let shutdown = self.shutdown.clone();
        let usage = stream::iter([Ok(ModelEvent::Usage(TokenUsage::new(9, 4)))]);
        Ok(Box::pin(usage.chain(stream::once(async move {
            shutdown.cancel();
            std::future::pending().await
        }))))
    }
}
fn tokens(usage: &Usage) -> (u64, u64) {
    (usage.input_tokens, usage.output_tokens)
}

#[tokio::test]
async fn a_childs_reported_usage_is_charged_once_even_when_its_turn_is_rejected_or_interrupted() {
    // Rejected: the child's stream reports usage and ends without a response, failing the child.
    let rejected = ScriptedModel::with_events(vec![vec![ModelEvent::Usage(TokenUsage::new(9, 4))]]);
    let agent = parent(child_agent(Arc::new(rejected)), 1, 1, Limits::default());
    let mut state = new_run(&agent);
    agent
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    assert_eq!(
        state.phase,
        Phase::Settled(Settlement::Completed(json!("done")))
    );
    assert!(matches!(
        outcome(&results(&state)[0]),
        ChildOutcome::Failed(_)
    ));
    assert_eq!(tokens(&state.children[0].state.usage), (9, 4));
    assert_eq!(tokens(&state.child_usage), (9, 4));
    assert_eq!(tokens(&state.usage), (9, 4));

    // Interrupted: the charge is committed with the child's interrupted turn, inside the
    // allocation its parent still reserves, and replaces that allocation once at settlement.
    let control = RunControl::default();
    let model = Arc::new(UsageThenShutdown {
        calls: AtomicUsize::new(0),
        shutdown: control.shutdown.clone(),
    });
    let agent = parent(child_agent(model.clone()), 1, 1, Limits::default());
    let mut state = new_run(&agent);
    let result = agent.drive(&mut state, &MemoryCheckpoint, control).await;
    assert!(matches!(result, Err(Error::Interrupted)), "{result:?}");
    let child = &state.children[0].state;
    assert_eq!(child.phase, Phase::Ready);
    assert_eq!(tokens(&child.usage), (9, 4));
    let reserved = (0, child_limits().max_tokens);
    assert_eq!(tokens(&state.child_usage), reserved);
    assert_eq!(tokens(&state.usage), reserved);
    agent
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    assert_eq!(
        state.phase,
        Phase::Settled(Settlement::Completed(json!("done")))
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    assert_eq!(tokens(&state.children[0].state.usage), (11, 7));
    assert_eq!(tokens(&state.child_usage), (11, 7));
    assert_eq!(tokens(&state.usage), (11, 7));
    assert_eq!(state.child_usage.model_calls, 2);
    assert_eq!(state.usage.model_calls, 2 + 2);
}

/// Records the conversation each turn's preparation is scoped to.
#[derive(Default)]
struct Scopes(Mutex<Vec<(RunId, String)>>);
#[async_trait]
impl ContextPolicy for Scopes {
    fn binding(&self) -> Value {
        json!("scopes-v1")
    }
    async fn prepare(
        &self,
        request: ContextRequest<'_>,
        _: CancellationToken,
    ) -> Result<Vec<Value>> {
        let scope = (request.run_id, request.conversation_id.to_owned());
        self.0.lock().unwrap().push(scope);
        Ok(vec![])
    }
}

#[tokio::test]
async fn a_childs_context_policy_is_scoped_to_its_derived_conversation() {
    let scopes = Arc::new(Scopes::default());
    let model = Arc::new(ScriptedModel::new(vec![
        ModelResponse::final_output(json!("a")),
        ModelResponse::final_output(json!("b")),
    ]));
    let child = child_builder("scoped-child", model)
        .context_policy(scopes.clone(), 256)
        .build()
        .unwrap();
    let agent = parent(child, 2, 1, Limits::default());
    let mut conversation = Conversation::new("trip-1");
    assert_eq!(agent.run(&mut conversation, "go").await.unwrap(), "done");
    let root = conversation.last_run.as_ref().unwrap();
    assert_eq!(root.conversation_id, "trip-1");
    let expected: Vec<_> = (root.children.iter())
        .map(|child| {
            let scope = format!("child:{}:{}", root.id, child.call_id);
            assert_eq!(child.state.conversation_id, scope);
            (child.state.id, scope)
        })
        .collect();
    assert_eq!(expected.len(), 2);
    assert_eq!(*scopes.0.lock().unwrap(), expected);
}

#[cfg(feature = "memory")]
#[tokio::test]
async fn a_childs_memory_policy_reads_its_own_scope_and_not_its_parents() {
    use agent_rs::memory::{
        ExtractiveSummarizer, InMemorySourceStore, MemoryPolicy, SourceStore, SourceTrust,
        Summarizer, index_summary,
    };
    /// Records each request's derived context.
    struct Derived(ScriptedModel, Mutex<Vec<Vec<Value>>>);
    #[async_trait]
    impl Model for Derived {
        async fn stream(
            &self,
            request: ModelRequest,
            cancel: CancellationToken,
        ) -> Result<ModelStream> {
            self.1.lock().unwrap().push(request.derived_context.clone());
            self.0.stream(request, cancel).await
        }
    }
    let sources = Arc::new(InMemorySourceStore::default());
    let summarizer = ExtractiveSummarizer::new(2, 256).unwrap();
    let policy = MemoryPolicy::new(sources.clone(), 32)
        .unwrap()
        .with_summaries(summarizer.producer())
        .unwrap();
    let model = Arc::new(Derived(
        ScriptedModel::new(vec![ModelResponse::final_output(json!("ok"))]),
        Mutex::default(),
    ));
    let child = child_builder("remembering-child", model.clone())
        .context_policy(Arc::new(policy), 1024)
        .build()
        .unwrap();
    let agent = parent(child, 1, 1, Limits::default());
    let mut state = agent.new_run("trip-1", json!("go"), &[], None).unwrap();
    let scope = format!("child:{}:child-0", state.id);
    for (conversation, text) in [
        ("trip-1", "Prefer the train."),
        (&*scope, "Leave after nine."),
    ] {
        let source = sources
            .upsert_source(conversation, "notes", text, SourceTrust::Approved)
            .await
            .unwrap();
        index_summary(sources.as_ref(), "summary", &[source], &summarizer)
            .await
            .unwrap();
    }
    agent
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    assert_eq!(state.children[0].state.conversation_id, scope);
    let seen = model.1.lock().unwrap();
    assert_eq!(seen.len(), 1);
    let derived = serde_json::to_string(&seen[0]).unwrap();
    assert!(derived.contains("Leave after nine."), "{derived}");
    assert!(!derived.contains("Prefer the train."), "{derived}");
}

/// Names an adapter in its binding, as the OpenAI adapters do.
struct Routed(ScriptedModel);
#[async_trait]
impl Model for Routed {
    fn binding(&self) -> Value {
        json!({"api": "responses", "endpoint": "https://example.test/v1/responses"})
    }
    async fn stream(
        &self,
        request: ModelRequest,
        cancel: CancellationToken,
    ) -> Result<ModelStream> {
        self.0.stream(request, cancel).await
    }
}

#[tokio::test]
async fn a_parents_binding_carries_each_childs_adapter_and_context_policy() {
    let routed = || -> Arc<dyn Model> { Arc::new(Routed(ScriptedModel::new(vec![]))) };
    let child = child_builder("routed-child", routed())
        .context_policy(Arc::new(Scopes::default()), 256)
        .build()
        .unwrap();
    let agent = parent(child.clone(), 1, 1, Limits::default());
    let binding = agent.binding();
    // The parent's own model has no adapter identity, so it adds no key of its own.
    assert!(binding.get("model_adapter").is_none());
    assert!(binding.get("context_policy").is_none());
    let nested = &binding["subagents"]["agents"][0];
    assert_eq!(nested["name"], json!("delegate"));
    assert_eq!(&nested["agent"], child.binding());
    assert!(nested["agent"].get("model_adapter").is_some());
    assert!(nested["agent"].get("context_policy").is_some());
    // A run accepted with a routed child cannot be driven by a parent whose child is not.
    let unrouted = child_builder("routed-child", Arc::new(ScriptedModel::new(vec![])))
        .context_policy(Arc::new(Scopes::default()), 256)
        .build()
        .unwrap();
    let unrouted = parent(unrouted, 1, 1, Limits::default());
    let mut state = new_run(&agent);
    assert!(matches!(
        unrouted
            .drive(&mut state, &MemoryCheckpoint, RunControl::default())
            .await,
        Err(Error::Incompatible(_))
    ));
    assert_eq!(state.usage.model_calls, 0);
}
