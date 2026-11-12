//! The public, content-free tracing contract, exercised through the engine API.
use agent_rs::*;
use async_trait::async_trait;
use futures::{StreamExt, stream};
use serde_json::json;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio_util::sync::CancellationToken;
use tracing::Level;
use tracing::instrument::WithSubscriber;

#[path = "../src/tracing_test.rs"]
mod tracing_capture;
use tracing_capture::{Capture, CaptureState, Captured};

#[test]
fn capturing_subscribers_recheck_each_instances_filter() {
    use tracing::Subscriber;
    let metadata = tracing::subscriber::with_default(Capture::default(), || {
        tracing::info_span!("capture_filter_probe")
            .metadata()
            .unwrap()
    });
    // These subscribers share callsites while parallel tests create and drop them. Keep
    // filtering in `enabled`, so an instance's cached answer cannot hide another's spans.
    for capture in [
        Capture::at(Level::INFO),
        Capture::at(Level::WARN),
        Capture::named("unrelated"),
    ] {
        assert!(capture.register_callsite(metadata).is_sometimes());
    }
}

#[test]
fn a_thread_without_a_subscriber_cannot_hide_a_callsite_from_a_capture() {
    fn probe() -> bool {
        !tracing::info_span!("capture_untraced_probe").is_disabled()
    }
    let capture = tracing::Dispatch::new(Capture::at(Level::INFO));
    // An untraced thread reaches the callsite first, as a parallel test without a subscriber
    // does. Its absent subscriber must not answer for this one.
    assert!(!std::thread::spawn(probe).join().unwrap());
    assert!(tracing::dispatcher::with_default(&capture, probe));
}

struct Context;
#[async_trait]
impl ContextPolicy for Context {
    fn binding(&self) -> serde_json::Value {
        json!("telemetry-context")
    }
}
const INSTRUCTIONS: &str = "private instructions 7759";
const INPUT: &str = "private user message 9284";
const ARGUMENT: &str = "private tool argument 6721";
const OUTPUT: &str = "private tool output 8356";
const ANSWER: &str = "private model answer 1927";

struct Script(Vec<Vec<ModelEvent>>);
#[async_trait]
impl Model for Script {
    async fn stream(&self, request: ModelRequest, _: CancellationToken) -> Result<ModelStream> {
        let events = self.0[request.turn as usize - 1].clone();
        Ok(Box::pin(stream::iter(events).then(|event| async move {
            tokio::time::sleep(Duration::from_millis(7)).await;
            Ok(event)
        })))
    }
}
fn script(responses: Vec<ModelResponse>) -> Arc<dyn Model> {
    Arc::new(Script(
        responses
            .into_iter()
            .map(|r| {
                vec![
                    ModelEvent::TextDelta(ANSWER.into()),
                    ModelEvent::Response(r),
                ]
            })
            .collect(),
    ))
}
fn call(name: &str) -> ToolCall {
    ToolCall {
        id: format!("call-{name}"),
        name: name.into(),
        arguments: json!(ARGUMENT),
    }
}
fn assert_fields(span: &Captured, expected: &[(&str, &str)]) {
    for &(key, value) in expected {
        assert_eq!(
            span.fields.get(key).map(String::as_str),
            Some(value),
            "{}: {key}",
            span.metadata.name()
        );
    }
}
fn assert_private(state: &CaptureState) {
    for item in state.spans.iter().chain(&state.events) {
        for value in item.fields.values() {
            for secret in [INPUT, ARGUMENT, OUTPUT, ANSWER, INSTRUCTIONS] {
                assert!(!value.contains(secret), "private content leaked: {item:?}");
            }
        }
    }
}

#[tokio::test(start_paused = true)]
async fn scripted_run_has_nested_content_free_spans() {
    let child = Agent::builder(
        "child",
        script(vec![
            ModelResponse::final_output(json!(ANSWER)).with_usage(TokenUsage::new(3, 4)),
        ]),
    )
    .limits(
        Limits::default()
            .with_max_model_calls(2)
            .with_max_tool_calls(1)
            .with_max_tokens(100)
            .with_cleanup_timeout(Duration::from_millis(10))
            .with_operation_timeout(Duration::from_millis(100))
            .with_active_budget(Duration::from_secs(1)),
    )
    .build()
    .unwrap();
    let tool = typed_tool::<String, String, _, _>(ToolSpec::new("echo", "1"), |_, _| async {
        Ok(ToolOutput::Complete(OUTPUT.into()))
    });
    let parent = Agent::builder(
        "parent",
        script(vec![
            ModelResponse::tools(vec![call("echo"), call("delegate")])
                .with_usage(TokenUsage::new(11, 12)),
            ModelResponse::final_output(json!(ANSWER)).with_usage(TokenUsage::new(21, 22)),
        ]),
    )
    .instructions(INSTRUCTIONS)
    .context_policy(Arc::new(Context), 0)
    .tools([tool])
    .subagents(1, [Subagent::new("delegate", "child work", child)])
    .build()
    .unwrap();
    let mut run = parent
        .new_run("conversation", json!(INPUT), &[], None)
        .unwrap();
    let capture = Capture::default();
    parent
        .drive(&mut run, &MemoryCheckpoint, RunControl::default())
        .with_subscriber(capture.clone())
        .await
        .unwrap();
    assert_eq!(
        run.phase,
        Phase::Settled(Settlement::Completed(json!(ANSWER)))
    );
    let state = capture.0.lock().unwrap();
    let info: Vec<_> = state
        .spans
        .iter()
        .enumerate()
        .filter(|(_, s)| *s.metadata.level() == Level::INFO)
        .collect();
    assert_eq!(
        info.iter()
            .map(|(_, s)| s.metadata.name())
            .collect::<Vec<_>>(),
        [
            "invoke_agent",
            "chat",
            "execute_tool",
            "execute_tool",
            "invoke_agent",
            "chat",
            "chat"
        ]
    );
    let root = info[0];
    assert_eq!(root.1.parent, None);
    assert_fields(
        root.1,
        &[
            ("gen_ai.operation.name", "invoke_agent"),
            ("gen_ai.agent.id", "parent"),
            ("gen_ai.conversation.id", "conversation"),
            ("run_id", &run.id.to_string()),
            ("outcome", "completed"),
            ("otel.name", "invoke_agent parent"),
        ],
    );
    assert_eq!(info[1].1.parent, Some(root.0));
    assert_eq!(info[2].1.parent, info[3].1.parent);
    let window = info[2].1.parent.unwrap();
    assert_eq!(state.spans[window].metadata.name(), "tool_window");
    assert_eq!(state.spans[window].parent, Some(root.0));
    assert_eq!(info[4].1.parent, Some(info[3].0));
    assert_eq!(info[5].1.parent, Some(info[4].0));
    assert_eq!(info[6].1.parent, Some(root.0));
    assert_fields(
        info[2].1,
        &[
            ("gen_ai.operation.name", "execute_tool"),
            ("gen_ai.tool.name", "echo"),
            ("run_id", &run.id.to_string()),
            ("turn", "1"),
            ("tool_call_index", "0"),
            ("operation.status", "complete"),
            (
                "output_bytes",
                &serde_json::to_vec(&json!(OUTPUT))
                    .unwrap()
                    .len()
                    .to_string(),
            ),
        ],
    );
    assert_fields(
        info[3].1,
        &[
            ("gen_ai.tool.name", "delegate"),
            ("operation.status", "complete"),
        ],
    );
    assert_fields(
        info[4].1,
        &[
            ("parent_run_id", &run.id.to_string()),
            ("gen_ai.agent.id", "child"),
            ("outcome", "completed"),
        ],
    );
    for (span, turn, input, output) in [
        (info[1].1, "1", "11", "12"),
        (info[5].1, "1", "3", "4"),
        (info[6].1, "2", "21", "22"),
    ] {
        assert_fields(
            span,
            &[
                ("gen_ai.operation.name", "chat"),
                ("gen_ai.request.model", "scripted"),
                ("turn", turn),
                ("gen_ai.usage.input_tokens", input),
                ("gen_ai.usage.output_tokens", output),
                ("time_to_first_delta_ms", "7"),
                ("outcome", "done"),
            ],
        );
        assert!(span.fields["response_bytes"].parse::<usize>().unwrap() > 0);
    }
    assert!(!info[4].1.fields.contains_key("gen_ai.conversation.id"));
    for span in &state.spans {
        assert!(matches!(*span.metadata.level(), Level::INFO | Level::DEBUG));
        if *span.metadata.level() == Level::DEBUG {
            assert!(matches!(
                span.metadata.name(),
                "commit"
                    | "assemble_context"
                    | "model_cleanup"
                    | "tool_cleanup"
                    | "tool_window"
                    | "settle"
                    | "context_cleanup"
            ));
            if span.metadata.name() == "commit" {
                assert!(span.fields.contains_key("revision"));
            }
        }
    }
    for name in [
        "commit",
        "assemble_context",
        "model_cleanup",
        "tool_cleanup",
        "tool_window",
        "settle",
        "context_cleanup",
    ] {
        assert!(
            state
                .spans
                .iter()
                .any(|s| s.metadata.name() == name && *s.metadata.level() == Level::DEBUG),
            "missing {name}"
        );
    }
    assert!(state.events.is_empty());
    assert_private(&state);
}

#[derive(Clone, Copy)]
enum Fault {
    Timeout,
    Interrupted,
    Rejected,
    CleanupError,
    CleanupTimeout,
}
struct FaultModel {
    fault: Fault,
    shutdown: CancellationToken,
}
#[async_trait]
impl Model for FaultModel {
    async fn stream(&self, _: ModelRequest, _: CancellationToken) -> Result<ModelStream> {
        let fault = self.fault;
        let shutdown = self.shutdown.clone();
        Ok(Box::pin(
            stream::iter([
                Ok(ModelEvent::Usage(TokenUsage::new(5, 6))),
                Ok(ModelEvent::TextDelta(ANSWER.into())),
            ])
            .chain(stream::once(async move {
                match fault {
                    Fault::Timeout => std::future::pending().await,
                    Fault::Interrupted => {
                        shutdown.cancel();
                        std::future::pending().await
                    }
                    Fault::Rejected => Err(Error::provider(INPUT)),
                    _ => Ok(ModelEvent::Response(
                        ModelResponse::final_output(json!(ANSWER))
                            .with_usage(TokenUsage::new(5, 6)),
                    )),
                }
            })),
        ))
    }
    async fn cleanup(&self, _: RunId) -> Result<()> {
        match self.fault {
            Fault::CleanupError => Err(Error::provider(OUTPUT)),
            Fault::CleanupTimeout => std::future::pending().await,
            _ => Ok(()),
        }
    }
}
fn small_limits() -> Limits {
    Limits::default()
        .with_operation_timeout(Duration::from_millis(100))
        .with_cleanup_timeout(Duration::from_millis(10))
}
fn event_named<'a>(state: &'a CaptureState, name: &str) -> &'a Captured {
    let event = state
        .events
        .iter()
        .find(|e| e.metadata.name() == name)
        .unwrap_or_else(|| panic!("missing {name}: {:?}", state.events));
    assert_eq!(*event.metadata.level(), Level::WARN);
    event
}
#[tokio::test(start_paused = true)]
async fn model_failures_keep_totals_and_emit_only_safe_diagnostics() {
    for (fault, outcome, warning) in [
        (Fault::Timeout, "timed_out", Some("operation_timed_out")),
        (Fault::Interrupted, "interrupted", None),
        (Fault::Rejected, "rejected", None),
        (Fault::CleanupError, "rejected", Some("cleanup_failed")),
        (Fault::CleanupTimeout, "rejected", Some("cleanup_timed_out")),
    ] {
        let shutdown = CancellationToken::new();
        let agent = Agent::builder(
            "fault",
            Arc::new(FaultModel {
                fault,
                shutdown: shutdown.clone(),
            }),
        )
        .limits(small_limits())
        .build()
        .unwrap();
        let mut run = agent
            .new_run("conversation", json!(INPUT), &[], None)
            .unwrap();
        let capture = Capture::default();
        let result = agent
            .drive(
                &mut run,
                &MemoryCheckpoint,
                RunControl::default().with_shutdown(shutdown),
            )
            .with_subscriber(capture.clone())
            .await;
        assert_eq!(result.is_err(), matches!(fault, Fault::Interrupted));
        let state = capture.0.lock().unwrap();
        let chat = state
            .spans
            .iter()
            .position(|s| s.metadata.name() == "chat")
            .unwrap();
        assert_fields(
            &state.spans[chat],
            &[
                ("outcome", outcome),
                ("gen_ai.usage.input_tokens", "5"),
                ("gen_ai.usage.output_tokens", "6"),
            ],
        );
        if let Some(name) = warning {
            let event = event_named(&state, name);
            let parent = event.parent.unwrap();
            assert!(parent == chat || state.spans[parent].parent == Some(chat));
        }
        assert_private(&state);
    }
}

struct FaultTool {
    spec: ToolSpec,
    fault: Fault,
}
#[async_trait]
impl Tool for FaultTool {
    fn spec(&self) -> &ToolSpec {
        &self.spec
    }
    fn validate(&self, _: &serde_json::Value) -> Result<()> {
        Ok(())
    }
    async fn execute(
        &self,
        _: serde_json::Value,
        _: ToolContext,
    ) -> Result<ToolOutput<serde_json::Value>> {
        if matches!(self.fault, Fault::Timeout) {
            std::future::pending().await
        } else {
            Ok(ToolOutput::Complete(json!(OUTPUT)))
        }
    }
    async fn cleanup(&self, _: ToolContext) -> Result<()> {
        match self.fault {
            Fault::CleanupError => Err(Error::tool(ARGUMENT)),
            Fault::CleanupTimeout => std::future::pending().await,
            _ => Ok(()),
        }
    }
}
#[tokio::test(start_paused = true)]
async fn uncertain_tools_and_recovery_warn_without_content() {
    for (fault, warning) in [
        (Fault::Timeout, "operation_timed_out"),
        (Fault::CleanupError, "cleanup_failed"),
        (Fault::CleanupTimeout, "cleanup_timed_out"),
    ] {
        let tool = Arc::new(FaultTool {
            spec: ToolSpec::new("danger", "1"),
            fault,
        });
        let agent = Agent::builder(
            "fault",
            script(vec![ModelResponse::tools(vec![call("danger")])]),
        )
        .limits(small_limits())
        .tools([tool as Arc<dyn Tool>])
        .build()
        .unwrap();
        let mut run = agent
            .new_run("conversation", json!(INPUT), &[], None)
            .unwrap();
        let capture = Capture::default();
        agent
            .drive(&mut run, &MemoryCheckpoint, RunControl::default())
            .with_subscriber(capture.clone())
            .await
            .unwrap();
        assert!(matches!(
            run.operations[0].status,
            OperationStatus::Unknown { .. }
        ));
        {
            let state = capture.0.lock().unwrap();
            let tool = state
                .spans
                .iter()
                .position(|s| s.metadata.name() == "execute_tool")
                .unwrap();
            assert_fields(&state.spans[tool], &[("operation.status", "unknown")]);
            assert_eq!(event_named(&state, "operation_unknown").parent, Some(tool));
            event_named(&state, warning);
            assert_private(&state);
        }
        // A crash after dispatch leaves exactly this operation to recover on the next drive.
        run.operations[0].status = OperationStatus::Dispatched;
        let recovered = Capture::default();
        agent
            .drive(&mut run, &MemoryCheckpoint, RunControl::default())
            .with_subscriber(recovered.clone())
            .await
            .unwrap();
        let state = recovered.0.lock().unwrap();
        assert_fields(
            event_named(&state, "work_recovered"),
            &[
                ("operation", "execute_tool"),
                ("run_id", &run.id.to_string()),
                ("turn", "1"),
                ("tool_call_index", "0"),
            ],
        );
        event_named(&state, "operation_unknown");
        assert_private(&state);
    }
}

#[tokio::test(start_paused = true)]
async fn model_recovery_and_output_validation_are_visible() {
    let agent = Agent::builder(
        "typed",
        script(vec![ModelResponse::final_output(json!(123))]),
    )
    .output::<String>()
    .build()
    .unwrap();
    let mut run = agent
        .new_run("conversation", json!(INPUT), &[], None)
        .unwrap();
    run.phase = Phase::ModelInFlight;
    let capture = Capture::default();
    agent
        .drive(&mut run, &MemoryCheckpoint, RunControl::default())
        .with_subscriber(capture.clone())
        .await
        .unwrap();
    let state = capture.0.lock().unwrap();
    assert_fields(
        event_named(&state, "work_recovered"),
        &[("operation", "chat")],
    );
    let chat = state
        .spans
        .iter()
        .find(|s| s.metadata.name() == "chat")
        .unwrap();
    assert_fields(
        chat,
        &[("outcome", "rejected"), ("error.type", "model_output")],
    );
    assert_fields(&state.spans[0], &[("outcome", "failed")]);
    assert_private(&state);
}

#[tokio::test]
async fn host_tasks_keep_the_subscriber_without_retaining_startup_spans() {
    use tracing::Instrument;
    let capture = Capture::default();
    async {
        let mut host = async {
            let agent = Agent::builder(
                "hosted",
                script(vec![ModelResponse::final_output(json!(ANSWER))]),
            )
            .build()
            .unwrap();
            Host::start(Arc::new(MemoryStore::new()), agent, HostOptions::default()).unwrap()
        }
        .instrument(tracing::info_span!("startup"))
        .await;
        assert!(
            capture.0.lock().unwrap().closed.contains(&0),
            "host retained the startup span"
        );
        let id = host
            .submit("conversation", "key", json!(INPUT), None)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !host.status(id).await.unwrap().is_terminal() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        host.shutdown().await.unwrap();
    }
    .with_subscriber(capture.clone())
    .await;
    let state = capture.0.lock().unwrap();
    let root = state
        .spans
        .iter()
        .position(|s| s.metadata.name() == "invoke_agent")
        .expect("spawn lost the subscriber");
    assert_eq!(state.spans[root].parent, None);
    assert_private(&state);
}

#[tokio::test(start_paused = true)]
async fn filtered_spans_never_record_on_the_application_span() {
    use tracing::Instrument;
    for level in [Level::INFO, Level::WARN] {
        for fault in [Fault::Rejected, Fault::CleanupError, Fault::CleanupTimeout] {
            let capture = Capture::at(level);
            let shutdown = CancellationToken::new();
            let agent = Agent::builder("filtered", Arc::new(FaultModel { fault, shutdown }))
                .limits(small_limits())
                .build()
                .unwrap();
            let mut run = agent
                .new_run("conversation", json!(INPUT), &[], None)
                .unwrap();
            async {
                let application = tracing::warn_span!("application", outcome = "untouched", error.type = "untouched", revision = 777);
                agent.drive(&mut run, &MemoryCheckpoint, RunControl::default()).instrument(application).await.unwrap();
            }.with_subscriber(capture.clone()).await;
            let state = capture.0.lock().unwrap();
            assert_fields(
                &state.spans[0],
                &[
                    ("outcome", "untouched"),
                    ("error.type", "untouched"),
                    ("revision", "777"),
                ],
            );
            assert!(
                !state.records.iter().any(|(index, _)| *index == 0),
                "engine wrote to its caller's span"
            );
            if level == Level::INFO {
                let chat = state
                    .spans
                    .iter()
                    .position(|span| span.metadata.name() == "chat")
                    .unwrap();
                let outcomes: Vec<_> = state
                    .records
                    .iter()
                    .filter(|(index, _)| *index == chat)
                    .filter_map(|(_, fields)| fields.get("outcome").map(String::as_str))
                    .collect();
                assert_eq!(outcomes, ["rejected"], "cleanup wrote a chat outcome");
            }
        }
    }
}

#[tokio::test(start_paused = true)]
async fn model_controlled_call_ids_never_enter_telemetry() {
    for level in [Level::DEBUG, Level::WARN] {
        let capture = Capture::at(level);
        let tool = Arc::new(FaultTool {
            spec: ToolSpec::new("danger", "1"),
            fault: Fault::Timeout,
        });
        let mut call = call("danger");
        call.id = INPUT.into();
        let agent = Agent::builder("privacy", script(vec![ModelResponse::tools(vec![call])]))
            .limits(small_limits())
            .tools([tool as Arc<dyn Tool>])
            .build()
            .unwrap();
        let mut run = agent
            .new_run("conversation", json!(INPUT), &[], None)
            .unwrap();
        agent
            .drive(&mut run, &MemoryCheckpoint, RunControl::default())
            .with_subscriber(capture.clone())
            .await
            .unwrap();
        let state = capture.0.lock().unwrap();
        assert_private(&state);
        for item in state.spans.iter().chain(&state.events) {
            assert!(!item.fields.contains_key("gen_ai.tool.call.id"));
        }
        for event in &state.events {
            assert_fields(
                event,
                &[
                    ("run_id", &run.id.to_string()),
                    ("turn", "1"),
                    ("tool_call_index", "0"),
                    ("gen_ai.tool.name", "danger"),
                ],
            );
        }
    }
}

#[tokio::test(start_paused = true)]
async fn warn_only_model_diagnostics_identify_the_run() {
    for fault in [Fault::Timeout, Fault::CleanupError, Fault::CleanupTimeout] {
        let capture = Capture::at(Level::WARN);
        let agent = Agent::builder(
            "warnings",
            Arc::new(FaultModel {
                fault,
                shutdown: CancellationToken::new(),
            }),
        )
        .limits(small_limits())
        .build()
        .unwrap();
        let mut run = agent
            .new_run("conversation", json!(INPUT), &[], None)
            .unwrap();
        agent
            .drive(&mut run, &MemoryCheckpoint, RunControl::default())
            .with_subscriber(capture.clone())
            .await
            .unwrap();
        let state = capture.0.lock().unwrap();
        assert!(!state.events.is_empty());
        for event in &state.events {
            assert_fields(event, &[("run_id", &run.id.to_string()), ("turn", "1")]);
        }
        assert_private(&state);
    }
}

#[derive(Default)]
struct DispatchSnapshot {
    child: bool,
    saved: Mutex<Option<RunState>>,
}
#[async_trait]
impl Checkpoint for DispatchSnapshot {
    async fn commit(&self, state: &mut RunState) -> Result<()> {
        state.revision += 1;
        let target = if self.child {
            state.children.first().map(|child| &*child.state)
        } else {
            Some(&*state)
        };
        if target.is_some_and(|run| {
            run.operations
                .iter()
                .any(|op| matches!(op.status, OperationStatus::Dispatched))
        }) {
            let mut saved = self.saved.lock().unwrap();
            if saved.is_none() {
                *saved = Some(state.clone());
            }
        }
        Ok(())
    }
}
fn completed_tool(name: &str, retry: RetryPolicy) -> Arc<dyn Tool> {
    typed_tool::<String, String, _, _>(ToolSpec::new(name, "1").with_retry(retry), |_, _| async {
        Ok(ToolOutput::Complete(OUTPUT.into()))
    })
}
#[tokio::test(start_paused = true)]
async fn recovery_is_reported_once_when_a_blocked_retry_really_resumes() {
    let agent = Agent::builder(
        "recover",
        script(vec![
            ModelResponse::tools(vec![call("retry"), call("unknown")]),
            ModelResponse::final_output(json!(ANSWER)),
        ]),
    )
    .tools([
        completed_tool("retry", RetryPolicy::ReadOnly),
        completed_tool("unknown", RetryPolicy::Never),
    ])
    .build()
    .unwrap();
    let snapshot = DispatchSnapshot::default();
    let mut run = agent
        .new_run("conversation", json!(INPUT), &[], None)
        .unwrap();
    agent
        .drive(&mut run, &snapshot, RunControl::default())
        .await
        .unwrap();
    let mut run = snapshot.saved.lock().unwrap().take().unwrap();
    run.operations[1].status = OperationStatus::Unknown {
        reason: "effect needs reconciliation".into(),
    };
    let capture = Capture::at(Level::WARN);
    for _ in 0..2 {
        agent
            .drive(&mut run, &MemoryCheckpoint, RunControl::default())
            .with_subscriber(capture.clone())
            .await
            .unwrap();
    }
    assert!(
        capture.0.lock().unwrap().events.is_empty(),
        "a blocked retry is not yet recovered"
    );
    run.resolve(
        "call-unknown",
        Resolution::Reconcile(ToolResult {
            call_id: "call-unknown".into(),
            value: json!(OUTPUT),
            is_error: false,
        }),
    )
    .unwrap();
    agent
        .drive(&mut run, &MemoryCheckpoint, RunControl::default())
        .with_subscriber(capture.clone())
        .await
        .unwrap();
    assert!(run.is_terminal());
    let state = capture.0.lock().unwrap();
    assert_eq!(
        state
            .events
            .iter()
            .filter(|e| e.metadata.name() == "work_recovered")
            .count(),
        1
    );
    assert_private(&state);
}
#[tokio::test(start_paused = true)]
async fn an_attached_child_reports_its_recovered_retry_once() {
    let child = Agent::builder(
        "child",
        script(vec![
            ModelResponse::tools(vec![call("retry")]),
            ModelResponse::final_output(json!(ANSWER)),
        ]),
    )
    .limits(
        small_limits()
            .with_max_model_calls(4)
            .with_max_tool_calls(4)
            .with_max_tokens(100)
            .with_active_budget(Duration::from_secs(2)),
    )
    .tools([completed_tool("retry", RetryPolicy::ReadOnly)])
    .build()
    .unwrap();
    let parent = Agent::builder(
        "parent",
        script(vec![
            ModelResponse::tools(vec![call("delegate")]),
            ModelResponse::final_output(json!(ANSWER)),
        ]),
    )
    .subagents(1, [Subagent::new("delegate", "child", child)])
    .build()
    .unwrap();
    let snapshot = DispatchSnapshot {
        child: true,
        saved: Mutex::default(),
    };
    let mut run = parent
        .new_run("conversation", json!(INPUT), &[], None)
        .unwrap();
    parent
        .drive(&mut run, &snapshot, RunControl::default())
        .await
        .unwrap();
    let mut run = snapshot.saved.lock().unwrap().take().unwrap();
    let capture = Capture::at(Level::WARN);
    parent
        .drive(&mut run, &MemoryCheckpoint, RunControl::default())
        .with_subscriber(capture.clone())
        .await
        .unwrap();
    assert!(run.is_terminal());
    let state = capture.0.lock().unwrap();
    assert_eq!(
        state
            .events
            .iter()
            .filter(|e| e.metadata.name() == "work_recovered")
            .count(),
        1
    );
    assert_private(&state);
}

struct ProtocolTool {
    inner: FaultTool,
    shutdown: CancellationToken,
}
#[async_trait]
impl Tool for ProtocolTool {
    fn spec(&self) -> &ToolSpec {
        self.inner.spec()
    }
    fn validate(&self, args: &serde_json::Value) -> Result<()> {
        self.inner.validate(args)
    }
    async fn execute(
        &self,
        args: serde_json::Value,
        context: ToolContext,
    ) -> Result<ToolOutput<serde_json::Value>> {
        if matches!(self.inner.fault, Fault::Interrupted) {
            self.shutdown.cancel();
            std::future::pending().await
        } else {
            self.inner.execute(args, context).await
        }
    }
    async fn cleanup(&self, context: ToolContext) -> Result<()> {
        self.inner.cleanup(context).await
    }
}
#[tokio::test(start_paused = true)]
async fn tool_protocol_agrees_with_and_without_tracing() {
    for fault in [
        Fault::Timeout,
        Fault::Interrupted,
        Fault::CleanupError,
        Fault::CleanupTimeout,
    ] {
        let mut outcomes = Vec::new();
        for traced in [false, true] {
            let shutdown = CancellationToken::new();
            let tool = Arc::new(ProtocolTool {
                inner: FaultTool {
                    spec: ToolSpec::new("tool", "1").with_retry(RetryPolicy::ReadOnly),
                    fault,
                },
                shutdown: shutdown.clone(),
            });
            let agent = Agent::builder(
                "protocol",
                script(vec![
                    ModelResponse::tools(vec![call("tool")]),
                    ModelResponse::final_output(json!(ANSWER)),
                ]),
            )
            .limits(small_limits())
            .tools([tool as Arc<dyn Tool>])
            .build()
            .unwrap();
            let mut run = agent
                .new_run("conversation", json!(INPUT), &[], None)
                .unwrap();
            let capture = Capture::default();
            let dispatch = if traced {
                tracing::Dispatch::new(capture.clone())
            } else {
                tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default())
            };
            let result = agent
                .drive(
                    &mut run,
                    &MemoryCheckpoint,
                    RunControl::default().with_shutdown(shutdown),
                )
                .with_subscriber(dispatch)
                .await;
            outcomes.push((
                result.is_err(),
                run.phase,
                run.operations
                    .into_iter()
                    .map(|op| op.status)
                    .collect::<Vec<_>>(),
            ));
            assert_private(&capture.0.lock().unwrap());
        }
        assert_eq!(outcomes[0], outcomes[1]);
    }
}

#[tokio::test(start_paused = true)]
async fn filters_for_individual_tool_diagnostics_still_enable_their_work() {
    for name in ["tool_cleanup", "cleanup_failed", "operation_timed_out"] {
        let capture = Capture::named(name);
        let fault = if name == "operation_timed_out" {
            Fault::Timeout
        } else {
            Fault::CleanupError
        };
        let tool = Arc::new(FaultTool {
            spec: ToolSpec::new("tool", "1"),
            fault,
        });
        let agent = Agent::builder(
            "filtered",
            script(vec![ModelResponse::tools(vec![call("tool")])]),
        )
        .limits(small_limits())
        .tools([tool as Arc<dyn Tool>])
        .build()
        .unwrap();
        let mut run = agent
            .new_run("conversation", json!(INPUT), &[], None)
            .unwrap();
        agent
            .drive(&mut run, &MemoryCheckpoint, RunControl::default())
            .with_subscriber(capture.clone())
            .await
            .unwrap();
        let state = capture.0.lock().unwrap();
        assert_eq!(
            state.spans.len() + state.events.len(),
            1,
            "missing selected diagnostic {name}"
        );
        assert_private(&state);
    }
}

#[tokio::test(start_paused = true)]
async fn cleanup_warnings_keep_the_nearest_enabled_operation_parent() {
    for level in [Level::INFO, Level::DEBUG] {
        for fault in [Fault::CleanupError, Fault::CleanupTimeout] {
            for is_tool in [false, true] {
                let capture = Capture::at(level);
                let agent = if is_tool {
                    let tool = Arc::new(FaultTool {
                        spec: ToolSpec::new("tool", "1"),
                        fault,
                    });
                    Agent::builder(
                        "parentage",
                        script(vec![ModelResponse::tools(vec![call("tool")])]),
                    )
                    .tools([tool as Arc<dyn Tool>])
                    .limits(small_limits())
                    .build()
                    .unwrap()
                } else {
                    Agent::builder(
                        "parentage",
                        Arc::new(FaultModel {
                            fault,
                            shutdown: CancellationToken::new(),
                        }),
                    )
                    .limits(small_limits())
                    .build()
                    .unwrap()
                };
                let mut run = agent
                    .new_run("conversation", json!(INPUT), &[], None)
                    .unwrap();
                agent
                    .drive(&mut run, &MemoryCheckpoint, RunControl::default())
                    .with_subscriber(capture.clone())
                    .await
                    .unwrap();
                let state = capture.0.lock().unwrap();
                let event = event_named(
                    &state,
                    if matches!(fault, Fault::CleanupError) {
                        "cleanup_failed"
                    } else {
                        "cleanup_timed_out"
                    },
                );
                let expected = match (is_tool, level) {
                    (false, Level::INFO) => "chat",
                    (true, Level::INFO) => "execute_tool",
                    (false, _) => "model_cleanup",
                    (true, _) => "tool_cleanup",
                };
                assert_eq!(
                    event.parent.map(|index| state.spans[index].metadata.name()),
                    Some(expected)
                );
                assert_private(&state);
            }
        }
    }
}

#[tokio::test(start_paused = true)]
async fn unknown_warnings_stay_in_the_drive_trace_during_recovery_and_settlement() {
    for level in [Level::INFO, Level::DEBUG] {
        for cancelled in [false, true] {
            let agent = Agent::builder(
                "parentage",
                script(vec![
                    ModelResponse::tools(vec![call("tool")]),
                    ModelResponse::final_output(json!(ANSWER)),
                ]),
            )
            .tools([completed_tool(
                "tool",
                if cancelled {
                    RetryPolicy::ReadOnly
                } else {
                    RetryPolicy::Never
                },
            )])
            .build()
            .unwrap();
            let snapshot = DispatchSnapshot::default();
            let mut run = agent
                .new_run("conversation", json!(INPUT), &[], None)
                .unwrap();
            agent
                .drive(&mut run, &snapshot, RunControl::default())
                .await
                .unwrap();
            let mut run = snapshot.saved.lock().unwrap().take().unwrap();
            run.cancel_requested = cancelled;
            let capture = Capture::at(level);
            agent
                .drive(&mut run, &MemoryCheckpoint, RunControl::default())
                .with_subscriber(capture.clone())
                .await
                .unwrap();
            let state = capture.0.lock().unwrap();
            let event = event_named(&state, "operation_unknown");
            assert_eq!(
                event.parent.map(|index| state.spans[index].metadata.name()),
                Some("invoke_agent")
            );
            assert_private(&state);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn cancelling_a_parent_warns_about_its_abandoned_child_effects() {
    for level in [Level::INFO, Level::DEBUG] {
        let shutdown = CancellationToken::new();
        let tool = Arc::new(ProtocolTool {
            inner: FaultTool {
                spec: ToolSpec::new("tool", "1").with_retry(RetryPolicy::ReadOnly),
                fault: Fault::Interrupted,
            },
            shutdown: shutdown.clone(),
        });
        let child = Agent::builder(
            "child",
            script(vec![ModelResponse::tools(vec![call("tool")])]),
        )
        .limits(
            small_limits()
                .with_max_model_calls(4)
                .with_max_tool_calls(4)
                .with_max_tokens(100)
                .with_active_budget(Duration::from_secs(2)),
        )
        .tools([tool as Arc<dyn Tool>])
        .build()
        .unwrap();
        let parent = Agent::builder(
            "parent",
            script(vec![ModelResponse::tools(vec![call("delegate")])]),
        )
        .subagents(1, [Subagent::new("delegate", "child", child)])
        .build()
        .unwrap();
        let mut run = parent
            .new_run("conversation", json!(INPUT), &[], None)
            .unwrap();
        assert!(matches!(
            parent
                .drive(
                    &mut run,
                    &MemoryCheckpoint,
                    RunControl::default().with_shutdown(shutdown)
                )
                .await,
            Err(Error::Interrupted)
        ));
        let child_id = run.children[0].state.id;
        assert!(matches!(
            run.children[0].state.operations[0].status,
            OperationStatus::Dispatched
        ));
        run.cancel_requested = true;
        let capture = Capture::at(level);
        parent
            .drive(&mut run, &MemoryCheckpoint, RunControl::default())
            .with_subscriber(capture.clone())
            .await
            .unwrap();
        assert!(matches!(
            run.children[0].state.operations[0].status,
            OperationStatus::Unknown { .. }
        ));
        let state = capture.0.lock().unwrap();
        let event = event_named(&state, "operation_unknown");
        assert_fields(
            event,
            &[
                ("run_id", &child_id.to_string()),
                ("turn", "1"),
                ("tool_call_index", "0"),
                ("gen_ai.tool.name", "tool"),
            ],
        );
        assert!(event.parent.is_some());
        assert_private(&state);
    }
}
