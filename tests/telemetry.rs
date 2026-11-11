//! The public, content-free tracing contract, exercised through the engine API.
use agent_rs::*;
use async_trait::async_trait;
use futures::{StreamExt, stream};
use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio_util::sync::CancellationToken;
use tracing::instrument::WithSubscriber;
use tracing::{
    Level, Metadata, Subscriber,
    field::{Field, Visit},
    span::{Attributes, Id, Record},
};
use tracing_core::span::Current;

#[derive(Clone, Debug)]
struct Captured {
    metadata: &'static Metadata<'static>,
    parent: Option<usize>,
    fields: BTreeMap<String, String>,
}
#[derive(Default)]
struct CaptureState {
    spans: Vec<Captured>,
    events: Vec<Captured>,
    stack: Vec<usize>,
}
#[derive(Clone, Default)]
struct Capture(Arc<Mutex<CaptureState>>);
struct Fields<'a>(&'a mut BTreeMap<String, String>);
impl Visit for Fields<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().into(), format!("{value:?}"));
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().into(), value.into());
    }
}
impl Subscriber for Capture {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, attrs: &Attributes<'_>) -> Id {
        let mut state = self.0.lock().unwrap();
        let parent = attrs
            .parent()
            .map(|id| id.into_u64() as usize - 1)
            .or_else(|| {
                attrs
                    .is_contextual()
                    .then(|| state.stack.last().copied())
                    .flatten()
            });
        let mut fields = BTreeMap::new();
        attrs.record(&mut Fields(&mut fields));
        state.spans.push(Captured {
            metadata: attrs.metadata(),
            parent,
            fields,
        });
        Id::from_u64(state.spans.len() as u64)
    }
    fn record(&self, id: &Id, record: &Record<'_>) {
        record.record(&mut Fields(
            &mut self.0.lock().unwrap().spans[id.into_u64() as usize - 1].fields,
        ));
    }
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        let mut state = self.0.lock().unwrap();
        let parent = event
            .parent()
            .map(|id| id.into_u64() as usize - 1)
            .or_else(|| {
                event
                    .is_contextual()
                    .then(|| state.stack.last().copied())
                    .flatten()
            });
        let mut fields = BTreeMap::new();
        event.record(&mut Fields(&mut fields));
        state.events.push(Captured {
            metadata: event.metadata(),
            parent,
            fields,
        });
    }
    fn enter(&self, id: &Id) {
        self.0
            .lock()
            .unwrap()
            .stack
            .push(id.into_u64() as usize - 1);
    }
    fn exit(&self, id: &Id) {
        assert_eq!(
            self.0.lock().unwrap().stack.pop(),
            Some(id.into_u64() as usize - 1)
        );
    }
    fn current_span(&self) -> Current {
        let state = self.0.lock().unwrap();
        state.stack.last().map_or_else(Current::none, |&index| {
            Current::new(Id::from_u64(index as u64 + 1), state.spans[index].metadata)
        })
    }
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
            ("gen_ai.tool.call.id", "call-echo"),
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
                ("gen_ai.tool.call.id", "call-danger"),
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
async fn host_tasks_inherit_the_callers_trace() {
    use tracing::Instrument;
    let capture = Capture::default();
    async {
        let parent = tracing::info_span!("application");
        async {
            let agent = Agent::builder(
                "hosted",
                script(vec![ModelResponse::final_output(json!(ANSWER))]),
            )
            .build()
            .unwrap();
            let mut host =
                Host::start(Arc::new(MemoryStore::new()), agent, HostOptions::default()).unwrap();
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
        .instrument(parent)
        .await;
    }
    .with_subscriber(capture.clone())
    .await;
    let state = capture.0.lock().unwrap();
    let root = state
        .spans
        .iter()
        .position(|s| s.metadata.name() == "invoke_agent")
        .expect("spawn lost the subscriber");
    assert_eq!(state.spans[root].parent, Some(0));
    assert_private(&state);
}
