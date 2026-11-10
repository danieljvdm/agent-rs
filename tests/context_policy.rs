//! The `ContextPolicy` seam, exercised by small test-only policies: one that pins entries and one
//! that contributes derived context.
use async_trait::async_trait;
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio_util::sync::CancellationToken;
use yielded_agent::*;

/// Records each request's history and derived context, then plays a script.
struct Capture {
    inner: ScriptedModel,
    seen: Mutex<Vec<(Vec<HistoryEntry>, Vec<Value>)>>,
}
impl Capture {
    fn new(responses: Vec<ModelResponse>) -> Arc<Self> {
        Arc::new(Self {
            inner: ScriptedModel::new(responses),
            seen: Mutex::new(vec![]),
        })
    }
    fn seen(&self) -> Vec<(Vec<HistoryEntry>, Vec<Value>)> {
        self.seen.lock().unwrap().clone()
    }
}
#[async_trait]
impl Model for Capture {
    async fn stream(
        &self,
        request: ModelRequest,
        cancel: CancellationToken,
    ) -> Result<ModelStream> {
        self.seen
            .lock()
            .unwrap()
            .push((request.history.clone(), request.derived_context.clone()));
        self.inner.stream(request, cancel).await
    }
}

/// Pins every exchange that called the `note` tool, standing in for steering instructions.
struct PinNotes;
#[async_trait]
impl ContextPolicy for PinNotes {
    fn binding(&self) -> Value {
        json!("pin-notes-v1")
    }
    fn pinned(&self, entry: &HistoryEntry) -> bool {
        matches!(entry, HistoryEntry::Exchange { response, .. }
            if response.tools.iter().any(|call| call.name == "note"))
    }
}

/// Returns fixed derived items and records what the engine asked for.
#[derive(Default)]
struct Derived {
    items: Vec<Value>,
    requests: Mutex<Vec<(RunId, String, Value, usize)>>,
    cleaned: Mutex<Vec<RunId>>,
}
#[async_trait]
impl ContextPolicy for Derived {
    fn binding(&self) -> Value {
        json!({"derived": 1})
    }
    async fn prepare(
        &self,
        request: ContextRequest<'_>,
        _: CancellationToken,
    ) -> Result<Vec<Value>> {
        self.requests.lock().unwrap().push((
            request.run_id,
            request.conversation_id.to_owned(),
            request.input.clone(),
            request.max_bytes,
        ));
        Ok(self.items.clone())
    }
    async fn cleanup(&self, run_id: RunId) -> Result<()> {
        self.cleaned.lock().unwrap().push(run_id);
        Ok(())
    }
}

fn spec(name: &str) -> ToolSpec {
    ToolSpec::new(name, "1")
        .with_description("test")
        .with_retry(RetryPolicy::ReadOnly)
}
/// `note` and `blob` both return `n` bytes of text.
fn tools() -> Vec<Arc<dyn Tool>> {
    ["note", "blob"]
        .into_iter()
        .map(|name| {
            typed_tool::<usize, String, _, _>(spec(name), |n, _| async move {
                Ok(ToolOutput::Complete("x".repeat(n)))
            })
        })
        .collect()
}
fn call(id: &str, name: &str, n: usize) -> ToolCall {
    ToolCall {
        id: id.into(),
        name: name.into(),
        arguments: json!(n),
    }
}
fn exchange(id: &str, name: &str, n: usize) -> HistoryEntry {
    HistoryEntry::Exchange {
        response: ModelResponse::tools(vec![call(id, name, n)]),
        results: vec![ToolResult {
            call_id: id.into(),
            value: json!("x".repeat(n)),
            is_error: false,
        }],
    }
}
fn agent(model: Arc<dyn Model>, max_context_bytes: usize) -> AgentBuilder {
    Agent::builder("context-policy", model)
        .instructions("test")
        .tools(tools())
        .limits(Limits::default().with_max_context_bytes(max_context_bytes))
}
fn len<T: serde::Serialize + ?Sized>(value: &T) -> usize {
    serde_json::to_vec(value).unwrap().len()
}
/// Bytes counted against `max_context_bytes`: both arrays, an empty derived array costing nothing.
fn sent(history: &[HistoryEntry], derived: &[Value]) -> usize {
    len(history) + if derived.is_empty() { 0 } else { len(derived) }
}

#[tokio::test]
async fn pinned_entries_survive_truncation_and_older_entries_fill_the_rest() {
    let input = HistoryEntry::Input(json!("go"));
    let (note, big, mid) = (
        exchange("note", "note", 1),
        exchange("big", "blob", 100),
        exchange("mid", "blob", 400),
    );
    // Prior context is never pinned, even when the policy would pin the entry.
    let (old_input, old_note) = (
        HistoryEntry::Input(json!("old")),
        exchange("old-note", "note", 1),
    );
    // The last turn's pinned entries fill the window exactly.
    let max = len(&[input.clone(), note.clone(), mid.clone()]);
    assert!(len(&big) <= len(&mid));
    assert!(len(&old_input) + len(&old_note) + 2 <= len(&mid) - len(&big));
    let model = Capture::new(vec![
        ModelResponse::tools(vec![call("note", "note", 1)]),
        ModelResponse::tools(vec![call("big", "blob", 100)]),
        ModelResponse::tools(vec![call("mid", "blob", 400)]),
        ModelResponse::final_output(json!("done")),
    ]);
    let agent = agent(model.clone(), max)
        .context_policy(Arc::new(PinNotes), 0)
        .build()
        .unwrap();
    let mut state = agent
        .new_run(
            "c",
            json!("go"),
            &[old_input.clone(), old_note.clone()],
            None,
        )
        .unwrap();
    agent
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    assert_eq!(
        state.phase,
        Phase::Settled(Settlement::Completed(json!("done")))
    );
    let prior = || vec![old_input.clone(), old_note.clone()];
    let history: Vec<_> = model.seen().into_iter().map(|(h, _)| h).collect();
    assert_eq!(
        history,
        [
            [prior(), vec![input.clone()]].concat(),
            [prior(), vec![input.clone(), note.clone()]].concat(),
            [prior(), vec![input.clone(), note.clone(), big]].concat(),
            // The older unpinned exchange and all prior context fall out; the pinned note stays
            // in chronological order. Without the policy this turn would send [input, mid].
            vec![input, note, mid],
        ]
    );
    assert!(history.iter().all(|h| len(h) <= max));
}

#[tokio::test]
async fn pinned_entries_that_cannot_fit_fail_without_reaching_the_model() {
    let input = HistoryEntry::Input(json!("go"));
    let (note, mid) = (exchange("note", "note", 1), exchange("mid", "blob", 400));
    // The default context [input, mid] fits; the pinned note no longer fits beside them.
    let max = len(&[input.clone(), note, mid.clone()]) - 1;
    assert!(len(&[input, mid]) <= max);
    let model = Capture::new(vec![
        ModelResponse::tools(vec![call("note", "note", 1)]),
        ModelResponse::tools(vec![call("mid", "blob", 400)]),
        ModelResponse::final_output(json!("done")),
    ]);
    let agent = agent(model.clone(), max)
        .context_policy(Arc::new(PinNotes), 0)
        .build()
        .unwrap();
    let mut state = agent.new_run("c", json!("go"), &[], None).unwrap();
    agent
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    assert!(
        matches!(&state.phase, Phase::Settled(Settlement::Failed(reason)) if reason.contains("exceed context limit")),
        "{:?}",
        state.phase
    );
    assert_eq!(model.seen().len(), 2);
}

#[tokio::test]
async fn derived_context_is_bounded_and_yields_to_pinned_history() {
    let input = HistoryEntry::Input(json!("go"));
    let exchange = exchange("blob", "blob", 40);
    let (first, oversized, second) = (
        json!({"fact": "first"}),
        json!({"fact": "o".repeat(200)}),
        json!({"fact": "second"}),
    );
    // The cap holds both small items; the oversized one between them is skipped.
    let cap = len(&[&first, &second]);
    // The second turn's pinned history fits beside the first item only.
    let max = len(&[input.clone(), exchange.clone()]) + len(&[&first]);
    assert!(cap <= max);
    // Prior context one byte too large to fit beside the first turn's derived context.
    let old = HistoryEntry::Input(json!("o".repeat(max - cap - len(&input) - 2 - 12)));
    assert_eq!(len(&[old.clone(), input.clone()]), max - cap + 1);
    let model = Capture::new(vec![
        ModelResponse::tools(vec![call("blob", "blob", 40)]),
        ModelResponse::final_output(json!("done")),
    ]);
    let policy = Arc::new(Derived {
        items: vec![first.clone(), oversized, second.clone()],
        ..Derived::default()
    });
    let agent = agent(model.clone(), max)
        .context_policy(policy.clone(), cap)
        .build()
        .unwrap();
    let mut state = agent
        .new_run("c", json!("go"), std::slice::from_ref(&old), None)
        .unwrap();
    agent
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    assert_eq!(
        state.phase,
        Phase::Settled(Settlement::Completed(json!("done")))
    );
    let seen = model.seen();
    assert_eq!(
        seen,
        [
            (vec![input.clone()], vec![first.clone(), second]),
            // The lowest-priority item is dropped so the latest exchange fits.
            (vec![input, exchange], vec![first]),
        ]
    );
    assert!(seen.iter().all(|(h, d)| sent(h, d) <= max));
    // Derived context never becomes history.
    assert_eq!(state.history.len(), 3);
    let request = (state.id, "c".to_owned(), json!("go"), cap);
    assert_eq!(*policy.requests.lock().unwrap(), [request.clone(), request]);
    assert_eq!(*policy.cleaned.lock().unwrap(), [state.id, state.id]);
}

#[tokio::test]
async fn policy_binding_is_part_of_the_agent_binding() {
    let plain = agent(Capture::new(vec![]), 1024).build().unwrap();
    assert!(plain.binding().get("context_policy").is_none());
    let bound = agent(Capture::new(vec![]), 1024)
        .context_policy(Arc::new(PinNotes), 16)
        .build()
        .unwrap();
    assert_eq!(
        bound.binding()["context_policy"],
        json!({"binding": "pin-notes-v1", "max_bytes": 16})
    );
    assert!(matches!(
        agent(Capture::new(vec![]), 1024)
            .context_policy(Arc::new(PinNotes), 1025)
            .build(),
        Err(Error::Config(message)) if message.to_string() == "derived context cap must fit the context limit"
    ));
    // A run accepted under the policy cannot be driven without it.
    let mut state = bound.new_run("c", json!("go"), &[], None).unwrap();
    assert!(matches!(
        plain
            .drive(&mut state, &MemoryCheckpoint, RunControl::default())
            .await,
        Err(Error::Incompatible(_))
    ));
    assert_eq!(state.usage.model_calls, 0);
}

/// Stored runs compare their binding with the agent's, so its encoding must not drift for an
/// unchanged definition. Changing it strands every accepted run behind `Incompatible`.
#[test]
fn binding_encoding_is_stable_for_an_unchanged_definition() {
    #[derive(serde::Deserialize, schemars::JsonSchema)]
    #[allow(dead_code)]
    struct Question {
        text: String,
    }
    #[derive(serde::Deserialize, schemars::JsonSchema)]
    #[allow(dead_code)]
    struct Answer {
        text: String,
        confidence: Option<u32>,
    }
    let agent = Agent::builder("binding", Capture::new(vec![]))
        .instructions("Answer briefly.")
        .config(ModelConfig::new("pinned").with_temperature(0.5))
        .tools(tools())
        .limits(Limits::default().with_max_context_bytes(4096))
        .input::<Question>()
        .output::<Answer>()
        .context_policy(Arc::new(PinNotes), 64)
        .build()
        .unwrap();
    assert_eq!(
        serde_json::to_string(&agent.binding()).unwrap(),
        r#"{"context_policy":{"binding":"pin-notes-v1","max_bytes":64},"id":"binding","input_schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","properties":{"text":{"type":"string"}},"required":["text"],"title":"Question","type":"object"},"instructions":"Answer briefly.","limits":{"active_budget_ms":600000,"cleanup_timeout_ms":1000,"max_context_bytes":4096,"max_model_calls":32,"max_parallel_tools":4,"max_response_bytes":1048576,"max_tokens":100000,"max_tool_calls":128,"max_tool_output_bytes":262144,"operation_timeout_ms":30000},"model":{"model":"pinned","temperature":0.5},"output_schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","properties":{"confidence":{"format":"uint32","minimum":0,"type":["integer","null"]},"text":{"type":"string"}},"required":["text"],"title":"Answer","type":"object"},"tools":[{"description":"test","execution":"Parallel","input_schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","format":"uint","minimum":0,"title":"uint","type":"integer"},"name":"note","requires_approval":false,"retry":"ReadOnly","version":"1"},{"description":"test","execution":"Parallel","input_schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","format":"uint","minimum":0,"title":"uint","type":"integer"},"name":"blob","requires_approval":false,"retry":"ReadOnly","version":"1"}]}"#
    );
}

/// A model adapter's identity adds one `model_adapter` key and changes nothing else; a model
/// without one (the default `null`) adds no key, as in the pinned encoding above.
#[tokio::test]
async fn model_adapter_binding_adds_one_key() {
    struct Routed(Capture);
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
    let unrouted = Agent::builder("binding", Capture::new(vec![]))
        .instructions("Answer briefly.")
        .build()
        .unwrap();
    let routed = Agent::builder(
        "binding",
        Arc::new(Routed(Arc::into_inner(Capture::new(vec![])).unwrap())),
    )
    .instructions("Answer briefly.")
    .build()
    .unwrap();
    assert!(unrouted.binding().get("model_adapter").is_none());
    assert_eq!(
        serde_json::to_string(&routed.binding()).unwrap(),
        r#"{"id":"binding","input_schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","title":"AnyValue"},"instructions":"Answer briefly.","limits":{"active_budget_ms":600000,"cleanup_timeout_ms":1000,"max_context_bytes":2097152,"max_model_calls":32,"max_parallel_tools":4,"max_response_bytes":1048576,"max_tokens":100000,"max_tool_calls":128,"max_tool_output_bytes":262144,"operation_timeout_ms":30000},"model":{"model":"scripted","temperature":null},"model_adapter":{"api":"responses","endpoint":"https://example.test/v1/responses"},"output_schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","title":"AnyValue"},"tools":[]}"#
    );
    let mut without = routed.binding().clone();
    without.as_object_mut().unwrap().remove("model_adapter");
    assert_eq!(&without, unrouted.binding());
    // A run accepted through the route cannot be driven without it.
    let mut state = routed.new_run("c", json!("go"), &[], None).unwrap();
    assert!(matches!(
        unrouted
            .drive(&mut state, &MemoryCheckpoint, RunControl::default())
            .await,
        Err(Error::Incompatible(_))
    ));
}

/// Never finishes preparing; counts cleanups.
#[derive(Default)]
struct Pending {
    entered: tokio::sync::Notify,
    cleaned: AtomicUsize,
}
#[async_trait]
impl ContextPolicy for Pending {
    fn binding(&self) -> Value {
        json!("pending-v1")
    }
    async fn prepare(&self, _: ContextRequest<'_>, _: CancellationToken) -> Result<Vec<Value>> {
        self.entered.notify_one();
        futures::future::pending().await
    }
    async fn cleanup(&self, _: RunId) -> Result<()> {
        self.cleaned.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn preparation_obeys_cancellation_and_deadline_then_cleans_up() {
    for cancel in [true, false] {
        let policy = Arc::new(Pending::default());
        let model = Arc::new(ScriptedModel::new(vec![]));
        let agent = Agent::builder("context-policy", model.clone())
            .instructions("test")
            .limits(Limits::default().with_operation_timeout(std::time::Duration::from_millis(20)))
            .context_policy(policy.clone(), 512)
            .build()
            .unwrap();
        let mut state = agent.new_run("c", json!("go"), &[], None).unwrap();
        let control = RunControl::default();
        let token = control.cancel.clone();
        let (result, ()) =
            tokio::join!(agent.drive(&mut state, &MemoryCheckpoint, control), async {
                if cancel {
                    policy.entered.notified().await;
                    token.cancel();
                }
            });
        result.unwrap();
        if cancel {
            assert_eq!(state.phase, Phase::Settled(Settlement::Cancelled));
        } else {
            assert!(
                matches!(&state.phase, Phase::Settled(Settlement::Failed(reason)) if reason.contains("timed out")),
                "{:?}",
                state.phase
            );
        }
        assert_eq!(model.calls(), 0);
        assert_eq!(policy.cleaned.load(Ordering::SeqCst), 1);
        assert_eq!(state.usage.model_calls, 1);
    }
}

#[tokio::test]
async fn in_memory_conversations_give_policies_a_stable_id_of_their_own() {
    let policy = Arc::new(Derived::default());
    let agent = agent(
        Capture::new(vec![ModelResponse::final_output(json!("done")); 4]),
        1024,
    )
    .context_policy(policy.clone(), 64)
    .build()
    .unwrap();
    let (mut first, mut second) = (Conversation::default(), Conversation::default());
    let mut named = Conversation::new("trip-1");
    assert_ne!(first.id(), second.id());
    assert_eq!(first.id().len(), 36);
    assert_eq!(named.id(), "trip-1");
    for turn in 0..4 {
        let conversation = match turn {
            0 | 2 => &mut first,
            1 => &mut second,
            _ => &mut named,
        };
        let _: Value = agent.run(conversation, "go").await.unwrap();
        let run = conversation.last_run.as_ref().unwrap();
        assert_eq!(run.conversation_id, conversation.id());
    }
    let seen: Vec<_> = policy
        .requests
        .lock()
        .unwrap()
        .iter()
        .map(|(_, conversation, _, _)| conversation.clone())
        .collect();
    assert_eq!(seen, [first.id(), second.id(), first.id(), "trip-1"]);

    // A retained run cannot move to another conversation and pick up its state.
    second.last_run = first.last_run.take();
    assert!(matches!(
        agent.run(&mut second, "go").await,
        Err(Error::Input(message)) if message.to_string().contains("another conversation ID")
    ));
    assert!(matches!(
        agent.resume(&mut second, RunControl::default()).await,
        Err(Error::Input(_))
    ));
    assert_eq!(policy.requests.lock().unwrap().len(), 4);
}

/// Prepares with each inner policy in turn, rebuilding the engine's request for each.
struct Both(Arc<Derived>, Arc<Derived>);
#[async_trait]
impl ContextPolicy for Both {
    fn binding(&self) -> Value {
        json!([self.0.binding(), self.1.binding()])
    }
    async fn prepare(
        &self,
        request: ContextRequest<'_>,
        cancel: CancellationToken,
    ) -> Result<Vec<Value>> {
        let (run_id, conversation_id, input) =
            (request.run_id, request.conversation_id, request.input);
        let half = request.max_bytes / 2;
        let mut items = self
            .0
            .prepare(
                ContextRequest::new(run_id, conversation_id, input, half),
                cancel.clone(),
            )
            .await?;
        items.extend(
            self.1
                .prepare(
                    ContextRequest::new(run_id, conversation_id, input, half),
                    cancel,
                )
                .await?,
        );
        Ok(items)
    }
}

#[tokio::test]
async fn policies_can_be_called_and_composed_outside_the_engine() {
    let (a, b) = (
        Arc::new(Derived {
            items: vec![json!("a")],
            ..Derived::default()
        }),
        Arc::new(Derived {
            items: vec![json!("b")],
            ..Derived::default()
        }),
    );
    let both = Both(a.clone(), b.clone());
    let input = json!("go");
    let items = both
        .prepare(
            ContextRequest::new(RunId::nil(), "c", &input, 64),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(items, [json!("a"), json!("b")]);
    let expected = (RunId::nil(), "c".to_owned(), input.clone(), 32);
    assert_eq!(*a.requests.lock().unwrap(), std::slice::from_ref(&expected));
    assert_eq!(*b.requests.lock().unwrap(), [expected]);

    // Driven by the engine, the composite sees the engine's request.
    let model = Capture::new(vec![ModelResponse::final_output(json!("done"))]);
    let agent = agent(model.clone(), 1024)
        .context_policy(Arc::new(Both(a.clone(), b)), 64)
        .build()
        .unwrap();
    let mut conversation = Conversation::new("c");
    let _: Value = agent.run(&mut conversation, "go").await.unwrap();
    assert_eq!(model.seen()[0].1, [json!("a"), json!("b")]);
    let run_id = conversation.last_run.as_ref().unwrap().id;
    assert_eq!(
        a.requests.lock().unwrap()[1],
        (run_id, "c".to_owned(), input, 32)
    );
}
