//! Identical durability assertions against every `DurableStore`. A new adapter adds a
//! `Fixture` and one `conformance_tests!` invocation.
use async_trait::async_trait;
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;
use yielded_agent::{Host, HostOptions, *};

/// Opens a store and simulates a host restart over the same durable contents.
#[async_trait]
trait Fixture: Send + Sync {
    async fn open(&self) -> Arc<dyn DurableStore>;
    async fn close(&self, store: Arc<dyn DurableStore>);
    async fn reopen(&self, store: Arc<dyn DurableStore>) -> Arc<dyn DurableStore> {
        self.close(store).await;
        self.open().await
    }
}

/// A restart keeps the process, so the same store is handed back.
#[derive(Default)]
struct Memory(Arc<MemoryStore>);
#[async_trait]
impl Fixture for Memory {
    async fn open(&self) -> Arc<dyn DurableStore> {
        self.0.clone()
    }
    async fn close(&self, _: Arc<dyn DurableStore>) {}
}

#[cfg(feature = "sqlite")]
struct Sqlite {
    directory: tempfile::TempDir,
    open: Mutex<Option<Arc<sqlite::SqliteStore>>>,
}
#[cfg(feature = "sqlite")]
#[async_trait]
impl Fixture for Sqlite {
    async fn open(&self) -> Arc<dyn DurableStore> {
        let store = Arc::new(
            sqlite::SqliteStore::open(self.directory.path().join("conformance.sqlite"))
                .await
                .unwrap(),
        );
        *self.open.lock().unwrap() = Some(store.clone());
        store
    }
    async fn close(&self, store: Arc<dyn DurableStore>) {
        drop(store);
        let open = self.open.lock().unwrap().take().unwrap();
        open.close().await.unwrap();
    }
}

fn agent(responses: Vec<ModelResponse>, tools: Vec<Arc<dyn Tool>>) -> Agent {
    Agent::builder("conformance-v1", Arc::new(ScriptedModel::new(responses)))
        .instructions("Be deterministic")
        .tools(tools)
        .build()
        .unwrap()
}
fn spec(name: &str, retry: RetryPolicy) -> ToolSpec {
    ToolSpec {
        name: name.into(),
        description: name.into(),
        input_schema: json!({}),
        version: "1".into(),
        retry,
        execution: Execution::Parallel,
        requires_approval: false,
    }
}
fn options() -> HostOptions {
    HostOptions {
        max_concurrent_runs: 2,
        poll_interval: Duration::from_millis(5),
        events: None,
    }
}
fn start(store: &Arc<dyn DurableStore>, agent: Agent) -> Host<dyn DurableStore> {
    Host::start(store.clone(), agent, options()).unwrap()
}
async fn wait_for(
    host: &Host<dyn DurableStore>,
    id: RunId,
    predicate: impl Fn(&RunState) -> bool,
) -> RunState {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let state = host.status(id).await.unwrap();
            if predicate(&state) {
                return state;
            }
            host.health().unwrap();
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("host did not reach the expected durable state")
}

async fn admission_and_fencing(fixture: &impl Fixture) {
    let store = fixture.open().await;
    let agent = agent(vec![ModelResponse::final_output(json!("done"))], vec![]);
    let (first, second) = tokio::join!(
        store.accept(&agent, "c", "key", json!(1), None),
        store.accept(&agent, "c", "key", json!(1), None),
    );
    let id = first.unwrap();
    assert_eq!(id, second.unwrap());
    assert!(matches!(
        store.accept(&agent, "c", "key", json!(2), None).await,
        Err(Error::IdempotencyConflict)
    ));
    assert!(matches!(
        store.accept(&agent, "c", "key", json!(1), Some(1)).await,
        Err(Error::IdempotencyConflict)
    ));
    assert!(matches!(
        store.accept(&agent, "c", "other", json!(1), None).await,
        Err(Error::ConversationBusy)
    ));
    assert!(matches!(
        store.accept(&agent, "", "key", json!(1), None).await,
        Err(Error::Input(_))
    ));
    assert_eq!(
        store.history("c").await.unwrap(),
        vec![HistoryEntry::Input(json!(1))]
    );

    let mut first = store.claim(id).await.unwrap();
    let mut current = store.claim(id).await.unwrap();
    assert_eq!(current.generation, first.generation + 1);
    assert_eq!(current.revision, first.revision + 1);
    first.phase = Phase::Settled(Settlement::Completed(json!("stale")));
    assert!(matches!(store.commit(&mut first).await, Err(Error::Stale)));
    assert_eq!(first.revision + 1, current.revision);
    // A commit persists progress only; acceptance data is fixed when the run is accepted.
    current.input = json!("rewritten");
    current.usage.model_calls = 1;
    let committed = current.revision + 1;
    store.commit(&mut current).await.unwrap();
    assert_eq!(current.revision, committed);
    let stored = store.status(id).await.unwrap();
    assert_eq!(stored.input, json!(1));
    assert_eq!(stored.usage.model_calls, 1);
    assert_eq!(stored.revision, committed);
    assert_eq!(store.history("c").await.unwrap().len(), 1);
    let mut shrunk = current.clone();
    shrunk.history.clear();
    assert!(matches!(
        store.commit(&mut shrunk).await,
        Err(Error::Input(_))
    ));
    assert_eq!(store.status(id).await.unwrap().revision, committed);

    let old_position = CheckpointPosition::from(&current);
    assert_eq!(store.position(id).await.unwrap(), old_position);
    store.request_cancel(id).await.unwrap();
    let cancelled = store.position(id).await.unwrap();
    assert_eq!(cancelled.revision, old_position.revision + 1);
    store.request_cancel(id).await.unwrap();
    assert_eq!(store.position(id).await.unwrap(), cancelled);
    current.phase = Phase::Settled(Settlement::Completed(json!("late")));
    assert!(matches!(
        store.commit(&mut current).await,
        Err(Error::Stale)
    ));
    assert!(
        !store
            .record_error(id, old_position, "late failure".into())
            .await
            .unwrap()
    );
    assert_eq!(store.last_error(id).await.unwrap(), None);

    let store = fixture.reopen(store).await;
    assert!(store.status(id).await.unwrap().cancel_requested);
    assert_eq!(store.due(now_ms(), 1).await.unwrap(), vec![id]);
    let mut host = start(&store, agent.clone());
    assert!(matches!(
        Host::start(store.clone(), agent.clone(), options()),
        Err(Error::StoreBusy)
    ));
    assert_eq!(
        wait_for(&host, id, RunState::is_terminal).await.phase,
        Phase::Settled(Settlement::Cancelled)
    );
    assert!(store.due(now_ms(), 10).await.unwrap().is_empty());
    assert!(matches!(store.position(id).await, Err(Error::NotFound)));
    assert!(matches!(store.claim(id).await, Err(Error::Stale)));
    assert!(matches!(store.retry(id).await, Err(Error::Stale)));
    assert_eq!(host.submit("c", "key", json!(1), None).await.unwrap(), id);
    let next = host.submit("c", "next", json!(2), None).await.unwrap();
    assert_eq!(
        wait_for(&host, next, RunState::is_terminal).await.phase,
        Phase::Settled(Settlement::Completed(json!("done")))
    );
    assert_eq!(store.history("c").await.unwrap().len(), 3);
    assert!(matches!(
        store.status(RunId::new_v4()).await,
        Err(Error::NotFound)
    ));
    host.shutdown().await.unwrap();
    // Shutdown releases residency.
    let mut host = start(&store, agent);
    host.shutdown().await.unwrap();
    fixture.close(store).await;
}

async fn safe_replay_and_budgets(fixture: &impl Fixture) {
    let store = fixture.open().await;
    let observed = Arc::new(Mutex::new(Vec::new()));
    let mut tools = Vec::new();
    for (name, retry) in [
        ("read", RetryPolicy::ReadOnly),
        (
            "deduplicate",
            RetryPolicy::Idempotent {
                contract: "external-key-v1".into(),
            },
        ),
    ] {
        let observed = observed.clone();
        tools.push(typed_tool::<Value, Value, _, _>(
            spec(name, retry),
            move |_, ctx| {
                observed
                    .lock()
                    .unwrap()
                    .push((ctx.call_id, ctx.idempotency_key));
                async { Ok(ToolOutput::Complete(json!("ok"))) }
            },
        ));
    }
    let agent = agent(
        vec![
            ModelResponse::tools(
                tools
                    .iter()
                    .map(|tool| ToolCall {
                        id: tool.spec().name.clone(),
                        name: tool.spec().name.clone(),
                        arguments: json!({}),
                    })
                    .collect(),
            ),
            ModelResponse::final_output(json!("recovered")),
        ],
        tools.clone(),
    );
    let id = store
        .accept(&agent, "c", "key", json!(1), None)
        .await
        .unwrap();
    let mut state = store.claim(id).await.unwrap();
    state.phase = Phase::Tools;
    state.usage.model_calls = 1;
    state.usage.tool_calls = 2;
    state.usage.active_ms = 31_000;
    state.operations = tools
        .iter()
        .map(|tool| {
            Operation::new(
                ToolCall {
                    id: tool.spec().name.clone(),
                    name: tool.spec().name.clone(),
                    arguments: json!({}),
                },
                tool.spec().clone(),
                format!("{id}/{}", tool.spec().name),
                OperationStatus::Dispatched,
            )
        })
        .collect();
    state.pending_response = Some(ModelResponse::tools(
        state.operations.iter().map(|op| op.call.clone()).collect(),
    ));
    store.commit(&mut state).await.unwrap();
    assert_eq!(store.recovery_page(None, 10).await.unwrap(), vec![id]);
    let store = fixture.reopen(store).await;
    let mut host = start(&store, agent);
    let done = wait_for(&host, id, RunState::is_terminal).await;
    assert_eq!(
        done.phase,
        Phase::Settled(Settlement::Completed(json!("recovered")))
    );
    assert_eq!(done.usage.model_calls, 2);
    assert_eq!(done.usage.tool_calls, 4);
    assert!(done.usage.active_ms >= 31_000);
    let mut calls = observed.lock().unwrap().clone();
    calls.sort();
    assert_eq!(
        calls,
        vec![
            ("deduplicate".into(), format!("{id}/deduplicate")),
            ("read".into(), format!("{id}/read"))
        ]
    );
    assert_eq!(store.operations(id).await.unwrap().len(), 2);
    assert_eq!(store.history("c").await.unwrap().len(), 3);
    assert!(store.recovery_page(None, 10).await.unwrap().is_empty());
    host.shutdown().await.unwrap();
    fixture.close(store).await;
}

async fn hidden_dispatch_and_repair(fixture: &impl Fixture) {
    let store = fixture.open().await;
    let tool = typed_tool::<Value, Value, _, _>(spec("effect", RetryPolicy::Never), |_, _| async {
        panic!("an uncertain ordinary mutation must not replay")
    });
    let original = agent(
        vec![ModelResponse::final_output(json!("done"))],
        vec![tool.clone()],
    );
    let id = store
        .accept(&original, "c", "key", json!(1), None)
        .await
        .unwrap();
    let mut state = store.claim(id).await.unwrap();
    state.phase = Phase::Tools;
    state.operations = ["wait", "effect"]
        .into_iter()
        .map(|name| {
            Operation::new(
                ToolCall {
                    id: name.into(),
                    name: "effect".into(),
                    arguments: json!({}),
                },
                tool.spec().clone(),
                name,
                if name == "wait" {
                    OperationStatus::Waiting(WaitRequest::Input { prompt: "?".into() })
                } else {
                    OperationStatus::Dispatched
                },
            )
        })
        .collect();
    state.pending_response = Some(ModelResponse::tools(
        state.operations.iter().map(|op| op.call.clone()).collect(),
    ));
    store.commit(&mut state).await.unwrap();
    assert!(store.due(now_ms(), 10).await.unwrap().is_empty());
    assert_eq!(store.recovery_page(None, 1).await.unwrap(), vec![id]);
    assert!(store.recovery_page(Some(id), 1).await.unwrap().is_empty());
    let position = CheckpointPosition::from(&state);
    assert!(
        store
            .record_error(id, position, "binding unavailable".into())
            .await
            .unwrap()
    );
    assert_eq!(store.position(id).await.unwrap(), position);
    assert!(store.recovery_page(None, 1).await.unwrap().is_empty());
    assert_eq!(
        store.last_error(id).await.unwrap().as_deref(),
        Some("binding unavailable")
    );

    let store = fixture.reopen(store).await;
    store.retry(id).await.unwrap();
    assert_eq!(store.due(now_ms(), 10).await.unwrap(), vec![id]);
    assert!(
        !store
            .record_error(id, position, "late failure".into())
            .await
            .unwrap()
    );
    let mut host = start(&store, original);
    wait_for(&host, id, |s| {
        matches!(s.operations[1].status, OperationStatus::Unknown { .. })
    })
    .await;
    assert_eq!(store.last_error(id).await.unwrap(), None);
    assert!(store.due(now_ms(), 10).await.unwrap().is_empty());
    let before = store.position(id).await.unwrap();
    host.resolve(id, "wait", Resolution::Input(json!("answer")))
        .await
        .unwrap();
    assert!(
        !store
            .record_error(id, before, "late wait failure".into())
            .await
            .unwrap()
    );
    assert!(matches!(
        host.resolve(id, "wait", Resolution::Input(json!("duplicate")))
            .await,
        Err(Error::NotWaiting)
    ));
    host.resolve(
        id,
        "effect",
        Resolution::Reconcile(ToolResult {
            call_id: "effect".into(),
            value: json!("confirmed"),
            is_error: false,
        }),
    )
    .await
    .unwrap();
    assert_eq!(
        wait_for(&host, id, RunState::is_terminal).await.phase,
        Phase::Settled(Settlement::Completed(json!("done")))
    );
    assert_eq!(store.operations(id).await.unwrap().len(), 2);
    host.shutdown().await.unwrap();
    fixture.close(store).await;
}

async fn history_is_ordered_and_context_is_bounded(fixture: &impl Fixture) {
    let store = fixture.open().await;
    let agent = agent(vec![], vec![]);
    let mut expected = Vec::new();
    for n in 0..40 {
        let id = store
            .accept(&agent, "history", &n.to_string(), json!(n), None)
            .await
            .unwrap();
        let mut state = store.claim(id).await.unwrap();
        state.phase = Phase::Settled(Settlement::Completed(json!(n)));
        state.history.push(HistoryEntry::Exchange {
            response: ModelResponse::final_output(json!(n)),
            results: vec![],
        });
        store.commit(&mut state).await.unwrap();
        assert_eq!(store.status(id).await.unwrap().history, state.history);
        expected.extend(state.history);
    }
    assert_eq!(store.history("history").await.unwrap(), expected);
    let id = store
        .accept(&agent, "history", "full-context", json!("next"), None)
        .await
        .unwrap();
    let mut state = store.claim(id).await.unwrap();
    assert_eq!(state.prior_context, expected);
    state.phase = Phase::Settled(Settlement::Cancelled);
    store.commit(&mut state).await.unwrap();
    let bounded = Agent::builder("bounded", Arc::new(ScriptedModel::new(vec![])))
        .instructions("test")
        .limits(Limits {
            max_context_bytes: 512,
            ..Limits::default()
        })
        .build()
        .unwrap();
    let id = store
        .accept(
            &bounded,
            "history",
            "bounded-context",
            json!("newest"),
            None,
        )
        .await
        .unwrap();
    let state = store.status(id).await.unwrap();
    assert!(state.prior_context.len() < expected.len());
    assert_eq!(
        state.prior_context.last(),
        Some(&HistoryEntry::Input(json!("next")))
    );
    assert!(serde_json::to_vec(&state.prior_context).unwrap().len() <= 512);
    let store = fixture.reopen(store).await;
    let reopened = store.status(id).await.unwrap();
    assert_eq!(
        serde_json::to_value(&reopened).unwrap(),
        serde_json::to_value(&state).unwrap()
    );
    fixture.close(store).await;
}

async fn due_order_waits_and_expiry(fixture: &impl Fixture) {
    let store = fixture.open().await;
    let agent = agent(vec![], vec![]);
    let now = now_ms();
    let (expiry, later) = (now + 30_000, now + 60_000);
    let waiting = async |conversation: &str, expires: Option<u64>, status: OperationStatus| {
        let id = store
            .accept(&agent, conversation, "k", json!(1), expires)
            .await
            .unwrap();
        let mut state = store.claim(id).await.unwrap();
        state.phase = Phase::Tools;
        state.operations = vec![Operation::new(
            ToolCall {
                id: "wait".into(),
                name: "wait".into(),
                arguments: json!({}),
            },
            spec("wait", RetryPolicy::ReadOnly),
            "wait",
            status,
        )];
        store.commit(&mut state).await.unwrap();
        (id, state.wake_at())
    };
    let ready = store
        .accept(&agent, "ready", "k", json!(0), None)
        .await
        .unwrap();
    let (timer, wake) = waiting(
        "timer",
        None,
        OperationStatus::Waiting(WaitRequest::Until { unix_ms: later }),
    )
    .await;
    assert_eq!(wake, Some(later));
    let (expiring, wake) = waiting("expiring", Some(expiry), OperationStatus::Approval).await;
    assert_eq!(wake, Some(expiry));
    let (_, wake) = waiting("blocked", None, OperationStatus::Approval).await;
    assert_eq!(wake, None);
    assert_eq!(store.due(now, 10).await.unwrap(), vec![ready]);
    assert_eq!(store.due(expiry, 10).await.unwrap(), vec![ready, expiring]);
    let all = vec![ready, expiring, timer];
    assert_eq!(store.due(u64::MAX, 10).await.unwrap(), all);
    assert_eq!(store.due(later, 2).await.unwrap(), all[..2]);
    fixture.close(store).await;
}

async fn several_agents_share_one_store(fixture: &impl Fixture) {
    let store = fixture.open().await;
    let named = |id: &str| {
        Agent::builder(
            id,
            Arc::new(ScriptedModel::new(vec![ModelResponse::final_output(
                json!(id),
            )])),
        )
        .instructions("Answer with your name")
        .build()
        .unwrap()
    };
    assert!(matches!(
        Host::start_agents(store.clone(), [named("alpha"), named("alpha")], options()),
        Err(Error::Config(_))
    ));
    assert!(matches!(
        Host::start_agents(store.clone(), [], options()),
        Err(Error::Config(_))
    ));
    let absent = store
        .accept(&named("gamma"), "gamma", "k", json!(0), None)
        .await
        .unwrap();
    let mut host =
        Host::start_agents(store.clone(), [named("alpha"), named("beta")], options()).unwrap();
    assert!(matches!(
        host.submit("c", "k", json!(1), None).await,
        Err(Error::Input(_))
    ));
    assert!(matches!(
        host.submit_to("gamma", "c", "k", json!(1), None).await,
        Err(Error::Input(_))
    ));
    let alpha = host
        .submit_to("alpha", "alpha", "k", json!(1), None)
        .await
        .unwrap();
    let beta = host
        .submit_to("beta", "beta", "k", json!(2), None)
        .await
        .unwrap();
    for (id, name) in [(alpha, "alpha"), (beta, "beta")] {
        let done = wait_for(&host, id, RunState::is_terminal).await;
        assert_eq!(done.agent_id, name);
        assert_eq!(
            done.phase,
            Phase::Settled(Settlement::Completed(json!(name)))
        );
    }
    tokio::time::timeout(Duration::from_secs(10), async {
        while store.last_error(absent).await.unwrap().is_none() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let error = store.last_error(absent).await.unwrap().unwrap();
    assert!(error.contains("gamma is not registered"), "{error}");
    assert!(!store.status(absent).await.unwrap().is_terminal());
    assert!(store.due(now_ms(), 10).await.unwrap().is_empty());
    host.shutdown().await.unwrap();
    let mut host = Host::start_agents(store.clone(), [named("gamma")], options()).unwrap();
    host.retry(absent).await.unwrap();
    assert_eq!(
        wait_for(&host, absent, RunState::is_terminal).await.phase,
        Phase::Settled(Settlement::Completed(json!("gamma")))
    );
    host.shutdown().await.unwrap();
    fixture.close(store).await;
}

struct BlockingTool {
    spec: ToolSpec,
    started: Notify,
    cleaned: AtomicBool,
}
#[async_trait]
impl Tool for BlockingTool {
    fn spec(&self) -> &ToolSpec {
        &self.spec
    }
    fn validate(&self, _: &Value) -> Result<()> {
        Ok(())
    }
    async fn execute(&self, _: Value, _: ToolContext) -> Result<ToolOutput<Value>> {
        self.started.notify_one();
        std::future::pending().await
    }
    async fn cleanup(&self, _: ToolContext) -> Result<()> {
        tokio::time::sleep(Duration::from_millis(5)).await;
        self.cleaned.store(true, Ordering::SeqCst);
        Ok(())
    }
}
async fn shutdown_and_recovery(fixture: &impl Fixture) {
    let store = fixture.open().await;
    let tool = Arc::new(BlockingTool {
        spec: spec("block", RetryPolicy::Never),
        started: Notify::new(),
        cleaned: AtomicBool::new(false),
    });
    let agent = agent(
        vec![ModelResponse::tools(vec![ToolCall {
            id: "op".into(),
            name: "block".into(),
            arguments: json!({}),
        }])],
        vec![tool.clone()],
    );
    let mut host = start(&store, agent.clone());
    let id = host.submit("c", "key", json!(1), None).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), tool.started.notified())
        .await
        .unwrap();
    host.shutdown().await.unwrap();
    assert!(tool.cleaned.load(Ordering::SeqCst));
    assert!(!store.status(id).await.unwrap().cancel_requested);
    let store = fixture.reopen(store).await;
    let mut host = start(&store, agent);
    wait_for(&host, id, |s| {
        matches!(s.operations[0].status, OperationStatus::Unknown { .. })
    })
    .await;
    host.cancel(id).await.unwrap();
    assert_eq!(
        wait_for(&host, id, RunState::is_terminal).await.phase,
        Phase::Settled(Settlement::Cancelled)
    );
    // The conversation moves on while the settled run's uncertainty is still open.
    host.submit("c", "next", json!(2), None).await.unwrap();
    host.resolve(
        id,
        "op",
        Resolution::Reconcile(ToolResult {
            call_id: "op".into(),
            value: json!("effect absent"),
            is_error: true,
        }),
    )
    .await
    .unwrap();
    assert!(matches!(
        host.submit("c", "third", json!(3), None).await,
        Err(Error::ConversationBusy)
    ));
    assert!(matches!(
        store.operations(id).await.unwrap()[0].status,
        OperationStatus::Complete(_)
    ));
    assert!(matches!(
        store.status(id).await.unwrap().operations[0].status,
        OperationStatus::Complete(_)
    ));
    host.shutdown().await.unwrap();
    fixture.close(store).await;
}

macro_rules! conformance_tests {
    ($make:expr) => {
        #[tokio::test]
        async fn acceptance_and_fencing() {
            admission_and_fencing(&$make).await;
        }
        #[tokio::test]
        async fn safe_replay_retains_keys_and_budgets() {
            safe_replay_and_budgets(&$make).await;
        }
        #[tokio::test]
        async fn startup_discovers_hidden_dispatch_and_explicit_repair() {
            hidden_dispatch_and_repair(&$make).await;
        }
        #[tokio::test]
        async fn shutdown_joins_cleanup_and_preserves_uncertainty() {
            shutdown_and_recovery(&$make).await;
        }
        #[tokio::test]
        async fn retained_history_and_bounded_context() {
            history_is_ordered_and_context_is_bounded(&$make).await;
        }
        #[tokio::test]
        async fn due_order_follows_waits_and_expiry() {
            due_order_waits_and_expiry(&$make).await;
        }
        #[tokio::test]
        async fn one_host_routes_runs_to_several_agents() {
            several_agents_share_one_store(&$make).await;
        }
    };
}

mod memory_contract {
    use super::*;
    conformance_tests!(Memory::default());
}

#[cfg(feature = "sqlite")]
mod sqlite_contract {
    use super::*;
    fn fixture() -> Sqlite {
        Sqlite {
            directory: tempfile::tempdir().unwrap(),
            open: Mutex::new(None),
        }
    }
    conformance_tests!(fixture());
}

/// The current UTC Unix time in milliseconds, the unit of run expiry and timer waits.
fn now_ms() -> u64 {
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH);
    u64::try_from(now.expect("clock after 1970").as_millis()).expect("milliseconds fit u64")
}
