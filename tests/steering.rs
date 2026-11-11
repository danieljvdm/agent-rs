//! The steering contract, asserted identically against every `SteeringStore`, including runs
//! that delegate to attached subagents. A new adapter adds a `Fixture` and one `steering_tests!`
//! invocation.
use async_trait::async_trait;
use futures::stream;
use serde_json::{Value, json};
#[cfg(feature = "sqlite")]
use std::sync::Mutex;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use yielded_agent::*;

/// Opens a store and simulates a host restart over the same durable contents.
#[async_trait]
trait Fixture: Send + Sync {
    async fn open(&self) -> Arc<dyn SteeringStore>;
    async fn close(&self, store: Arc<dyn SteeringStore>);
    async fn reopen(&self, store: Arc<dyn SteeringStore>) -> Arc<dyn SteeringStore> {
        self.close(store).await;
        self.open().await
    }
}
/// A restart keeps the process, so the same store is handed back.
#[derive(Default)]
struct Memory(Arc<MemoryStore>);
#[async_trait]
impl Fixture for Memory {
    async fn open(&self) -> Arc<dyn SteeringStore> {
        self.0.clone()
    }
    async fn close(&self, _: Arc<dyn SteeringStore>) {}
}
#[cfg(feature = "sqlite")]
struct Sqlite {
    directory: tempfile::TempDir,
    open: Mutex<Option<Arc<sqlite::SqliteStore>>>,
}
#[cfg(feature = "sqlite")]
#[async_trait]
impl Fixture for Sqlite {
    async fn open(&self) -> Arc<dyn SteeringStore> {
        let store = Arc::new(
            sqlite::SqliteStore::open(self.directory.path().join("steering.sqlite"))
                .await
                .unwrap(),
        );
        *self.open.lock().unwrap() = Some(store.clone());
        store
    }
    async fn close(&self, store: Arc<dyn SteeringStore>) {
        drop(store);
        let open = self.open.lock().unwrap().take().unwrap();
        open.close().await.unwrap();
    }
}

/// One model call in progress: the test sees its request and decides its response.
struct Attempt {
    request: ModelRequest,
    cancel: CancellationToken,
    respond: oneshot::Sender<Result<ModelResponse>>,
}
struct ControlledModel(mpsc::UnboundedSender<Attempt>);
#[async_trait]
impl Model for ControlledModel {
    async fn stream(
        &self,
        request: ModelRequest,
        cancel: CancellationToken,
    ) -> Result<ModelStream> {
        let (respond, response) = oneshot::channel();
        self.0
            .send(Attempt {
                request,
                cancel,
                respond,
            })
            .unwrap();
        let response = response.await.map_err(|_| Error::Interrupted)??;
        Ok(Box::pin(stream::once(async {
            Ok(ModelEvent::Response(response))
        })))
    }
}
fn model() -> (Arc<ControlledModel>, mpsc::UnboundedReceiver<Attempt>) {
    let (tx, rx) = mpsc::unbounded_channel();
    (Arc::new(ControlledModel(tx)), rx)
}
fn agent(model: Arc<dyn Model>, tools: Vec<Arc<dyn Tool>>, limits: Limits) -> Agent {
    Agent::builder("steering-test", model)
        .instructions("Follow the user's instructions.")
        .tools(tools)
        .limits(limits)
        .build()
        .unwrap()
}
fn options() -> HostOptions {
    HostOptions::default()
        .with_max_concurrent_runs(1)
        .with_poll_interval(Duration::from_millis(2))
}
fn start(store: &Arc<dyn SteeringStore>, agent: Agent) -> Host<dyn SteeringStore> {
    Host::start(store.clone(), agent, options()).unwrap()
}
async fn next(rx: &mut mpsc::UnboundedReceiver<Attempt>) -> Attempt {
    tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .unwrap()
        .unwrap()
}
async fn wait_for(
    host: &Host<dyn SteeringStore>,
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
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("run did not reach expected boundary")
}
fn instructions(history: &[HistoryEntry]) -> Vec<&str> {
    history
        .iter()
        .filter_map(|entry| match entry {
            HistoryEntry::Steering { instruction, .. } => Some(instruction.as_str()),
            _ => None,
        })
        .collect()
}
fn call(id: &str, name: &str) -> ToolCall {
    ToolCall {
        id: id.into(),
        name: name.into(),
        arguments: json!({}),
    }
}
fn spec(name: &str, approval: bool, retry: RetryPolicy) -> ToolSpec {
    ToolSpec::new(name, "1")
        .with_description(name)
        .with_retry(retry)
        .with_execution(Execution::Sequential)
        .with_requires_approval(approval)
}
fn complete(attempt: Attempt, output: Value) {
    attempt
        .respond
        .send(Ok(ModelResponse::final_output(output)))
        .unwrap();
}
fn exchange(output: Value) -> HistoryEntry {
    HistoryEntry::Exchange {
        response: ModelResponse::final_output(output),
        results: vec![],
    }
}
fn now_ms() -> u64 {
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH);
    u64::try_from(now.unwrap().as_millis()).unwrap()
}

async fn commits_change_only_what_checkpoint_allows(fixture: &impl Fixture) {
    let store = fixture.open().await;
    let agent = agent(
        Arc::new(ScriptedModel::new(vec![])),
        vec![],
        Limits::default(),
    );
    let id = store
        .accept(&agent, "c", "run", json!("original"), None)
        .await
        .unwrap();
    let mut state = store.claim(id).await.unwrap();
    let a = store.steer(id, "a", "first".into()).await.unwrap();
    // Neither the generation nor the revision moved: the owner is not fenced.
    assert_eq!(
        store.position(id).await.unwrap(),
        CheckpointPosition::from(&state)
    );
    // A commit outside the two boundaries leaves the queue alone.
    state.usage.model_calls = 1;
    store.commit(&mut state).await.unwrap();
    assert_eq!(state.history.len(), 1);
    assert_eq!(
        store.steering(id).await.unwrap()[0].status,
        SteeringStatus::Queued
    );
    // Queued steering is not history until it is applied.
    assert_eq!(store.history("c").await.unwrap().len(), 1);

    // Dispatch: the caller's state gains every queued entry, durably and once.
    state.phase = Phase::ModelInFlight;
    store.commit(&mut state).await.unwrap();
    let applied = HistoryEntry::Steering {
        id: a.id,
        instruction: "first".into(),
    };
    assert_eq!(state.history[1..], [applied]);
    assert_eq!(store.status(id).await.unwrap().history, state.history);
    assert_eq!(store.history("c").await.unwrap(), state.history);
    assert_eq!(
        store.steering(id).await.unwrap()[0].status,
        SteeringStatus::Applied {
            revision: state.revision
        }
    );
    // A second dispatch commit with nothing queued appends nothing.
    store.commit(&mut state).await.unwrap();
    assert_eq!(state.history.len(), 2);

    // Completion with steering queued commits `Ready` instead, keeping the exchange.
    state.phase = Phase::Ready;
    let b = store.steer(id, "b", "second".into()).await.unwrap();
    assert_eq!(b.sequence, a.sequence + 1);
    state.history.push(exchange(json!("done")));
    state.phase = Phase::Settled(Settlement::Completed(json!("done")));
    store.commit(&mut state).await.unwrap();
    assert_eq!(state.phase, Phase::Ready);
    let stored = store.status(id).await.unwrap();
    assert_eq!(stored.phase, Phase::Ready);
    assert_eq!(stored.revision, state.revision);
    assert_eq!(stored.history, state.history);
    assert_eq!(store.due(now_ms(), 10).await.unwrap(), vec![id]);
    assert_eq!(
        store.steering(id).await.unwrap()[1].status,
        SteeringStatus::Queued
    );
    // Any other settlement is final and leaves queued steering unapplied.
    state.phase = Phase::Settled(Settlement::Failed("gave up".into()));
    store.commit(&mut state).await.unwrap();
    assert!(state.is_terminal());
    assert!(store.status(id).await.unwrap().is_terminal());
    assert_eq!(
        store.steering(id).await.unwrap()[1].status,
        SteeringStatus::NotApplied
    );
    assert!(matches!(
        store.steer(id, "c", "late".into()).await,
        Err(Error::RunNotActive)
    ));
    assert_eq!(store.steer(id, "b", "second".into()).await.unwrap(), b);
    assert_eq!(store.history("c").await.unwrap().len(), 3);
    fixture.close(store).await;
}

async fn acceptance_is_ordered_idempotent_and_durable(fixture: &impl Fixture) {
    let store = fixture.open().await;
    let (model, mut attempts) = model();
    let agent = agent(model, vec![], Limits::default());
    let id = store
        .accept(&agent, "c", "run", json!("original"), None)
        .await
        .unwrap();
    let owner = store.claim(id).await.unwrap();
    let (first, second, duplicate) = tokio::join!(
        store.steer(id, "a", "Use metric units.".into()),
        store.steer(id, "b", "Keep it concise.".into()),
        store.steer(id, "a", "Use metric units.".into()),
    );
    let first = first.unwrap();
    let second = second.unwrap();
    assert_eq!(first, duplicate.unwrap());
    assert_ne!(first.id, second.id);
    assert_eq!(first.run_id, id);
    assert!(matches!(
        store.steer(id, "a", "Different payload".into()).await,
        Err(Error::IdempotencyConflict)
    ));
    for (key, instruction) in [("", "text"), ("k", " \n "), (&"k".repeat(1025), "text")] {
        assert!(matches!(
            store.steer(id, key, instruction.into()).await,
            Err(Error::Input(_))
        ));
    }
    assert!(matches!(
        store.steer(RunId::new_v4(), "a", "text".into()).await,
        Err(Error::NotFound)
    ));
    assert!(matches!(
        store.steering(RunId::new_v4()).await,
        Err(Error::NotFound)
    ));
    assert_eq!(
        store.position(id).await.unwrap(),
        CheckpointPosition::from(&owner)
    );
    let accepted = store.steering(id).await.unwrap();
    assert_eq!(accepted.len(), 2);
    assert_eq!(
        accepted
            .iter()
            .map(|input| input.receipt.sequence)
            .collect::<Vec<_>>(),
        [0, 1]
    );
    assert!(
        accepted
            .iter()
            .all(|input| input.status == SteeringStatus::Queued)
    );
    let expected: Vec<_> = accepted
        .iter()
        .map(|input| input.instruction.clone())
        .collect();
    assert_eq!(store.status(id).await.unwrap().history.len(), 1);

    let store = fixture.reopen(store).await;
    assert_eq!(store.steering(id).await.unwrap(), accepted);
    let mut host = start(&store, agent);
    let attempt = next(&mut attempts).await;
    assert_eq!(attempt.request.turn, 1);
    assert_eq!(instructions(&attempt.request.history), expected);
    complete(attempt, json!("done"));
    wait_for(&host, id, RunState::is_terminal).await;
    assert_eq!(
        store.history("c").await.unwrap().len(),
        4,
        "input, two instructions and the exchange, each once"
    );
    assert!(
        host.steering(id)
            .await
            .unwrap()
            .iter()
            .all(|input| matches!(input.status, SteeringStatus::Applied { .. }))
    );
    assert_eq!(
        host.steer(id, "a", "Use metric units.").await.unwrap(),
        first
    );
    assert!(matches!(
        host.steer(id, "late", "Too late").await,
        Err(Error::RunNotActive)
    ));
    host.shutdown().await.unwrap();
    fixture.close(store).await;
}

async fn model_work_retains_its_response_and_defers_settlement(fixture: &impl Fixture) {
    let store = fixture.open().await;
    let (model, mut attempts) = model();
    let agent = agent(model, vec![], Limits::default());
    let (events, mut observed) = mpsc::channel(64);
    let mut host = Host::start(store.clone(), agent, options().with_events(events)).unwrap();
    let id = host
        .submit("c", "run", json!("original"), None)
        .await
        .unwrap();
    let first = next(&mut attempts).await;
    let before = host.status(id).await.unwrap();
    let receipt = host
        .steer(id, "a", "Change the destination to Oslo.")
        .await
        .unwrap();
    assert!(!first.cancel.is_cancelled());
    assert!(instructions(&first.request.history).is_empty());
    assert_eq!(
        CheckpointPosition::from(&host.status(id).await.unwrap()),
        CheckpointPosition::from(&before)
    );
    complete(first, json!("original answer"));

    let second = next(&mut attempts).await;
    assert_eq!(second.request.turn, 2);
    let steering = HistoryEntry::Steering {
        id: receipt.id,
        instruction: "Change the destination to Oslo.".into(),
    };
    assert_eq!(
        second.request.history,
        vec![
            HistoryEntry::Input(json!("original")),
            exchange(json!("original answer")),
            steering.clone(),
        ]
    );
    assert_eq!(host.status(id).await.unwrap().generation, before.generation);
    while let Ok(event) = observed.try_recv() {
        assert!(
            !matches!(event.kind, ProgressKind::Settled),
            "a reopened completion is not a settlement"
        );
    }
    complete(second, json!("Oslo"));
    let done = wait_for(&host, id, RunState::is_terminal).await;
    assert_eq!(
        done.phase,
        Phase::Settled(Settlement::Completed(json!("Oslo")))
    );
    assert_eq!(done.usage.model_calls, 2);
    // Conversation history is the run's history, in the order the model saw it.
    assert_eq!(store.history("c").await.unwrap(), done.history);
    assert_eq!(done.history[2], steering);
    let successor = host
        .submit("c", "next", json!("next run"), None)
        .await
        .unwrap();
    assert_ne!(id, successor);
    assert!(matches!(
        host.steer(id, "after", "late").await,
        Err(Error::RunNotActive)
    ));
    assert!(host.steering(successor).await.unwrap().is_empty());
    // The applied instruction is ordinary prior context for the next run, never pinned there.
    let next_attempt = next(&mut attempts).await;
    assert!(next_attempt.request.history.contains(&steering));
    complete(next_attempt, json!("next done"));
    wait_for(&host, successor, RunState::is_terminal).await;
    host.shutdown().await.unwrap();
    fixture.close(store).await;
}

async fn tool_work_keeps_its_effects_and_reconciliation(fixture: &impl Fixture, restart: bool) {
    let store = fixture.open().await;
    let (model, mut attempts) = model();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let (started, mut effects) = mpsc::unbounded_channel();
    let tool = typed_tool::<Value, Value, _, _>(
        spec("write", false, RetryPolicy::Never),
        move |_, context| {
            count.fetch_add(1, Ordering::SeqCst);
            let (finish, outcome) = oneshot::channel::<Value>();
            started.send((context, finish)).unwrap();
            async move {
                Ok(ToolOutput::Complete(
                    outcome.await.map_err(|_| Error::Interrupted)?,
                ))
            }
        },
    );
    let agent = agent(model, vec![tool], Limits::default());
    let mut host = start(&store, agent.clone());
    let id = host
        .submit("c", "run", json!("original"), None)
        .await
        .unwrap();
    next(&mut attempts)
        .await
        .respond
        .send(Ok(ModelResponse::tools(vec![call("effect", "write")])))
        .unwrap();
    let (context, finish) = tokio::time::timeout(Duration::from_secs(10), effects.recv())
        .await
        .unwrap()
        .unwrap();
    let before = host.status(id).await.unwrap();
    assert!(matches!(
        before.operations[0].status,
        OperationStatus::Dispatched
    ));
    host.steer(id, "a", "Keep the existing receipt.")
        .await
        .unwrap();
    assert!(!context.cancel.is_cancelled());
    assert_eq!(
        CheckpointPosition::from(&host.status(id).await.unwrap()),
        CheckpointPosition::from(&before)
    );
    let store = if restart {
        host.shutdown().await.unwrap();
        drop(finish);
        let store = fixture.reopen(store).await;
        host = start(&store, agent);
        let unknown = wait_for(&host, id, |state| {
            matches!(state.operations[0].status, OperationStatus::Unknown { .. })
        })
        .await;
        assert_eq!(
            unknown.operations[0].idempotency_key,
            context.idempotency_key
        );
        assert!(store.due(now_ms(), 10).await.unwrap().is_empty());
        assert_eq!(
            host.steering(id).await.unwrap()[0].status,
            SteeringStatus::Queued
        );
        let reconciled = ToolResult {
            call_id: "effect".into(),
            value: json!("receipt-1"),
            is_error: false,
        };
        host.resolve(id, "effect", Resolution::Reconcile(reconciled))
            .await
            .unwrap();
        store
    } else {
        finish.send(json!("receipt-1")).unwrap();
        store
    };
    let next = next(&mut attempts).await;
    assert_eq!(
        instructions(&next.request.history),
        ["Keep the existing receipt."]
    );
    assert!(
        matches!(&next.request.history[1], HistoryEntry::Exchange { results, .. } if results[0].value == json!("receipt-1"))
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    complete(next, json!("done"));
    let done = wait_for(&host, id, RunState::is_terminal).await;
    assert_eq!(done.usage.tool_calls, 1);
    assert!(matches!(
        store.operations(id).await.unwrap()[0].status,
        OperationStatus::Complete(_)
    ));
    host.shutdown().await.unwrap();
    fixture.close(store).await;
}

async fn waits_keep_steering_queued_without_resolving_them(fixture: &impl Fixture) {
    let store = fixture.open().await;
    let (model, mut attempts) = model();
    let tool =
        typed_tool::<Value, Value, _, _>(spec("ask", true, RetryPolicy::ReadOnly), |_, _| async {
            Ok(ToolOutput::Wait(WaitRequest::Input {
                prompt: "How many minutes?".into(),
            }))
        });
    let agent = agent(model, vec![tool], Limits::default());
    let mut host = start(&store, agent.clone());
    let id = host
        .submit("c", "run", json!("original"), None)
        .await
        .unwrap();
    next(&mut attempts)
        .await
        .respond
        .send(Ok(ModelResponse::tools(vec![call("question", "ask")])))
        .unwrap();
    wait_for(&host, id, |state| {
        state
            .operations
            .first()
            .is_some_and(|op| matches!(op.status, OperationStatus::Approval))
    })
    .await;
    host.steer(id, "a", "Use local time.").await.unwrap();
    assert!(store.due(now_ms(), 10).await.unwrap().is_empty());
    host.shutdown().await.unwrap();
    let approval = store.status(id).await.unwrap();

    let store = fixture.reopen(store).await;
    let mut host = start(&store, agent.clone());
    assert_eq!(host.status(id).await.unwrap().usage, approval.usage);
    assert!(matches!(
        host.resolve(id, "question", Resolution::Input(json!(30)))
            .await,
        Err(Error::NotWaiting)
    ));
    host.resolve(id, "question", Resolution::Approve(true))
        .await
        .unwrap();
    wait_for(&host, id, |state| {
        state.operations.first().is_some_and(|op| {
            matches!(
                op.status,
                OperationStatus::Waiting(WaitRequest::Input { .. })
            )
        })
    })
    .await;
    host.steer(id, "b", "Use a short title.").await.unwrap();
    assert!(store.due(now_ms(), 10).await.unwrap().is_empty());
    assert!(
        host.steering(id)
            .await
            .unwrap()
            .iter()
            .all(|input| input.status == SteeringStatus::Queued)
    );
    host.shutdown().await.unwrap();
    // Waiting is persisted before the worker refunds its active-time reservation. Compare
    // restart usage only after the shutdown barrier joins that worker.
    let input = store.status(id).await.unwrap();

    let store = fixture.reopen(store).await;
    let mut host = start(&store, agent);
    assert_eq!(host.status(id).await.unwrap().usage, input.usage);
    host.resolve(id, "question", Resolution::Input(json!(30)))
        .await
        .unwrap();
    let next = next(&mut attempts).await;
    assert_eq!(
        instructions(&next.request.history),
        ["Use local time.", "Use a short title."]
    );
    assert!(
        matches!(&next.request.history[1], HistoryEntry::Exchange { results, .. } if results[0].value == json!(30))
    );
    complete(next, json!("done"));
    wait_for(&host, id, RunState::is_terminal).await;
    host.shutdown().await.unwrap();
    fixture.close(store).await;
}

/// Commits through the store, then loses the acknowledgement of the first dispatch commit.
struct LoseDispatchAck(Arc<dyn SteeringStore>, AtomicBool);
#[async_trait]
impl Checkpoint for LoseDispatchAck {
    async fn commit(&self, state: &mut RunState) -> Result<()> {
        self.0.commit(state).await?;
        if matches!(state.phase, Phase::ModelInFlight) && !self.1.swap(true, Ordering::SeqCst) {
            return Err(Error::storage("lost acknowledgement after dispatch commit"));
        }
        Ok(())
    }
}

async fn restart_applies_steering_once_and_charges_prior_attempts(
    fixture: &impl Fixture,
    applied_before_restart: bool,
) {
    let store = fixture.open().await;
    let (model, mut attempts) = model();
    let agent = agent(model, vec![], Limits::default());
    let id = store
        .accept(&agent, "c", "run", json!("original"), None)
        .await
        .unwrap();
    let mut old = store.claim(id).await.unwrap();
    if applied_before_restart {
        store
            .steer(id, "a", "Survive this restart.".into())
            .await
            .unwrap();
        let lossy = LoseDispatchAck(store.clone(), AtomicBool::new(false));
        assert!(matches!(
            agent.drive(&mut old, &lossy, RunControl::default()).await,
            Err(Error::Storage(_))
        ));
        assert!(matches!(
            store.steering(id).await.unwrap()[0].status,
            SteeringStatus::Applied { .. }
        ));
    } else {
        old.phase = Phase::ModelInFlight;
        old.usage.model_calls = 1;
        old.usage.active_ms = 31_000;
        store.commit(&mut old).await.unwrap();
        store
            .steer(id, "a", "Survive this restart.".into())
            .await
            .unwrap();
        assert_eq!(
            store.steering(id).await.unwrap()[0].status,
            SteeringStatus::Queued
        );
    }
    let before = store.status(id).await.unwrap();
    assert_eq!(before.usage.model_calls, 1);
    assert!(attempts.try_recv().is_err());

    let store = fixture.reopen(store).await;
    let mut host = start(&store, agent);
    let attempt = next(&mut attempts).await;
    assert_eq!(attempt.request.turn, 2);
    assert_eq!(
        instructions(&attempt.request.history),
        ["Survive this restart."]
    );
    assert!(matches!(store.commit(&mut old).await, Err(Error::Stale)));
    complete(attempt, json!("recovered"));
    let done = wait_for(&host, id, RunState::is_terminal).await;
    assert_eq!(done.usage.model_calls, 2);
    assert!(done.usage.active_ms >= before.usage.active_ms);
    assert!(done.generation > before.generation);
    assert_eq!(
        instructions(&store.history("c").await.unwrap()),
        ["Survive this restart."]
    );
    host.shutdown().await.unwrap();
    fixture.close(store).await;
}

async fn cancellation_leaves_steering_unapplied_and_out_of_later_runs(fixture: &impl Fixture) {
    let store = fixture.open().await;
    let agent = agent(
        Arc::new(ScriptedModel::new(vec![])),
        vec![],
        Limits::default(),
    );
    let id = store
        .accept(&agent, "c", "run", json!("original"), None)
        .await
        .unwrap();
    let mut owner = store.claim(id).await.unwrap();
    let deadline = now_ms() + 86_400_000;
    let call = call("deadline", "sleep");
    owner.phase = Phase::Tools;
    owner.pending_response = Some(ModelResponse::tools(vec![call.clone()]));
    owner.operations.push(Operation::new(
        call,
        spec("sleep", false, RetryPolicy::ReadOnly),
        "deadline-key",
        OperationStatus::Waiting(WaitRequest::Until { unix_ms: deadline }),
    ));
    store.commit(&mut owner).await.unwrap();
    let receipt = store
        .steer(id, "a", "Do not lose this instruction.".into())
        .await
        .unwrap();
    let usage = store.status(id).await.unwrap().usage;
    // Steering never makes a waiting run due.
    assert!(store.due(deadline - 1, 10).await.unwrap().is_empty());
    assert_eq!(store.due(deadline, 10).await.unwrap(), [id]);

    let store = fixture.reopen(store).await;
    let restored = store.status(id).await.unwrap();
    assert_eq!(restored.usage, usage);
    store.request_cancel(id).await.unwrap();
    assert!(matches!(
        store.steer(id, "b", "late".into()).await,
        Err(Error::RunNotActive)
    ));
    let mut cancelled = store.claim(id).await.unwrap();
    agent
        .drive(&mut cancelled, store.as_ref(), RunControl::default())
        .await
        .unwrap();
    assert_eq!(cancelled.phase, Phase::Settled(Settlement::Cancelled));
    assert_eq!(
        store.steering(id).await.unwrap()[0].status,
        SteeringStatus::NotApplied
    );
    assert_eq!(
        store
            .steer(id, "a", "Do not lose this instruction.".into())
            .await
            .unwrap(),
        receipt
    );
    assert!(instructions(&store.history("c").await.unwrap()).is_empty());
    let next = store
        .accept(&agent, "c", "next", json!("new task"), None)
        .await
        .unwrap();
    assert!(instructions(&store.status(next).await.unwrap().prior_context).is_empty());
    fixture.close(store).await;
}

async fn steering_cannot_replenish_a_budget_or_retry_a_failure(fixture: &impl Fixture, fail: bool) {
    let store = fixture.open().await;
    let (model, mut attempts) = model();
    let agent = agent(model, vec![], Limits::default().with_max_model_calls(1));
    let mut host = start(&store, agent);
    let id = host
        .submit("c", "run", json!("original"), None)
        .await
        .unwrap();
    let attempt = next(&mut attempts).await;
    host.steer(id, "a", "One more thing.").await.unwrap();
    if fail {
        attempt
            .respond
            .send(Err(Error::provider("offline")))
            .unwrap();
    } else {
        complete(attempt, json!("before steering"));
    }
    let done = wait_for(&host, id, RunState::is_terminal).await;
    assert_eq!(done.usage.model_calls, 1);
    if fail {
        assert!(matches!(done.phase, Phase::Settled(Settlement::Failed(_))));
    } else {
        // The completion reopened, and the budget then stopped the run with its exchange kept.
        assert_eq!(
            done.phase,
            Phase::Settled(Settlement::Stopped(StopReason::ModelCalls))
        );
        assert_eq!(done.history[1], exchange(json!("before steering")));
    }
    assert_eq!(
        host.steering(id).await.unwrap()[0].status,
        SteeringStatus::NotApplied
    );
    assert!(attempts.try_recv().is_err());
    host.shutdown().await.unwrap();
    fixture.close(store).await;
}

async fn steering_and_settlement_have_one_atomic_winner(fixture: &impl Fixture) {
    let store = fixture.open().await;
    let agent = agent(
        Arc::new(ScriptedModel::new(vec![])),
        vec![],
        Limits::default(),
    );
    for order in 0..2 {
        let id = store
            .accept(&agent, &format!("c-{order}"), "run", Value::Null, None)
            .await
            .unwrap();
        let mut owner = store.claim(id).await.unwrap();
        owner.phase = Phase::Settled(Settlement::Completed(json!("done")));
        owner.history.push(exchange(json!("done")));
        let steer = store.steer(id, "a", "new instruction".into());
        let (steering, commit) = if order == 0 {
            tokio::join!(biased; steer, store.commit(&mut owner))
        } else {
            let (commit, steering) = tokio::join!(biased; store.commit(&mut owner), steer);
            (steering, commit)
        };
        commit.unwrap();
        let authoritative = store.status(id).await.unwrap();
        if order == 0 {
            let receipt = steering.unwrap();
            assert_eq!(authoritative.phase, Phase::Ready);
            assert_eq!(owner.phase, Phase::Ready);
            assert_eq!(authoritative.history.len(), 2);
            assert_eq!(store.steering(id).await.unwrap()[0].receipt, receipt);
        } else {
            assert!(matches!(steering, Err(Error::RunNotActive)));
            assert!(authoritative.is_terminal());
            assert!(store.steering(id).await.unwrap().is_empty());
        }
    }
    fixture.close(store).await;
}

async fn steering_is_bounded_and_pinned_when_old_exchanges_are_trimmed(fixture: &impl Fixture) {
    let store = fixture.open().await;
    let (model, mut attempts) = model();
    let tool = typed_tool::<Value, Value, _, _>(
        spec("read", false, RetryPolicy::ReadOnly),
        |_, _| async { Ok(ToolOutput::Complete(json!("x".repeat(220)))) },
    );
    let agent = agent(
        model,
        vec![tool],
        Limits::default().with_max_context_bytes(800),
    );
    let id = store
        .accept(&agent, "c", "run", json!("original"), None)
        .await
        .unwrap();
    store
        .steer(id, "a", "Keep using metric units.".into())
        .await
        .unwrap();
    assert!(matches!(
        store.steer(id, "oversize", "x".repeat(800)).await,
        Err(Error::Input(_))
    ));
    assert_eq!(store.steering(id).await.unwrap().len(), 1);
    let mut host = start(&store, agent);
    for turn in 0..4 {
        let attempt = next(&mut attempts).await;
        assert_eq!(
            instructions(&attempt.request.history),
            ["Keep using metric units."]
        );
        assert_eq!(
            attempt.request.history[0],
            HistoryEntry::Input(json!("original"))
        );
        assert!(serde_json::to_vec(&attempt.request.history).unwrap().len() <= 800);
        if turn == 3 {
            assert!(!attempt.request.history.iter().any(|entry| matches!(entry,
                HistoryEntry::Exchange { response, .. } if response.tools[0].id == "read-0")));
            complete(attempt, json!("done"));
        } else {
            let read = call(&format!("read-{turn}"), "read");
            attempt
                .respond
                .send(Ok(ModelResponse::tools(vec![read])))
                .unwrap();
        }
    }
    wait_for(&host, id, RunState::is_terminal).await;
    host.shutdown().await.unwrap();
    fixture.close(store).await;
}

async fn steering_count_limit_preserves_retries(fixture: &impl Fixture) {
    let store = fixture.open().await;
    let agent = agent(
        Arc::new(ScriptedModel::new(vec![])),
        vec![],
        Limits::default(),
    );
    let id = store
        .accept(&agent, "c", "run", json!("original"), None)
        .await
        .unwrap();
    for i in 0..SteeringInput::MAX_PER_RUN {
        store
            .steer(id, &format!("s-{i}"), "small".into())
            .await
            .unwrap();
    }
    let first = store.steering(id).await.unwrap()[0].receipt.clone();
    assert!(matches!(
        store.steer(id, "overflow", "small".into()).await,
        Err(Error::Input(_))
    ));
    assert_eq!(store.steer(id, "s-0", "small".into()).await.unwrap(), first);
    assert_eq!(
        store.steering(id).await.unwrap().len(),
        SteeringInput::MAX_PER_RUN
    );
    fixture.close(store).await;
}

/// A root that delegates to a child; both call `model`. The child asks a question if its model
/// calls `ask`.
fn delegating(model: Arc<ControlledModel>) -> Agent {
    let ask =
        typed_tool::<Value, Value, _, _>(spec("ask", false, RetryPolicy::ReadOnly), |_, _| async {
            Ok(ToolOutput::Wait(WaitRequest::Input {
                prompt: "Which day?".into(),
            }))
        });
    let child = Agent::builder("steering-child", model.clone())
        .instructions("Do the delegated task.")
        .tool(ask)
        .limits(
            Limits::default()
                .with_max_model_calls(4)
                .with_max_tool_calls(2)
                .with_max_tokens(1_000)
                .with_active_budget(Duration::from_secs(60)),
        )
        .input::<String>()
        .output::<String>()
        .build()
        .unwrap();
    Agent::builder("steering-root", model)
        .instructions("Delegate, then follow the user's instructions.")
        .subagents(1, [Subagent::new("delegate", "Do one task.", child)])
        .build()
        .unwrap()
}
fn delegation() -> ToolCall {
    ToolCall {
        id: "delegation".into(),
        name: "delegate".into(),
        arguments: json!("task"),
    }
}

async fn steering_targets_the_root_while_its_children_work_or_wait(fixture: &impl Fixture) {
    let store = fixture.open().await;
    let (model, mut attempts) = model();
    let root = delegating(model);
    let mut host = start(&store, root.clone());
    let id = host
        .submit("c", "run", json!("original"), None)
        .await
        .unwrap();
    next(&mut attempts)
        .await
        .respond
        .send(Ok(ModelResponse::tools(vec![delegation()])))
        .unwrap();
    // The child's model call is in flight: its dispatch was a commit of the root, in `Tools`.
    let turn = next(&mut attempts).await;
    let child = turn.request.run_id;
    assert_ne!(child, id);
    // A child is not a run of the store, so it has no steering of its own.
    assert!(matches!(
        host.steer(child, "x", "Steer the child.").await,
        Err(Error::NotFound)
    ));
    assert!(matches!(host.steering(child).await, Err(Error::NotFound)));
    host.steer(id, "a", "Prefer trains.").await.unwrap();
    turn.respond
        .send(Ok(ModelResponse::tools(vec![call("question", "ask")])))
        .unwrap();
    wait_for(&host, id, |state| {
        state.children.first().is_some_and(|child| {
            matches!(
                child.state.operations.first().map(|op| &op.status),
                Some(OperationStatus::Waiting(WaitRequest::Input { .. }))
            )
        })
    })
    .await;
    // Blocked: the root is in `Tools`, not due, and everything stays queued.
    host.steer(id, "b", "Use local time.").await.unwrap();
    host.shutdown().await.unwrap();
    let waiting = store.status(id).await.unwrap();
    assert_eq!(waiting.phase, Phase::Tools);
    assert!(store.due(now_ms(), 10).await.unwrap().is_empty());
    assert!(instructions(&waiting.history).is_empty());
    assert!(instructions(&waiting.children[0].state.history).is_empty());
    assert!((store.steering(id).await.unwrap().iter()).all(|s| s.status == SteeringStatus::Queued));

    let store = fixture.reopen(store).await;
    let mut host = start(&store, root);
    host.resolve_child(id, child, "question", Resolution::Input(json!("Monday")))
        .await
        .unwrap();
    let turn = next(&mut attempts).await;
    assert_eq!(turn.request.run_id, child);
    assert!(instructions(&turn.request.history).is_empty());
    complete(turn, json!("child done"));
    // The root's next dispatch applies both, after the delegation's exchange.
    let turn = next(&mut attempts).await;
    assert_eq!(turn.request.run_id, id);
    assert_eq!(
        instructions(&turn.request.history),
        ["Prefer trains.", "Use local time."]
    );
    assert!(matches!(
        &turn.request.history[1],
        HistoryEntry::Exchange { results, .. } if !results[0].is_error
    ));
    complete(turn, json!("done"));
    let done = wait_for(&host, id, RunState::is_terminal).await;
    assert_eq!(
        done.phase,
        Phase::Settled(Settlement::Completed(json!("done")))
    );
    let applied: Vec<_> = (host.steering(id).await.unwrap().into_iter())
        .map(|s| s.status)
        .collect();
    assert!(matches!(applied[0], SteeringStatus::Applied { .. }));
    assert_eq!(applied[0], applied[1]);
    // No child history, in the run or in the store, ever holds an instruction.
    let retained = &done.child(child).unwrap().state;
    assert!(instructions(&retained.history).is_empty());
    let conversation = store.history(&retained.conversation_id).await.unwrap();
    assert!(instructions(&conversation).is_empty());
    assert_eq!(
        instructions(&store.history("c").await.unwrap()),
        ["Prefer trains.", "Use local time."]
    );
    host.shutdown().await.unwrap();
    fixture.close(store).await;
}

async fn a_completion_reopened_by_steering_keeps_its_settled_children(fixture: &impl Fixture) {
    let store = fixture.open().await;
    let (model, mut attempts) = model();
    let mut host = start(&store, delegating(model));
    let id = host
        .submit("c", "run", json!("original"), None)
        .await
        .unwrap();
    next(&mut attempts)
        .await
        .respond
        .send(Ok(ModelResponse::tools(vec![delegation()])))
        .unwrap();
    complete(next(&mut attempts).await, json!("child done"));
    let turn = next(&mut attempts).await;
    assert_eq!(turn.request.run_id, id);
    host.steer(id, "a", "Answer again.").await.unwrap();
    complete(turn, json!("first"));
    // The completion reopened. Every child had settled before it, and none runs again.
    let turn = next(&mut attempts).await;
    assert_eq!(turn.request.run_id, id);
    assert_eq!(instructions(&turn.request.history), ["Answer again."]);
    let reopened = host.status(id).await.unwrap();
    assert_eq!(reopened.children.len(), 1);
    let settled = reopened.children[0].state.clone();
    assert_eq!(
        settled.phase,
        Phase::Settled(Settlement::Completed(json!("child done")))
    );
    complete(turn, json!("second"));
    let done = wait_for(&host, id, RunState::is_terminal).await;
    assert_eq!(
        done.phase,
        Phase::Settled(Settlement::Completed(json!("second")))
    );
    assert_eq!(done.children.len(), 1);
    assert_eq!(done.children[0].state.revision, settled.revision);
    assert_eq!(done.child_usage, reopened.child_usage);
    assert!(attempts.try_recv().is_err());
    host.shutdown().await.unwrap();
    fixture.close(store).await;
}

macro_rules! steering_tests {
    ($make:expr) => {
        #[tokio::test]
        async fn commits_change_only_what_checkpoint_allows() {
            super::commits_change_only_what_checkpoint_allows(&$make).await;
        }
        #[tokio::test]
        async fn acceptance_is_ordered_idempotent_and_durable() {
            super::acceptance_is_ordered_idempotent_and_durable(&$make).await;
        }
        #[tokio::test]
        async fn model_work_retains_its_response_and_defers_settlement() {
            super::model_work_retains_its_response_and_defers_settlement(&$make).await;
        }
        #[tokio::test]
        async fn tool_work_keeps_its_effects() {
            super::tool_work_keeps_its_effects_and_reconciliation(&$make, false).await;
        }
        #[tokio::test]
        async fn tool_work_keeps_its_reconciliation_across_restart() {
            super::tool_work_keeps_its_effects_and_reconciliation(&$make, true).await;
        }
        #[tokio::test]
        async fn waits_keep_steering_queued_without_resolving_them() {
            super::waits_keep_steering_queued_without_resolving_them(&$make).await;
        }
        #[tokio::test]
        async fn restart_keeps_queued_steering() {
            super::restart_applies_steering_once_and_charges_prior_attempts(&$make, false).await;
        }
        #[tokio::test]
        async fn restart_keeps_applied_steering_once() {
            super::restart_applies_steering_once_and_charges_prior_attempts(&$make, true).await;
        }
        #[tokio::test]
        async fn cancellation_leaves_steering_unapplied_and_out_of_later_runs() {
            super::cancellation_leaves_steering_unapplied_and_out_of_later_runs(&$make).await;
        }
        #[tokio::test]
        async fn steering_cannot_replenish_a_budget() {
            super::steering_cannot_replenish_a_budget_or_retry_a_failure(&$make, false).await;
        }
        #[tokio::test]
        async fn steering_cannot_retry_a_failure() {
            super::steering_cannot_replenish_a_budget_or_retry_a_failure(&$make, true).await;
        }
        #[tokio::test]
        async fn steering_and_settlement_have_one_atomic_winner() {
            super::steering_and_settlement_have_one_atomic_winner(&$make).await;
        }
        #[tokio::test]
        async fn steering_is_bounded_and_pinned_when_old_exchanges_are_trimmed() {
            super::steering_is_bounded_and_pinned_when_old_exchanges_are_trimmed(&$make).await;
        }
        #[tokio::test]
        async fn steering_count_limit_preserves_retries() {
            super::steering_count_limit_preserves_retries(&$make).await;
        }
        #[tokio::test]
        async fn steering_targets_the_root_while_its_children_work_or_wait() {
            super::steering_targets_the_root_while_its_children_work_or_wait(&$make).await;
        }
        #[tokio::test]
        async fn a_completion_reopened_by_steering_keeps_its_settled_children() {
            super::a_completion_reopened_by_steering_keeps_its_settled_children(&$make).await;
        }
    };
}

mod memory_contract {
    use super::*;
    steering_tests!(Memory::default());
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
    steering_tests!(fixture());
}

// ---------------------------------------------------------------------------------------------
// Engine-side handling, independent of any store.
// ---------------------------------------------------------------------------------------------

/// Appends an entry no commit may append.
struct Overreaching;
#[async_trait]
impl Checkpoint for Overreaching {
    async fn commit(&self, state: &mut RunState) -> Result<()> {
        state.revision += 1;
        if matches!(state.phase, Phase::ModelInFlight) {
            state.history.push(HistoryEntry::Input(json!("injected")));
        }
        Ok(())
    }
}
#[tokio::test]
async fn the_engine_rejects_commits_beyond_the_checkpoint_contract() {
    let agent = agent(
        Arc::new(ScriptedModel::new(vec![ModelResponse::final_output(
            json!("done"),
        )])),
        vec![],
        Limits::default(),
    );
    let mut state = agent.new_run("c", json!("go"), &[], None).unwrap();
    let error = agent
        .drive(&mut state, &Overreaching, RunControl::default())
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Storage(_)), "{error:?}");
    assert!(!state.is_terminal());
}

/// Fills context with derived items and pins nothing: steering must still survive.
struct Crowding;
#[async_trait]
impl ContextPolicy for Crowding {
    fn binding(&self) -> Value {
        json!("crowding-v1")
    }
    async fn prepare(&self, _: ContextRequest<'_>, _: CancellationToken) -> Result<Vec<Value>> {
        Ok((0..64)
            .map(|n| json!(format!("{n:02}{}", "d".repeat(60))))
            .collect())
    }
}
#[tokio::test]
async fn steering_stays_pinned_beside_a_context_policy() {
    let store = Arc::new(MemoryStore::new());
    let (model, mut attempts) = model();
    let agent = Agent::builder("steering-test", model)
        .limits(Limits::default().with_max_context_bytes(600))
        .context_policy(Arc::new(Crowding), 600)
        .build()
        .unwrap();
    let id = store
        .accept(&agent, "c", "run", json!("original"), None)
        .await
        .unwrap();
    let receipt = store
        .steer(id, "a", "Prefer the newest data.".into())
        .await
        .unwrap();
    let mut state = store.claim(id).await.unwrap();
    let drive = agent.drive(&mut state, store.as_ref(), RunControl::default());
    let check = async {
        let attempt = next(&mut attempts).await;
        assert_eq!(
            attempt.request.history,
            [
                HistoryEntry::Input(json!("original")),
                HistoryEntry::Steering {
                    id: receipt.id,
                    instruction: "Prefer the newest data.".into()
                }
            ]
        );
        // Derived items gave way to the pinned history, which they never evict.
        assert!(!attempt.request.derived_context.is_empty());
        assert!(attempt.request.derived_context.len() < 64);
        complete(attempt, json!("done"));
    };
    let (driven, ()) = tokio::join!(drive, check);
    driven.unwrap();
    assert!(state.is_terminal());
}
