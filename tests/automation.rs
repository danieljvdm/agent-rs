//! The automation contract, asserted identically against every `AutomationStore`. Firing times
//! are explicit, so no test waits for a real interval.
use async_trait::async_trait;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use yielded_agent::*;

#[cfg(feature = "sqlite")]
#[path = "../examples/support/recurring_workflow.rs"]
mod recurring_workflow;

/// Opens a store and simulates a restart over the same durable contents.
#[async_trait]
trait Fixture: Send + Sync {
    async fn open(&self) -> Arc<dyn AutomationStore>;
    async fn close(&self, store: Arc<dyn AutomationStore>);
    /// The open store as a `SteeringStore`: an admitted run is steered like any other.
    fn steering(&self) -> Arc<dyn SteeringStore>;
    async fn reopen(&self, store: Arc<dyn AutomationStore>) -> Arc<dyn AutomationStore> {
        self.close(store).await;
        self.open().await
    }
}

/// A restart keeps the process, so the same store is handed back.
#[derive(Default)]
struct Memory(Arc<MemoryStore>);
#[async_trait]
impl Fixture for Memory {
    async fn open(&self) -> Arc<dyn AutomationStore> {
        self.0.clone()
    }
    async fn close(&self, _: Arc<dyn AutomationStore>) {}
    fn steering(&self) -> Arc<dyn SteeringStore> {
        self.0.clone()
    }
}

#[cfg(feature = "sqlite")]
struct Sqlite {
    directory: tempfile::TempDir,
    open: std::sync::Mutex<Option<Arc<sqlite::SqliteStore>>>,
}
#[cfg(feature = "sqlite")]
#[async_trait]
impl Fixture for Sqlite {
    async fn open(&self) -> Arc<dyn AutomationStore> {
        let store = Arc::new(
            sqlite::SqliteStore::open(self.directory.path().join("automation.sqlite"))
                .await
                .unwrap(),
        );
        *self.open.lock().unwrap() = Some(store.clone());
        store
    }
    async fn close(&self, store: Arc<dyn AutomationStore>) {
        drop(store);
        let open = self.open.lock().unwrap().take().unwrap();
        open.close().await.unwrap();
    }
    fn steering(&self) -> Arc<dyn SteeringStore> {
        self.open.lock().unwrap().clone().expect("store is open")
    }
}

const AGENT: &str = "interval-test";

fn agent(instructions: &str, responses: Vec<ModelResponse>, tools: Vec<Arc<dyn Tool>>) -> Agent {
    named(AGENT, instructions, responses, tools)
}
fn named(
    id: &str,
    instructions: &str,
    responses: Vec<ModelResponse>,
    tools: Vec<Arc<dyn Tool>>,
) -> Agent {
    Agent::builder(id, Arc::new(ScriptedModel::new(responses)))
        .instructions(instructions)
        .tools(tools)
        .input::<String>()
        .output::<String>()
        .build()
        .unwrap()
        .erased()
}
fn final_agent() -> Agent {
    agent(
        "Check the inbox",
        vec![ModelResponse::final_output(json!("done"))],
        vec![],
    )
}
fn schedule(conversation: &str) -> IntervalAutomation {
    IntervalAutomation::new(conversation, json!("Review my inbox"), 100, 100)
}
async fn fire(store: &Arc<dyn AutomationStore>, agent: &Agent, now: u64) -> FiringReport {
    store
        .fire_due(
            std::slice::from_ref(agent),
            now,
            AutomationOptions::default(),
        )
        .await
        .unwrap()
}
fn admitted(occurrence: &Occurrence) -> RunId {
    match occurrence.outcome {
        OccurrenceOutcome::Admitted { run_id } => run_id,
        _ => panic!("expected admission: {occurrence:?}"),
    }
}
async fn drive(store: &Arc<dyn AutomationStore>, agent: &Agent, id: RunId) -> RunState {
    let mut state = store.claim(id).await.unwrap();
    let checkpoint: &dyn Checkpoint = store.as_ref();
    agent
        .drive(&mut state, checkpoint, RunControl::default())
        .await
        .unwrap();
    store.status(id).await.unwrap()
}
fn approval_tool() -> Arc<dyn Tool> {
    typed_tool::<Value, Value, _, _>(
        ToolSpec::new("review", "1")
            .with_description("review")
            .with_retry(RetryPolicy::ReadOnly)
            .with_requires_approval(true),
        |_, _| async { Ok(ToolOutput::Complete(json!(true))) },
    )
}

async fn restarts_and_duplicate_firings(fixture: &impl Fixture) {
    let agent = final_agent();
    let store = fixture.open().await;
    let created = store
        .create_automation(&agent, "inbox", schedule("personal"))
        .await
        .unwrap();
    assert_eq!(created.agent_id, AGENT);
    assert_eq!(
        store
            .create_automation(&agent, "inbox", schedule("personal"))
            .await
            .unwrap(),
        created,
        "repeating a creation returns the current record"
    );
    assert!(matches!(
        store
            .create_automation(&agent, "inbox", schedule("other"))
            .await,
        Err(Error::IdempotencyConflict)
    ));
    let store = fixture.reopen(store).await;

    let agents = [agent.clone()];
    let options = AutomationOptions::default();
    let (a, b) = tokio::join!(
        store.fire_due(&agents, 450, options),
        store.fire_due(&agents, 450, options)
    );
    let occurrences: Vec<_> = a
        .unwrap()
        .occurrences
        .into_iter()
        .chain(b.unwrap().occurrences)
        .collect();
    assert_eq!(occurrences.len(), 1, "one identity fires once");
    assert_eq!(occurrences[0].scheduled_at_ms, 400);
    assert_eq!(occurrences[0].coalesced, 3);
    assert_eq!(occurrences[0].observed_at_ms, 450);
    let id = admitted(&occurrences[0]);
    let current = store.automation("inbox").await.unwrap();
    assert_eq!(current.next_at_ms, 500);
    assert_eq!(current.last_run_id, Some(id));
    assert_eq!(
        current.revision, created.revision,
        "firing is not management"
    );
    assert!(
        fire(&store, &agent, 100).await.occurrences.is_empty(),
        "a backward clock cannot replay old work"
    );
    // An ordinary run: due to the host, keyed by the occurrence identity.
    assert_eq!(store.status(id).await.unwrap().phase, Phase::Ready);
    assert_eq!(store.due(0, 10).await.unwrap(), vec![id]);
    assert_eq!(
        store
            .accept(
                &agent,
                "personal",
                "automation/inbox/1/400",
                json!("Review my inbox"),
                None
            )
            .await
            .unwrap(),
        id,
    );
    let store = fixture.reopen(store).await;

    assert!(fire(&store, &agent, 450).await.occurrences.is_empty());
    assert_eq!(
        store.occurrences("inbox", 0, 10).await.unwrap(),
        occurrences
    );
    assert_eq!(
        drive(&store, &agent, id).await.phase,
        Phase::Settled(Settlement::Completed(json!("done")))
    );
    let second = admitted(&fire(&store, &agent, 500).await.occurrences[0]);
    assert_ne!(id, second);
    let state = store.status(second).await.unwrap();
    assert_eq!(state.limits, *agent.limits());
    assert_eq!(
        state.prior_context.len(),
        2,
        "recurring runs share their conversation's history"
    );
    assert_eq!(state.usage.model_calls, 0);
    fixture.close(store).await;
}

async fn overlap_is_skipped_and_advances(fixture: &impl Fixture) {
    let store = fixture.open().await;
    let agent = agent(
        "Request approval",
        vec![
            ModelResponse::tools(vec![ToolCall {
                id: "approve".into(),
                name: "review".into(),
                arguments: json!({}),
            }]),
            ModelResponse::final_output(json!("done")),
        ],
        vec![approval_tool()],
    );
    // A run submitted directly makes the conversation busy for the automation too.
    let direct = store
        .accept(&agent, "personal", "direct", json!("Direct request"), None)
        .await
        .unwrap();
    store
        .create_automation(&agent, "inbox", schedule("personal"))
        .await
        .unwrap();
    let skipped = fire(&store, &agent, 100).await;
    assert_eq!(
        skipped.occurrences[0].outcome,
        OccurrenceOutcome::SkippedOverlap
    );
    assert_eq!(store.automation("inbox").await.unwrap().next_at_ms, 200);
    assert_eq!(store.automation("inbox").await.unwrap().last_run_id, None);
    store.request_cancel(direct).await.unwrap();
    drive(&store, &agent, direct).await;

    // The automation's own run, waiting for approval, holds no slot but still overlaps.
    let id = admitted(&fire(&store, &agent, 200).await.occurrences[0]);
    let waiting = drive(&store, &agent, id).await;
    assert!(matches!(
        waiting.operations[0].status,
        OperationStatus::Approval
    ));
    assert_eq!(
        fire(&store, &agent, 300).await.occurrences[0].outcome,
        OccurrenceOutcome::SkippedOverlap
    );
    // Moving to another conversation does not bypass the automation's own unsettled run.
    let current = store.automation("inbox").await.unwrap();
    let replacement = schedule("different-conversation").with_first_at_ms(400);
    store
        .change_automation(
            "inbox",
            current.revision,
            AutomationChange::Update {
                agent: agent.clone(),
                schedule: replacement,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        fire(&store, &agent, 400).await.occurrences[0].outcome,
        OccurrenceOutcome::SkippedOverlap
    );
    store.request_cancel(id).await.unwrap();
    assert_eq!(
        drive(&store, &agent, id).await.phase,
        Phase::Settled(Settlement::Cancelled)
    );
    let next = admitted(&fire(&store, &agent, 500).await.occurrences[0]);
    assert_ne!(next, id);
    assert_eq!(
        store.status(next).await.unwrap().conversation_id,
        "different-conversation"
    );
    let outcomes: Vec<_> = store
        .occurrences("inbox", 0, 10)
        .await
        .unwrap()
        .into_iter()
        .map(|occurrence| occurrence.outcome)
        .collect();
    assert_eq!(outcomes.len(), 5);
    fixture.close(store).await;
}

async fn management_keeps_accepted_work_and_tombstones(fixture: &impl Fixture) {
    let agent = final_agent();
    let store = fixture.open().await;
    store
        .create_automation(&agent, "inbox", schedule("personal"))
        .await
        .unwrap();
    let paused = store
        .change_automation("inbox", 1, AutomationChange::Pause)
        .await
        .unwrap();
    assert_eq!(paused.revision, 2);
    assert!(matches!(
        store
            .change_automation("inbox", 1, AutomationChange::Resume)
            .await,
        Err(Error::Stale)
    ));
    assert!(matches!(
        store
            .change_automation("missing", 1, AutomationChange::Pause)
            .await,
        Err(Error::NotFound)
    ));
    let store = fixture.reopen(store).await;

    assert_eq!(store.automation("inbox").await.unwrap(), paused);
    assert!(fire(&store, &agent, 450).await.occurrences.is_empty());
    let updated = store
        .change_automation(
            "inbox",
            paused.revision,
            AutomationChange::Update {
                agent: agent.clone(),
                schedule: schedule("personal"),
            },
        )
        .await
        .unwrap();
    assert_eq!(updated.state, AutomationState::Paused, "update keeps pause");
    assert_eq!(updated.schedule_version, 2);
    let enabled = store
        .change_automation("inbox", updated.revision, AutomationChange::Resume)
        .await
        .unwrap();
    // Time spent paused coalesces into one firing.
    let report = fire(&store, &agent, 450).await;
    assert_eq!(report.occurrences[0].scheduled_at_ms, 400);
    assert_eq!(report.occurrences[0].coalesced, 3);
    let id = admitted(&report.occurrences[0]);
    let deleted = store
        .change_automation("inbox", enabled.revision, AutomationChange::Delete)
        .await
        .unwrap();
    assert_eq!(deleted.state, AutomationState::Deleted);
    assert_eq!(deleted.last_run_id, Some(id));
    assert!(!store.status(id).await.unwrap().cancel_requested);
    assert!(fire(&store, &agent, 100_000).await.occurrences.is_empty());
    assert!(matches!(
        store
            .create_automation(&agent, "inbox", schedule("personal"))
            .await,
        Err(Error::IdempotencyConflict)
    ));
    assert!(matches!(
        store
            .change_automation("inbox", deleted.revision, AutomationChange::Resume)
            .await,
        Err(Error::Input(_))
    ));
    assert_eq!(
        store
            .change_automation("inbox", deleted.revision, AutomationChange::Delete)
            .await
            .unwrap(),
        deleted
    );
    assert!(drive(&store, &agent, id).await.is_terminal());
    assert_eq!(
        store.occurrences("inbox", 0, 10).await.unwrap(),
        report.occurrences
    );
    store
        .create_automation(&agent, "other", schedule("other"))
        .await
        .unwrap();
    assert_eq!(store.automations(None, 1).await.unwrap()[0].id, "inbox");
    assert_eq!(
        store.automations(Some("inbox"), 1).await.unwrap()[0].id,
        "other"
    );
    assert!(
        store
            .automations(Some("other"), 1)
            .await
            .unwrap()
            .is_empty()
    );
    fixture.close(store).await;
}

async fn updates_and_bindings(fixture: &impl Fixture) {
    let store = fixture.open().await;
    let original = final_agent();
    store
        .create_automation(&original, "inbox", schedule("personal"))
        .await
        .unwrap();
    let first = admitted(&fire(&store, &original, 100).await.occurrences[0]);
    drive(&store, &original, first).await;
    let updated = store
        .change_automation(
            "inbox",
            1,
            AutomationChange::Update {
                agent: original.clone(),
                schedule: schedule("personal"),
            },
        )
        .await
        .unwrap();
    assert_eq!(updated.schedule_version, 2);
    let second = admitted(&fire(&store, &original, 100).await.occurrences[0]);
    assert_ne!(
        first, second,
        "a deliberate update starts a new identity namespace"
    );
    drive(&store, &original, second).await;

    // Same agent ID, new definition: the recorded binding no longer matches.
    let changed = agent(
        "New instructions",
        vec![ModelResponse::final_output(json!("done"))],
        vec![],
    );
    let report = fire(&store, &changed, 200).await;
    assert_eq!(report.paused, vec!["inbox"]);
    assert!(report.occurrences.is_empty());
    let paused = store.automation("inbox").await.unwrap();
    assert_eq!(paused.state, AutomationState::Paused);
    assert!(paused.last_error.unwrap().contains("changed its binding"));
    assert_eq!(paused.next_at_ms, 200, "a pause keeps the cursor");
    // Resuming does not adopt the binding: the next firing pauses again.
    let resumed = store
        .change_automation("inbox", paused.revision, AutomationChange::Resume)
        .await
        .unwrap();
    assert_eq!(resumed.last_error, None);
    assert_eq!(fire(&store, &changed, 200).await.paused, vec!["inbox"]);
    let paused = store.automation("inbox").await.unwrap();
    let adopted = store
        .change_automation(
            "inbox",
            paused.revision,
            AutomationChange::Update {
                agent: changed.clone(),
                schedule: schedule("personal"),
            },
        )
        .await
        .unwrap();
    assert_eq!(adopted.state, AutomationState::Paused);
    assert_eq!(adopted.schedule_version, 3);
    store
        .change_automation("inbox", adopted.revision, AutomationChange::Resume)
        .await
        .unwrap();
    let third = admitted(&fire(&store, &changed, 200).await.occurrences[0]);
    assert_eq!(
        *store.status(third).await.unwrap().binding,
        *changed.binding()
    );
    let page = store.occurrences("inbox", 0, 1).await.unwrap();
    assert_eq!(admitted(&page[0]), first);
    let rest = store
        .occurrences("inbox", page[0].sequence, 10)
        .await
        .unwrap();
    assert_eq!(rest.len(), 2);
    assert_eq!(rest[0].schedule_version, 2);
    assert_eq!(rest[1].schedule_version, 3);
    fixture.close(store).await;
}

/// A scripted model behind a fixed adapter route, as `Model::binding` reports one.
struct Routed(ScriptedModel, Value);
#[async_trait]
impl Model for Routed {
    fn binding(&self) -> Value {
        self.1.clone()
    }
    async fn stream(
        &self,
        request: ModelRequest,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<ModelStream> {
        self.0.stream(request, cancel).await
    }
}
fn routed(endpoint: &str) -> Agent {
    let model = Routed(
        ScriptedModel::new(vec![ModelResponse::final_output(json!("done"))]),
        json!({"api": "chat", "endpoint": endpoint}),
    );
    Agent::builder(AGENT, Arc::new(model))
        .instructions("Check the inbox")
        .input::<String>()
        .output::<String>()
        .build()
        .unwrap()
        .erased()
}

async fn model_routes_are_part_of_the_binding(fixture: &impl Fixture) {
    let store = fixture.open().await;
    let original = routed("https://a.example/v1");
    store
        .create_automation(&original, "inbox", schedule("personal"))
        .await
        .unwrap();
    // The same route in a newly built agent, as after a restart or a rotated credential.
    let rebuilt = routed("https://a.example/v1");
    let first = admitted(&fire(&store, &rebuilt, 100).await.occurrences[0]);
    drive(&store, &rebuilt, first).await;

    // Same ID and definition, another route: only `model_adapter` differs, and it pauses.
    let moved = routed("https://b.example/v1");
    let (mut a, mut b) = (original.binding().clone(), moved.binding().clone());
    assert_ne!(a["model_adapter"], b["model_adapter"]);
    a.as_object_mut().unwrap().remove("model_adapter");
    b.as_object_mut().unwrap().remove("model_adapter");
    assert_eq!(a, b);
    let report = fire(&store, &moved, 200).await;
    assert_eq!(report.paused, vec!["inbox"]);
    assert!(report.occurrences.is_empty());
    let paused = store.automation("inbox").await.unwrap();
    assert_eq!(paused.state, AutomationState::Paused);
    assert!(paused.last_error.unwrap().contains("changed its binding"));
    let adopted = store
        .change_automation(
            "inbox",
            paused.revision,
            AutomationChange::Update {
                agent: moved.clone(),
                schedule: schedule("personal"),
            },
        )
        .await
        .unwrap();
    store
        .change_automation("inbox", adopted.revision, AutomationChange::Resume)
        .await
        .unwrap();
    let second = admitted(&fire(&store, &moved, 200).await.occurrences[0]);
    assert_eq!(
        *store.status(second).await.unwrap().binding,
        *moved.binding()
    );
    fixture.close(store).await;
}

async fn fired_runs_are_steered_like_any_other(fixture: &impl Fixture) {
    let agent = final_agent();
    let store = fixture.open().await;
    let steering = fixture.steering();
    store
        .create_automation(&agent, "inbox", schedule("personal"))
        .await
        .unwrap();
    let first = admitted(&fire(&store, &agent, 100).await.occurrences[0]);
    let fired = store.automation("inbox").await.unwrap();
    let instruction = || "Only flag urgent mail".to_owned();
    let receipt = steering.steer(first, "focus", instruction()).await.unwrap();
    assert_eq!(
        steering.steer(first, "focus", instruction()).await.unwrap(),
        receipt,
        "a retried instruction returns its receipt"
    );
    assert_eq!(
        store.automation("inbox").await.unwrap(),
        fired,
        "steering leaves the automation as the firing left it"
    );
    // The steered run is still the automation's unsettled run: the next interval overlaps.
    let report = fire(&store, &agent, 200).await;
    assert_eq!(
        report.occurrences[0].outcome,
        OccurrenceOutcome::SkippedOverlap
    );

    let state = drive(&store, &agent, first).await;
    assert_eq!(
        state.phase,
        Phase::Settled(Settlement::Completed(json!("done")))
    );
    assert_eq!(
        state.history[1],
        HistoryEntry::Steering {
            id: receipt.id,
            instruction: instruction(),
        },
        "the dispatch commit applied the instruction after the automation's input"
    );
    assert!(matches!(
        steering.steering(first).await.unwrap()[0].status,
        SteeringStatus::Applied { .. }
    ));
    // Settled: the run takes no more steering, and the refusal does not reach the automation.
    assert!(matches!(
        steering
            .steer(first, "late", "And the calendar".into())
            .await,
        Err(Error::RunNotActive)
    ));
    let after = store.automation("inbox").await.unwrap();
    assert_eq!(after.state, AutomationState::Enabled);
    assert_eq!(after.revision, fired.revision, "firings never advance it");
    assert_eq!(after.last_error, None);
    assert_eq!(after.last_run_id, Some(first));
    assert_eq!(after.next_at_ms, 300);

    let second = admitted(&fire(&store, &agent, 300).await.occurrences[0]);
    assert_ne!(first, second);
    // Steering keys are scoped to their run, apart from the occurrence's acceptance key.
    let again = steering
        .steer(second, "focus", instruction())
        .await
        .unwrap();
    assert_ne!(again.id, receipt.id);
    assert!(fire(&store, &agent, 300).await.occurrences.is_empty());
    drop(steering);
    let store = fixture.reopen(store).await;
    let occurrences = store.occurrences("inbox", 0, 10).await.unwrap();
    let outcomes: Vec<_> = occurrences
        .iter()
        .map(|o| (o.scheduled_at_ms, o.outcome))
        .collect();
    assert_eq!(
        outcomes,
        [
            (100, OccurrenceOutcome::Admitted { run_id: first }),
            (200, OccurrenceOutcome::SkippedOverlap),
            (300, OccurrenceOutcome::Admitted { run_id: second }),
        ]
    );
    let queued = fixture.steering().steering(second).await.unwrap();
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].status, SteeringStatus::Queued);
    fixture.close(store).await;
}

/// A parent that delegates once to a researcher whose one tool asks for input, then answers.
/// The researcher's instructions are part of the parent's binding.
fn delegating(researcher: &str) -> Agent {
    let ask = typed_tool::<Value, Value, _, _>(
        ToolSpec::new("ask", "1")
            .with_description("ask")
            .with_retry(RetryPolicy::ReadOnly),
        |_, _| async {
            Ok(ToolOutput::Wait(WaitRequest::Input {
                prompt: "which folder?".into(),
            }))
        },
    );
    let child = Agent::builder(
        "researcher",
        Arc::new(ScriptedModel::new(vec![
            ModelResponse::tools(vec![ToolCall {
                id: "ask".into(),
                name: "ask".into(),
                arguments: Value::Null,
            }]),
            ModelResponse::final_output(json!("found")),
        ])),
    )
    .instructions(researcher)
    .tool(ask)
    .limits(
        Limits::default()
            .with_max_model_calls(3)
            .with_max_tool_calls(2)
            .with_max_tokens(100)
            .with_active_budget(Duration::from_secs(5)),
    )
    .input::<String>()
    .output::<String>()
    .build()
    .unwrap();
    Agent::builder(
        AGENT,
        Arc::new(ScriptedModel::new(vec![
            ModelResponse::tools(vec![ToolCall {
                id: "research".into(),
                name: "research".into(),
                arguments: json!("the inbox"),
            }]),
            ModelResponse::final_output(json!("done")),
        ])),
    )
    .instructions("Check the inbox")
    .input::<String>()
    .output::<String>()
    .subagents(1, [Subagent::new("research", "Research one topic.", child)])
    .build()
    .unwrap()
    .erased()
}
fn limited(max_in_flight: usize) -> AutomationOptions {
    AutomationOptions::default().with_max_in_flight(max_in_flight)
}
async fn fire_with(
    store: &Arc<dyn AutomationStore>,
    agent: &Agent,
    now: u64,
    options: AutomationOptions,
) -> FiringReport {
    store
        .fire_due(std::slice::from_ref(agent), now, options)
        .await
        .unwrap()
}

async fn delegating_firings_admit_only_the_root(fixture: &impl Fixture) {
    let store = fixture.open().await;
    let agent = delegating("Search the archive");
    store
        .create_automation(&agent, "inbox", schedule("personal"))
        .await
        .unwrap();
    let report = fire_with(&store, &agent, 100, limited(1)).await;
    assert_eq!(report.occurrences.len(), 1);
    let root = admitted(&report.occurrences[0]);
    let state = drive(&store, &agent, root).await;
    assert_eq!(state.phase, Phase::Tools, "the root waits on its child");
    assert_eq!(state.children.len(), 1, "the child is retained by its root");
    let child = state.children[0].state.id;
    assert_eq!(
        state.children[0].state.parent.as_ref().unwrap().run_id,
        root
    );
    assert!(!state.children[0].state.is_terminal());
    // The child is not a run of the store, and not an occurrence of the automation.
    assert!(matches!(store.status(child).await, Err(Error::NotFound)));
    assert!(matches!(
        fixture.steering().steer(child, "s", "hurry".into()).await,
        Err(Error::NotFound)
    ));
    let occurrences = store.occurrences("inbox", 0, 10).await.unwrap();
    assert_eq!(occurrences.len(), 1);
    assert_eq!(
        store.automation("inbox").await.unwrap().last_run_id,
        Some(root)
    );

    // Capacity counts the root once: at one in flight a second automation waits, at two it
    // is admitted beside the root and its unsettled child.
    let other = IntervalAutomation::new("work", json!("Review my inbox"), 100, 100);
    store
        .create_automation(&agent, "calendar", other)
        .await
        .unwrap();
    let report = fire_with(&store, &agent, 150, limited(1)).await;
    assert!(report.at_capacity);
    assert!(report.occurrences.is_empty());
    let report = fire_with(&store, &agent, 150, limited(2)).await;
    assert!(!report.at_capacity);
    assert_eq!(report.occurrences.len(), 1);
    assert_eq!(report.occurrences[0].automation_id, "calendar");
    admitted(&report.occurrences[0]);

    // The child's wait keeps its root unsettled: later intervals overlap until it is resolved.
    let report = fire(&store, &agent, 200).await;
    let inbox: Vec<_> = report
        .occurrences
        .iter()
        .filter(|o| o.automation_id == "inbox")
        .collect();
    assert_eq!(inbox[0].outcome, OccurrenceOutcome::SkippedOverlap);
    assert_eq!(store.automation("inbox").await.unwrap().next_at_ms, 300);
    let answer = RunUpdate::ResolveChild {
        child_id: child,
        call_id: "ask".into(),
        resolution: Resolution::Input(json!("receipts")),
    };
    store.update(root, answer).await.unwrap();
    let state = drive(&store, &agent, root).await;
    assert_eq!(
        state.phase,
        Phase::Settled(Settlement::Completed(json!("done")))
    );
    let result = state.child(child).unwrap().result::<String>().unwrap();
    assert_eq!(result.outcome, ChildOutcome::Completed("found".into()));
    let next = fire(&store, &agent, 300).await;
    let next: Vec<_> = next
        .occurrences
        .iter()
        .filter(|o| o.automation_id == "inbox")
        .collect();
    assert_ne!(admitted(next[0]), root);
    let outcomes: Vec<_> = store
        .occurrences("inbox", 0, 10)
        .await
        .unwrap()
        .iter()
        .map(|o| (o.scheduled_at_ms, o.outcome))
        .collect();
    assert_eq!(outcomes.len(), 3);
    assert_eq!(
        outcomes[0],
        (100, OccurrenceOutcome::Admitted { run_id: root })
    );
    assert_eq!(outcomes[1], (200, OccurrenceOutcome::SkippedOverlap));
    fixture.close(store).await;
}

async fn a_child_definition_is_part_of_the_binding(fixture: &impl Fixture) {
    let store = fixture.open().await;
    let original = delegating("Search the archive");
    store
        .create_automation(&original, "inbox", schedule("personal"))
        .await
        .unwrap();
    // The same parent with a changed child: only the child's definition differs.
    let changed = delegating("Search the web");
    assert_ne!(original.binding(), changed.binding());
    let report = fire(&store, &changed, 100).await;
    assert_eq!(report.paused, vec!["inbox"]);
    assert!(report.occurrences.is_empty());
    let paused = store.automation("inbox").await.unwrap();
    assert_eq!(paused.state, AutomationState::Paused);
    assert!(paused.last_error.unwrap().contains("changed its binding"));
    // Resuming alone does not adopt it.
    let resumed = store
        .change_automation("inbox", paused.revision, AutomationChange::Resume)
        .await
        .unwrap();
    assert_eq!(fire(&store, &changed, 100).await.paused, vec!["inbox"]);
    let paused = store.automation("inbox").await.unwrap();
    assert!(paused.revision > resumed.revision);
    let adopted = store
        .change_automation(
            "inbox",
            paused.revision,
            AutomationChange::Update {
                agent: changed.clone(),
                schedule: schedule("personal"),
            },
        )
        .await
        .unwrap();
    store
        .change_automation("inbox", adopted.revision, AutomationChange::Resume)
        .await
        .unwrap();
    let run = admitted(&fire(&store, &changed, 100).await.occurrences[0]);
    assert_eq!(
        *store.status(run).await.unwrap().binding,
        *changed.binding()
    );
    fixture.close(store).await;
}

async fn capacity_is_bounded_including_deleted_automations(fixture: &impl Fixture) {
    let store = fixture.open().await;
    let agent = final_agent();
    let agents = [agent.clone()];
    let options = AutomationOptions::default()
        .with_max_in_flight(1)
        .with_max_per_fire(1);
    for name in ["a", "b", "c"] {
        store
            .create_automation(&agent, name, schedule(name))
            .await
            .unwrap();
    }
    let first = store.fire_due(&agents, 100, options).await.unwrap();
    assert_eq!(first.occurrences.len(), 1);
    assert_eq!(first.occurrences[0].automation_id, "a");
    let id = admitted(&first.occurrences[0]);
    // The run of a deleted automation still holds capacity.
    store
        .change_automation("a", 1, AutomationChange::Delete)
        .await
        .unwrap();
    for _ in 0..3 {
        let saturated = store.fire_due(&agents, 450, options).await.unwrap();
        assert!(saturated.at_capacity);
        assert!(saturated.occurrences.is_empty());
    }
    assert_eq!(store.automation("b").await.unwrap().next_at_ms, 100);
    assert_eq!(store.due(450, 10).await.unwrap(), vec![id]);
    drive(&store, &agent, id).await;
    let second = store.fire_due(&agents, 450, options).await.unwrap();
    assert_eq!(second.occurrences[0].automation_id, "b");
    assert_eq!(second.occurrences[0].scheduled_at_ms, 400);
    assert_eq!(second.occurrences[0].coalesced, 3);
    drive(&store, &agent, admitted(&second.occurrences[0])).await;
    assert_eq!(
        store
            .fire_due(&agents, 450, options)
            .await
            .unwrap()
            .occurrences[0]
            .automation_id,
        "c"
    );
    fixture.close(store).await;
}

async fn settled_runs_are_not_retried_and_host_errors_block_overlap(fixture: &impl Fixture) {
    let store = fixture.open().await;
    // A number is not the agent's `String` output: the run fails.
    let broken = agent(
        "Invalid model output",
        vec![ModelResponse::final_output(json!(123))],
        vec![],
    );
    store
        .create_automation(&broken, "inbox", schedule("personal"))
        .await
        .unwrap();
    let first = admitted(&fire(&store, &broken, 100).await.occurrences[0]);
    let failed = drive(&store, &broken, first).await;
    assert!(matches!(
        failed.phase,
        Phase::Settled(Settlement::Failed(_))
    ));
    assert_eq!(failed.usage.model_calls, 1);
    assert!(fire(&store, &broken, 100).await.occurrences.is_empty());
    let second = admitted(&fire(&store, &broken, 200).await.occurrences[0]);
    assert_ne!(first, second);
    let state = store.status(second).await.unwrap();
    assert!(
        store
            .record_error(second, (&state).into(), "repair required".into())
            .await
            .unwrap()
    );
    assert_eq!(
        fire(&store, &broken, 300).await.occurrences[0].outcome,
        OccurrenceOutcome::SkippedOverlap
    );
    assert_eq!(
        store.last_error(second).await.unwrap().as_deref(),
        Some("repair required")
    );
    assert_eq!(store.status(first).await.unwrap().usage, failed.usage);
    fixture.close(store).await;
}

async fn definitions_and_limits_are_validated(fixture: &impl Fixture) {
    let store = fixture.open().await;
    let agent = final_agent();
    for invalid in [
        schedule("personal").with_interval_ms(0),
        schedule("personal").with_input(json!({"wrong": "schema"})),
        schedule("personal").with_first_at_ms(u64::MAX),
        schedule(""),
    ] {
        assert!(matches!(
            store.create_automation(&agent, "bad", invalid).await,
            Err(Error::Input(_))
        ));
    }
    for id in [String::new(), "x".repeat(257)] {
        assert!(matches!(
            store
                .create_automation(&agent, &id, schedule("personal"))
                .await,
            Err(Error::Input(_))
        ));
    }
    assert!(store.automations(None, 10).await.unwrap().is_empty());
    let agents = [agent.clone()];
    assert!(matches!(
        store
            .fire_due(&agents, u64::MAX, AutomationOptions::default())
            .await,
        Err(Error::Input(_))
    ));
    for options in [
        AutomationOptions::default().with_max_in_flight(0),
        AutomationOptions::default().with_max_per_fire(1025),
    ] {
        assert!(matches!(
            store.fire_due(&agents, 0, options).await,
            Err(Error::Input(_))
        ));
    }
    assert!(matches!(
        store.automations(None, 0).await,
        Err(Error::Input(_))
    ));
    let exhausted = schedule("personal").with_first_at_ms(i64::MAX as u64);
    store
        .create_automation(&agent, "exhausted", exhausted)
        .await
        .unwrap();
    assert_eq!(
        fire(&store, &agent, i64::MAX as u64).await.paused,
        vec!["exhausted"]
    );
    assert!(
        store
            .occurrences("exhausted", 0, 10)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .automation("exhausted")
            .await
            .unwrap()
            .last_error
            .unwrap()
            .contains("time range")
    );
    fixture.close(store).await;
}

async fn firings_run_with_the_recorded_agent(fixture: &impl Fixture) {
    let store = fixture.open().await;
    let inbox = named(
        "inbox-agent",
        "Inbox",
        vec![ModelResponse::final_output(json!("inbox"))],
        vec![],
    );
    let calendar = named(
        "calendar-agent",
        "Calendar",
        vec![ModelResponse::final_output(json!("calendar"))],
        vec![],
    );
    let absent = named("absent-agent", "Absent", vec![], vec![]);
    let now = now_ms();
    for (agent, id) in [
        (&inbox, "inbox"),
        (&calendar, "calendar"),
        (&absent, "absent"),
    ] {
        let every_day = IntervalAutomation::new(id, json!("go"), now - 1_000, 86_400_000);
        store.create_automation(agent, id, every_day).await.unwrap();
    }
    let host = Host::start_agents(
        store.clone(),
        [inbox, calendar],
        HostOptions::default().with_poll_interval(Duration::from_millis(5)),
    )
    .unwrap();
    let report = host
        .fire_automations(AutomationOptions::default())
        .await
        .unwrap();
    let mut fired: Vec<_> = report
        .occurrences
        .iter()
        .map(|occurrence| occurrence.automation_id.as_str())
        .collect();
    fired.sort();
    assert_eq!(fired, ["calendar", "inbox"]);
    for occurrence in &report.occurrences {
        let state = wait_terminal(&host, admitted(occurrence)).await;
        let agent_id = format!("{}-agent", occurrence.automation_id);
        assert_eq!(state.agent_id, agent_id);
        assert_eq!(
            state.phase,
            Phase::Settled(Settlement::Completed(json!(occurrence.automation_id)))
        );
    }
    // An unregistered agent's automation is left due and unchanged.
    let absent = store.automation("absent").await.unwrap();
    assert_eq!(absent.state, AutomationState::Enabled);
    assert_eq!(absent.next_at_ms, now - 1_000);
    let mut host = host;
    host.shutdown().await.unwrap();
    fixture.close(store).await;
}
async fn wait_terminal(host: &Host<dyn AutomationStore>, id: RunId) -> RunState {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            host.health().unwrap();
            let state = host.status(id).await.unwrap();
            if state.is_terminal() {
                return state;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("run did not settle")
}

macro_rules! automation_tests {
    ($make:expr) => {
        #[tokio::test]
        async fn restart_and_concurrent_duplicate_firings_admit_only_the_latest_occurrence() {
            restarts_and_duplicate_firings(&$make).await;
        }
        #[tokio::test]
        async fn busy_conversations_and_waiting_runs_skip_occurrences_and_advance() {
            overlap_is_skipped_and_advances(&$make).await;
        }
        #[tokio::test]
        async fn pause_update_resume_and_delete_preserve_accepted_work_and_tombstones() {
            management_keeps_accepted_work_and_tombstones(&$make).await;
        }
        #[tokio::test]
        async fn updates_version_identities_and_changed_bindings_require_explicit_adoption() {
            updates_and_bindings(&$make).await;
        }
        #[tokio::test]
        async fn a_changed_model_route_pauses_until_an_update_adopts_it() {
            model_routes_are_part_of_the_binding(&$make).await;
        }
        #[tokio::test]
        async fn a_fired_run_is_steered_like_any_other_without_disturbing_occurrences() {
            fired_runs_are_steered_like_any_other(&$make).await;
        }
        #[tokio::test]
        async fn a_delegating_firing_admits_only_its_root_and_its_child_keeps_it_unsettled() {
            delegating_firings_admit_only_the_root(&$make).await;
        }
        #[tokio::test]
        async fn a_changed_child_definition_pauses_the_parent_until_an_update_adopts_it() {
            a_child_definition_is_part_of_the_binding(&$make).await;
        }
        #[tokio::test]
        async fn admission_is_bounded_including_deleted_automations() {
            capacity_is_bounded_including_deleted_automations(&$make).await;
        }
        #[tokio::test]
        async fn settled_occurrences_are_not_retried_and_host_errors_still_block_overlap() {
            settled_runs_are_not_retried_and_host_errors_block_overlap(&$make).await;
        }
        #[tokio::test]
        async fn definitions_and_limits_are_validated_before_persistence() {
            definitions_and_limits_are_validated(&$make).await;
        }
        #[tokio::test]
        async fn a_host_fires_each_automation_with_its_recorded_agent() {
            firings_run_with_the_recorded_agent(&$make).await;
        }
    };
}

mod memory_contract {
    use super::*;
    automation_tests!(Memory::default());
}

#[cfg(feature = "sqlite")]
mod sqlite_contract {
    use super::*;
    fn fixture() -> Sqlite {
        Sqlite {
            directory: tempfile::tempdir().unwrap(),
            open: std::sync::Mutex::new(None),
        }
    }
    automation_tests!(fixture());

    #[tokio::test]
    async fn recurring_personal_agent_example_runs_across_restart_and_coalesces_downtime() {
        let directory = tempfile::tempdir().unwrap();
        let runs = recurring_workflow::run(directory.path()).await.unwrap();
        assert_eq!(runs.len(), 3);
        assert_ne!(runs[0].id, runs[1].id);
        assert_ne!(runs[1].id, runs[2].id);
        assert!(!runs[2].prior_context.is_empty());
        let store = sqlite::SqliteStore::open(directory.path().join("recurring.sqlite"))
            .await
            .unwrap();
        for state in &runs {
            assert_eq!(store.status(state.id).await.unwrap().usage.tool_calls, 1);
        }
        store.close().await.unwrap();
    }
}

/// The current UTC Unix time in milliseconds.
fn now_ms() -> u64 {
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH);
    u64::try_from(now.expect("clock after 1970").as_millis()).expect("milliseconds fit u64")
}
