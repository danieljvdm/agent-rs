//! Real process-death recovery checks. `process_child` is an ignored subprocess entrypoint.
#![cfg(all(feature = "sqlite", unix))]

use agent_rs::{Host, HostOptions, sqlite::SqliteStore, *};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::process::ExitStatusExt,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

const CHILD_CONFIG: &str = "AGENT_RS_PROCESS_CHILD";
const READY_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
enum Scenario {
    Final,
    Mutation,
    IdempotentMutation,
    Waits,
    LongWait,
    Delegation,
    /// The run is admitted by an automation firing instead of `accept`.
    Recurring,
}
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
enum CrashPoint {
    #[default]
    None,
    Accepted,
    ModelIntent,
    ToolIntent,
    BeforeOutcome,
    AfterOutcome,
    BeforeChildAdmission,
    AfterChildAdmission,
    BeforeChildCompletion,
    AfterChildCompletion,
}
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
enum Action {
    #[default]
    None,
    Approve,
    Reply,
    Reconcile {
        exists: bool,
    },
    Cancel,
}
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
enum ReadyWhen {
    #[default]
    Terminal,
    Unknown,
    Approval,
    Input,
    Timer,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Config {
    directory: PathBuf,
    marker: PathBuf,
    scenario: Scenario,
    crash: CrashPoint,
    action: Action,
    ready: ReadyWhen,
    max_model_calls: u32,
    wait_ms: u64,
}
impl Config {
    fn new(directory: &Path, scenario: Scenario) -> Self {
        Self {
            directory: directory.to_owned(),
            marker: PathBuf::new(),
            scenario,
            crash: CrashPoint::None,
            action: Action::None,
            ready: ReadyWhen::Terminal,
            max_model_calls: 8,
            wait_ms: 2_000,
        }
    }
    fn resume(&self, ready: ReadyWhen, action: Action) -> Self {
        Self {
            crash: CrashPoint::None,
            action,
            ready,
            ..self.clone()
        }
    }
}
#[derive(Debug, Serialize, Deserialize)]
struct Observation {
    state: RunState,
    history: Vec<HistoryEntry>,
    operations: Vec<Operation>,
    due: Vec<RunId>,
}

fn storage(error: impl std::fmt::Display) -> Error {
    Error::storage(error.to_string())
}
fn append_synced(path: &Path, value: &str) -> std::io::Result<()> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    writeln!(file, "{value}")?;
    file.sync_all()
}
fn lines(directory: &Path, name: &str) -> Vec<String> {
    match fs::read_to_string(directory.join(name)) {
        Ok(text) => text.lines().map(str::to_owned).collect(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => panic!("read {name}: {error}"),
    }
}

struct LoggedModel {
    inner: ScriptedModel,
    directory: PathBuf,
    log_name: &'static str,
}
#[async_trait]
impl Model for LoggedModel {
    async fn stream(
        &self,
        request: ModelRequest,
        cancel: CancellationToken,
    ) -> Result<ModelStream> {
        let path = self.directory.join(self.log_name);
        let turn = request.turn.to_string();
        tokio::task::spawn_blocking(move || append_synced(&path, &turn))
            .await
            .map_err(storage)?
            .map_err(storage)?;
        self.inner.stream(request, cancel).await
    }
}
fn spec(name: &str, retry: RetryPolicy, approval: bool) -> ToolSpec {
    ToolSpec::new(name, "1")
        .with_description(name)
        .with_input_schema(Value::Null)
        .with_retry(retry)
        .with_execution(Execution::Sequential)
        .with_requires_approval(approval)
}
fn call(name: &str) -> ModelResponse {
    ModelResponse::tools(vec![ToolCall {
        id: name.into(),
        name: name.into(),
        arguments: json!({}),
    }])
}
fn agent(config: &Config) -> Agent {
    let mut tools = Vec::<Arc<dyn Tool>>::new();
    let mut responses = Vec::new();
    match config.scenario {
        Scenario::Delegation => return delegation_agent(config),
        Scenario::Final => (),
        Scenario::Mutation | Scenario::IdempotentMutation => {
            let idempotent = matches!(config.scenario, Scenario::IdempotentMutation);
            let retry = if idempotent {
                RetryPolicy::Idempotent {
                    contract: "receipt-file-v1".into(),
                }
            } else {
                RetryPolicy::Never
            };
            let directory = config.directory.clone();
            tools.push(typed_tool::<Value, Value, _, _>(
                spec("mutate", retry, false),
                move |_, context| {
                    let directory = directory.clone();
                    async move {
                        let value = tokio::task::spawn_blocking(move || {
                            let key = context.idempotency_key;
                            append_synced(&directory.join("tool-attempts.log"), &key)?;
                            let created = if idempotent {
                                // This separate receipt represents the external service's deduplication.
                                match OpenOptions::new()
                                    .write(true)
                                    .create_new(true)
                                    .open(directory.join(format!("receipt-{key}")))
                                {
                                    Ok(mut receipt) => {
                                        receipt.write_all(b"external-receipt-1")?;
                                        receipt.sync_all()?;
                                        true
                                    }
                                    Err(error)
                                        if error.kind() == std::io::ErrorKind::AlreadyExists =>
                                    {
                                        false
                                    }
                                    Err(error) => return Err(error),
                                }
                            } else {
                                true
                            };
                            if created {
                                append_synced(&directory.join("external-effects.log"), &key)?;
                            }
                            Ok::<_, std::io::Error>(json!("external-receipt-1"))
                        })
                        .await
                        .map_err(storage)?
                        .map_err(storage)?;
                        Ok(ToolOutput::Complete(value))
                    }
                },
            ));
            responses.push(call("mutate"));
        }
        Scenario::Waits | Scenario::LongWait => {
            if matches!(config.scenario, Scenario::Waits) {
                tools.push(typed_tool::<Value, Value, _, _>(
                    spec("approval", RetryPolicy::ReadOnly, true),
                    |_, _| async { Ok(ToolOutput::Complete(json!("approved"))) },
                ));
                tools.push(typed_tool::<Value, Value, _, _>(
                    spec("input", RetryPolicy::ReadOnly, false),
                    |_, _| async {
                        Ok(ToolOutput::Wait(WaitRequest::Input {
                            prompt: "Which reminder?".into(),
                        }))
                    },
                ));
                responses.extend([call("approval"), call("input")]);
            }
            let wait_ms = config.wait_ms;
            tools.push(typed_tool::<Value, Value, _, _>(
                spec("timer", RetryPolicy::ReadOnly, false),
                move |_, _| async move {
                    Ok(ToolOutput::Wait(WaitRequest::Until {
                        unix_ms: now_ms() + wait_ms,
                    }))
                },
            ));
            responses.push(call("timer"));
        }
        Scenario::Recurring => (),
    }
    responses.push(ModelResponse::final_output(json!("done")));
    Agent::builder(
        "process-recovery-v1",
        Arc::new(LoggedModel {
            inner: ScriptedModel::new(responses),
            directory: config.directory.clone(),
            log_name: "model-attempts.log",
        }),
    )
    .instructions("Run the deterministic process recovery scenario.")
    .tools(tools)
    .limits(
        Limits::default()
            .with_max_model_calls(config.max_model_calls)
            .with_max_tool_calls(8)
            .with_operation_timeout(Duration::from_millis(5_000))
            .with_cleanup_timeout(Duration::from_millis(100))
            .with_active_budget(Duration::from_millis(30_000)),
    )
    .build()
    .unwrap()
}
/// A parent that delegates once to a child whose one tool call has an external effect.
fn delegation_agent(config: &Config) -> Agent {
    let directory = config.directory.clone();
    let tool = typed_tool::<Value, Value, _, _>(
        spec("mutate", RetryPolicy::Never, false),
        move |_, context| {
            let directory = directory.clone();
            async move {
                tokio::task::spawn_blocking(move || {
                    let path = directory.join("child-effects.log");
                    append_synced(&path, &context.idempotency_key)
                })
                .await
                .map_err(storage)?
                .map_err(storage)?;
                Ok(ToolOutput::Complete(json!("child-receipt")))
            }
        },
    );
    let child = Agent::builder(
        "attached-child-v1",
        Arc::new(LoggedModel {
            inner: ScriptedModel::new(vec![
                call("mutate"),
                ModelResponse::final_output(json!("child-done")),
                ModelResponse::final_output(json!("child-done")),
                ModelResponse::final_output(json!("child-done")),
            ]),
            directory: config.directory.clone(),
            log_name: "child-attempts.log",
        }),
    )
    .instructions("Perform the delegated mutation.")
    .tool(tool)
    .limits(
        Limits::default()
            .with_max_model_calls(4)
            .with_max_tool_calls(2)
            .with_max_tokens(100)
            .with_operation_timeout(Duration::from_millis(1_000))
            .with_cleanup_timeout(Duration::from_millis(100))
            .with_active_budget(Duration::from_millis(10_000)),
    )
    .build()
    .unwrap();
    Agent::builder(
        "delegating-parent-v1",
        Arc::new(LoggedModel {
            inner: ScriptedModel::new(vec![
                call("delegate"),
                ModelResponse::final_output(json!("done")),
            ]),
            directory: config.directory.clone(),
            log_name: "model-attempts.log",
        }),
    )
    .instructions("Delegate one task and await its result.")
    .limits(
        Limits::default()
            .with_max_model_calls(8)
            .with_max_tool_calls(8)
            .with_operation_timeout(Duration::from_millis(1_000))
            .with_cleanup_timeout(Duration::from_millis(100))
            .with_active_budget(Duration::from_millis(30_000)),
    )
    .subagents(1, [Subagent::new("delegate", "Run the child task.", child)])
    .build()
    .unwrap()
}

async fn park(config: &Config, store: &SqliteStore, id: RunId) -> Result<()> {
    let observation = Observation {
        state: store.status(id).await?,
        history: store.history("conversation").await?,
        operations: store.operations(id).await?,
        due: store.due(now_ms(), 10).await?,
    };
    let temporary = config.marker.with_extension("partial");
    let mut marker = File::create(&temporary).map_err(storage)?;
    marker
        .write_all(&serde_json::to_vec(&observation).map_err(storage)?)
        .map_err(storage)?;
    marker.sync_all().map_err(storage)?;
    fs::rename(temporary, &config.marker).map_err(storage)?;
    // The parent must terminate this process. No cleanup or SQLite close may run first.
    std::future::pending::<()>().await;
    unreachable!()
}
struct BoundaryCheckpoint<'a> {
    store: &'a SqliteStore,
    config: &'a Config,
}
#[async_trait]
impl Checkpoint for BoundaryCheckpoint<'_> {
    async fn commit(&self, state: &mut RunState) -> Result<()> {
        // A child commits through its parent, so these are commits of the parent.
        let child_admitted = !state.children.is_empty();
        let child_complete =
            (state.children.first()).is_some_and(|child| child.state.is_terminal());
        if (matches!(self.config.crash, CrashPoint::BeforeChildAdmission) && child_admitted)
            || (matches!(self.config.crash, CrashPoint::BeforeChildCompletion) && child_complete)
        {
            return park(self.config, self.store, state.id).await;
        }
        let outcome = state.operations.iter().any(|operation| {
            operation.call.id == "mutate"
                && matches!(operation.status, OperationStatus::Complete(_))
        });
        if matches!(self.config.crash, CrashPoint::BeforeOutcome) && outcome {
            return park(self.config, self.store, state.id).await;
        }
        self.store.commit(state).await?;
        let at_boundary = match self.config.crash {
            CrashPoint::ModelIntent => matches!(state.phase, Phase::ModelInFlight),
            CrashPoint::ToolIntent => state
                .operations
                .iter()
                .any(|operation| matches!(operation.status, OperationStatus::Dispatched)),
            CrashPoint::AfterOutcome => outcome,
            CrashPoint::AfterChildAdmission => child_admitted,
            CrashPoint::AfterChildCompletion => child_complete,
            _ => false,
        };
        if at_boundary {
            park(self.config, self.store, state.id).await?;
        }
        Ok(())
    }
}
fn ready(state: &RunState, condition: ReadyWhen) -> bool {
    match condition {
        ReadyWhen::Terminal => state.is_terminal(),
        ReadyWhen::Unknown => state
            .operations
            .iter()
            .any(|op| matches!(op.status, OperationStatus::Unknown { .. })),
        ReadyWhen::Approval => state
            .operations
            .iter()
            .any(|op| matches!(op.status, OperationStatus::Approval)),
        ReadyWhen::Input => state.operations.iter().any(|op| {
            matches!(
                op.status,
                OperationStatus::Waiting(WaitRequest::Input { .. })
            )
        }),
        ReadyWhen::Timer => state.operations.iter().any(|op| {
            matches!(
                op.status,
                OperationStatus::Waiting(WaitRequest::Until { .. })
            )
        }),
    }
}
async fn child(config: Config) -> Result<()> {
    let store = Arc::new(SqliteStore::open(config.directory.join("runtime.sqlite")).await?);
    let agent = agent(&config);
    let id = accept(&config, &store, &agent).await?;
    match config.action {
        Action::None => (),
        Action::Approve => {
            store
                .resolve(id, "approval", Resolution::Approve(true))
                .await?
        }
        Action::Reply => {
            store
                .resolve(id, "input", Resolution::Input(json!("Call the dentist")))
                .await?
        }
        Action::Reconcile { exists } => {
            store
                .resolve(
                    id,
                    "mutate",
                    Resolution::Reconcile(ToolResult {
                        call_id: "mutate".into(),
                        value: if exists {
                            json!("external-receipt-1")
                        } else {
                            json!({"confirmed_absent":true})
                        },
                        is_error: !exists,
                    }),
                )
                .await?
        }
        Action::Cancel => store.request_cancel(id).await?,
    }
    if matches!(config.crash, CrashPoint::Accepted) {
        return park(&config, &store, id).await;
    }
    if !matches!(config.crash, CrashPoint::None) {
        let mut state = store.claim(id).await?;
        agent
            .drive(
                &mut state,
                &BoundaryCheckpoint {
                    store: &store,
                    config: &config,
                },
                RunControl::default(),
            )
            .await?;
        panic!("did not hit configured crash boundary: {:?}", config.crash);
    }
    if matches!(
        config.ready,
        ReadyWhen::Approval | ReadyWhen::Input | ReadyWhen::Timer
    ) {
        // Returning from drive is the public quiescence barrier for a wait. Merely
        // observing its first checkpoint can race the subsequent budget refund.
        let mut state = store.claim(id).await?;
        agent
            .drive(&mut state, store.as_ref(), RunControl::default())
            .await?;
        assert!(ready(&state, config.ready), "expected a persisted wait");
        return park(&config, &store, id).await;
    }
    let host = Host::start(
        store.clone(),
        agent,
        HostOptions::default()
            .with_max_concurrent_runs(2)
            .with_poll_interval(Duration::from_millis(2)),
    )?;
    tokio::time::timeout(READY_TIMEOUT, async {
        loop {
            host.health()?;
            if let Some(error) = store.last_error(id).await? {
                return Err(Error::storage(format!("host error: {error}")));
            }
            let state = host.status(id).await?;
            if ready(&state, config.ready) {
                return park(&config, &store, id).await;
            }
            if state.is_terminal() {
                panic!(
                    "unexpected terminal state while waiting for {:?}: {:?}",
                    config.ready, state.phase
                );
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("child did not reach requested durable state")
}

/// Accept the scenario's run directly, or by firing an automation whose definition and clock
/// are the same in every relaunched child.
async fn accept(config: &Config, store: &SqliteStore, agent: &Agent) -> Result<RunId> {
    let input = json!({"task":"process recovery"});
    if !matches!(config.scenario, Scenario::Recurring) {
        return store
            .accept(agent, "conversation", "request-1", input, None)
            .await;
    }
    let schedule = IntervalAutomation::new("conversation", input, 100, 100);
    store
        .create_automation(agent, "recurring", schedule)
        .await?;
    store
        .fire_due(
            std::slice::from_ref(agent),
            100,
            AutomationOptions::default(),
        )
        .await?;
    let occurrences = store.occurrences("recurring", 0, 10).await?;
    assert_eq!(
        occurrences.len(),
        1,
        "duplicate occurrence after process death"
    );
    assert_eq!(store.automation("recurring").await?.next_at_ms, 200);
    match occurrences[0].outcome {
        OccurrenceOutcome::Admitted { run_id } => Ok(run_id),
        _ => panic!("expected the recurring run to be admitted"),
    }
}

// A normal test run skips this entrypoint. Each parent launches only this test with --ignored.
#[test]
#[ignore = "subprocess entrypoint; exercised by the process recovery tests"]
fn process_child() {
    let config: Config = serde_json::from_str(
        &std::env::var(CHILD_CONFIG).expect("process_child must be launched by its parent test"),
    )
    .unwrap();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(child(config))
        .unwrap();
}

struct RunningChild {
    child: Child,
    marker: PathBuf,
    log: PathBuf,
}
impl RunningChild {
    fn start(mut config: Config) -> Self {
        let ordinal = RunId::new_v4();
        config.marker = config.directory.join(format!("ready-{ordinal}.json"));
        let log = config.directory.join(format!("child-{ordinal}.log"));
        let output = File::create(&log).unwrap();
        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "process_child",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD_CONFIG, serde_json::to_string(&config).unwrap())
            .stdin(Stdio::null())
            .stdout(Stdio::from(output.try_clone().unwrap()))
            .stderr(Stdio::from(output))
            .spawn()
            .unwrap();
        Self {
            child,
            marker: config.marker,
            log,
        }
    }
    fn wait_ready(&mut self) -> Observation {
        let deadline = Instant::now() + READY_TIMEOUT;
        loop {
            if self.marker.exists() {
                break;
            }
            if let Some(status) = self.child.try_wait().unwrap() {
                panic!(
                    "child exited before readiness: {status}\n{}",
                    fs::read_to_string(&self.log).unwrap()
                );
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for child\n{}",
                fs::read_to_string(&self.log).unwrap()
            );
            std::thread::sleep(Duration::from_millis(2));
        }
        serde_json::from_slice(&fs::read(&self.marker).unwrap()).unwrap()
    }
    fn kill_at_ready(mut self) -> Observation {
        let observation = self.wait_ready();
        self.child
            .kill()
            .expect("terminate child at durable boundary");
        let status = self.child.wait().expect("reap terminated child");
        assert_eq!(
            status.signal(),
            Some(libc::SIGKILL),
            "child must die abruptly, without graceful cleanup"
        );
        observation
    }
}
impl Drop for RunningChild {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}
fn step(config: Config) -> Observation {
    RunningChild::start(config).kill_at_ready()
}
fn assert_done(observation: &Observation) {
    assert_eq!(
        observation.state.phase,
        Phase::Settled(Settlement::Completed(json!("done")))
    );
    assert!(observation.due.is_empty());
}

#[test]
fn attached_child_admission_and_completion_survive_process_death() {
    for boundary in [
        CrashPoint::BeforeChildAdmission,
        CrashPoint::AfterChildAdmission,
        CrashPoint::BeforeChildCompletion,
        CrashPoint::AfterChildCompletion,
    ] {
        let directory = TempDir::new().unwrap();
        let mut config = Config::new(directory.path(), Scenario::Delegation);
        config.crash = boundary;
        let interrupted = step(config.clone());
        let completed = step(config.resume(ReadyWhen::Terminal, Action::None));
        assert_done(&completed);
        assert_eq!(completed.state.children.len(), 1, "{boundary:?}");
        let child = &completed.state.children[0];
        assert_eq!(
            child.result::<String>().unwrap().outcome,
            ChildOutcome::Completed("child-done".into()),
            "{boundary:?}"
        );
        assert_eq!(
            lines(directory.path(), "child-effects.log").len(),
            1,
            "the child's effect happens once: {boundary:?}"
        );
        assert_eq!(
            lines(directory.path(), "model-attempts.log"),
            ["1", "2"],
            "the parent never repeats its delegation turn: {boundary:?}"
        );
        if let Some(admitted) = interrupted.state.children.first() {
            assert_eq!(
                admitted.state.id, child.state.id,
                "an admitted child keeps its identity: {boundary:?}"
            );
        }
        // Only the child's final turn is repeated, when its settlement was not committed.
        let attempts = lines(directory.path(), "child-attempts.log");
        let repeated = matches!(boundary, CrashPoint::BeforeChildCompletion);
        assert_eq!(attempts.len(), if repeated { 3 } else { 2 }, "{boundary:?}");
        assert_eq!(completed.state.usage.model_calls, 2 + attempts.len() as u32);
        assert_eq!(
            completed.state.child_usage.model_calls,
            attempts.len() as u32
        );
        if matches!(boundary, CrashPoint::AfterChildCompletion) {
            assert_eq!(
                interrupted.state.child_usage, completed.state.child_usage,
                "settlement is reconciled once"
            );
        }
        // A settled run is never driven again.
        let again = step(config.resume(ReadyWhen::Terminal, Action::None));
        assert_eq!(
            serde_json::to_value(&completed.state).unwrap(),
            serde_json::to_value(&again.state).unwrap()
        );
    }
}

#[test]
fn durable_acceptance_and_reserved_model_budget_survive_process_death() {
    let dir = TempDir::new().unwrap();
    let mut config = Config::new(dir.path(), Scenario::Final);
    config.crash = CrashPoint::Accepted;
    let mut child = RunningChild::start(config.clone());
    let accepted = child.wait_ready();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    assert!(matches!(
        runtime.block_on(SqliteStore::open(dir.path().join("runtime.sqlite"))),
        Err(Error::StoreBusy)
    ));
    drop(runtime);
    child.kill_at_ready();
    assert_eq!(accepted.state.usage.model_calls, 0);
    assert_eq!(accepted.history.len(), 1);
    let completed = step(config.resume(ReadyWhen::Terminal, Action::None));
    assert_done(&completed);
    assert_eq!(accepted.state.id, completed.state.id);
    assert_eq!(lines(dir.path(), "model-attempts.log"), ["1"]);

    let dir = TempDir::new().unwrap();
    let mut config = Config::new(dir.path(), Scenario::Final);
    config.max_model_calls = 1;
    config.crash = CrashPoint::ModelIntent;
    let interrupted = step(config.clone());
    assert_eq!(interrupted.state.phase, Phase::ModelInFlight);
    assert_eq!(interrupted.state.usage.model_calls, 1);
    assert_eq!(interrupted.state.usage.active_ms, 5_100);
    assert!(lines(dir.path(), "model-attempts.log").is_empty());
    let stopped = step(config.resume(ReadyWhen::Terminal, Action::None));
    assert_eq!(
        stopped.state.phase,
        Phase::Settled(Settlement::Stopped(StopReason::ModelCalls))
    );
    assert_eq!(
        stopped.state.usage.active_ms,
        interrupted.state.usage.active_ms
    );
    assert!(lines(dir.path(), "model-attempts.log").is_empty());
}

#[test]
fn killed_ordinary_mutations_require_reconciliation_even_before_handler_entry() {
    for (boundary, effects) in [(CrashPoint::ToolIntent, 0), (CrashPoint::BeforeOutcome, 1)] {
        let dir = TempDir::new().unwrap();
        let mut config = Config::new(dir.path(), Scenario::Mutation);
        config.crash = boundary;
        let interrupted = step(config.clone());
        assert!(matches!(
            interrupted.state.operations[0].status,
            OperationStatus::Dispatched
        ));
        assert_eq!(lines(dir.path(), "external-effects.log").len(), effects);
        assert_eq!(lines(dir.path(), "tool-attempts.log").len(), effects);
        let unknown = step(config.resume(ReadyWhen::Unknown, Action::None));
        assert!(matches!(
            unknown.state.operations[0].status,
            OperationStatus::Unknown { .. }
        ));
        assert_eq!(
            unknown.state.usage.active_ms,
            interrupted.state.usage.active_ms
        );
        assert_eq!(unknown.state.usage.tool_calls, 1);
        assert_eq!(lines(dir.path(), "external-effects.log").len(), effects);
        let completed = step(config.resume(
            ReadyWhen::Terminal,
            Action::Reconcile {
                exists: effects == 1,
            },
        ));
        assert_done(&completed);
        assert_eq!(completed.state.usage.model_calls, 2);
        assert_eq!(completed.state.usage.tool_calls, 1);
        assert_eq!(lines(dir.path(), "external-effects.log").len(), effects);
        assert_eq!(lines(dir.path(), "tool-attempts.log").len(), effects);
        assert_eq!(lines(dir.path(), "model-attempts.log"), ["1", "2"]);
    }
}

#[test]
fn killed_idempotent_mutation_reuses_key_and_recorded_results_are_never_repeated() {
    let dir = TempDir::new().unwrap();
    let mut config = Config::new(dir.path(), Scenario::IdempotentMutation);
    config.crash = CrashPoint::BeforeOutcome;
    let interrupted = step(config.clone());
    assert_eq!(lines(dir.path(), "external-effects.log").len(), 1);
    let completed = step(config.resume(ReadyWhen::Terminal, Action::None));
    assert_done(&completed);
    let attempts = lines(dir.path(), "tool-attempts.log");
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0], attempts[1]);
    assert_eq!(attempts[0], interrupted.state.operations[0].idempotency_key);
    assert_eq!(
        lines(dir.path(), "external-effects.log"),
        [attempts[0].clone()]
    );
    assert_eq!(completed.state.usage.tool_calls, 2);
    assert!(completed.state.usage.active_ms >= interrupted.state.usage.active_ms);
    assert_eq!(lines(dir.path(), "model-attempts.log"), ["1", "2"]);

    let dir = TempDir::new().unwrap();
    let mut config = Config::new(dir.path(), Scenario::Mutation);
    config.crash = CrashPoint::AfterOutcome;
    let recorded = step(config.clone());
    assert!(matches!(
        recorded.state.operations[0].status,
        OperationStatus::Complete(_)
    ));
    let completed = step(config.resume(ReadyWhen::Terminal, Action::None));
    assert_done(&completed);
    assert_eq!(completed.state.usage.tool_calls, 1);
    assert_eq!(lines(dir.path(), "external-effects.log").len(), 1);
    assert_eq!(lines(dir.path(), "tool-attempts.log").len(), 1);
}

#[test]
fn approval_input_and_timer_waits_resume_after_abrupt_process_restarts() {
    let dir = TempDir::new().unwrap();
    let config = Config::new(dir.path(), Scenario::Waits);
    let approval = step(config.resume(ReadyWhen::Approval, Action::None));
    assert_eq!(approval.state.usage.tool_calls, 0);
    let waiting = step(config.resume(ReadyWhen::Approval, Action::None));
    assert_eq!(waiting.state.usage, approval.state.usage);
    let input = step(config.resume(ReadyWhen::Input, Action::Approve));
    let waiting = step(config.resume(ReadyWhen::Input, Action::None));
    assert_eq!(waiting.state.usage, input.state.usage);
    let timer = step(config.resume(ReadyWhen::Timer, Action::Reply));
    assert_eq!(timer.state.usage.model_calls, 3);
    let deadline = match timer.state.operations[0].status {
        OperationStatus::Waiting(WaitRequest::Until { unix_ms }) => unix_ms,
        _ => panic!("timer wait was not retained"),
    };
    // No notification survives the killed process. A new host discovers and waits for the UTC deadline.
    let completed = step(config.resume(ReadyWhen::Terminal, Action::None));
    assert_done(&completed);
    assert!(now_ms() >= deadline);
    assert_eq!(completed.state.id, approval.state.id);
    assert_eq!(completed.state.usage.model_calls, 4);
    assert_eq!(completed.state.usage.tool_calls, 3);
    assert_eq!(
        lines(dir.path(), "model-attempts.log"),
        ["1", "2", "3", "4"]
    );
    assert_eq!(completed.history.len(), 5);
}

#[test]
fn day_long_wait_keeps_budget_and_deadline_across_repeated_process_deaths() {
    let dir = TempDir::new().unwrap();
    let mut config = Config::new(dir.path(), Scenario::LongWait);
    config.wait_ms = 24 * 60 * 60 * 1_000;
    let initial = step(config.resume(ReadyWhen::Timer, Action::None));
    for _ in 0..6 {
        let waiting = step(config.resume(ReadyWhen::Timer, Action::None));
        assert_eq!(waiting.state.id, initial.state.id);
        assert_eq!(waiting.state.usage, initial.state.usage);
        assert_eq!(waiting.state.operations, initial.state.operations);
        assert!(waiting.due.is_empty());
    }
    let cancelled = step(config.resume(ReadyWhen::Terminal, Action::Cancel));
    assert_eq!(cancelled.state.phase, Phase::Settled(Settlement::Cancelled));
    assert_eq!(cancelled.state.usage.model_calls, 1);
    assert_eq!(cancelled.state.usage.tool_calls, 1);
    assert_eq!(
        cancelled.state.usage.active_ms,
        initial.state.usage.active_ms
    );
    assert_eq!(lines(dir.path(), "model-attempts.log"), ["1"]);
}

#[test]
#[ignore = "opt-in real-time wait/restart soak; AGENT_RS_SOAK_SECONDS defaults to 60"]
fn persisted_wait_process_restart_soak() {
    let seconds = std::env::var("AGENT_RS_SOAK_SECONDS")
        .unwrap_or_else(|_| "60".into())
        .parse::<u64>()
        .unwrap();
    assert!((1..=3_600).contains(&seconds));
    let dir = TempDir::new().unwrap();
    let mut config = Config::new(dir.path(), Scenario::LongWait);
    config.wait_ms = 24 * 60 * 60 * 1_000;
    let initial = step(config.resume(ReadyWhen::Timer, Action::None));
    let started = Instant::now();
    let mut kills = 1;
    while started.elapsed() < Duration::from_secs(seconds) {
        // A deliberate soak cadence, not a guessed readiness delay: readiness uses the persisted marker.
        std::thread::sleep(Duration::from_millis(250));
        let waiting = step(config.resume(ReadyWhen::Timer, Action::None));
        assert_eq!(waiting.state.usage, initial.state.usage);
        assert_eq!(waiting.state.operations, initial.state.operations);
        assert!(waiting.due.is_empty());
        kills += 1;
    }
    let cancelled = step(config.resume(ReadyWhen::Terminal, Action::Cancel));
    assert_eq!(cancelled.state.phase, Phase::Settled(Settlement::Cancelled));
    assert_eq!(
        cancelled.state.usage.active_ms,
        initial.state.usage.active_ms
    );
    eprintln!(
        "process-recovery soak: {kills} SIGKILL/relaunch cycles in {:.3}s; active budget unchanged at {}ms",
        started.elapsed().as_secs_f64(),
        initial.state.usage.active_ms
    );
}

#[test]
fn automation_admission_survives_sigkill_without_duplicate_occurrences_or_runs() {
    let dir = TempDir::new().unwrap();
    let mut config = Config::new(dir.path(), Scenario::Recurring);
    config.crash = CrashPoint::Accepted;
    // Each child repeats the creation and the firing at the same clock reading.
    let first = step(config.clone());
    let retried = step(config.clone());
    assert_eq!(first.state.id, retried.state.id);
    assert_eq!(first.state.phase, Phase::Ready);
    assert!(lines(dir.path(), "model-attempts.log").is_empty());
    let completed = step(config.resume(ReadyWhen::Terminal, Action::None));
    assert_done(&completed);
    let restarted = step(config.resume(ReadyWhen::Terminal, Action::None));
    assert_done(&restarted);
    assert_eq!(first.state.id, restarted.state.id);
    assert_eq!(lines(dir.path(), "model-attempts.log"), ["1"]);
}

/// The current UTC Unix time in milliseconds, the unit of run expiry and timer waits.
fn now_ms() -> u64 {
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH);
    u64::try_from(now.expect("clock after 1970").as_millis()).expect("milliseconds fit u64")
}
