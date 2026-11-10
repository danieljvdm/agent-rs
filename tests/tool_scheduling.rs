//! Invariants of tool scheduling, observed through every committed checkpoint and through what
//! each tool saw when it started. Tests run under paused Tokio time, so every timing is exact.
use async_trait::async_trait;
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::time::Instant;
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
/// Calls `c0`, `c1`, ... to the `sleep` tool, each sleeping for the given milliseconds.
fn sleeps(ms: &[u64]) -> Vec<ToolCall> {
    ms.iter()
        .enumerate()
        .map(|(index, ms)| call(&format!("c{index}"), "sleep", json!(ms)))
        .collect()
}
/// An agent whose model asks for each batch of calls in turn, then answers "done".
fn agent(batches: Vec<Vec<ToolCall>>, tools: Vec<Arc<dyn Tool>>, limits: Limits) -> Agent {
    builder(batches, tools, limits).build().unwrap()
}
fn builder(
    batches: Vec<Vec<ToolCall>>,
    tools: Vec<Arc<dyn Tool>>,
    limits: Limits,
) -> AgentBuilder<String, String> {
    let mut responses: Vec<_> = batches.into_iter().map(ModelResponse::tools).collect();
    responses.push(ModelResponse::final_output(json!("done")));
    Agent::builder("test", Arc::new(ScriptedModel::new(responses)))
        .instructions("test")
        .tools(tools)
        .limits(limits)
        .input::<String>()
        .output::<String>()
}
fn state(agent: &Agent) -> RunState {
    agent.new_run("test", json!("go"), &[], None).unwrap()
}
fn results(state: &RunState) -> &[ToolResult] {
    match &state.history[1] {
        HistoryEntry::Exchange { results, .. } => results,
        other => panic!("missing tool exchange: {other:?}"),
    }
}
fn ids(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| value.to_string()).collect()
}

/// What one commit made durable about the current tool batch.
#[derive(Clone, Debug)]
struct Snapshot {
    dispatched: Vec<String>,
    pending: Vec<String>,
    active_ms: u64,
}
/// A checkpoint that keeps every committed snapshot.
#[derive(Clone, Default)]
struct Journal(Arc<Mutex<Vec<Snapshot>>>);
impl Journal {
    fn last(&self) -> Snapshot {
        self.0
            .lock()
            .unwrap()
            .last()
            .cloned()
            .expect("no commit yet")
    }
    fn snapshots(&self) -> Vec<Snapshot> {
        self.0.lock().unwrap().clone()
    }
    /// Every call ID any checkpoint recorded as dispatched, sorted.
    fn ever_dispatched(&self) -> Vec<String> {
        let mut ids: Vec<_> = self
            .snapshots()
            .into_iter()
            .flat_map(|snapshot| snapshot.dispatched)
            .collect();
        ids.sort();
        ids.dedup();
        ids
    }
}
#[async_trait]
impl Checkpoint for Journal {
    async fn commit(&self, state: &mut RunState) -> Result<()> {
        let ids = |wanted: fn(&OperationStatus) -> bool| {
            state
                .operations
                .iter()
                .filter(|op| wanted(&op.status))
                .map(|op| op.call.id.clone())
                .collect()
        };
        self.0.lock().unwrap().push(Snapshot {
            dispatched: ids(|status| matches!(status, OperationStatus::Dispatched)),
            pending: ids(|status| matches!(status, OperationStatus::Pending)),
            active_ms: state.usage.active_ms,
        });
        state.revision += 1;
        Ok(())
    }
}

/// One execution as the tool saw it, in virtual milliseconds since the test started.
#[derive(Clone, Debug)]
struct Started {
    id: String,
    start: u64,
    end: Option<u64>,
    /// The last committed checkpoint when `execute` was first polled.
    committed: Snapshot,
}
/// Shared observations of every `sleep` tool execution.
#[derive(Clone)]
struct Sleeper {
    epoch: Instant,
    journal: Journal,
    runs: Arc<Mutex<Vec<Started>>>,
    cleanups: Arc<AtomicUsize>,
}
impl Sleeper {
    fn new(journal: &Journal) -> Self {
        Self {
            epoch: Instant::now(),
            journal: journal.clone(),
            runs: Arc::default(),
            cleanups: Arc::default(),
        }
    }
    fn now(&self) -> u64 {
        self.epoch.elapsed().as_millis() as u64
    }
    fn tool(&self, spec: ToolSpec) -> Arc<dyn Tool> {
        Arc::new(SleepTool {
            spec,
            sleeper: self.clone(),
        })
    }
    fn runs(&self) -> Vec<Started> {
        self.runs.lock().unwrap().clone()
    }
    /// IDs of every call that started, sorted.
    fn started(&self) -> Vec<String> {
        let mut ids: Vec<_> = self.runs().into_iter().map(|run| run.id).collect();
        ids.sort();
        ids
    }
    fn cleanups(&self) -> usize {
        self.cleanups.load(Ordering::SeqCst)
    }
}
/// Sleeps for its argument in milliseconds and returns it.
struct SleepTool {
    spec: ToolSpec,
    sleeper: Sleeper,
}
#[async_trait]
impl Tool for SleepTool {
    fn spec(&self) -> &ToolSpec {
        &self.spec
    }
    fn validate(&self, _: &Value) -> Result<()> {
        Ok(())
    }
    async fn execute(&self, arguments: Value, context: ToolContext) -> Result<ToolOutput<Value>> {
        let sleeper = &self.sleeper;
        let index = {
            let mut runs = sleeper.runs.lock().unwrap();
            runs.push(Started {
                id: context.call_id.clone(),
                start: sleeper.now(),
                end: None,
                committed: sleeper.journal.last(),
            });
            runs.len() - 1
        };
        tokio::time::sleep(Duration::from_millis(arguments.as_u64().unwrap())).await;
        sleeper.runs.lock().unwrap()[index].end = Some(sleeper.now());
        Ok(ToolOutput::Complete(arguments))
    }
    async fn cleanup(&self, _: ToolContext) -> Result<()> {
        self.sleeper.cleanups.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
/// The most calls in flight at any instant.
fn peak(runs: &[Started]) -> usize {
    runs.iter()
        .map(|at| {
            runs.iter()
                .filter(|run| run.start <= at.start && run.end.is_none_or(|end| end > at.start))
                .count()
        })
        .max()
        .unwrap_or(0)
}
/// `(start, end)` of each call, by call ID.
fn spans(runs: &[Started]) -> Vec<(String, u64, Option<u64>)> {
    let mut spans: Vec<_> = runs
        .iter()
        .map(|run| (run.id.clone(), run.start, run.end))
        .collect();
    spans.sort();
    spans
}

#[tokio::test(start_paused = true)]
async fn next_call_starts_when_a_slot_frees_and_results_keep_declaration_order() {
    let journal = Journal::default();
    let sleeper = Sleeper::new(&journal);
    let delays = [300, 10, 10, 10, 300];
    let agent = agent(
        vec![sleeps(&delays)],
        vec![sleeper.tool(spec("sleep"))],
        Limits {
            max_parallel_tools: 4,
            ..Limits::default()
        },
    );
    let mut state = state(&agent);
    Engine::new(agent)
        .drive(&mut state, &journal, RunControl::default())
        .await
        .unwrap();
    let runs = sleeper.runs();
    assert_eq!(
        spans(&runs),
        [
            ("c0".into(), 0, Some(300)),
            ("c1".into(), 0, Some(10)),
            ("c2".into(), 0, Some(10)),
            ("c3".into(), 0, Some(10)),
            ("c4".into(), 10, Some(310)),
        ]
    );
    assert_eq!(peak(&runs), 4);
    assert_eq!(
        results(&state)
            .iter()
            .map(|result| (result.call_id.as_str(), result.value.clone()))
            .collect::<Vec<_>>(),
        delays
            .iter()
            .enumerate()
            .map(|(index, ms)| (["c0", "c1", "c2", "c3", "c4"][index], json!(ms)))
            .collect::<Vec<_>>()
    );
    assert_eq!(state.usage.tool_calls, 5);
    // The window was busy for 310 ms; overlapping calls are not charged again.
    assert_eq!(state.usage.active_ms, 310);
}

#[tokio::test(start_paused = true)]
async fn each_call_times_out_from_its_own_start() {
    let journal = Journal::default();
    let sleeper = Sleeper::new(&journal);
    let agent = agent(
        vec![sleeps(&[10, 30, 45, 60])],
        vec![sleeper.tool(spec("sleep"))],
        Limits {
            max_parallel_tools: 2,
            operation_timeout_ms: 50,
            ..Limits::default()
        },
    );
    let mut state = state(&agent);
    Engine::new(agent)
        .drive(&mut state, &journal, RunControl::default())
        .await
        .unwrap();
    // `c2` starts at 10 ms and ends past 50 ms after the window opened, within its own timeout.
    // `c3` starts at 30 ms and is stopped 50 ms later.
    assert_eq!(
        spans(&sleeper.runs()),
        [
            ("c0".into(), 0, Some(10)),
            ("c1".into(), 0, Some(30)),
            ("c2".into(), 10, Some(55)),
            ("c3".into(), 30, None),
        ]
    );
    let results = results(&state);
    assert_eq!(
        (&results[2].value, results[2].is_error),
        (&json!(45), false)
    );
    assert_eq!(
        (&results[3].value, results[3].is_error),
        (&json!("tool: tool operation timed out"), true)
    );
}

#[tokio::test(start_paused = true)]
async fn sequential_call_waits_for_running_calls_and_runs_alone() {
    let journal = Journal::default();
    let sleeper = Sleeper::new(&journal);
    let mut alone = spec("alone");
    alone.execution = Execution::Sequential;
    let mut calls = sleeps(&[30, 10, 10, 10, 10]);
    calls[2].name = "alone".into();
    let agent = agent(
        vec![calls],
        vec![sleeper.tool(spec("sleep")), sleeper.tool(alone)],
        Limits {
            max_parallel_tools: 2,
            ..Limits::default()
        },
    );
    let mut state = state(&agent);
    Engine::new(agent)
        .drive(&mut state, &journal, RunControl::default())
        .await
        .unwrap();
    assert_eq!(
        spans(&sleeper.runs()),
        [
            ("c0".into(), 0, Some(30)),
            ("c1".into(), 0, Some(10)),
            ("c2".into(), 30, Some(40)),
            ("c3".into(), 40, Some(50)),
            ("c4".into(), 40, Some(50)),
        ]
    );
}

/// Fails every commit once the given call has an outcome, like a database that went away.
struct FailAfterOutcome {
    journal: Journal,
    id: &'static str,
}
#[async_trait]
impl Checkpoint for FailAfterOutcome {
    async fn commit(&self, state: &mut RunState) -> Result<()> {
        if state
            .operations
            .iter()
            .any(|op| op.call.id == self.id && matches!(op.status, OperationStatus::Complete(_)))
        {
            return Err(Error::Storage("injected outcome commit failure".into()));
        }
        self.journal.commit(state).await
    }
}
#[tokio::test(start_paused = true)]
async fn failed_commit_never_starts_the_next_call_and_still_drains_running_calls() {
    let journal = Journal::default();
    let sleeper = Sleeper::new(&journal);
    let agent = agent(
        vec![sleeps(&[10, 50, 10])],
        vec![sleeper.tool(spec("sleep"))],
        Limits {
            max_parallel_tools: 2,
            ..Limits::default()
        },
    );
    let mut state = state(&agent);
    let result = Engine::new(agent)
        .drive(
            &mut state,
            &FailAfterOutcome {
                journal: journal.clone(),
                id: "c0",
            },
            RunControl::default(),
        )
        .await;
    assert!(matches!(result, Err(Error::Storage(_))), "{result:?}");
    // `c2` would take `c0`'s slot, but the commit carrying its intent failed.
    assert_eq!(
        spans(&sleeper.runs()),
        [("c0".into(), 0, Some(10)), ("c1".into(), 0, Some(50))]
    );
    assert_eq!(sleeper.cleanups(), 2);
    assert_eq!(journal.ever_dispatched(), ids(&["c0", "c1"]));
}

#[tokio::test(start_paused = true)]
async fn intent_and_active_time_are_committed_before_each_call_starts() {
    let journal = Journal::default();
    let sleeper = Sleeper::new(&journal);
    let limits = Limits {
        max_parallel_tools: 2,
        ..Limits::default()
    };
    let lease = limits.operation_timeout_ms + limits.cleanup_timeout_ms;
    let agent = agent(
        vec![sleeps(&[30, 10, 20, 10, 10])],
        vec![sleeper.tool(spec("sleep"))],
        limits,
    );
    let mut state = state(&agent);
    Engine::new(agent)
        .drive(&mut state, &journal, RunControl::default())
        .await
        .unwrap();
    assert_eq!(
        state.phase,
        Phase::Settled(Settlement::Completed(json!("done")))
    );
    let runs = sleeper.runs();
    assert_eq!(runs.len(), 5);
    for run in &runs {
        assert!(
            run.committed.dispatched.contains(&run.id),
            "started before its intent was committed: {run:?}"
        );
        // Model turns take no virtual time, so all active time so far is the tool window's.
        assert!(
            run.committed.active_ms >= run.start + lease,
            "started without a durable reservation covering its timeout: {run:?}"
        );
    }
    // No checkpoint ever recorded a call as dispatched that did not then start.
    assert_eq!(journal.ever_dispatched(), sleeper.started());
    assert_eq!(state.usage.tool_calls, 5);
    assert_eq!(sleeper.cleanups(), 5);
}

#[tokio::test(start_paused = true)]
async fn calls_behind_a_wait_are_never_committed_as_dispatched() {
    let journal = Journal::default();
    let sleeper = Sleeper::new(&journal);
    let wait = typed_tool::<u32, Value, _, _>(spec("wait"), |_, _| async {
        Ok(ToolOutput::Wait(WaitRequest::Input { prompt: "?".into() }))
    });
    let mut calls = vec![call("block", "wait", json!(0))];
    calls.extend(sleeps(&[10, 10, 10]));
    let agent = agent(
        vec![calls],
        vec![sleeper.tool(spec("sleep")), wait],
        Limits {
            max_parallel_tools: 2,
            ..Limits::default()
        },
    );
    let mut state = state(&agent);
    Engine::new(agent)
        .drive(&mut state, &journal, RunControl::default())
        .await
        .unwrap();
    assert_eq!(state.phase, Phase::Tools);
    assert_eq!(sleeper.started(), ids(&["c0"]));
    assert_eq!(journal.ever_dispatched(), ids(&["block", "c0"]));
    assert_eq!(journal.last().pending, ids(&["c1", "c2"]));
    assert_eq!(state.usage.tool_calls, 2);
}

struct DenyCall(&'static str);
#[async_trait]
impl Authorizer for DenyCall {
    async fn authorize(&self, _: &RunState, call: &ToolCall) -> Result<bool> {
        Ok(call.id != self.0)
    }
}
#[tokio::test(start_paused = true)]
async fn denial_stops_dispatch_and_keeps_outcomes_already_recorded() {
    // With one slot the first call runs before the second is authorized. With two, both are
    // authorized before either starts, so the denial starts nothing.
    for (slots, started) in [(1, ids(&["c0"])), (2, vec![])] {
        let journal = Journal::default();
        let sleeper = Sleeper::new(&journal);
        let agent = builder(
            vec![sleeps(&[10, 10])],
            vec![sleeper.tool(spec("sleep"))],
            Limits {
                max_parallel_tools: slots,
                ..Limits::default()
            },
        )
        .authorizer(Arc::new(DenyCall("c1")))
        .build()
        .unwrap();
        let mut state = state(&agent);
        Engine::new(agent)
            .drive(&mut state, &journal, RunControl::default())
            .await
            .unwrap();
        assert!(
            matches!(&state.phase, Phase::Settled(Settlement::Failed(reason)) if reason.contains("denied")),
            "{:?}",
            state.phase
        );
        assert_eq!(sleeper.started(), started, "{slots} slots");
        assert_eq!(journal.ever_dispatched(), started, "{slots} slots");
        assert_eq!(
            matches!(state.operations[0].status, OperationStatus::Complete(_)),
            slots == 1
        );
        assert_eq!(state.operations[1].status, OperationStatus::Pending);
    }
}

#[tokio::test(start_paused = true)]
async fn shutdown_drains_running_calls_and_leaves_unstarted_calls_pending() {
    let journal = Journal::default();
    let sleeper = Sleeper::new(&journal);
    let agent = agent(
        vec![sleeps(&[50, 10, 100, 10])],
        vec![sleeper.tool(spec("sleep"))],
        Limits {
            max_parallel_tools: 2,
            ..Limits::default()
        },
    );
    let mut state = state(&agent);
    let shutdown = CancellationToken::new();
    let trigger = shutdown.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        trigger.cancel();
    });
    let result = Engine::new(agent)
        .drive(
            &mut state,
            &journal,
            RunControl {
                shutdown,
                ..RunControl::default()
            },
        )
        .await;
    assert!(matches!(result, Err(Error::Interrupted)), "{result:?}");
    let started = sleeper.started();
    assert!(!started.contains(&"c3".to_owned()));
    assert_eq!(sleeper.cleanups(), started.len());
    // Interrupted read-only calls stay retryable, and nothing that never ran becomes uncertain.
    for op in &state.operations {
        assert!(
            !matches!(op.status, OperationStatus::Unknown { .. }),
            "{op:?}"
        );
    }
    assert_eq!(state.operations[3].status, OperationStatus::Pending);
    assert!(journal.last().pending.contains(&"c3".to_owned()));
    assert_eq!(state.usage.tool_calls as usize, started.len());
}

#[tokio::test(start_paused = true)]
async fn tool_call_limit_stops_a_batch_before_any_of_it_starts() {
    let journal = Journal::default();
    let sleeper = Sleeper::new(&journal);
    let agent = agent(
        vec![sleeps(&[1, 1]), sleeps(&[1, 1])]
            .into_iter()
            .enumerate()
            .map(|(round, calls)| {
                calls
                    .into_iter()
                    .map(|mut call| {
                        call.id = format!("r{round}{}", call.id);
                        call
                    })
                    .collect()
            })
            .collect(),
        vec![sleeper.tool(spec("sleep"))],
        Limits {
            max_tool_calls: 3,
            ..Limits::default()
        },
    );
    let mut state = state(&agent);
    Engine::new(agent)
        .drive(&mut state, &journal, RunControl::default())
        .await
        .unwrap();
    assert_eq!(
        state.phase,
        Phase::Settled(Settlement::Stopped(StopReason::ToolCalls))
    );
    assert_eq!(sleeper.started(), ids(&["r0c0", "r0c1"]));
    assert_eq!(state.usage.tool_calls, 2);
}

#[tokio::test(start_paused = true)]
async fn parallel_calls_charge_their_shared_wall_time_once() {
    let journal = Journal::default();
    let sleeper = Sleeper::new(&journal);
    let agent = agent(
        vec![sleeps(&[100, 100, 100, 100])],
        vec![sleeper.tool(spec("sleep"))],
        Limits::default(),
    );
    let mut state = state(&agent);
    Engine::new(agent)
        .drive(&mut state, &journal, RunControl::default())
        .await
        .unwrap();
    assert_eq!(state.usage.tool_calls, 4);
    assert_eq!(state.usage.active_ms, 100);
}

#[tokio::test(start_paused = true)]
async fn active_budget_caps_the_last_call_and_is_never_exceeded() {
    let journal = Journal::default();
    let sleeper = Sleeper::new(&journal);
    let agent = agent(
        vec![sleeps(&[600, 600, 600])],
        vec![sleeper.tool(spec("sleep"))],
        Limits {
            max_parallel_tools: 1,
            operation_timeout_ms: 1_000,
            cleanup_timeout_ms: 100,
            active_budget_ms: 1_500,
            ..Limits::default()
        },
    );
    let mut state = state(&agent);
    Engine::new(agent)
        .drive(&mut state, &journal, RunControl::default())
        .await
        .unwrap();
    assert_eq!(
        state.phase,
        Phase::Settled(Settlement::Stopped(StopReason::ActiveBudget))
    );
    // The third call gets only the 200 ms the budget leaves before its cleanup allowance.
    let runs = sleeper.runs();
    assert_eq!(
        runs.iter()
            .map(|run| (run.start, run.end))
            .collect::<Vec<_>>(),
        [(0, Some(600)), (600, Some(1_200)), (1_200, None)]
    );
    let results = results(&state);
    assert!(results[2].is_error);
    assert_eq!(results[2].value, json!("tool: tool operation timed out"));
    assert_eq!(state.usage.active_ms, 1_400);
    for snapshot in journal.snapshots() {
        assert!(snapshot.active_ms <= 1_500, "{snapshot:?}");
    }
}
