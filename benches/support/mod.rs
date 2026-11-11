#![allow(dead_code)]

use agent_rs::*;
use async_trait::async_trait;
use futures::{StreamExt, stream};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

pub const FIXTURE_VERSION: &str = "agent-loop-v4";
pub const WORKERS: usize = 4;
pub const CONCURRENT_TOTAL: usize = 64;

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Input {
    pub request: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Output {
    pub answer: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Arguments {
    n: usize,
}

#[derive(Default)]
pub struct Counts {
    pub model: AtomicUsize,
    pub model_cleanup: AtomicUsize,
    pub tools: AtomicUsize,
    pub tool_cleanup: AtomicUsize,
    pub active: AtomicUsize,
    pub peak: AtomicUsize,
    pub chunks: AtomicUsize,
}
#[derive(Clone, Debug, Default, Serialize)]
pub struct Snapshot {
    pub model: usize,
    pub model_cleanup: usize,
    pub tools: usize,
    pub tool_cleanup: usize,
    pub active: usize,
    pub peak: usize,
    pub chunks: usize,
}
impl Counts {
    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            model: self.model.load(Ordering::SeqCst),
            model_cleanup: self.model_cleanup.load(Ordering::SeqCst),
            tools: self.tools.load(Ordering::SeqCst),
            tool_cleanup: self.tool_cleanup.load(Ordering::SeqCst),
            active: self.active.load(Ordering::SeqCst),
            peak: self.peak.load(Ordering::SeqCst),
            chunks: self.chunks.load(Ordering::SeqCst),
        }
    }
}

struct CheckedModel {
    counts: Arc<Counts>,
    turns: Vec<Vec<ModelEvent>>,
    rounds: usize,
    batch: usize,
    expected_seed_entries: usize,
}
#[async_trait]
impl Model for CheckedModel {
    async fn stream(&self, request: ModelRequest, _: CancellationToken) -> Result<ModelStream> {
        self.counts.model.fetch_add(1, Ordering::SeqCst);
        assert!(
            (1..=self.turns.len()).contains(&(request.turn as usize)),
            "unexpected model turn"
        );
        let seed_entries = request
            .history
            .iter()
            .filter(|entry| match entry {
                HistoryEntry::Input(value) => value["request"]
                    .as_str()
                    .is_some_and(|text| text.starts_with("prior question ")),
                HistoryEntry::Exchange { response, .. } => response
                    .output
                    .as_ref()
                    .and_then(|value| value["answer"].as_str())
                    .is_some_and(|text| text.starts_with("prior answer ")),
                _ => false,
            })
            .count();
        assert_eq!(
            seed_entries, self.expected_seed_entries,
            "model must receive all seeded context"
        );
        let exchanges = request
            .history
            .iter()
            .filter_map(|entry| match entry {
                HistoryEntry::Exchange { response, results } if !response.tools.is_empty() => {
                    Some((response, results))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            exchanges.len(),
            request.turn as usize - 1,
            "model must receive every completed round"
        );
        for (round, (response, results)) in exchanges.iter().enumerate() {
            assert_eq!(results.len(), self.batch);
            assert_eq!(response.tools.len(), self.batch);
            for (index, result) in results.iter().enumerate() {
                let n = round * self.batch + index;
                assert_eq!(result.call_id, format!("round-{round}-{index}"));
                assert_eq!(
                    response.tools[index].id, result.call_id,
                    "history order changed"
                );
                assert_eq!(result.value, json!(n));
                assert!(!result.is_error);
            }
        }
        assert!(exchanges.len() <= self.rounds);
        let counts = self.counts.clone();
        let events = self.turns[request.turn as usize - 1].clone();
        Ok(Box::pin(stream::iter(events.into_iter().map(Ok)).inspect(
            move |event| {
                if matches!(event, Ok(ModelEvent::TextDelta(_))) {
                    counts.chunks.fetch_add(1, Ordering::SeqCst);
                }
            },
        )))
    }
    async fn cleanup(&self, _: RunId) -> Result<()> {
        self.counts.model_cleanup.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
struct CheckedTool {
    inner: Arc<dyn Tool>,
    counts: Arc<Counts>,
}
#[async_trait]
impl Tool for CheckedTool {
    fn spec(&self) -> &ToolSpec {
        self.inner.spec()
    }
    fn validate(&self, args: &Value) -> Result<()> {
        self.inner.validate(args)
    }
    async fn execute(&self, args: Value, context: ToolContext) -> Result<ToolOutput<Value>> {
        self.counts.tools.fetch_add(1, Ordering::SeqCst);
        let active = self.counts.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.counts.peak.fetch_max(active, Ordering::SeqCst);
        // A deterministic cooperative suspension makes concurrency observable without sleeps or I/O.
        tokio::task::yield_now().await;
        self.inner.execute(args, context).await
    }
    async fn cleanup(&self, context: ToolContext) -> Result<()> {
        self.inner.cleanup(context).await?;
        self.counts.active.fetch_sub(1, Ordering::SeqCst);
        self.counts.tool_cleanup.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

pub struct PreparedRun {
    conversation: String,
    input: Value,
    history: Vec<HistoryEntry>,
}
impl PreparedRun {
    pub fn admit<I, O>(self, agent: &Agent<I, O>) -> RunState {
        agent
            .new_run(self.conversation, self.input, &self.history, None)
            .unwrap()
    }
}

pub struct Fixture {
    pub agent: Agent<Input, Output>,
    pub counts: Arc<Counts>,
    pub rounds: usize,
    pub batch: usize,
    pub chunks: usize,
    pub bound: usize,
    pub expected: Output,
    pub history: Vec<HistoryEntry>,
}
impl Fixture {
    pub fn new(
        rounds: usize,
        batch: usize,
        bound: usize,
        chunks: usize,
        history_entries: usize,
    ) -> Self {
        let counts = Arc::new(Counts::default());
        let expected = Output {
            answer: if chunks > 0 {
                "x".repeat(1024)
            } else {
                "done".into()
            },
        };
        let mut turns = (0..rounds)
            .map(|round| {
                vec![ModelEvent::Response(ModelResponse::tools(
                    (0..batch)
                        .map(|index| ToolCall {
                            id: format!("round-{round}-{index}"),
                            name: "echo".into(),
                            arguments: json!({"n": round * batch + index}),
                        })
                        .collect(),
                ))]
            })
            .collect::<Vec<_>>();
        let mut last = if chunks == 0 {
            Vec::new()
        } else {
            assert_eq!(1024 % chunks, 0);
            vec![ModelEvent::TextDelta("x".repeat(1024 / chunks)); chunks]
        };
        last.push(ModelEvent::Response(ModelResponse::final_output(
            serde_json::to_value(&expected).unwrap(),
        )));
        turns.push(last);
        let model = Arc::new(CheckedModel {
            counts: counts.clone(),
            turns,
            rounds,
            batch,
            expected_seed_entries: history_entries,
        });
        let spec = ToolSpec::new("echo", "1")
            .with_description("Return a deterministic integer")
            .with_retry(RetryPolicy::ReadOnly);
        let inner = typed_tool::<Arguments, usize, _, _>(spec, |args, _| async move {
            Ok(ToolOutput::Complete(args.n))
        });
        let tool = Arc::new(CheckedTool {
            inner,
            counts: counts.clone(),
        });
        let agent = Agent::builder("benchmark", model)
            .instructions("Return the scripted result.")
            .tool(tool)
            .limits(Limits::default().with_max_parallel_tools(bound))
            .input::<Input>()
            .output::<Output>()
            .build()
            .unwrap();
        let history = (0..history_entries)
            .map(|n| {
                if n % 2 == 0 {
                    HistoryEntry::Input(json!({"request": format!("prior question {n}")}))
                } else {
                    HistoryEntry::Exchange {
                        response: ModelResponse::final_output(
                            json!({"answer": format!("prior answer {n}")}),
                        ),
                        results: vec![],
                    }
                }
            })
            .collect();
        Self {
            agent,
            counts,
            rounds,
            batch,
            chunks,
            bound,
            expected,
            history,
        }
    }
    pub fn prepare(&self) -> PreparedRun {
        PreparedRun {
            conversation: uuid::Uuid::new_v4().to_string(),
            input: self.input(),
            history: self.history.clone(),
        }
    }
    pub fn input(&self) -> Value {
        json!({"request": "complete the fixed workload"})
    }
    pub async fn execute(&self, prepared: PreparedRun) -> RunState {
        let mut state = prepared.admit(&self.agent);
        self.agent
            .drive(&mut state, &MemoryCheckpoint, RunControl::default())
            .await
            .unwrap();
        state
    }
    pub fn verify_state(&self, state: &RunState) {
        assert_eq!(
            state.phase,
            Phase::Settled(Settlement::Completed(
                serde_json::to_value(&self.expected).unwrap()
            ))
        );
        assert_eq!(state.usage.model_calls as usize, self.rounds + 1);
        assert_eq!(state.usage.tool_calls as usize, self.rounds * self.batch);
        assert_eq!(
            state.history.len(),
            self.rounds + 2,
            "input, each tool round, and final output must be retained"
        );
        for entry in &state.history {
            if let HistoryEntry::Exchange { response, results } = entry {
                assert_eq!(response.tools.len(), results.len());
                for (call, result) in response.tools.iter().zip(results) {
                    assert_eq!(call.id, result.call_id);
                    assert_eq!(call.arguments["n"], result.value);
                    assert!(!result.is_error);
                }
            }
        }
    }
    pub fn verify_counts(&self, before: &Snapshot, runs: usize, concurrent: usize) {
        let after = self.counts.snapshot();
        assert_eq!(after.model - before.model, runs * (self.rounds + 1));
        assert_eq!(
            after.model_cleanup - before.model_cleanup,
            runs * (self.rounds + 1)
        );
        assert_eq!(after.tools - before.tools, runs * self.rounds * self.batch);
        assert_eq!(
            after.tool_cleanup - before.tool_cleanup,
            runs * self.rounds * self.batch
        );
        assert_eq!(after.chunks - before.chunks, runs * self.chunks);
        assert_eq!(
            after.active, 0,
            "all run-owned tool work must be cleaned up"
        );
        assert!(after.peak <= self.bound * concurrent);
    }
}

pub fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(WORKERS)
        .enable_all()
        .build()
        .unwrap()
}

/// Fixed total work across concurrency levels, with each run's submission-to-completion latency.
pub async fn concurrent(
    fixture: &Fixture,
    states: Vec<PreparedRun>,
    active: usize,
) -> Vec<(RunState, Duration)> {
    stream::iter(states.into_iter().map(|prepared| {
        let agent = fixture.agent.clone();
        async move {
            let started = Instant::now();
            let state = tokio::spawn(async move {
                let mut state = prepared.admit(&agent);
                agent
                    .drive(&mut state, &MemoryCheckpoint, RunControl::default())
                    .await
                    .unwrap();
                state
            })
            .await
            .unwrap();
            (state, started.elapsed())
        }
    }))
    .buffer_unordered(active)
    .collect()
    .await
}

pub fn process_cpu_ns() -> u64 {
    // CLOCK_PROCESS_CPUTIME_ID includes CPU consumed by all Tokio and SQLite workers.
    let mut value = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    let result = unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut value) };
    assert_eq!(result, 0);
    value.tv_sec as u64 * 1_000_000_000 + value.tv_nsec as u64
}

pub fn percentile(values: &[u64], quantile: f64) -> u64 {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    sorted[((sorted.len() - 1) as f64 * quantile).round() as usize]
}

#[cfg(feature = "sqlite")]
pub mod durable {
    use super::*;
    use agent_rs::sqlite::SqliteStore;
    use agent_rs::{Host, HostOptions};
    use std::collections::{HashMap, HashSet};
    use std::future::Future;
    use std::path::Path;

    pub struct Resident {
        pub store: Arc<SqliteStore>,
        pub host: Host<SqliteStore>,
        pub events: tokio::sync::mpsc::Receiver<ProgressEvent>,
        _directory: tempfile::TempDir,
    }
    impl Resident {
        pub async fn new(fixture: &Fixture, active: usize) -> Self {
            Self::start(fixture, active, tempfile::tempdir().unwrap()).await
        }
        /// A resident host over a copy of the template's database and retained conversation.
        pub async fn with_context(
            fixture: &Fixture,
            active: usize,
            template: &ContextTemplate,
        ) -> Self {
            let directory = tempfile::tempdir().unwrap();
            template.copy_to(&directory.path().join("runtime.sqlite"));
            Self::start(fixture, active, directory).await
        }
        async fn start(fixture: &Fixture, active: usize, directory: tempfile::TempDir) -> Self {
            let store = Arc::new(
                SqliteStore::open(directory.path().join("runtime.sqlite"))
                    .await
                    .unwrap(),
            );
            let (sender, events) = tokio::sync::mpsc::channel(16_384);
            let host = Host::start(
                store.clone(),
                fixture.agent.clone(),
                HostOptions::default()
                    .with_max_concurrent_runs(active)
                    .with_poll_interval(Duration::from_millis(1))
                    .with_events(sender),
            )
            .unwrap();
            let mut resident = Self {
                store,
                host,
                events,
                _directory: directory,
            };
            let warmup = resident.run(fixture).await;
            fixture.verify_state(&warmup);
            resident
        }
        pub async fn run(&mut self, fixture: &Fixture) -> RunState {
            let conversation = uuid::Uuid::new_v4().to_string();
            self.run_in(fixture, &conversation).await
        }
        pub async fn run_in(&mut self, fixture: &Fixture, conversation: &str) -> RunState {
            let id = self
                .host
                .submit(conversation, "input", fixture.input(), None)
                .await
                .unwrap();
            self.wait(id).await
        }
        pub async fn wait(&mut self, id: RunId) -> RunState {
            tokio::time::timeout(Duration::from_secs(30), async {
                loop {
                    let event = self
                        .events
                        .recv()
                        .await
                        .expect("host progress channel closed");
                    if event.run_id == id && matches!(event.kind, ProgressKind::Settled) {
                        return self.host.status(id).await.unwrap();
                    }
                }
            })
            .await
            .expect("durable benchmark failed to settle")
        }
        pub async fn batch(
            &mut self,
            fixture: &Fixture,
            count: usize,
        ) -> Vec<(RunState, Duration)> {
            assert!((1..=CONCURRENT_TOTAL).contains(&count));
            let host = &self.host;
            let events = &mut self.events;
            let submit = async {
                let mut starts = Vec::with_capacity(count);
                for _ in 0..count {
                    let conversation = uuid::Uuid::new_v4().to_string();
                    let started = Instant::now();
                    let id = host
                        .submit(&conversation, "input", fixture.input(), None)
                        .await
                        .unwrap();
                    starts.push((id, started));
                }
                starts
            };
            let observe = async {
                let mut completed = Vec::with_capacity(count);
                let mut seen = HashSet::with_capacity(count);
                let mut statuses = stream::FuturesUnordered::new();
                while completed.len() < count {
                    tokio::select! {
                        event = events.recv() => {
                            let event = event.expect("host progress channel closed");
                            if matches!(event.kind, ProgressKind::Settled) {
                                let run_id = event.run_id;
                                assert!(seen.insert(run_id), "duplicate settlement event");
                                assert!(seen.len() <= count, "unexpected settlement event");
                                statuses.push(async move {
                                    let state = host.status(run_id).await.unwrap();
                                    let completed_at = Instant::now();
                                    assert!(state.is_terminal(), "settlement event preceded durable completion");
                                    (state, completed_at)
                                });
                            }
                        }
                        result = statuses.next(), if !statuses.is_empty() => {
                            completed.push(result.expect("missing status result"));
                        }
                    }
                }
                assert!(statuses.is_empty(), "unjoined status query");
                completed
            };
            tokio::time::timeout(
                Duration::from_secs(30),
                join_batch_observation(count, submit, observe),
            )
            .await
            .expect("durable concurrent benchmark failed to settle")
        }
        pub async fn close(mut self) {
            self.host.shutdown().await.unwrap();
            drop(self.host);
            self.store.close().await.unwrap();
        }
    }

    /// Poll admission and observation together. Completion may arrive before its
    /// admission future returns, so join by RunId only after both sides finish.
    /// Both futures are owned here: cancellation never leaves an observer task behind.
    pub async fn join_batch_observation(
        count: usize,
        submit: impl Future<Output = Vec<(RunId, Instant)>>,
        observe: impl Future<Output = Vec<(RunState, Instant)>>,
    ) -> Vec<(RunState, Duration)> {
        let (starts, completed) = tokio::join!(submit, observe);
        assert_eq!(starts.len(), count, "missing admission");
        assert_eq!(completed.len(), count, "missing completion observation");
        let mut starts: HashMap<_, _> = starts.into_iter().collect();
        assert_eq!(starts.len(), count, "duplicate admission ID");
        let completed = completed
            .into_iter()
            .map(|(state, completed_at)| {
                let started = starts.remove(&state.id).expect("unmatched completion ID");
                let latency = completed_at
                    .checked_duration_since(started)
                    .expect("completion timestamp preceded admission");
                (state, latency)
            })
            .collect();
        assert!(starts.is_empty(), "admission was not observed completing");
        completed
    }
    pub const CONTEXT_CONVERSATION: &str = "retained-context";
    /// A closed database whose conversation retains `seeds` completed runs, each with an input
    /// of `SEED_BYTES` text. Copied per sample so seeding stays outside the timed interval.
    pub struct ContextTemplate {
        directory: tempfile::TempDir,
        pub seeds: usize,
    }
    impl ContextTemplate {
        pub const SEED_BYTES: usize = 64 * 1024;
        pub async fn new(seeds: usize) -> Self {
            let directory = tempfile::tempdir().unwrap();
            let store = SqliteStore::open(directory.path().join("template.sqlite"))
                .await
                .unwrap();
            // Final-only seed runs: their inputs and answers do not match the measured
            // fixture's checks for seeded history or tool rounds.
            let seed = Fixture::new(0, 0, 4, 0, 0);
            for index in 0..seeds {
                let input = serde_json::json!({
                    "request": format!("retained context {index:02} {}", "x".repeat(Self::SEED_BYTES)),
                });
                let id = store
                    .accept(
                        &seed.agent.erased(),
                        CONTEXT_CONVERSATION,
                        &format!("seed-{index}"),
                        input,
                        None,
                    )
                    .await
                    .unwrap();
                let mut state = store.claim(id).await.unwrap();
                seed.agent
                    .drive(&mut state, &store, RunControl::default())
                    .await
                    .unwrap();
                seed.verify_state(&state);
            }
            store.close().await.unwrap();
            Self { directory, seeds }
        }
        pub fn copy_to(&self, path: &Path) {
            std::fs::copy(self.directory.path().join("template.sqlite"), path).unwrap();
        }
        pub fn verify_state(&self, fixture: &Fixture, state: &RunState) {
            fixture.verify_state(state);
            assert_eq!(
                state.prior_context.len(),
                self.seeds * 2,
                "the run must retain every seeded input and answer"
            );
        }
    }

    struct StopAfterRound {
        store: Arc<SqliteStore>,
    }
    #[async_trait]
    impl Checkpoint for StopAfterRound {
        async fn commit(&self, state: &mut RunState) -> Result<()> {
            self.store.commit(state).await?;
            if matches!(state.phase, Phase::Ready)
                && state.history.iter().any(|entry| {
                    matches!(entry,
                HistoryEntry::Exchange { response, .. } if !response.tools.is_empty())
                })
            {
                return Err(Error::Interrupted);
            }
            Ok(())
        }
    }
    pub struct RecoveryTemplate {
        directory: tempfile::TempDir,
        pub id: RunId,
        prior_runs: usize,
    }
    impl RecoveryTemplate {
        pub async fn new(fixture: &Fixture, prior_runs: usize) -> Self {
            let directory = tempfile::tempdir().unwrap();
            let store = Arc::new(
                SqliteStore::open(directory.path().join("template.sqlite"))
                    .await
                    .unwrap(),
            );
            let seed = Fixture::new(0, 0, 4, 0, 0);
            for index in 0..prior_runs {
                let id = store
                    .accept(
                        &seed.agent.erased(),
                        "recovery",
                        &format!("seed-{index}"),
                        seed.input(),
                        None,
                    )
                    .await
                    .unwrap();
                let mut state = store.claim(id).await.unwrap();
                seed.agent
                    .drive(&mut state, &*store, RunControl::default())
                    .await
                    .unwrap();
                seed.verify_state(&state);
            }
            let id = store
                .accept(
                    &fixture.agent.erased(),
                    "recovery",
                    "resume-me",
                    fixture.input(),
                    None,
                )
                .await
                .unwrap();
            let mut state = store.claim(id).await.unwrap();
            let result = fixture
                .agent
                .drive(
                    &mut state,
                    &StopAfterRound {
                        store: store.clone(),
                    },
                    RunControl::default(),
                )
                .await;
            assert!(
                matches!(result, Err(Error::Storage(_))),
                "recovery fixture must stop at its retained commit boundary: {result:?}"
            );
            assert_eq!(state.usage.model_calls, 1);
            assert_eq!(state.usage.tool_calls, 1);
            assert!(matches!(state.phase, Phase::Ready));
            store.close().await.unwrap();
            Self {
                directory,
                id,
                prior_runs,
            }
        }
        pub fn copy_to(&self, path: &Path) {
            std::fs::copy(self.directory.path().join("template.sqlite"), path).unwrap();
        }
        pub async fn resume(&self, fixture: &Fixture, path: &Path) -> (RunState, Arc<SqliteStore>) {
            let store = Arc::new(SqliteStore::open(path).await.unwrap());
            let mut state = store.claim(self.id).await.unwrap();
            assert_eq!(
                state.prior_context.len(),
                self.prior_runs * 2,
                "recovery must retain the full bounded seed context"
            );
            fixture
                .agent
                .drive(&mut state, &*store, RunControl::default())
                .await
                .unwrap();
            (state, store)
        }
        pub fn verify_counts(&self, fixture: &Fixture, before: &Snapshot) {
            let after = fixture.counts.snapshot();
            assert_eq!(after.model - before.model, 4);
            assert_eq!(after.model_cleanup - before.model_cleanup, 4);
            assert_eq!(after.tools - before.tools, 3);
            assert_eq!(after.tool_cleanup - before.tool_cleanup, 3);
            assert_eq!(after.active, 0);
        }
    }
}
