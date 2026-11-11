//! Attached subagents under Criterion: a parent that delegates to 1 or 4 children, in memory and
//! through a resident SQLite host. Scripts and children are deterministic, and every iteration
//! verifies the typed result, each child's settlement and the exact work counts, so skipping
//! work fails the benchmark. See `docs/benchmarks.md`.
use agent_rs::{sqlite::SqliteStore, *};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use serde_json::{Value, json};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

fn configured() -> Criterion {
    let quick = std::env::var_os("AGENT_BENCH_QUICK").is_some();
    Criterion::default()
        .sample_size(if quick { 10 } else { 30 })
        .warm_up_time(Duration::from_millis(if quick { 100 } else { 1_000 }))
        .measurement_time(Duration::from_millis(if quick { 250 } else { 2_000 }))
}
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap()
}

/// A parent that delegates `children` tasks at once, each to a child that does one tool round.
fn parent(children: usize) -> Agent<String, Vec<String>> {
    let echo = typed_tool::<String, String, _, _>(
        ToolSpec::new("echo", "1").with_retry(RetryPolicy::ReadOnly),
        |text, _| async move {
            tokio::task::yield_now().await;
            Ok(ToolOutput::Complete(text))
        },
    );
    let child = Agent::builder(
        "bench-child",
        Arc::new(ScriptedModel::new(vec![
            ModelResponse::tools(vec![ToolCall {
                id: "echo".into(),
                name: "echo".into(),
                arguments: json!("found"),
            }]),
            ModelResponse::final_output(json!("found")),
        ])),
    )
    .tool(echo)
    .limits(
        Limits::default()
            .with_max_model_calls(2)
            .with_max_tool_calls(1)
            .with_max_tokens(1_000)
            .with_active_budget(Duration::from_secs(60)),
    )
    .input::<String>()
    .output::<String>()
    .build()
    .unwrap();
    let delegations = (0..children)
        .map(|index| ToolCall {
            id: format!("child-{index}"),
            name: "research".into(),
            arguments: json!(format!("task {index}")),
        })
        .collect();
    Agent::builder(
        "bench-parent",
        Arc::new(ScriptedModel::new(vec![
            ModelResponse::tools(delegations),
            ModelResponse::final_output(json!(vec!["found"; children])),
        ])),
    )
    .input::<String>()
    .output::<Vec<String>>()
    .subagents(
        children,
        [Subagent::new("research", "Research one task.", child)],
    )
    .build()
    .unwrap()
}
fn verify(state: &RunState, children: usize) {
    assert_eq!(
        state.phase,
        Phase::Settled(Settlement::Completed(json!(vec!["found"; children])))
    );
    assert_eq!(state.children.len(), children);
    for child in &state.children {
        let result = child.result::<String>().unwrap();
        assert_eq!(result.outcome, ChildOutcome::Completed("found".into()));
    }
    let children = children as u32;
    assert_eq!(state.usage.model_calls, 2 + 2 * children);
    assert_eq!(state.usage.tool_calls, 2 * children);
}

fn workloads(c: &mut Criterion) {
    let runtime = runtime();
    let mut group = c.benchmark_group("attached-children-v2");
    group.throughput(Throughput::Elements(1));
    for children in [1, 4] {
        let agent = parent(children);
        group.bench_with_input(BenchmarkId::new("memory", children), &children, |b, _| {
            b.iter_custom(|iterations| {
                runtime.block_on(async {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iterations {
                        let started = Instant::now();
                        let mut state = agent.new_run("bench", json!("go"), &[], None).unwrap();
                        agent
                            .drive(&mut state, &MemoryCheckpoint, RunControl::default())
                            .await
                            .unwrap();
                        elapsed += started.elapsed();
                        verify(&state, children);
                    }
                    elapsed
                })
            })
        });
        group.bench_with_input(BenchmarkId::new("durable", children), &children, |b, _| {
            b.iter_custom(|iterations| {
                runtime.block_on(async {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iterations {
                        let resident = Resident::start(&agent).await;
                        resident.run("warm-up").await;
                        let started = Instant::now();
                        let state = resident.run("measured").await;
                        elapsed += started.elapsed();
                        verify(&state, children);
                        resident.stop().await;
                    }
                    elapsed
                })
            })
        });
    }
    group.finish();
}

/// A fresh database and resident host.
struct Resident {
    _directory: tempfile::TempDir,
    store: Arc<SqliteStore>,
    host: Host<SqliteStore>,
}
impl Resident {
    async fn start(agent: &Agent<String, Vec<String>>) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteStore::open(directory.path().join("bench.sqlite"))
                .await
                .unwrap(),
        );
        let options = HostOptions::default().with_poll_interval(Duration::from_millis(1));
        let host = Host::start(store.clone(), agent.clone(), options).unwrap();
        Self {
            _directory: directory,
            store,
            host,
        }
    }
    /// Submits one run and reads its status once it settles.
    async fn run(&self, conversation: &str) -> RunState {
        let id = self
            .host
            .submit(conversation, "key", Value::from("go"), None)
            .await
            .unwrap();
        loop {
            let state = self.host.status(id).await.unwrap();
            if state.is_terminal() {
                return state;
            }
            tokio::task::yield_now().await;
        }
    }
    async fn stop(mut self) {
        self.host.shutdown().await.unwrap();
        self.store.close().await.unwrap();
    }
}

criterion_group! { name = benches; config = configured(); targets = workloads }
criterion_main!(benches);
