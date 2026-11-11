//! Fixed work on the same durable host: an instruction delivered in the original input against
//! the same instruction queued with `Host::steer` during the first model request. Both complete
//! 64 runs of two model calls and one read-only tool each. Setup, verification and shutdown are
//! outside the measured interval; see `docs/steering.md`.
use async_trait::async_trait;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures::stream;
use serde_json::{Value, json};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use yielded_agent::{sqlite::SqliteStore, *};

const RUNS: usize = 64;

/// Holds each run's first request until the benchmark releases it, then asks for one tool. The
/// second request must carry the revised instruction and the tool's receipt.
struct ModelGate(mpsc::Sender<(RunId, oneshot::Sender<()>)>);
#[async_trait]
impl Model for ModelGate {
    async fn stream(&self, request: ModelRequest, _: CancellationToken) -> Result<ModelStream> {
        let response = if request.turn == 1 {
            let (release, wait) = oneshot::channel();
            self.0.send((request.run_id, release)).await.unwrap();
            wait.await.unwrap();
            ModelResponse::tools(vec![ToolCall {
                id: "read".into(),
                name: "read".into(),
                arguments: json!({}),
            }])
        } else {
            assert_eq!(request.turn, 2);
            assert!(request.history.iter().any(|entry| match entry {
                HistoryEntry::Input(value) => value == &json!("revised"),
                HistoryEntry::Steering { instruction, .. } => instruction == "revised",
                _ => false,
            }));
            assert!(request.history.iter().any(|entry| matches!(entry,
                HistoryEntry::Exchange { results, .. } if results.len() == 1 && results[0].value == json!(42))));
            ModelResponse::final_output(json!("revised"))
        };
        Ok(Box::pin(stream::once(async {
            Ok(ModelEvent::Response(response))
        })))
    }
}
struct Fixture {
    _directory: tempfile::TempDir,
    store: Arc<SqliteStore>,
    host: Host<SqliteStore>,
    started: mpsc::Receiver<(RunId, oneshot::Sender<()>)>,
    events: mpsc::Receiver<ProgressEvent>,
    steering: bool,
}
impl Fixture {
    async fn new(workers: usize, steering: bool) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteStore::open(directory.path().join("runtime.sqlite"))
                .await
                .unwrap(),
        );
        let (started, receiver) = mpsc::channel(RUNS);
        let tool = typed_tool::<Value, Value, _, _>(
            ToolSpec::new("read", "1")
                .with_description("Return a fixed result")
                .with_retry(RetryPolicy::ReadOnly),
            |_, _| async { Ok(ToolOutput::Complete(json!(42))) },
        );
        let agent = Agent::builder("steering-benchmark-v1", Arc::new(ModelGate(started)))
            .instructions("Follow the revised instruction.")
            .tool(tool)
            .build()
            .unwrap();
        let (sender, events) = mpsc::channel(16_384);
        let options = HostOptions::default()
            .with_max_concurrent_runs(workers)
            .with_poll_interval(Duration::from_millis(1))
            .with_events(sender);
        let host = Host::start(store.clone(), agent, options).unwrap();
        Self {
            _directory: directory,
            store,
            host,
            started: receiver,
            events,
            steering,
        }
    }
    async fn execute(&mut self) -> Vec<RunState> {
        let host = &self.host;
        let started = &mut self.started;
        let events = &mut self.events;
        let steering = self.steering;
        let submit = async {
            for i in 0..RUNS {
                let input = json!(if steering { "original" } else { "revised" });
                host.submit(&format!("conversation-{i}"), "run", input, None)
                    .await
                    .unwrap();
            }
        };
        let direct = async {
            for _ in 0..RUNS {
                let (id, release) = started.recv().await.unwrap();
                if steering {
                    host.steer(id, "revision", "revised").await.unwrap();
                }
                release.send(()).unwrap();
            }
        };
        let observe = async {
            let mut states = Vec::with_capacity(RUNS);
            while states.len() < RUNS {
                let event = events.recv().await.unwrap();
                if matches!(event.kind, ProgressKind::Settled) {
                    states.push(host.status(event.run_id).await.unwrap());
                }
            }
            states
        };
        let (_, _, states) = tokio::time::timeout(Duration::from_secs(30), async {
            tokio::join!(submit, direct, observe)
        })
        .await
        .unwrap();
        states
    }
    async fn verify_and_close(mut self, states: Vec<RunState>) {
        assert_eq!(states.len(), RUNS);
        let mut ids = std::collections::HashSet::new();
        for state in states {
            assert!(ids.insert(state.id));
            assert_eq!(
                state.phase,
                Phase::Settled(Settlement::Completed(json!("revised")))
            );
            assert_eq!((state.usage.model_calls, state.usage.tool_calls), (2, 1));
            let inputs = self.store.steering(state.id).await.unwrap();
            assert_eq!(inputs.len(), usize::from(self.steering));
            assert!(
                inputs
                    .iter()
                    .all(|input| matches!(input.status, SteeringStatus::Applied { .. }))
            );
        }
        self.host.shutdown().await.unwrap();
        self.store.close().await.unwrap();
    }
}
fn configured() -> Criterion {
    let quick = std::env::var_os("AGENT_BENCH_QUICK").is_some();
    Criterion::default()
        .sample_size(if quick { 10 } else { 30 })
        .warm_up_time(Duration::from_millis(if quick { 100 } else { 1_000 }))
        .measurement_time(Duration::from_millis(if quick { 250 } else { 2_000 }))
}
fn scheduling(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("steering_schedule");
    group.throughput(Throughput::Elements(RUNS as u64));
    for workers in [1, 16, 64] {
        for steering in [false, true] {
            let name = if steering {
                "queued_input"
            } else {
                "direct_input"
            };
            group.bench_function(BenchmarkId::new(name, workers), |b| {
                b.iter_custom(|iterations| {
                    runtime.block_on(async {
                        let mut elapsed = Duration::ZERO;
                        for _ in 0..iterations {
                            let mut fixture = Fixture::new(workers, steering).await;
                            let start = Instant::now();
                            let states = fixture.execute().await;
                            elapsed += start.elapsed();
                            fixture.verify_and_close(states).await;
                        }
                        elapsed
                    })
                });
            });
        }
    }
    group.finish();
}
criterion_group! { name = benches; config = configured(); targets = scheduling }
criterion_main!(benches);
