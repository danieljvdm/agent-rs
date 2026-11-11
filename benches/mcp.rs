//! Warm calls through an MCP tool against the same contract served in-process, and cold
//! connection cost. See `docs/mcp.md`; run with `--features mcp`.
use agent_rs::mcp::{McpClient, McpOptions, ServerIdentity, ToolPolicy};
use agent_rs::{
    Agent, Error, Execution, HistoryEntry, MemoryCheckpoint, ModelResponse, Phase, Result,
    RetryPolicy, RunControl, RunState, ScriptedModel, Settlement, Tool, ToolCall, ToolContext,
    ToolOutput, ToolSpec,
};
use async_trait::async_trait;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use tempfile::TempDir;
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

const FIXTURE: &str = "mcp-stdio-v1";
const SERVER: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/support/mcp_server.py");

fn root(paginated: bool) -> TempDir {
    let directory = tempfile::tempdir().unwrap();
    let config = if paginated {
        json!({"catalog_size":32, "list_mode":"paginate", "page_size":8})
    } else {
        json!({})
    };
    std::fs::write(
        directory.path().join("control.json"),
        serde_json::to_vec(&config).unwrap(),
    )
    .unwrap();
    directory
}
async fn connect(root: &Path) -> McpClient {
    let mut command = Command::new("python3");
    command.arg("-u").arg(SERVER).arg(root).arg("--benchmark");
    McpClient::connect(
        ServerIdentity::new("benchmark", FIXTURE),
        command,
        McpOptions::default(),
        CancellationToken::new(),
    )
    .await
    .unwrap()
}
fn tool(client: &McpClient) -> Arc<dyn Tool> {
    client
        .tool(
            "echo",
            "echo",
            ToolPolicy::default()
                .with_retry(RetryPolicy::ReadOnly)
                .with_execution(Execution::Parallel)
                .with_requires_approval(false),
        )
        .unwrap()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Echo {
    text: String,
}
/// Keep identical advertised/persisted contracts and output bytes on both paths.
/// The local path uses ordinary typed deserialization and an in-process handler.
struct LocalTool(ToolSpec);
#[async_trait]
impl Tool for LocalTool {
    fn spec(&self) -> &ToolSpec {
        &self.0
    }
    fn validate(&self, arguments: &Value) -> Result<()> {
        let value = Echo::deserialize(arguments).map_err(Error::model_output)?;
        if value.text.is_empty() {
            return Err(Error::model_output("empty echo"));
        }
        Ok(())
    }
    async fn execute(&self, arguments: Value, _: ToolContext) -> Result<ToolOutput<Value>> {
        let value: Echo = serde_json::from_value(arguments).map_err(Error::model_output)?;
        tokio::task::yield_now().await;
        Ok(ToolOutput::Complete(json!({
            "content":[{"type":"text", "text":value.text}], "structuredContent":{"text":value.text},
            "_meta":{"fixture":"retained"},
        })))
    }
}
fn agent(tool: Arc<dyn Tool>, batch: usize) -> Agent {
    let calls = (0..batch)
        .map(|n| ToolCall {
            id: n.to_string(),
            name: "echo".into(),
            arguments: json!({"text":format!("echo-{n}")}),
        })
        .collect();
    Agent::builder(
        FIXTURE,
        Arc::new(ScriptedModel::new(vec![
            ModelResponse::tools(calls),
            ModelResponse::final_output(json!("done")),
        ])),
    )
    .instructions("Echo the requested text.")
    .tool(tool)
    .build()
    .unwrap()
}
fn verify(state: &RunState, batch: usize) {
    assert_eq!(
        state.phase,
        Phase::Settled(Settlement::Completed(json!("done")))
    );
    assert_eq!(state.usage.model_calls, 2);
    assert_eq!(state.usage.tool_calls, batch as u32);
    let HistoryEntry::Exchange { results, .. } = &state.history[1] else {
        panic!("missing exchange")
    };
    assert_eq!(results.len(), batch);
    for (n, result) in results.iter().enumerate() {
        assert_eq!(result.call_id, n.to_string());
        assert!(!result.is_error);
        assert_eq!(
            result.value,
            json!({
                "content":[{"type":"text", "text":format!("echo-{n}")}],
                "structuredContent":{"text":format!("echo-{n}")}, "_meta":{"fixture":"retained"},
            })
        );
    }
}
fn measurements(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    for (catalog, paginated) in [("small_3", false), ("paginated_32_pages_4", true)] {
        let directory = root(paginated);
        let client = runtime.block_on(connect(directory.path()));
        assert_eq!(client.tools().len(), if paginated { 32 } else { 3 });
        let remote = tool(&client);
        let local: Arc<dyn Tool> = Arc::new(LocalTool(remote.spec().clone()));
        let mut group = c.benchmark_group(format!("mcp_warm/{catalog}"));
        group.throughput(Throughput::Elements(1));
        for batch in [1, 8] {
            for (path, registered) in [("local", &local), ("stdio", &remote)] {
                let agent = agent(registered.clone(), batch);
                group.bench_with_input(BenchmarkId::new(path, batch), &batch, |b, &batch| {
                    b.iter_custom(|iterations| {
                        runtime.block_on(async {
                            let mut elapsed = Duration::ZERO;
                            for _ in 0..iterations {
                                let started = Instant::now();
                                let mut state =
                                    agent.new_run("bench", json!("go"), &[], None).unwrap();
                                agent
                                    .drive(&mut state, &MemoryCheckpoint, RunControl::default())
                                    .await
                                    .unwrap();
                                elapsed += started.elapsed();
                                verify(&state, batch);
                            }
                            elapsed
                        })
                    });
                });
            }
        }
        group.finish();
        runtime.block_on(client.close()).unwrap();

        let mut group = c.benchmark_group("mcp_cold");
        group.throughput(Throughput::Elements(1));
        group.bench_function(catalog, |b| {
            b.iter_custom(|iterations| {
                runtime.block_on(async {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iterations {
                        let started = Instant::now();
                        let client = connect(directory.path()).await;
                        let registered = tool(&client);
                        elapsed += started.elapsed();
                        assert_eq!(client.tools().len(), if paginated { 32 } else { 3 });
                        registered.validate(&json!({"text":"ready"})).unwrap();
                        client.close().await.unwrap();
                    }
                    elapsed
                })
            })
        });
        group.finish();
    }
}
fn configured() -> Criterion {
    let quick = std::env::var_os("AGENT_BENCH_QUICK").is_some();
    Criterion::default()
        .sample_size(if quick { 10 } else { 30 })
        .warm_up_time(Duration::from_millis(if quick { 100 } else { 1_000 }))
        .measurement_time(Duration::from_millis(if quick { 250 } else { 2_000 }))
}
criterion_group! {
    name = benches;
    config = configured();
    targets = measurements
}
criterion_main!(benches);
