//! The MCP adapter against an independent Python stdio server (`tests/support/mcp_server.py`)
//! whose effects and controls live in a temporary directory.
#![cfg(feature = "mcp")]

use async_trait::async_trait;
use serde_json::{Value, json};
use std::{path::Path, sync::Arc, time::Duration};
use tempfile::TempDir;
use tokio::process::Command;
use tokio_util::sync::CancellationToken;
use yielded_agent::{
    mcp::{McpClient, McpOptions, ServerIdentity, ToolPolicy},
    *,
};

fn command(root: &Path) -> Command {
    let mut command = Command::new("python3");
    command
        .arg("-u")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/support/mcp_server.py"
        ))
        .arg(root);
    command
}
fn identity() -> ServerIdentity {
    ServerIdentity::new("test-filesystem", "fixture-v1")
}
fn policy(retry: RetryPolicy) -> ToolPolicy {
    ToolPolicy::default()
        .with_retry(retry)
        .with_execution(Execution::Parallel)
        .with_requires_approval(false)
}
async fn connect_with(root: &Path, options: McpOptions) -> McpClient {
    McpClient::connect(identity(), command(root), options, CancellationToken::new())
        .await
        .unwrap()
}
async fn connect(root: &Path) -> McpClient {
    connect_with(root, McpOptions::default()).await
}
fn control(root: &Path, value: Value) {
    let temporary = root.join("control.next");
    std::fs::write(&temporary, serde_json::to_vec(&value).unwrap()).unwrap();
    std::fs::rename(temporary, root.join("control.json")).unwrap();
}
fn events(root: &Path) -> Vec<Value> {
    std::fs::read_to_string(root.join("events.jsonl"))
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}
fn calls(root: &Path) -> usize {
    events(root).iter().filter(|e| e["kind"] == "call").count()
}
async fn wait_event(root: &Path, kind: &str) -> Value {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(event) = events(root).into_iter().find(|e| e["kind"] == kind) {
                return event;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("fixture event deadline")
}
#[cfg(unix)]
fn alive(pid: &Value) -> bool {
    use rustix::process::{Pid, test_kill_process};
    let pid = Pid::from_raw(pid.as_i64().unwrap() as i32).unwrap();
    match test_kill_process(pid) {
        Ok(()) => true,
        Err(rustix::io::Errno::SRCH) => false,
        Err(error) => panic!("signal 0 to {pid}: {error}"),
    }
}
/// Every server the fixture started has been reaped: the client waits for it before returning.
fn assert_reaped(root: &Path) {
    #[cfg(unix)]
    for event in events(root).into_iter().filter(|e| e["kind"] == "started") {
        assert!(!alive(&event["pid"]), "fixture server was not reaped");
    }
}
/// Every descendant the fixture started in its process group was killed. A killed descendant
/// is reaped by init once its parent is gone, so this allows that a moment; it would otherwise
/// live for a minute.
async fn assert_group_killed(root: &Path) {
    #[cfg(unix)]
    for event in events(root)
        .into_iter()
        .filter(|e| e["kind"] == "descendant")
    {
        tokio::time::timeout(Duration::from_secs(5), async {
            while alive(&event["pid"]) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("a descendant in the server's process group survived");
    }
}
fn call(id: &str, name: &str, arguments: Value) -> ToolCall {
    ToolCall {
        id: id.into(),
        name: name.into(),
        arguments,
    }
}
fn builder(
    responses: Vec<ModelResponse>,
    tools: Vec<Arc<dyn Tool>>,
    limits: Limits,
) -> AgentBuilder<String, String> {
    Agent::builder("mcp-test", Arc::new(ScriptedModel::new(responses)))
        .instructions("Use the supplied tools")
        .tools(tools)
        .limits(limits)
        .input::<String>()
        .output::<String>()
}
fn agent(
    responses: Vec<ModelResponse>,
    tools: Vec<Arc<dyn Tool>>,
    limits: Limits,
) -> Agent<String, String> {
    builder(responses, tools, limits).build().unwrap()
}
fn responses(calls: Vec<ToolCall>) -> Vec<ModelResponse> {
    vec![
        ModelResponse::tools(calls),
        ModelResponse::final_output(json!("done")),
    ]
}
async fn drive(agent: Agent<String, String>) -> RunState {
    let mut state = agent.new_run("mcp", json!("go"), &[], None).unwrap();
    agent
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    state
}
fn results(state: &RunState) -> &[ToolResult] {
    let HistoryEntry::Exchange { results, .. } = &state.history[1] else {
        panic!("missing exchange")
    };
    results
}
fn done() -> Phase {
    Phase::Settled(Settlement::Completed(json!("done")))
}

#[tokio::test]
async fn discovers_and_adapts_real_stdio_file_effects_and_results() {
    let root = TempDir::new().unwrap();
    control(root.path(), json!({"list_mode":"paginate"}));
    let client = connect(root.path()).await;
    assert_eq!(
        client
            .tools()
            .iter()
            .map(|t| t.name.as_ref())
            .collect::<Vec<_>>(),
        ["echo", "read_file", "write_file"]
    );
    let write = client
        .tool("write_file", "fs_write", policy(RetryPolicy::Never))
        .unwrap();
    let read = client
        .tool("read_file", "fs_read", policy(RetryPolicy::ReadOnly))
        .unwrap();
    assert_eq!(write.server(), &identity());
    assert_eq!(write.remote_name(), "write_file");
    assert_eq!(write.spec().name, "fs_write");
    assert_eq!(
        write.spec().input_schema["required"],
        json!(["path", "content"])
    );
    let contract: Value = serde_json::from_str(&write.spec().version).unwrap();
    assert_eq!(contract["server"]["id"], "test-filesystem");
    assert_eq!(contract["tool"]["name"], "write_file");
    let state = drive(agent(
        vec![
            ModelResponse::tools(vec![call(
                "write",
                "fs_write",
                json!({"path":"note.txt", "content":"hello"}),
            )]),
            ModelResponse::tools(vec![call("read", "fs_read", json!({"path":"note.txt"}))]),
            ModelResponse::final_output(json!("done")),
        ],
        vec![write, read],
        Limits::default(),
    ))
    .await;
    assert_eq!(state.phase, done());
    assert_eq!(
        std::fs::read_to_string(root.path().join("note.txt")).unwrap(),
        "hello"
    );
    let HistoryEntry::Exchange { results, .. } = &state.history[2] else {
        panic!()
    };
    assert_eq!(
        results[0].value["structuredContent"],
        json!({"text":"hello"})
    );
    assert_eq!(results[0].value["content"][0]["text"], "hello");
    assert_eq!(results[0].value["_meta"]["fixture"], "retained");
    assert!(!results[0].is_error);
    assert_eq!(calls(root.path()), 2);
    client.close().await.unwrap();
    client.close().await.unwrap();
    assert_reaped(root.path());
}

#[tokio::test]
async fn validates_entire_model_response_and_bundled_argument_schema_before_dispatch() {
    let root = TempDir::new().unwrap();
    control(root.path(), json!({"bundled_schema":true}));
    let client = connect(root.path()).await;
    let tool = client
        .tool("echo", "echo", policy(RetryPolicy::ReadOnly))
        .unwrap();
    assert!(matches!(
        tool.validate(&json!({"text":42})),
        Err(Error::ModelOutput(_))
    ));
    for invalid in [
        json!({"text":42}),
        json!({"text":""}),
        json!({"text":"hi", "extra":true}),
    ] {
        let state = drive(agent(
            responses(vec![
                call("valid", "echo", json!({"text":"hi"})),
                call("invalid", "echo", invalid),
            ]),
            vec![tool.clone()],
            Limits::default(),
        ))
        .await;
        assert!(matches!(state.phase, Phase::Settled(Settlement::Failed(_))));
        assert_eq!(state.pending_response.unwrap().tools.len(), 2);
    }
    assert_eq!(calls(root.path()), 0);
    client.close().await.unwrap();
}

struct Deny;
#[async_trait]
impl Authorizer for Deny {
    async fn authorize(&self, _: &RunState, _: &ToolCall) -> Result<bool> {
        Ok(false)
    }
}

#[tokio::test]
async fn default_mutation_policy_waits_and_denial_or_revoked_authorization_dispatches_nothing() {
    let root = TempDir::new().unwrap();
    let client = connect(root.path()).await;
    let tool = client
        .tool("write_file", "write", ToolPolicy::default())
        .unwrap();
    assert_eq!(tool.spec().retry, RetryPolicy::Never);
    assert_eq!(tool.spec().execution, Execution::Sequential);
    assert!(tool.spec().requires_approval);
    for approve in [false, true] {
        let mut definition = builder(
            responses(vec![call(
                "write",
                "write",
                json!({"path":"forbidden", "content":"no"}),
            )]),
            vec![tool.clone()],
            Limits::default(),
        );
        if approve {
            definition = definition.authorizer(Arc::new(Deny));
        }
        let definition = definition.build().unwrap();
        let mut state = definition
            .new_run("approval", json!("go"), &[], None)
            .unwrap();
        definition
            .drive(&mut state, &MemoryCheckpoint, RunControl::default())
            .await
            .unwrap();
        assert!(matches!(
            state.operations[0].status,
            OperationStatus::Approval
        ));
        assert_eq!(state.usage.tool_calls, 0);
        state
            .resolve("write", Resolution::Approve(approve))
            .unwrap();
        definition
            .drive(&mut state, &MemoryCheckpoint, RunControl::default())
            .await
            .unwrap();
        if approve {
            assert!(matches!(state.phase, Phase::Settled(Settlement::Failed(_))));
        } else {
            assert!(results(&state)[0].is_error);
        }
    }
    assert_eq!(calls(root.path()), 0);
    assert!(!root.path().join("forbidden").exists());
    client.close().await.unwrap();
}

#[tokio::test]
async fn preserves_engine_and_connection_parallel_limits_and_result_order() {
    for (engine_bound, connection_bound) in [(2, 8), (8, 2)] {
        let root = TempDir::new().unwrap();
        let client = connect_with(
            root.path(),
            McpOptions::default().with_max_in_flight(connection_bound),
        )
        .await;
        let tool = client
            .tool("echo", "echo", policy(RetryPolicy::ReadOnly))
            .unwrap();
        let batch = (0..6)
            .map(|n| {
                call(
                    &n.to_string(),
                    "echo",
                    json!({"text":n.to_string(), "delay_ms":50}),
                )
            })
            .collect();
        let state = drive(agent(
            responses(batch),
            vec![tool],
            Limits::default().with_max_parallel_tools(engine_bound),
        ))
        .await;
        assert_eq!(state.usage.tool_calls, 6);
        assert_eq!(
            results(&state)
                .iter()
                .map(|r| r.value["structuredContent"]["text"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["0", "1", "2", "3", "4", "5"]
        );
        let peak = events(root.path())
            .iter()
            .filter_map(|event| event["active"].as_u64())
            .max()
            .unwrap();
        assert!(peak <= 2, "peak={peak}");
        client.close().await.unwrap();
    }
}

/// Each way a call can end once `tools/call` was sent: a failure the server reports is
/// `Failed` and the run continues; anything that leaves the outcome open is an execution error,
/// so a mutation becomes `Unknown`.
#[tokio::test]
async fn reported_errors_are_recorded_while_invalid_or_lost_mutation_results_remain_unknown() {
    for mode in [
        "reported_error",
        "rpc_error",
        "invalid_output",
        "missing_content",
        "large",
        "truncated",
        "disconnect",
        "request_timeout",
    ] {
        let root = TempDir::new().unwrap();
        let hold = mode == "request_timeout";
        if !hold {
            control(root.path(), json!({"call_mode":mode}));
        }
        let client = connect_with(
            root.path(),
            McpOptions::default()
                .with_max_message_bytes(8192)
                .with_request_timeout(Duration::from_secs(if hold { 1 } else { 30 })),
        )
        .await;
        let tool = client
            .tool("echo", "echo", policy(RetryPolicy::Never))
            .unwrap();
        let state = drive(agent(
            responses(vec![call(
                "echo",
                "echo",
                json!({"text":"hello", "hold":hold}),
            )]),
            vec![tool],
            Limits::default(),
        ))
        .await;
        assert_eq!(calls(root.path()), 1, "{mode}");
        if matches!(mode, "reported_error" | "rpc_error") {
            assert_eq!(state.phase, done(), "{mode}");
            assert!(results(&state)[0].is_error);
            if mode == "rpc_error" {
                assert_eq!(results(&state)[0].value["error"]["code"], -32602);
            } else {
                assert_eq!(
                    results(&state)[0].value["content"][0]["text"],
                    "controlled failure"
                );
            }
            assert!(!client.is_closed(), "{mode}");
        } else {
            assert_eq!(state.phase, Phase::Tools, "{mode}");
            assert!(
                matches!(state.operations[0].status, OperationStatus::Unknown { .. }),
                "{mode}: {:?}",
                state.operations
            );
            if mode != "invalid_output" {
                assert!(client.is_closed(), "{mode}");
            }
        }
        client.close().await.unwrap();
        assert_reaped(root.path());
    }
}

#[tokio::test]
async fn engine_output_limit_applies_to_success_and_reported_failure_payloads() {
    for mode in ["normal", "reported_error"] {
        let root = TempDir::new().unwrap();
        control(root.path(), json!({"call_mode":mode}));
        let client = connect(root.path()).await;
        let tool = client
            .tool("echo", "echo", policy(RetryPolicy::Never))
            .unwrap();
        let state = drive(agent(
            responses(vec![call("echo", "echo", json!({"text":"hello"}))]),
            vec![tool],
            Limits::default().with_max_tool_output_bytes(32),
        ))
        .await;
        assert!(matches!(
            state.operations[0].status,
            OperationStatus::Unknown { .. }
        ));
        client.close().await.unwrap();
    }
}

#[tokio::test]
async fn catalog_bounds_external_references_and_unsupported_retries_fail_closed() {
    for (config, config_error) in [
        (json!({"list_mode":"loop"}), false),
        (json!({"list_mode":"duplicate"}), false),
        (json!({"protocol":"2099-01-01"}), true),
    ] {
        let root = TempDir::new().unwrap();
        control(root.path(), config.clone());
        let error = McpClient::connect(
            identity(),
            command(root.path()),
            McpOptions::default(),
            CancellationToken::new(),
        )
        .await
        .err()
        .expect("connect must fail");
        assert_eq!(
            matches!(error, Error::Config(_)),
            config_error,
            "{config}: {error}"
        );
        assert_eq!(calls(root.path()), 0);
        assert_reaped(root.path());
    }
    let root = TempDir::new().unwrap();
    assert!(matches!(
        McpClient::connect(
            identity(),
            command(root.path()),
            McpOptions::default().with_max_tools(2),
            CancellationToken::new()
        )
        .await,
        Err(Error::Tool(_))
    ));
    assert_reaped(root.path());
    for options in [
        McpOptions::default().with_max_in_flight(0),
        McpOptions::default().with_request_timeout(Duration::ZERO),
    ] {
        assert!(matches!(
            McpClient::connect(
                identity(),
                command(root.path()),
                options,
                CancellationToken::new()
            )
            .await,
            Err(Error::Config(_))
        ));
    }
    control(root.path(), json!({"external_schema":true}));
    let client = connect(root.path()).await;
    for (remote, alias, retry) in [
        ("echo", "echo", RetryPolicy::ReadOnly),
        ("missing", "missing", RetryPolicy::ReadOnly),
        ("read_file", "not an alias", RetryPolicy::ReadOnly),
        (
            "write_file",
            "write",
            RetryPolicy::Idempotent {
                contract: "not-supported".into(),
            },
        ),
    ] {
        assert!(matches!(
            client.tool(remote, alias, policy(retry)),
            Err(Error::Config(_))
        ));
    }
    client.close().await.unwrap();
}

/// A catalog change found before `tools/call` is sent closes the connection, and the call is a
/// known failure: nothing was invoked, so the model sees it and the run continues.
#[tokio::test]
async fn catalog_change_after_approval_and_list_changed_notification_prevent_invocation() {
    for change in [
        json!({"schema_revision":2}),
        json!({"removed_tool":true}),
        json!({"notify_on_list":true}),
    ] {
        let root = TempDir::new().unwrap();
        let client = connect(root.path()).await;
        let tool = client.tool("echo", "echo", ToolPolicy::default()).unwrap();
        let definition = agent(
            responses(vec![call("echo", "echo", json!({"text":"hello"}))]),
            vec![tool],
            Limits::default(),
        );
        let mut state = definition
            .new_run("change", json!("go"), &[], None)
            .unwrap();
        definition
            .drive(&mut state, &MemoryCheckpoint, RunControl::default())
            .await
            .unwrap();
        control(root.path(), change.clone());
        state.resolve("echo", Resolution::Approve(true)).unwrap();
        definition
            .drive(&mut state, &MemoryCheckpoint, RunControl::default())
            .await
            .unwrap();
        assert_eq!(state.phase, done(), "{change}");
        let result = &results(&state)[0];
        assert!(result.is_error);
        assert!(
            result.value["error"]["message"]
                .as_str()
                .unwrap()
                .contains("catalog"),
            "{change}: {}",
            result.value
        );
        assert!(client.is_closed());
        assert_eq!(calls(root.path()), 0);
        client.close().await.unwrap();
        assert_reaped(root.path());
    }
}

/// Specs are durable contracts: a registered tool's spec depends on its own definition and the
/// deployment, not on unrelated tools the server adds.
#[tokio::test]
async fn registered_contract_ignores_unrelated_catalog_growth_but_not_the_tools_own_schema() {
    let root = TempDir::new().unwrap();
    let client = connect(root.path()).await;
    let original = client
        .tool("echo", "echo", policy(RetryPolicy::ReadOnly))
        .unwrap();
    client.close().await.unwrap();
    control(root.path(), json!({"catalog_size":5}));
    let grown = connect(root.path()).await;
    assert_eq!(grown.tools().len(), 5);
    let same = grown
        .tool("echo", "echo", policy(RetryPolicy::ReadOnly))
        .unwrap();
    assert_eq!(same.spec(), original.spec());
    grown.close().await.unwrap();
    control(root.path(), json!({"schema_revision":2}));
    let changed = connect(root.path()).await;
    let other = changed
        .tool("echo", "echo", policy(RetryPolicy::ReadOnly))
        .unwrap();
    assert_ne!(other.spec(), original.spec());
    changed.close().await.unwrap();
}

#[tokio::test]
async fn dropping_the_owner_stops_tools_and_cleanup_joins_the_process_group() {
    let root = TempDir::new().unwrap();
    control(root.path(), json!({"spawn_descendant":true}));
    let client = connect(root.path()).await;
    wait_event(root.path(), "descendant").await;
    let tool = client
        .tool("echo", "echo", policy(RetryPolicy::ReadOnly))
        .unwrap();
    drop(client);
    let context = ToolContext::new(RunId::new_v4(), "unused", "unused");
    let output = tool
        .execute(json!({"text":"hello"}), context.clone())
        .await
        .unwrap();
    let ToolOutput::Failed(value) = output else {
        panic!("a closed connection must report a known failure: {output:?}")
    };
    assert_eq!(value["error"]["message"], "MCP client dropped");
    tool.cleanup(context).await.unwrap();
    assert_eq!(calls(root.path()), 0);
    assert_reaped(root.path());
    assert_group_killed(root.path()).await;
}

#[tokio::test]
async fn close_kills_the_servers_whole_process_group() {
    let root = TempDir::new().unwrap();
    control(root.path(), json!({"spawn_descendant":true}));
    let client = connect(root.path()).await;
    let descendant = wait_event(root.path(), "descendant").await;
    #[cfg(unix)]
    assert!(alive(&descendant["pid"]));
    let _ = descendant;
    client.close().await.unwrap();
    assert_reaped(root.path());
    assert_group_killed(root.path()).await;
}

/// A runtime shutting down drops the task that owns the server without letting it clean up;
/// the process group is killed anyway.
#[test]
fn dropping_the_runtime_kills_the_process_group() {
    let root = TempDir::new().unwrap();
    control(root.path(), json!({"spawn_descendant":true}));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let client = runtime.block_on(async {
        let client = connect(root.path()).await;
        wait_event(root.path(), "descendant").await;
        client
    });
    drop(runtime);
    drop(client);
    #[cfg(unix)]
    for event in events(root.path())
        .into_iter()
        .filter(|e| e["kind"] == "descendant")
    {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while alive(&event["pid"]) {
            assert!(
                std::time::Instant::now() < deadline,
                "a descendant in the server's process group survived the runtime"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

/// The engine drops a call at its operation timeout and then runs cleanup, which returns only
/// once the server's process group has been killed and the server reaped.
#[tokio::test]
async fn engine_timeout_closes_the_connection_and_cleanup_joins_the_process_group() {
    let root = TempDir::new().unwrap();
    control(root.path(), json!({"spawn_descendant":true}));
    let client = connect(root.path()).await;
    let tool = client
        .tool("echo", "echo", policy(RetryPolicy::ReadOnly))
        .unwrap();
    let state = drive(agent(
        responses(vec![call(
            "echo",
            "echo",
            json!({"text":"hello", "hold":true}),
        )]),
        vec![tool],
        Limits::default()
            .with_operation_timeout(Duration::from_millis(500))
            .with_cleanup_timeout(Duration::from_secs(5)),
    ))
    .await;
    assert_eq!(state.phase, done());
    let result = &results(&state)[0];
    assert!(result.is_error);
    assert!(result.value.as_str().unwrap().contains("timed out"));
    assert!(client.is_closed());
    assert_reaped(root.path());
    assert_group_killed(root.path()).await;
    client.close().await.unwrap();
}

#[cfg(feature = "sqlite")]
async fn interrupted_mutation_reopens_as_unknown_and_only_reconciliation_continues() {
    use yielded_agent::sqlite::SqliteStore;
    let root = TempDir::new().unwrap();
    let db_path = root.path().join("state.sqlite");
    let store = Arc::new(SqliteStore::open(&db_path).await.unwrap());
    let client = connect(root.path()).await;
    let tool = client
        .tool("write_file", "write", policy(RetryPolicy::Never))
        .unwrap();
    let write = || {
        responses(vec![call(
            "write",
            "write",
            json!({"path":"effect.txt", "content":"once", "hold":true}),
        )])
    };
    let definition = agent(write(), vec![tool], Limits::default());
    let id = store
        .accept(
            &definition.erased(),
            "mutation",
            "request",
            json!("go"),
            None,
        )
        .await
        .unwrap();
    let mut state = store.claim(id).await.unwrap();
    let shutdown = CancellationToken::new();
    let task_store = store.clone();
    let task_control = RunControl::default().with_shutdown(shutdown.clone());
    let task = tokio::spawn(async move {
        definition
            .drive(&mut state, task_store.as_ref(), task_control)
            .await
    });
    wait_event(root.path(), "effect").await;
    shutdown.cancel();
    assert!(matches!(task.await.unwrap(), Err(Error::Interrupted)));
    assert!(client.is_closed());
    assert_reaped(root.path());
    let interrupted = store.status(id).await.unwrap();
    assert!(matches!(
        interrupted.operations[0].status,
        OperationStatus::Unknown { .. }
    ));
    assert_eq!(interrupted.usage.tool_calls, 1);
    let key = interrupted.operations[0].idempotency_key.clone();
    store.close().await.unwrap();
    drop(client);

    let store = SqliteStore::open(&db_path).await.unwrap();
    let client = connect(root.path()).await;
    let tool = client
        .tool("write_file", "write", policy(RetryPolicy::Never))
        .unwrap();
    let recovering = agent(write(), vec![tool], Limits::default());
    let mut recovered = store.claim(id).await.unwrap();
    recovering
        .drive(&mut recovered, &store, RunControl::default())
        .await
        .unwrap();
    assert_eq!(recovered.operations[0].idempotency_key, key);
    assert_eq!(calls(root.path()), 1);
    assert_eq!(
        std::fs::read_to_string(root.path().join("effect.txt")).unwrap(),
        "once"
    );
    store
        .resolve(
            id,
            "write",
            Resolution::Reconcile(ToolResult {
                call_id: "write".into(),
                value: json!({"verified":"once"}),
                is_error: false,
            }),
        )
        .await
        .unwrap();
    let mut recovered = store.claim(id).await.unwrap();
    recovering
        .drive(&mut recovered, &store, RunControl::default())
        .await
        .unwrap();
    assert_eq!(recovered.phase, done());
    assert_eq!(recovered.usage.tool_calls, 1);
    assert_eq!(calls(root.path()), 1);
    client.close().await.unwrap();
    store.close().await.unwrap();
}

/// A tool's spec pins the deployment and the tool's definition: a stored run does not resume
/// against a changed schema, server version or deployment revision, and does against the same.
#[cfg(feature = "sqlite")]
async fn read_only_recovery_rejects_changed_server_and_schema_then_retries_compatible_contract() {
    use yielded_agent::sqlite::SqliteStore;
    let root = TempDir::new().unwrap();
    let db_path = root.path().join("state.sqlite");
    let store = Arc::new(SqliteStore::open(&db_path).await.unwrap());
    let client = connect(root.path()).await;
    let tool = client
        .tool("echo", "echo", policy(RetryPolicy::ReadOnly))
        .unwrap();
    let echo = || {
        responses(vec![call(
            "echo",
            "echo",
            json!({"text":"hello", "hold":true}),
        )])
    };
    let definition = agent(echo(), vec![tool], Limits::default());
    let id = store
        .accept(&definition.erased(), "read", "request", json!("go"), None)
        .await
        .unwrap();
    let mut state = store.claim(id).await.unwrap();
    let shutdown = CancellationToken::new();
    let task_store = store.clone();
    let task_control = RunControl::default().with_shutdown(shutdown.clone());
    let task = tokio::spawn(async move {
        definition
            .drive(&mut state, task_store.as_ref(), task_control)
            .await
    });
    wait_event(root.path(), "call").await;
    shutdown.cancel();
    assert!(matches!(task.await.unwrap(), Err(Error::Interrupted)));
    let before = store.status(id).await.unwrap();
    assert!(matches!(
        before.operations[0].status,
        OperationStatus::Dispatched
    ));
    assert_reaped(root.path());
    store.close().await.unwrap();
    drop(client);

    let store = SqliteStore::open(&db_path).await.unwrap();
    for (config, revision) in [
        (json!({"schema_revision":2}), "fixture-v1"),
        (json!({"server_version":"2"}), "fixture-v1"),
        (json!({}), "deployment-v2"),
    ] {
        control(root.path(), config);
        let changed = McpClient::connect(
            ServerIdentity::new("test-filesystem", revision),
            command(root.path()),
            McpOptions::default(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let tool = changed
            .tool("echo", "echo", policy(RetryPolicy::ReadOnly))
            .unwrap();
        let recovering = agent(vec![], vec![tool], Limits::default());
        let mut recovered = store.claim(id).await.unwrap();
        assert!(matches!(
            recovering
                .drive(&mut recovered, &store, RunControl::default())
                .await,
            Err(Error::Incompatible(_))
        ));
        assert_eq!(calls(root.path()), 1);
        changed.close().await.unwrap();
    }
    control(root.path(), json!({}));
    std::fs::write(root.path().join("release"), "continue").unwrap();
    let client = connect(root.path()).await;
    let tool = client
        .tool("echo", "echo", policy(RetryPolicy::ReadOnly))
        .unwrap();
    let recovering = agent(echo(), vec![tool], Limits::default());
    let mut recovered = store.claim(id).await.unwrap();
    assert_eq!(
        recovered.operations[0].idempotency_key,
        before.operations[0].idempotency_key
    );
    recovering
        .drive(&mut recovered, &store, RunControl::default())
        .await
        .unwrap();
    assert_eq!(recovered.phase, done());
    assert_eq!(recovered.usage.tool_calls, 2);
    assert!(recovered.usage.active_ms >= before.usage.active_ms);
    assert_eq!(calls(root.path()), 2);
    client.close().await.unwrap();
    store.close().await.unwrap();
}

#[cfg(feature = "sqlite")]
#[test]
fn durable_mcp_recovery_contracts() {
    // Unrelated concurrent test forks can briefly inherit Unix file locks before exec. Give the
    // complete store and process handoff its own process, as the process-recovery suite does,
    // without retrying a busy database.
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "mcp_recovery_child", "--ignored", "--nocapture"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[cfg(feature = "sqlite")]
#[tokio::test]
#[ignore = "subprocess entry point for durable_mcp_recovery_contracts"]
async fn mcp_recovery_child() {
    interrupted_mutation_reopens_as_unknown_and_only_reconciliation_continues().await;
    read_only_recovery_rejects_changed_server_and_schema_then_retries_compatible_contract().await;
}

#[tokio::test]
async fn cancellation_during_initialization_joins_the_child() {
    let root = TempDir::new().unwrap();
    control(
        root.path(),
        json!({"hold_initialization":true, "spawn_descendant":true}),
    );
    let cancel = CancellationToken::new();
    let token = cancel.clone();
    let process = command(root.path());
    let task = tokio::spawn(async move {
        McpClient::connect(identity(), process, McpOptions::default(), token).await
    });
    wait_event(root.path(), "initializing").await;
    cancel.cancel();
    assert!(matches!(task.await.unwrap(), Err(Error::Interrupted)));
    assert_reaped(root.path());
    assert_group_killed(root.path()).await;
}

#[tokio::test]
async fn user_cancellation_preserves_mutation_uncertainty_and_joins_cleanup() {
    let root = TempDir::new().unwrap();
    let client = connect(root.path()).await;
    let tool = client
        .tool("write_file", "write", policy(RetryPolicy::Never))
        .unwrap();
    let definition = agent(
        responses(vec![call(
            "write",
            "write",
            json!({"path":"cancelled.txt", "content":"effect", "hold":true}),
        )]),
        vec![tool],
        Limits::default(),
    );
    let cancel = CancellationToken::new();
    let control = RunControl::default().with_cancel(cancel.clone());
    let task = tokio::spawn(async move {
        let mut state = definition
            .new_run("cancel", json!("go"), &[], None)
            .unwrap();
        definition
            .drive(&mut state, &MemoryCheckpoint, control)
            .await
            .unwrap();
        state
    });
    wait_event(root.path(), "effect").await;
    cancel.cancel();
    let state = task.await.unwrap();
    assert_eq!(state.phase, Phase::Settled(Settlement::Cancelled));
    assert!(matches!(
        state.operations[0].status,
        OperationStatus::Unknown { .. }
    ));
    assert_eq!(
        std::fs::read_to_string(root.path().join("cancelled.txt")).unwrap(),
        "effect"
    );
    assert_eq!(calls(root.path()), 1);
    assert!(client.is_closed());
    assert_reaped(root.path());
    client.close().await.unwrap();
}
