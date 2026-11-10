#![cfg(feature = "sqlite")]
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tempfile::TempDir;
use yielded_agent::{
    host::{Host, HostOptions},
    sqlite::SqliteStore,
    *,
};

fn agent(responses: Vec<ModelResponse>, tools: Vec<Arc<dyn Tool>>) -> Agent {
    Agent::builder("test", Arc::new(ScriptedModel::new(responses)))
        .instructions("Be deterministic")
        .tools(tools)
        .build()
        .unwrap()
}
fn final_agent() -> Agent {
    agent(vec![ModelResponse::final_output(json!("done"))], vec![])
}
fn spec(name: &str, retry: RetryPolicy, requires_approval: bool) -> ToolSpec {
    ToolSpec {
        name: name.into(),
        description: name.into(),
        input_schema: json!({}),
        version: "1".into(),
        retry,
        execution: Execution::Parallel,
        requires_approval,
    }
}
async fn store(dir: &TempDir) -> Arc<SqliteStore> {
    Arc::new(
        SqliteStore::open(dir.path().join("state.sqlite"))
            .await
            .unwrap(),
    )
}
async fn wait_for(
    host: &Host<SqliteStore>,
    id: RunId,
    predicate: impl Fn(&RunState) -> bool,
) -> RunState {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let state = host.status(id).await.unwrap();
            if predicate(&state) {
                return state;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("state did not reach expected condition")
}
fn options() -> HostOptions {
    HostOptions {
        max_concurrent_runs: 4,
        poll_interval: Duration::from_millis(2),
        events: None,
    }
}

#[tokio::test]
async fn acceptance_is_durable_idempotent_and_exclusive() {
    let dir = TempDir::new().unwrap();
    let db = store(&dir).await;
    let agent = final_agent();
    let id = db
        .accept(
            &agent,
            "conversation",
            "request",
            json!({"question":1}),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        db.accept(
            &agent,
            "conversation",
            "request",
            json!({"question":1}),
            None
        )
        .await
        .unwrap(),
        id
    );
    assert!(matches!(
        db.accept(
            &agent,
            "conversation",
            "request",
            json!({"question":2}),
            None
        )
        .await,
        Err(Error::IdempotencyConflict)
    ));
    assert!(matches!(
        db.accept(&agent, "conversation", "another", json!(1), None)
            .await,
        Err(Error::ConversationBusy)
    ));
    assert!(matches!(
        SqliteStore::open(dir.path().join("state.sqlite")).await,
        Err(Error::DatabaseBusy)
    ));
    db.close().await.unwrap();
    let db = store(&dir).await;
    assert_eq!(db.status(id).await.unwrap().input, json!({"question":1}));
    assert_eq!(db.due(now_ms(), 10).await.unwrap(), vec![id]);
    let mut host = Host::start(db.clone(), agent, options()).unwrap();
    let completed = wait_for(&host, id, RunState::is_terminal).await;
    assert_eq!(
        completed.phase,
        Phase::Settled(Settlement::Completed(json!("done")))
    );
    assert!(db.due(now_ms(), 10).await.unwrap().is_empty());
    assert_eq!(db.history("conversation").await.unwrap().len(), 2);
    host.shutdown().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn settled_run_state_history_and_operations_survive_reopening() {
    let dir = TempDir::new().unwrap();
    let db = store(&dir).await;
    let tool = typed_tool::<Value, Value, _, _>(
        spec("echo", RetryPolicy::ReadOnly, false),
        |arguments, _| async move { Ok(ToolOutput::Complete(arguments)) },
    );
    let calls = |round: u32| {
        ModelResponse::tools(
            (0..2)
                .map(|index| ToolCall {
                    id: format!("{round}-{index}"),
                    name: "echo".into(),
                    arguments: json!({"round": round, "index": index}),
                })
                .collect(),
        )
    };
    let agent = agent(
        vec![
            calls(0),
            calls(1),
            ModelResponse::final_output(json!("done")),
        ],
        vec![tool],
    );
    let earlier = db
        .accept(&agent, "c", "earlier", json!(0), None)
        .await
        .unwrap();
    let mut state = db.claim(earlier).await.unwrap();
    Engine::new(agent.clone())
        .drive(&mut state, db.as_ref(), RunControl::default())
        .await
        .unwrap();
    let id = db
        .accept(&agent, "c", "measured", json!(1), None)
        .await
        .unwrap();
    let mut state = db.claim(id).await.unwrap();
    Engine::new(agent)
        .drive(&mut state, db.as_ref(), RunControl::default())
        .await
        .unwrap();
    assert_eq!(
        state.phase,
        Phase::Settled(Settlement::Completed(json!("done")))
    );
    assert_eq!(state.prior_context.len(), 4);
    let encoded = |state: &RunState| serde_json::to_value(state).unwrap();
    let status = db.status(id).await.unwrap();
    assert_eq!(encoded(&status), encoded(&state));
    let history = db.history("c").await.unwrap();
    assert_eq!(history.len(), 8);
    assert_eq!(&history[4..], state.history.as_slice());
    let operations = db.operations(id).await.unwrap();
    assert_eq!(
        operations
            .iter()
            .map(|op| op.call.id.as_str())
            .collect::<Vec<_>>(),
        ["0-0", "0-1", "1-0", "1-1"]
    );
    assert!(
        operations
            .iter()
            .all(|op| matches!(op.status, OperationStatus::Complete(_)))
    );
    db.close().await.unwrap();
    let db = store(&dir).await;
    assert_eq!(encoded(&db.status(id).await.unwrap()), encoded(&status));
    assert_eq!(db.history("c").await.unwrap(), history);
    assert_eq!(db.operations(id).await.unwrap(), operations);
    assert!(db.due(now_ms(), 10).await.unwrap().is_empty());
    db.close().await.unwrap();
}

#[tokio::test]
async fn stale_generations_and_cancellation_cannot_overwrite_progress() {
    let dir = TempDir::new().unwrap();
    let db = store(&dir).await;
    let agent = final_agent();
    let id = db.accept(&agent, "c", "k", json!(1), None).await.unwrap();
    let mut old = db.claim(id).await.unwrap();
    let mut current = db.claim(id).await.unwrap();
    old.phase = Phase::Settled(Settlement::Completed(json!("stale")));
    assert!(matches!(db.commit(&mut old).await, Err(Error::Stale)));
    assert_eq!(db.history("c").await.unwrap().len(), 1);
    db.request_cancel(id).await.unwrap();
    current.phase = Phase::Settled(Settlement::Completed(json!("also stale")));
    assert!(matches!(db.commit(&mut current).await, Err(Error::Stale)));
    let mut host = Host::start(db.clone(), agent, options()).unwrap();
    assert_eq!(
        wait_for(&host, id, RunState::is_terminal).await.phase,
        Phase::Settled(Settlement::Cancelled)
    );
    host.shutdown().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn ordinary_dispatched_effect_recovers_as_unknown_and_can_be_reconciled() {
    let dir = TempDir::new().unwrap();
    let db = store(&dir).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let tool =
        typed_tool::<Value, Value, _, _>(spec("mutate", RetryPolicy::Never, false), move |_, _| {
            count.fetch_add(1, Ordering::SeqCst);
            async { Ok(ToolOutput::Complete(json!("executed"))) }
        });
    let agent = agent(
        vec![
            ModelResponse::tools(vec![ToolCall {
                id: "op".into(),
                name: "mutate".into(),
                arguments: json!({}),
            }]),
            ModelResponse::final_output(json!("done")),
        ],
        vec![tool.clone()],
    );
    let id = db.accept(&agent, "c", "k", json!(1), None).await.unwrap();
    let mut interrupted = db.claim(id).await.unwrap();
    interrupted.phase = Phase::Tools;
    interrupted.usage.model_calls = 1;
    interrupted.usage.tool_calls = 1;
    interrupted.usage.active_ms = 30_000;
    let call = ToolCall {
        id: "op".into(),
        name: "mutate".into(),
        arguments: json!({}),
    };
    interrupted.pending_response = Some(ModelResponse::tools(vec![call.clone()]));
    interrupted.operations = vec![Operation {
        call,
        spec: tool.spec().clone(),
        idempotency_key: format!("{id}/op"),
        status: OperationStatus::Dispatched,
        approved: true,
    }];
    db.commit(&mut interrupted).await.unwrap();
    db.close().await.unwrap();
    let db = store(&dir).await;
    let mut host = Host::start(db.clone(), agent, options()).unwrap();
    let unknown = wait_for(&host, id, |s| {
        matches!(s.operations[0].status, OperationStatus::Unknown { .. })
    })
    .await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(unknown.usage.active_ms >= 30_000);
    assert!(db.due(now_ms(), 10).await.unwrap().is_empty());
    host.resolve(
        id,
        "op",
        Resolution::Reconcile(ToolResult {
            call_id: "op".into(),
            value: json!("confirmed"),
            is_error: false,
        }),
    )
    .await
    .unwrap();
    let done = wait_for(&host, id, RunState::is_terminal).await;
    assert_eq!(
        done.phase,
        Phase::Settled(Settlement::Completed(json!("done")))
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(done.usage.tool_calls, 1);
    let history = db.history("c").await.unwrap();
    assert!(
        matches!(&history[1], HistoryEntry::Exchange { results, .. } if results[0].value == json!("confirmed"))
    );
    host.shutdown().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn approval_and_input_waits_survive_restart_and_duplicate_resolution_rejects() {
    let dir = TempDir::new().unwrap();
    let db = store(&dir).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let tool =
        typed_tool::<Value, Value, _, _>(spec("ask", RetryPolicy::ReadOnly, true), move |_, _| {
            count.fetch_add(1, Ordering::SeqCst);
            async {
                Ok(ToolOutput::Wait(WaitRequest::Input {
                    prompt: "Which option?".into(),
                }))
            }
        });
    let agent = agent(
        vec![
            ModelResponse::tools(vec![ToolCall {
                id: "ask-1".into(),
                name: "ask".into(),
                arguments: json!({}),
            }]),
            ModelResponse::final_output(json!("done")),
        ],
        vec![tool],
    );
    let mut host = Host::start(db.clone(), agent.clone(), options()).unwrap();
    let id = host.submit("c", "k", json!(1), None).await.unwrap();
    wait_for(&host, id, |s| {
        s.operations
            .first()
            .is_some_and(|o| matches!(o.status, OperationStatus::Approval))
    })
    .await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    host.shutdown().await.unwrap();
    db.close().await.unwrap();
    let db = store(&dir).await;
    let mut host = Host::start(db.clone(), agent.clone(), options()).unwrap();
    host.resolve(id, "ask-1", Resolution::Approve(true))
        .await
        .unwrap();
    wait_for(&host, id, |s| {
        s.operations.first().is_some_and(|o| {
            matches!(
                o.status,
                OperationStatus::Waiting(WaitRequest::Input { .. })
            )
        })
    })
    .await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    host.shutdown().await.unwrap();
    db.close().await.unwrap();
    let db = store(&dir).await;
    let mut host = Host::start(db.clone(), agent, options()).unwrap();
    let before = host.status(id).await.unwrap().usage;
    host.resolve(id, "ask-1", Resolution::Input(json!("A")))
        .await
        .unwrap();
    assert!(matches!(
        host.resolve(id, "ask-1", Resolution::Input(json!("B")))
            .await,
        Err(Error::NotWaiting)
    ));
    let done = wait_for(&host, id, RunState::is_terminal).await;
    assert_eq!(
        done.phase,
        Phase::Settled(Settlement::Completed(json!("done")))
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(done.usage.tool_calls, before.tool_calls);
    host.shutdown().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn due_time_wait_recovers_without_a_notification() {
    let dir = TempDir::new().unwrap();
    let db = store(&dir).await;
    let tool = typed_tool::<Value, Value, _, _>(
        spec("later", RetryPolicy::ReadOnly, false),
        |_, _| async {
            Ok(ToolOutput::Wait(WaitRequest::Until {
                unix_ms: now_ms() + 150,
            }))
        },
    );
    let agent = agent(
        vec![
            ModelResponse::tools(vec![ToolCall {
                id: "timer".into(),
                name: "later".into(),
                arguments: json!({}),
            }]),
            ModelResponse::final_output(json!("done")),
        ],
        vec![tool],
    );
    let mut host = Host::start(db.clone(), agent.clone(), options()).unwrap();
    let id = host.submit("c", "k", json!(1), None).await.unwrap();
    let waiting = wait_for(&host, id, |s| {
        s.operations.first().is_some_and(|o| {
            matches!(
                o.status,
                OperationStatus::Waiting(WaitRequest::Until { .. })
            )
        })
    })
    .await;
    host.shutdown().await.unwrap();
    db.close().await.unwrap();
    tokio::time::sleep(Duration::from_millis(175)).await;
    let db = store(&dir).await;
    let mut host = Host::start(db.clone(), agent, options()).unwrap();
    let done = wait_for(&host, id, RunState::is_terminal).await;
    assert_eq!(
        done.phase,
        Phase::Settled(Settlement::Completed(json!("done")))
    );
    assert_eq!(done.usage.tool_calls, waiting.usage.tool_calls);
    assert!(done.usage.active_ms < waiting.usage.active_ms + 100);
    host.shutdown().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn shutdown_keeps_accepted_work_and_single_host_ownership() {
    let dir = TempDir::new().unwrap();
    let db = store(&dir).await;
    let agent = final_agent();
    let id = db.accept(&agent, "c", "k", json!(1), None).await.unwrap();
    let mut host = Host::start(db.clone(), agent.clone(), options()).unwrap();
    assert!(matches!(
        Host::start(db.clone(), agent.clone(), options()),
        Err(Error::DatabaseBusy)
    ));
    host.shutdown().await.unwrap();
    assert!(!db.status(id).await.unwrap().cancel_requested);
    let mut host = Host::start(db.clone(), agent, options()).unwrap();
    assert_eq!(
        wait_for(&host, id, RunState::is_terminal).await.phase,
        Phase::Settled(Settlement::Completed(json!("done")))
    );
    host.shutdown().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn terminal_uncertainty_remains_resolvable() {
    let dir = TempDir::new().unwrap();
    let db = store(&dir).await;
    let agent = final_agent();
    let id = db.accept(&agent, "c", "k", json!(1), None).await.unwrap();
    let mut state = db.claim(id).await.unwrap();
    state.phase = Phase::Settled(Settlement::Cancelled);
    state.cancel_requested = true;
    state.operations.push(Operation {
        call: ToolCall {
            id: "effect".into(),
            name: "mutate".into(),
            arguments: json!({}),
        },
        spec: spec("mutate", RetryPolicy::Never, false),
        idempotency_key: "stable".into(),
        status: OperationStatus::Unknown {
            reason: "cancelled in flight".into(),
        },
        approved: true,
    });
    db.commit(&mut state).await.unwrap();
    assert!(db.due(now_ms(), 10).await.unwrap().is_empty());
    db.resolve(
        id,
        "effect",
        Resolution::Reconcile(ToolResult {
            call_id: "effect".into(),
            value: json!("booked"),
            is_error: false,
        }),
    )
    .await
    .unwrap();
    let retained = db.status(id).await.unwrap();
    assert_eq!(retained.phase, Phase::Settled(Settlement::Cancelled));
    assert!(
        matches!(&retained.operations[0].status, OperationStatus::Complete(result) if result.value == json!("booked"))
    );
    assert!(matches!(
        &db.operations(id).await.unwrap()[0].status,
        OperationStatus::Complete(_)
    ));
    db.close().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn symlink_owner_is_exclusive_and_hardlinked_databases_are_rejected() {
    let dir = TempDir::new().unwrap();
    let db = store(&dir).await;
    std::os::unix::fs::symlink(
        dir.path().join("state.sqlite"),
        dir.path().join("alias.sqlite"),
    )
    .unwrap();
    assert!(matches!(
        SqliteStore::open(dir.path().join("alias.sqlite")).await,
        Err(Error::DatabaseBusy)
    ));
    db.close().await.unwrap();
    std::fs::hard_link(
        dir.path().join("state.sqlite"),
        dir.path().join("hard.sqlite"),
    )
    .unwrap();
    assert!(matches!(
        SqliteStore::open(dir.path().join("hard.sqlite")).await,
        Err(Error::Invalid(_))
    ));
}

#[tokio::test]
async fn safe_retry_reuses_key_and_completed_batch_members_are_not_repeated() {
    let dir = TempDir::new().unwrap();
    let db = store(&dir).await;
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen_handler = seen.clone();
    let tool = typed_tool::<Value, Value, _, _>(
        spec(
            "safe",
            RetryPolicy::Idempotent {
                contract: "provider-key-v1".into(),
            },
            false,
        ),
        move |arguments, context| {
            seen_handler
                .lock()
                .unwrap()
                .push((arguments.clone(), context.idempotency_key));
            async move { Ok(ToolOutput::Complete(arguments)) }
        },
    );
    let first = ToolCall {
        id: "already".into(),
        name: "safe".into(),
        arguments: json!(1),
    };
    let second = ToolCall {
        id: "retry".into(),
        name: "safe".into(),
        arguments: json!(2),
    };
    let response = ModelResponse::tools(vec![first.clone(), second.clone()]);
    let agent = agent(
        vec![response.clone(), ModelResponse::final_output(json!("done"))],
        vec![tool.clone()],
    );
    let id = db.accept(&agent, "c", "k", json!(1), None).await.unwrap();
    let mut state = db.claim(id).await.unwrap();
    state.phase = Phase::Tools;
    state.pending_response = Some(response);
    state.usage.model_calls = 1;
    state.usage.tool_calls = 2;
    state.usage.active_ms = 30_000;
    state.operations = vec![
        Operation {
            call: first,
            spec: tool.spec().clone(),
            idempotency_key: "key-a".into(),
            status: OperationStatus::Complete(ToolResult {
                call_id: "already".into(),
                value: json!(1),
                is_error: false,
            }),
            approved: true,
        },
        Operation {
            call: second,
            spec: tool.spec().clone(),
            idempotency_key: "key-b".into(),
            status: OperationStatus::Dispatched,
            approved: true,
        },
    ];
    db.commit(&mut state).await.unwrap();
    db.close().await.unwrap();
    let db = store(&dir).await;
    let mut host = Host::start(db.clone(), agent, options()).unwrap();
    let done = wait_for(&host, id, RunState::is_terminal).await;
    assert_eq!(
        done.phase,
        Phase::Settled(Settlement::Completed(json!("done")))
    );
    assert_eq!(*seen.lock().unwrap(), vec![(json!(2), "key-b".into())]);
    assert_eq!(done.usage.tool_calls, 3);
    assert!(done.usage.active_ms >= 30_000);
    assert!(
        matches!(&db.history("c").await.unwrap()[1], HistoryEntry::Exchange { results, .. } if results.iter().map(|r|r.call_id.as_str()).collect::<Vec<_>>() == vec!["already", "retry"])
    );
    host.shutdown().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn incompatible_binding_is_visible_and_stays_blocked_until_repaired() {
    let dir = TempDir::new().unwrap();
    let db = store(&dir).await;
    let original = final_agent();
    let id = db
        .accept(&original, "c", "k", json!(1), None)
        .await
        .unwrap();
    let changed = Agent::builder(
        "test",
        Arc::new(ScriptedModel::new(vec![ModelResponse::final_output(
            json!("changed"),
        )])),
    )
    .instructions("Changed instructions")
    .build()
    .unwrap();
    let mut host = Host::start(db.clone(), changed, options()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if db.last_error(id).await.unwrap().is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        db.last_error(id)
            .await
            .unwrap()
            .unwrap()
            .contains("incompatible")
    );
    assert!(db.due(now_ms(), 10).await.unwrap().is_empty());
    assert!(!db.status(id).await.unwrap().is_terminal());
    host.shutdown().await.unwrap();
    let mut host = Host::start(db.clone(), original, options()).unwrap();
    host.retry(id).await.unwrap();
    assert_eq!(
        wait_for(&host, id, RunState::is_terminal).await.phase,
        Phase::Settled(Settlement::Completed(json!("done")))
    );
    host.shutdown().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn waiting_sibling_does_not_hide_an_abandoned_dispatched_effect() {
    let dir = TempDir::new().unwrap();
    let db = store(&dir).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let tool =
        typed_tool::<Value, Value, _, _>(spec("mutate", RetryPolicy::Never, false), move |_, _| {
            count.fetch_add(1, Ordering::SeqCst);
            async { Ok(ToolOutput::Complete(json!("executed"))) }
        });
    let agent = agent(
        vec![ModelResponse::final_output(json!("unused"))],
        vec![tool.clone()],
    );
    let id = db.accept(&agent, "c", "k", json!(1), None).await.unwrap();
    let mut state = db.claim(id).await.unwrap();
    let calls = ["waiting", "dispatched"].map(|id| ToolCall {
        id: id.into(),
        name: "mutate".into(),
        arguments: json!({}),
    });
    state.phase = Phase::Tools;
    state.pending_response = Some(ModelResponse::tools(calls.to_vec()));
    state.usage.model_calls = 1;
    state.usage.tool_calls = 2;
    state.operations = vec![
        Operation {
            call: calls[0].clone(),
            spec: tool.spec().clone(),
            idempotency_key: "a".into(),
            status: OperationStatus::Waiting(WaitRequest::Input {
                prompt: "continue?".into(),
            }),
            approved: true,
        },
        Operation {
            call: calls[1].clone(),
            spec: tool.spec().clone(),
            idempotency_key: "b".into(),
            status: OperationStatus::Dispatched,
            approved: true,
        },
    ];
    db.commit(&mut state).await.unwrap();
    db.close().await.unwrap();
    let db = store(&dir).await;
    let mut host = Host::start(db.clone(), agent, options()).unwrap();
    let recovered = wait_for(&host, id, |s| {
        matches!(s.operations[1].status, OperationStatus::Unknown { .. })
    })
    .await;
    assert!(matches!(
        recovered.operations[0].status,
        OperationStatus::Waiting(_)
    ));
    assert!(db.due(now_ms(), 10).await.unwrap().is_empty());
    host.shutdown().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn late_error_cannot_suppress_cancel_resolution_or_repair() {
    use yielded_agent::CheckpointPosition;
    let dir = TempDir::new().unwrap();
    let db = store(&dir).await;
    let agent = final_agent();
    let cancelled = db
        .accept(&agent, "cancel", "k", json!(1), None)
        .await
        .unwrap();
    let state = db.claim(cancelled).await.unwrap();
    let old = CheckpointPosition::from(&state);
    db.request_cancel(cancelled).await.unwrap();
    assert!(
        !db.record_error(cancelled, old, "late".into())
            .await
            .unwrap()
    );
    assert!(db.due(now_ms(), 10).await.unwrap().contains(&cancelled));

    let resolved = db
        .accept(&agent, "resolve", "k", json!(1), None)
        .await
        .unwrap();
    let mut state = db.claim(resolved).await.unwrap();
    state.phase = Phase::Tools;
    state.operations.push(Operation {
        call: ToolCall {
            id: "wait".into(),
            name: "ask".into(),
            arguments: json!({}),
        },
        spec: spec("ask", RetryPolicy::ReadOnly, false),
        idempotency_key: "wait".into(),
        status: OperationStatus::Waiting(WaitRequest::Input { prompt: "?".into() }),
        approved: true,
    });
    db.commit(&mut state).await.unwrap();
    let old = CheckpointPosition::from(&state);
    db.resolve(resolved, "wait", Resolution::Input(json!("reply")))
        .await
        .unwrap();
    assert!(!db.record_error(resolved, old, "late".into()).await.unwrap());
    assert!(db.due(now_ms(), 10).await.unwrap().contains(&resolved));

    let repaired = db
        .accept(&agent, "repair", "k", json!(1), None)
        .await
        .unwrap();
    let state = db.claim(repaired).await.unwrap();
    let old = CheckpointPosition::from(&state);
    assert!(
        db.record_error(repaired, old, "temporarily unavailable".into())
            .await
            .unwrap()
    );
    assert!(!db.due(now_ms(), 10).await.unwrap().contains(&repaired));
    db.retry(repaired).await.unwrap();
    assert!(!db.record_error(repaired, old, "late".into()).await.unwrap());
    assert!(db.due(now_ms(), 10).await.unwrap().contains(&repaired));
    assert_eq!(db.last_error(repaired).await.unwrap(), None);
    db.close().await.unwrap();
}

#[tokio::test]
async fn safe_dispatched_sibling_with_wait_is_recovered_once_without_busy_loop() {
    let dir = TempDir::new().unwrap();
    let db = store(&dir).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let tool = typed_tool::<Value, Value, _, _>(
        spec("safe", RetryPolicy::ReadOnly, false),
        move |_, _| {
            counter.fetch_add(1, Ordering::SeqCst);
            async { Ok(ToolOutput::Complete(json!("unused"))) }
        },
    );
    let agent = agent(vec![], vec![tool.clone()]);
    let id = db.accept(&agent, "c", "k", json!(1), None).await.unwrap();
    let mut state = db.claim(id).await.unwrap();
    state.phase = Phase::Tools;
    state.operations = ["wait", "effect"]
        .iter()
        .map(|id| Operation {
            call: ToolCall {
                id: (*id).into(),
                name: "safe".into(),
                arguments: json!({}),
            },
            spec: tool.spec().clone(),
            idempotency_key: (*id).into(),
            status: if *id == "wait" {
                OperationStatus::Waiting(WaitRequest::Input { prompt: "?".into() })
            } else {
                OperationStatus::Dispatched
            },
            approved: true,
        })
        .collect();
    state.pending_response = Some(ModelResponse::tools(
        state.operations.iter().map(|o| o.call.clone()).collect(),
    ));
    db.commit(&mut state).await.unwrap();
    let generation = state.generation;
    let mut host = Host::start(db.clone(), agent, options()).unwrap();
    let recovered = wait_for(&host, id, |s| s.generation > generation).await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    let stable = db.status(id).await.unwrap();
    assert_eq!(stable.generation, recovered.generation);
    assert!(db.due(now_ms(), 10).await.unwrap().is_empty());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    host.shutdown().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn repaired_binding_retries_abandoned_dispatch_behind_an_input_wait() {
    let dir = TempDir::new().unwrap();
    let db = store(&dir).await;
    let tool =
        typed_tool::<Value, Value, _, _>(spec("mutate", RetryPolicy::Never, false), |_, _| async {
            Ok(ToolOutput::Complete(json!("must not run")))
        });
    let original = agent(vec![], vec![tool.clone()]);
    let id = db
        .accept(&original, "c", "k", json!(1), None)
        .await
        .unwrap();
    let mut state = db.claim(id).await.unwrap();
    state.phase = Phase::Tools;
    state.operations = ["wait", "effect"]
        .iter()
        .map(|id| Operation {
            call: ToolCall {
                id: (*id).into(),
                name: "mutate".into(),
                arguments: json!({}),
            },
            spec: tool.spec().clone(),
            idempotency_key: (*id).into(),
            status: if *id == "wait" {
                OperationStatus::Waiting(WaitRequest::Input { prompt: "?".into() })
            } else {
                OperationStatus::Dispatched
            },
            approved: true,
        })
        .collect();
    state.pending_response = Some(ModelResponse::tools(
        state.operations.iter().map(|o| o.call.clone()).collect(),
    ));
    db.commit(&mut state).await.unwrap();
    let changed = Agent::builder("test", Arc::new(ScriptedModel::new(vec![])))
        .instructions("Changed instructions")
        .tool(tool)
        .build()
        .unwrap();
    let mut host = Host::start(db.clone(), changed, options()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while db.last_error(id).await.unwrap().is_none() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    host.shutdown().await.unwrap();
    let mut host = Host::start(db.clone(), original, options()).unwrap();
    // Let the initial pass exclude the run with a retained binding error.
    tokio::time::sleep(Duration::from_millis(10)).await;
    host.retry(id).await.unwrap();
    let recovered = wait_for(&host, id, |s| {
        matches!(s.operations[1].status, OperationStatus::Unknown { .. })
    })
    .await;
    assert!(matches!(
        recovered.operations[0].status,
        OperationStatus::Waiting(_)
    ));
    assert!(db.due(now_ms(), 10).await.unwrap().is_empty());
    assert_eq!(db.last_error(id).await.unwrap(), None);
    host.shutdown().await.unwrap();
    db.close().await.unwrap();
}
