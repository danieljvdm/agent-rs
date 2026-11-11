//! PostgreSQL-only boundaries: ownership, failure, versions, and deterministic costs. The shared
//! durability contract runs from `tests/storage_conformance.rs`.
//!
//! Tests that need a server are ignored by default; `scripts/test-postgres.sh` provisions an
//! isolated database and runs them with `--include-ignored`. Each test creates its own schema.
#![cfg(feature = "postgres")]
use agent_rs::{
    postgres::{Config, NoTls, PostgresOptions, PostgresStore, StoreMetrics},
    *,
};
use async_trait::async_trait;
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;
use tokio_postgres::Client;

// ---------------------------------------------------------------------------------------------
// Budget table (exact). Each write is one statement, so one transaction and one round trip.
// ---------------------------------------------------------------------------------------------

/// Transactions for `claim` plus `Agent::drive` to settlement: equal to SQLite's in
/// `tests/cost_budget.rs`, one per claim and one per commit.
const TRANSACTIONS: [(&str, usize, usize, u64); 4] = [
    ("no_tools", 0, 0, 3),
    ("one_tool_round", 1, 1, 9),
    ("four_tool_rounds", 4, 1, 27),
    ("four_parallel_tools", 1, 4, 12),
];
/// Round trips for `claim` (load, then the fenced update) plus `drive`: one per commit.
const CLAIM_ROUND_TRIPS: u64 = 2;
const COMMIT_ROUND_TRIPS: u64 = 1;
/// Round trips for a new `accept` in a conversation with under one page of history: lookup,
/// one context page, and the acceptance insert.
const ACCEPT_ROUND_TRIPS: u64 = 3;
/// Round trips for a replayed `accept` of the same key: the lookup alone.
const REPLAY_ROUND_TRIPS: u64 = 1;
/// Round trips for `status`: one query reassembles acceptance, checkpoint and history.
const STATUS_ROUND_TRIPS: u64 = 1;
/// Round trips for `status` of a run with children: one more query reads every child, with
/// its acceptance and history, however many there are.
const STATUS_WITH_CHILDREN_ROUND_TRIPS: u64 = 2;
/// Transactions for `claim` plus `drive` of a parent that delegates once, then answers: equal
/// to SQLite's in `tests/cost_budget.rs`, 10 when the child answers at once and 6 more for each
/// of the child's tool rounds. A child's commits are its parent's, so each is one round trip.
const TRANSACTIONS_DELEGATE_ONCE: u64 = 10;
const TRANSACTIONS_PER_CHILD_TOOL_ROUND: u64 = 6;

struct Fixture {
    config: Config,
    schema: String,
}
impl Fixture {
    fn new() -> Self {
        Self::named(&format!("postgres_{}", RunId::new_v4().simple()))
    }
    fn named(schema: &str) -> Self {
        let config = std::env::var("AGENT_RS_TEST_POSTGRES_URL")
            .expect("run scripts/test-postgres.sh against an isolated PostgreSQL database")
            .parse()
            .unwrap();
        Self {
            config,
            schema: schema.into(),
        }
    }
    async fn open(&self) -> Result<PostgresStore> {
        self.open_with(PostgresOptions::default()).await
    }
    async fn open_with(&self, options: PostgresOptions) -> Result<PostgresStore> {
        let mut config = self.config.clone();
        config.application_name(&self.schema);
        PostgresStore::connect(&config, &self.schema, NoTls, options).await
    }
    /// Open once the previous owner's server process has released the schema. A terminated
    /// backend releases its locks as it exits, shortly after `pg_terminate_backend` returns.
    async fn reopen(&self) -> PostgresStore {
        for _ in 0..200 {
            match self.open().await {
                Err(Error::StoreBusy) => tokio::time::sleep(Duration::from_millis(25)).await,
                opened => return opened.unwrap(),
            }
        }
        panic!("schema {} stayed owned", self.schema);
    }
    async fn observer(&self) -> Client {
        let mut config = self.config.clone();
        config.application_name("agent-rs-test-observer");
        let (client, connection) = config.connect(NoTls).await.unwrap();
        tokio::spawn(connection);
        client
    }
}
fn agent(responses: Vec<ModelResponse>, tools: Vec<Arc<dyn Tool>>) -> Agent {
    Agent::builder("postgres-test", Arc::new(ScriptedModel::new(responses)))
        .instructions("test")
        .tools(tools)
        .build()
        .unwrap()
}
fn options() -> HostOptions {
    HostOptions::default().with_poll_interval(Duration::from_millis(5))
}
async fn eventually<T>(mut check: impl AsyncFnMut() -> Option<T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(value) = check().await {
                return value;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("condition not reached")
}
/// Work done between two readings of `PostgresStore::metrics`.
#[derive(Debug)]
struct Cost {
    transactions: u64,
    writes: u64,
    round_trips: u64,
    payload_bytes: u64,
}
fn since(after: StoreMetrics, before: StoreMetrics) -> Cost {
    Cost {
        transactions: after.transactions - before.transactions,
        writes: after.writes - before.writes,
        round_trips: after.round_trips - before.round_trips,
        payload_bytes: after.payload_bytes - before.payload_bytes,
    }
}

#[tokio::test]
async fn invalid_options_are_rejected_before_connecting() {
    // An unreachable configuration: rejection must not depend on a server.
    let config = Config::new();
    for schema in [
        "",
        "pg_catalog",
        "information_schema",
        "MixedCase",
        "x\"; DROP SCHEMA public; --",
        &"x".repeat(64),
    ] {
        assert!(matches!(
            PostgresStore::connect(&config, schema, NoTls, PostgresOptions::default()).await,
            Err(Error::Config(_))
        ));
    }
    let zero = PostgresOptions::default().with_operation_timeout(Duration::ZERO);
    assert!(matches!(
        PostgresStore::connect(&config, "valid", NoTls, zero).await,
        Err(Error::Config(_))
    ));
}

#[tokio::test]
#[ignore = "needs PostgreSQL: scripts/test-postgres.sh"]
async fn ownership_is_exclusive_per_schema_and_released_by_close() {
    let fixture = Fixture::new();
    let store = fixture.open().await.unwrap();
    assert!(matches!(fixture.open().await, Err(Error::StoreBusy)));
    let other = Fixture::new();
    other.open().await.unwrap().close().await.unwrap();
    let agent = agent(vec![], vec![]);
    let id = store
        .accept(&agent, "c", "key", json!(1), None)
        .await
        .unwrap();
    let mut stale = store.claim(id).await.unwrap();
    store.close().await.unwrap();
    assert!(matches!(store.lease(), Err(Error::Storage(_))));
    assert!(matches!(
        store.commit(&mut stale).await,
        Err(Error::Storage(_))
    ));
    // An orderly close releases ownership before it returns.
    let replacement = fixture.open().await.unwrap();
    assert_eq!(replacement.status(id).await.unwrap().input, json!(1));
    replacement.claim(id).await.unwrap();
    assert!(matches!(
        replacement.commit(&mut stale).await,
        Err(Error::Stale)
    ));
    replacement.close().await.unwrap();
}

#[tokio::test]
#[ignore = "needs PostgreSQL: scripts/test-postgres.sh"]
async fn a_failed_write_rolls_back_checkpoint_history_and_operations_together() {
    let fixture = Fixture::new();
    let store = fixture.open().await.unwrap();
    let agent = agent(vec![], vec![]);
    let id = store
        .accept(&agent, "c", "key", json!(1), None)
        .await
        .unwrap();
    let mut state = store.claim(id).await.unwrap();
    let revision = state.revision;
    let observer = fixture.observer().await;
    // Fail the history append, after the fenced checkpoint update in the same statement.
    observer
        .batch_execute(&format!(
            "CREATE FUNCTION {0}.fail_append() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN RAISE EXCEPTION 'injected append failure'; END $$;
             CREATE TRIGGER fail_append BEFORE INSERT ON {0}.history
                 FOR EACH ROW WHEN (NEW.ordinal = 1) EXECUTE FUNCTION {0}.fail_append();",
            fixture.schema,
        ))
        .await
        .unwrap();
    state.phase = Phase::Settled(Settlement::Completed(json!("done")));
    state.history.push(HistoryEntry::Exchange {
        response: ModelResponse::final_output(json!("done")),
        results: vec![],
    });
    state.operations.push(Operation::new(
        ToolCall {
            id: "op".into(),
            name: "effect".into(),
            arguments: json!({}),
        },
        ToolSpec::new("effect", "1"),
        "effect-key",
        OperationStatus::Complete(ToolResult {
            call_id: "op".into(),
            value: json!("done"),
            is_error: false,
        }),
    ));
    assert!(matches!(
        store.commit(&mut state).await,
        Err(Error::Storage(_))
    ));
    assert_eq!(state.revision, revision);
    let retained = store.status(id).await.unwrap();
    assert_eq!(retained.phase, Phase::Ready);
    assert_eq!(retained.revision, revision);
    assert_eq!(store.history("c").await.unwrap().len(), 1);
    assert!(store.operations(id).await.unwrap().is_empty());
    assert_eq!(store.due(now_ms(), 10).await.unwrap(), vec![id]);
    // A failed statement does not end the session.
    observer
        .batch_execute(&format!(
            "DROP TRIGGER fail_append ON {}.history",
            fixture.schema
        ))
        .await
        .unwrap();
    store.commit(&mut state).await.unwrap();
    assert_eq!(store.history("c").await.unwrap().len(), 2);
    assert_eq!(store.operations(id).await.unwrap().len(), 1);
    assert!(matches!(store.position(id).await, Err(Error::NotFound)));
    assert!(store.due(now_ms(), 10).await.unwrap().is_empty());
    store.close().await.unwrap();
}

#[tokio::test]
#[ignore = "needs PostgreSQL: scripts/test-postgres.sh"]
async fn commits_without_a_history_hint_append_each_entry_once() {
    let fixture = Fixture::new();
    let store = fixture.open().await.unwrap();
    let agent = agent(vec![], vec![]);
    let id = store
        .accept(&agent, "c", "k", json!(1), None)
        .await
        .unwrap();
    let mut state = store.claim(id).await.unwrap();
    let exchange = |n: u64| HistoryEntry::Exchange {
        response: ModelResponse::final_output(json!(n)),
        results: vec![],
    };
    state.history.extend([exchange(1), exchange(2)]);
    store.commit(&mut state).await.unwrap();
    store.close().await.unwrap();
    // A new store has no hint for this run, so it sends the whole history; stored entries are
    // skipped and only the new one is appended.
    let store = fixture.open().await.unwrap();
    state.history.push(exchange(3));
    store.commit(&mut state).await.unwrap();
    assert_eq!(store.status(id).await.unwrap().history, state.history);
    assert_eq!(store.history("c").await.unwrap(), state.history);
    let mut shrunk = state.clone();
    shrunk.history.pop();
    assert!(matches!(
        store.commit(&mut shrunk).await,
        Err(Error::Input(_))
    ));
    store.close().await.unwrap();
}

#[tokio::test]
#[ignore = "needs PostgreSQL: scripts/test-postgres.sh"]
async fn an_operation_timeout_fails_the_store_without_reconnecting() {
    let fixture = Fixture::new();
    let store = fixture
        .open_with(PostgresOptions::default().with_operation_timeout(Duration::from_millis(500)))
        .await
        .unwrap();
    let agent = agent(vec![], vec![]);
    let observer = fixture.observer().await;
    observer
        .batch_execute(&format!(
            "BEGIN; LOCK TABLE {}.runs IN ACCESS EXCLUSIVE MODE",
            fixture.schema
        ))
        .await
        .unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        store.accept(&agent, "blocked", "key", json!(1), None),
    )
    .await
    .unwrap();
    assert!(matches!(result, Err(Error::Storage(_))));
    observer.batch_execute("ROLLBACK").await.unwrap();
    assert!(matches!(store.lease(), Err(Error::Storage(_))));
    assert!(matches!(
        store.accept(&agent, "later", "key", json!(1), None).await,
        Err(Error::Storage(_))
    ));
    assert!(matches!(store.close().await, Err(Error::Storage(_))));
    let replacement = fixture.reopen().await;
    assert!(replacement.history("blocked").await.unwrap().is_empty());
    replacement.close().await.unwrap();
}

#[tokio::test]
#[ignore = "needs PostgreSQL: scripts/test-postgres.sh"]
async fn dropping_a_caller_does_not_cancel_queued_acceptance() {
    let fixture = Fixture::new();
    let store = Arc::new(fixture.open().await.unwrap());
    let agent = agent(vec![], vec![]);
    let observer = fixture.observer().await;
    observer
        .batch_execute(&format!(
            "BEGIN; LOCK TABLE {}.runs IN ACCESS EXCLUSIVE MODE",
            fixture.schema
        ))
        .await
        .unwrap();
    let pending = {
        let (store, agent) = (store.clone(), agent.clone());
        tokio::spawn(async move { store.accept(&agent, "c", "key", json!(1), None).await })
    };
    eventually(async || {
        let blocked: bool = observer
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity
                    WHERE application_name=$1 AND wait_event_type='Lock')",
                &[&fixture.schema],
            )
            .await
            .unwrap()
            .get(0);
        blocked.then_some(())
    })
    .await;
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    observer.batch_execute("ROLLBACK").await.unwrap();
    // This queued read follows the acceptance, though its caller stopped waiting.
    assert_eq!(
        store.history("c").await.unwrap(),
        vec![HistoryEntry::Input(json!(1))]
    );
    let id = store
        .accept(&agent, "c", "key", json!(1), None)
        .await
        .unwrap();
    assert_eq!(store.status(id).await.unwrap().input, json!(1));
    store.close().await.unwrap();
}

struct PendingEffect {
    spec: ToolSpec,
    started: Notify,
    calls: AtomicUsize,
    cleaned: AtomicBool,
}
#[async_trait]
impl Tool for PendingEffect {
    fn spec(&self) -> &ToolSpec {
        &self.spec
    }
    fn validate(&self, _: &Value) -> Result<()> {
        Ok(())
    }
    async fn execute(&self, _: Value, _: ToolContext) -> Result<ToolOutput<Value>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.started.notify_one();
        std::future::pending().await
    }
    async fn cleanup(&self, _: ToolContext) -> Result<()> {
        tokio::time::sleep(Duration::from_millis(5)).await;
        self.cleaned.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
#[ignore = "needs PostgreSQL: scripts/test-postgres.sh"]
async fn a_lost_session_stops_the_host_and_recovery_keeps_mutations_unknown() {
    let fixture = Fixture::new();
    let store = Arc::new(fixture.open().await.unwrap());
    let tool = Arc::new(PendingEffect {
        spec: ToolSpec::new("effect", "1").with_retry(RetryPolicy::Never),
        started: Notify::new(),
        calls: AtomicUsize::new(0),
        cleaned: AtomicBool::new(false),
    });
    let agent = agent(
        vec![ModelResponse::tools(vec![ToolCall {
            id: "op".into(),
            name: "effect".into(),
            arguments: json!({}),
        }])],
        vec![tool.clone()],
    );
    let mut host = Host::start(store.clone(), agent.clone(), options()).unwrap();
    let id = host.submit("c", "key", json!(1), None).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), tool.started.notified())
        .await
        .unwrap();
    let observer = fixture.observer().await;
    let terminated = observer
        .query(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name=$1",
            &[&fixture.schema],
        )
        .await
        .unwrap();
    assert_eq!(terminated.len(), 1);
    assert!(terminated[0].get::<_, bool>(0));
    eventually(async || host.health().is_err().then_some(())).await;
    assert!(host.shutdown().await.is_err());
    assert!(tool.cleaned.load(Ordering::SeqCst));
    // The failed store never reconnects and refuses another host.
    assert!(matches!(store.lease(), Err(Error::Storage(_))));
    assert!(store.close().await.is_err());
    let replacement = Arc::new(fixture.reopen().await);
    let mut host = Host::start(replacement.clone(), agent, options()).unwrap();
    let recovered = eventually(async || {
        let state = host.status(id).await.unwrap();
        matches!(state.operations[0].status, OperationStatus::Unknown { .. }).then_some(state)
    })
    .await;
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
    assert_eq!(recovered.usage.tool_calls, 1);
    assert!(recovered.usage.active_ms >= 31_000);
    host.shutdown().await.unwrap();
    replacement.close().await.unwrap();
}

#[tokio::test]
#[ignore = "needs PostgreSQL: scripts/test-postgres.sh"]
async fn incompatible_layouts_and_states_fail_explicitly() {
    let fixture = Fixture::new();
    let store = fixture.open().await.unwrap();
    let id = store
        .accept(&agent(vec![], vec![]), "c", "key", json!(1), None)
        .await
        .unwrap();
    store.close().await.unwrap();
    let observer = fixture.observer().await;
    let schema = &fixture.schema;
    // Earlier layouts, including 3 without `children`, are refused rather than migrated.
    for version in [2, 3] {
        observer
            .batch_execute(&format!(
                "UPDATE {schema}.store_metadata SET version={version}"
            ))
            .await
            .unwrap();
        assert!(matches!(fixture.open().await, Err(Error::Incompatible(_))));
    }
    observer
        .batch_execute(&format!(
            "UPDATE {schema}.store_metadata SET version=4; UPDATE {schema}.runs SET version=999"
        ))
        .await
        .unwrap();
    let store = fixture.open().await.unwrap();
    assert!(matches!(
        store.status(id).await,
        Err(Error::Incompatible(_))
    ));
    store.close().await.unwrap();
    // A schema holding relations this adapter did not create is refused, not adopted.
    let foreign = Fixture::new();
    observer
        .batch_execute(&format!(
            "CREATE SCHEMA {0}; CREATE TABLE {0}.runs (id TEXT)",
            foreign.schema
        ))
        .await
        .unwrap();
    assert!(matches!(foreign.open().await, Err(Error::Incompatible(_))));
}

#[tokio::test]
#[ignore = "needs PostgreSQL: scripts/test-postgres.sh"]
async fn accepted_json_with_nul_survives_restart_and_discovery() {
    let fixture = Fixture::new();
    let store = fixture.open().await.unwrap();
    let agent = agent(
        vec![ModelResponse::final_output(json!("complete\u{0}value"))],
        vec![],
    );
    let input = json!({"nested": ["before\u{0}after"]});
    let with_nul = store
        .accept(&agent, "with-nul", "key", input.clone(), None)
        .await
        .unwrap();
    let unrelated = store
        .accept(&agent, "unrelated", "key", json!("normal"), None)
        .await
        .unwrap();
    store.close().await.unwrap();

    let store = Arc::new(fixture.open().await.unwrap());
    assert_eq!(store.status(with_nul).await.unwrap().input, input);
    let mut host = Host::start(store.clone(), agent, options()).unwrap();
    eventually(async || {
        host.health()
            .expect("accepted JSON must not stop discovery of other runs");
        let done = host.status(with_nul).await.unwrap().is_terminal()
            && host.status(unrelated).await.unwrap().is_terminal();
        done.then_some(())
    })
    .await;
    for id in [with_nul, unrelated] {
        assert_eq!(
            store.status(id).await.unwrap().phase,
            Phase::Settled(Settlement::Completed(json!("complete\u{0}value")))
        );
    }
    assert_eq!(
        store.history("with-nul").await.unwrap()[0],
        HistoryEntry::Input(input)
    );
    host.shutdown().await.unwrap();
    store.close().await.unwrap();
}

#[derive(serde::Deserialize, schemars::JsonSchema)]
struct Echo {
    n: u64,
}
fn responses(rounds: usize, batch: usize) -> Vec<ModelResponse> {
    let mut responses: Vec<_> = (0..rounds)
        .map(|round| {
            ModelResponse::tools(
                (0..batch)
                    .map(|index| ToolCall {
                        id: format!("round-{round}-{index}"),
                        name: "echo".into(),
                        arguments: json!({"n": round * batch + index}),
                    })
                    .collect(),
            )
        })
        .collect();
    responses.push(ModelResponse::final_output(json!("done")));
    responses
}
fn echo() -> Arc<dyn Tool> {
    typed_tool::<Echo, u64, _, _>(
        ToolSpec::new("echo", "1").with_retry(RetryPolicy::ReadOnly),
        |args, _| async move { Ok(ToolOutput::Complete(args.n)) },
    )
}
fn costed(rounds: usize, batch: usize) -> Agent {
    agent(responses(rounds, batch), vec![echo()])
}
/// A parent that delegates once to `child`, then answers.
fn delegating(child: Agent) -> Agent {
    Agent::builder(
        "postgres-parent",
        Arc::new(ScriptedModel::new(vec![
            ModelResponse::tools(vec![ToolCall {
                id: "delegate".into(),
                name: "delegate".into(),
                arguments: json!("go"),
            }]),
            ModelResponse::final_output(json!("done")),
        ])),
    )
    .instructions("Delegate, then answer.")
    .subagents(1, [Subagent::new("delegate", "Do the task.", child)])
    .build()
    .unwrap()
}
/// A child doing `rounds` rounds of one tool, within an allocation its parent can afford.
fn child(rounds: usize) -> Agent {
    Agent::builder(
        "postgres-child",
        Arc::new(ScriptedModel::new(responses(rounds, 1))),
    )
    .instructions("test")
    .tools(vec![echo()])
    .limits(
        Limits::default()
            .with_max_model_calls(16)
            .with_max_tool_calls(16)
            .with_max_tokens(10_000)
            .with_active_budget(Duration::from_secs(120)),
    )
    .build()
    .unwrap()
}
/// A child that waits for input, so its parent stays unsettled with it.
fn waiting_child() -> Agent {
    let ask = typed_tool::<Value, Value, _, _>(
        ToolSpec::new("ask", "1").with_retry(RetryPolicy::ReadOnly),
        |_, _| async {
            Ok(ToolOutput::Wait(WaitRequest::Input {
                prompt: "which?".into(),
            }))
        },
    );
    Agent::builder(
        "postgres-child",
        Arc::new(ScriptedModel::new(vec![ModelResponse::tools(vec![
            ToolCall {
                id: "ask".into(),
                name: "ask".into(),
                arguments: json!(null),
            },
        ])])),
    )
    .tool(ask)
    .limits(
        Limits::default()
            .with_max_model_calls(2)
            .with_max_tool_calls(1)
            .with_active_budget(Duration::from_secs(10)),
    )
    .build()
    .unwrap()
}

#[tokio::test]
#[ignore = "needs PostgreSQL: scripts/test-postgres.sh"]
async fn transactions_and_round_trips_per_run() {
    let fixture = Fixture::new();
    let store = fixture.open().await.unwrap();
    for (name, rounds, batch, transactions) in TRANSACTIONS {
        let agent = costed(rounds, batch);
        let before = store.metrics();
        let id = store
            .accept(&agent, name, "measured", json!("go"), None)
            .await
            .unwrap();
        let accepted = store.metrics();
        assert_eq!(since(accepted, before).transactions, 1, "{name}");
        assert_eq!(
            since(accepted, before).round_trips,
            ACCEPT_ROUND_TRIPS,
            "{name}"
        );
        let mut state = store.claim(id).await.unwrap();
        agent
            .drive(&mut state, &store, RunControl::default())
            .await
            .unwrap();
        assert!(state.is_terminal(), "{name}");
        let run = since(store.metrics(), accepted);
        eprintln!("postgres_{name}: {run:?}");
        assert_eq!(run.transactions, transactions, "{name}");
        assert_eq!(
            run.round_trips,
            CLAIM_ROUND_TRIPS + (transactions - 1) * COMMIT_ROUND_TRIPS,
            "{name}"
        );
        let before = store.metrics();
        assert_eq!(
            store
                .accept(&agent, name, "measured", json!("go"), None)
                .await
                .unwrap(),
            id
        );
        store.status(id).await.unwrap();
        let reads = since(store.metrics(), before);
        assert_eq!(reads.transactions, 0, "{name}");
        assert_eq!(
            reads.round_trips,
            REPLAY_ROUND_TRIPS + STATUS_ROUND_TRIPS,
            "{name}"
        );
    }
    store.close().await.unwrap();
}

#[tokio::test]
#[ignore = "needs PostgreSQL: scripts/test-postgres.sh"]
async fn delegation_costs_one_round_trip_per_commit() {
    let fixture = Fixture::new();
    let store = fixture.open().await.unwrap();
    for rounds in [0, 1, 4, 8] {
        let agent = delegating(child(rounds));
        let conversation = format!("delegation-{rounds}");
        let id = store
            .accept(&agent, &conversation, "measured", json!("go"), None)
            .await
            .unwrap();
        let accepted = store.metrics();
        let mut state = store.claim(id).await.unwrap();
        agent
            .drive(&mut state, &store, RunControl::default())
            .await
            .unwrap();
        assert_eq!(
            state.phase,
            Phase::Settled(Settlement::Completed(json!("done")))
        );
        let run = since(store.metrics(), accepted);
        eprintln!("postgres_delegation_child_rounds_{rounds}: {run:?}");
        let transactions =
            TRANSACTIONS_DELEGATE_ONCE + rounds as u64 * TRANSACTIONS_PER_CHILD_TOOL_ROUND;
        assert_eq!(run.transactions, transactions, "{rounds} child rounds");
        // No commit was refused or resent: the hints were right.
        assert_eq!(
            run.round_trips,
            CLAIM_ROUND_TRIPS + (transactions - 1) * COMMIT_ROUND_TRIPS,
            "{rounds} child rounds"
        );
        let before = store.metrics();
        let stored = store.status(id).await.unwrap();
        assert_eq!(
            since(store.metrics(), before).round_trips,
            STATUS_WITH_CHILDREN_ROUND_TRIPS
        );
        assert_eq!(
            serde_json::to_value(&stored.children).unwrap(),
            serde_json::to_value(&state.children).unwrap()
        );
        assert_eq!(stored.usage, state.usage);
        assert_eq!(stored.child_usage, state.child_usage);
        let child = &state.children[0].state;
        assert_eq!(child.usage.tool_calls as usize, rounds);
        assert_eq!(
            store.history(&child.conversation_id).await.unwrap(),
            child.history
        );
        assert_eq!(store.operations(child.id).await.unwrap().len(), rounds);
    }
    store.close().await.unwrap();
}

#[tokio::test]
#[ignore = "needs PostgreSQL: scripts/test-postgres.sh"]
async fn children_are_written_only_behind_their_roots_fence() {
    let fixture = Fixture::new();
    let store = fixture.open().await.unwrap();
    let agent = delegating(waiting_child());
    let id = store
        .accept(&agent, "parent", "key", json!("go"), None)
        .await
        .unwrap();
    let mut state = store.claim(id).await.unwrap();
    agent
        .drive(&mut state, &store, RunControl::default())
        .await
        .unwrap();
    assert_eq!(state.phase, Phase::Tools);
    let child = state.children[0].state.clone();
    assert_eq!(child.history.len(), 1);
    let observer = fixture.observer().await;
    let schema = &fixture.schema;

    // A child whose stored progress no longer matches this store's hint (here, from the last
    // commit of `drive`) is resent whole, once, in one transaction.
    observer
        .batch_execute(&format!(
            "UPDATE {schema}.children SET history_len = 0 WHERE child_id = '{0}';
             DELETE FROM {schema}.history WHERE run_id = '{0}'",
            child.id
        ))
        .await
        .unwrap();
    state.children[0].state.usage.input_tokens = 7;
    state.children[0].state.revision += 1;
    let before = store.metrics();
    store.commit(&mut state).await.unwrap();
    let resent = since(store.metrics(), before);
    // The refused statement, the read that classifies it, and the whole resend.
    assert_eq!(resent.round_trips, 3);
    assert_eq!(resent.transactions, 1);
    let stored = store.status(id).await.unwrap();
    assert_eq!(
        serde_json::to_value(&stored.children).unwrap(),
        serde_json::to_value(&state.children).unwrap()
    );
    assert_eq!(
        store.history(&child.conversation_id).await.unwrap(),
        child.history
    );

    // A refused fence admits no child: no acceptance, progress, history or operations. The
    // failed commit also makes the store forget its hints for every child it named.
    let mut stale = state.clone();
    let mut admitted = stale.children[0].clone();
    admitted.call_id = "other".into();
    admitted.state.id = RunId::new_v4();
    admitted.state.conversation_id = format!("child:{id}:other");
    stale.children.push(admitted.clone());
    stale.revision -= 1;
    assert!(matches!(store.commit(&mut stale).await, Err(Error::Stale)));
    let other = admitted.state.id.to_string();
    for (table, column) in [
        ("runs", "id"),
        ("children", "child_id"),
        ("history", "run_id"),
    ] {
        let rows: i64 = observer
            .query_one(
                &format!("SELECT count(*) FROM {schema}.{table} WHERE {column} = $1"),
                &[&other],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(rows, 0, "{table}");
    }
    assert!(
        store
            .operations(admitted.state.id)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(store.status(id).await.unwrap().children.len(), 1);

    // A child's history is append-only, and a refused child refuses its root. Without hints,
    // the commit sends the root and child whole.
    let mut shrunk = state.clone();
    shrunk.children[0].state.history.clear();
    shrunk.children[0].state.revision += 1;
    assert!(matches!(
        store.commit(&mut shrunk).await,
        Err(Error::Input(_))
    ));
    assert_eq!(store.position(id).await.unwrap().revision, state.revision);
    store.close().await.unwrap();
}

#[tokio::test]
#[ignore = "needs PostgreSQL: scripts/test-postgres.sh"]
async fn bytes_written_after_acceptance_do_not_grow_with_retained_context() {
    let fixture = Fixture::new();
    let store = fixture.open().await.unwrap();
    let seed = agent(vec![ModelResponse::final_output(json!("ok")); 8], vec![]);
    let measure = async |conversation: &str| {
        let agent = costed(4, 1);
        let id = store
            .accept(&agent, conversation, "measured", json!("go"), None)
            .await
            .unwrap();
        let before = store.metrics();
        let mut state = store.claim(id).await.unwrap();
        agent
            .drive(&mut state, &store, RunControl::default())
            .await
            .unwrap();
        (since(store.metrics(), before), state.prior_context.len())
    };
    let (empty, none) = measure("empty").await;
    assert_eq!(none, 0);
    for index in 0..4 {
        let input = json!({"retained": format!("{index:04}{}", "x".repeat(32 * 1024))});
        let id = store
            .accept(&seed, "retained", &format!("seed-{index}"), input, None)
            .await
            .unwrap();
        let mut state = store.claim(id).await.unwrap();
        seed.drive(&mut state, &store, RunControl::default())
            .await
            .unwrap();
    }
    let (retained, entries) = measure("retained").await;
    assert_eq!(entries, 8);
    eprintln!("postgres claim-to-settlement: {empty:?} vs {retained:?} with retained context");
    assert_eq!(retained.transactions, empty.transactions);
    assert_eq!(retained.writes, empty.writes);
    assert_eq!(retained.round_trips, empty.round_trips);
    // Only the digits of wall-clock active time may differ.
    assert!(retained.payload_bytes.abs_diff(empty.payload_bytes) < 64);
    store.close().await.unwrap();
}

/// The current UTC Unix time in milliseconds, the unit of run expiry and timer waits.
fn now_ms() -> u64 {
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH);
    u64::try_from(now.expect("clock after 1970").as_millis()).expect("milliseconds fit u64")
}
