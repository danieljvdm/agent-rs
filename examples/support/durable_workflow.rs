//! An inspectable workflow: approval → input → time → uncertain mutation → reconciliation,
//! closing and reopening storage at each boundary. All model and tool work is deterministic.
//! Each storage example supplies how to open and close its store.
use agent_rs::*;
use async_trait::async_trait;
use serde_json::{Value, json};
use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

fn spec(name: &str, approval: bool, retry: RetryPolicy) -> ToolSpec {
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
// Simulates process loss after an external effect, before its outcome transaction.
struct LoseOutcome<'a, S>(&'a S);
#[async_trait]
impl<S: DurableStore> Checkpoint for LoseOutcome<'_, S> {
    async fn commit(&self, state: &mut RunState) -> Result<()> {
        if state
            .operations
            .iter()
            .any(|op| op.call.id == "record" && matches!(op.status, OperationStatus::Complete(_)))
        {
            return Err(Error::storage(
                "injected loss before recording external outcome",
            ));
        }
        self.0.commit(state).await
    }
}

/// Runs the workflow over the store `open` returns, closing each store with `close` before
/// reopening it, as a restarted process would.
pub async fn run<S, Open, Opened, Close, Closed>(open: Open, close: Close) -> Result<()>
where
    S: DurableStore,
    Open: Fn() -> Opened,
    Opened: Future<Output = Result<S>>,
    Close: Fn(S) -> Closed,
    Closed: Future<Output = Result<()>>,
{
    let reopen = async |store: S| {
        close(store).await?;
        open().await
    };
    let effects = Arc::new(AtomicUsize::new(0));
    let record_effects = effects.clone();
    let tools = vec![
        typed_tool::<Value, Value, _, _>(
            spec("review", true, RetryPolicy::ReadOnly),
            |_, _| async { Ok(ToolOutput::Complete(json!("approved"))) },
        ),
        typed_tool::<Value, Value, _, _>(
            spec("details", false, RetryPolicy::ReadOnly),
            |_, _| async {
                Ok(ToolOutput::Wait(WaitRequest::Input {
                    prompt: "What should be recorded?".into(),
                }))
            },
        ),
        typed_tool::<Value, Value, _, _>(
            spec("later", false, RetryPolicy::ReadOnly),
            |_, _| async {
                Ok(ToolOutput::Wait(WaitRequest::Until {
                    unix_ms: now_ms() + 25,
                }))
            },
        ),
        typed_tool::<Value, Value, _, _>(spec("record", false, RetryPolicy::Never), move |_, _| {
            let effects = record_effects.clone();
            async move {
                effects.fetch_add(1, Ordering::SeqCst);
                Ok(ToolOutput::Complete(json!("record-123")))
            }
        }),
    ];
    let model = Arc::new(ScriptedModel::new(vec![
        call("review"),
        call("details"),
        call("later"),
        call("record"),
        ModelResponse::final_output(json!("Recorded record-123")),
    ]));
    let agent = Agent::builder("personal-task-v1", model)
        .instructions("Review and record a task.")
        .tools(tools)
        .input::<String>()
        .output::<String>()
        .build()?;
    let mut store = open().await?;
    let id = store
        .accept(
            &agent.erased(),
            "personal",
            "request-1",
            json!("Record a reminder"),
            None,
        )
        .await?;
    println!("Accepted {id}");
    let mut state = store.claim(id).await?;
    agent
        .drive(&mut state, &store, RunControl::default())
        .await?;
    assert!(matches!(
        state.operations[0].status,
        OperationStatus::Approval
    ));
    println!("Approval wait committed; restart");

    store = reopen(store).await?;
    store
        .resolve(id, "review", Resolution::Approve(true))
        .await?;
    state = store.claim(id).await?;
    agent
        .drive(&mut state, &store, RunControl::default())
        .await?;
    assert!(matches!(
        state.operations[0].status,
        OperationStatus::Waiting(WaitRequest::Input { .. })
    ));
    println!("Input wait committed; restart");

    store = reopen(store).await?;
    store
        .resolve(id, "details", Resolution::Input(json!("Call the dentist")))
        .await?;
    state = store.claim(id).await?;
    agent
        .drive(&mut state, &store, RunControl::default())
        .await?;
    let deadline = match state.operations[0].status {
        OperationStatus::Waiting(WaitRequest::Until { unix_ms }) => unix_ms,
        _ => panic!("expected timed wait"),
    };
    let budget_before_wait = state.usage.active_ms;
    println!("Timed wait committed at {deadline}; restart");

    store = reopen(store).await?;
    tokio::time::sleep(Duration::from_millis(deadline.saturating_sub(now_ms()) + 1)).await;
    assert!(store.due(now_ms(), 8).await?.contains(&id));
    state = store.claim(id).await?;
    assert_eq!(state.usage.active_ms, budget_before_wait);
    let failure = agent
        .drive(&mut state, &LoseOutcome(&store), RunControl::default())
        .await;
    assert!(matches!(failure, Err(Error::Storage(_))));
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    println!("External effect happened; outcome commit lost; restart");

    store = reopen(store).await?;
    state = store.claim(id).await?;
    agent
        .drive(&mut state, &store, RunControl::default())
        .await?;
    assert!(matches!(
        state.operations[0].status,
        OperationStatus::Unknown { .. }
    ));
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    println!("Unknown outcome retained; application reconciles record-123");
    store
        .resolve(
            id,
            "record",
            Resolution::Reconcile(ToolResult {
                call_id: "record".into(),
                value: json!("record-123"),
                is_error: false,
            }),
        )
        .await?;
    state = store.claim(id).await?;
    agent
        .drive(&mut state, &store, RunControl::default())
        .await?;
    assert_eq!(
        state.phase,
        Phase::Settled(Settlement::Completed(json!("Recorded record-123")))
    );
    assert_eq!(state.usage.model_calls, 5);
    assert_eq!(state.usage.tool_calls, 4);
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert!(!store.due(now_ms(), 8).await?.contains(&id));
    println!("{}", serde_json::to_string_pretty(&state).unwrap());
    println!(
        "Retained {} history groups and {} operations; effect executed exactly once in this fixture (not an external exactly-once guarantee).",
        store.history("personal").await?.len(),
        store.operations(id).await?.len()
    );
    close(store).await
}

/// The current UTC Unix time in milliseconds, the unit of run expiry and timer waits.
fn now_ms() -> u64 {
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH);
    u64::try_from(now.expect("clock after 1970").as_millis()).expect("milliseconds fit u64")
}
