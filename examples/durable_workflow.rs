//! An inspectable local workflow: approval → input → time → uncertain mutation → reconciliation.
//! All model and tool work is deterministic. No credentials or network access are needed.
#[cfg(feature = "sqlite")]
mod demo {
    use async_trait::async_trait;
    use serde_json::{Value, json};
    use std::{
        path::Path,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };
    use yielded_agent::{sqlite::SqliteStore, *};

    fn spec(name: &str, approval: bool, retry: RetryPolicy) -> ToolSpec {
        ToolSpec {
            name: name.into(),
            description: name.into(),
            input_schema: Value::Null,
            version: "1".into(),
            retry,
            execution: Execution::Sequential,
            requires_approval: approval,
        }
    }
    fn call(name: &str) -> ModelResponse {
        ModelResponse::tools(vec![ToolCall {
            id: name.into(),
            name: name.into(),
            arguments: json!({}),
        }])
    }
    // Simulates process loss after an external effect, before its outcome transaction.
    struct LoseOutcome<'a>(&'a SqliteStore);
    #[async_trait]
    impl Checkpoint for LoseOutcome<'_> {
        async fn commit(&self, state: &mut RunState) -> Result<()> {
            if state.operations.iter().any(|op| {
                op.call.id == "record" && matches!(op.status, OperationStatus::Complete(_))
            }) {
                return Err(Error::storage(
                    "injected loss before recording external outcome",
                ));
            }
            self.0.commit(state).await
        }
    }
    async fn reopen(path: &Path, old: SqliteStore) -> Result<SqliteStore> {
        old.close().await?;
        drop(old);
        SqliteStore::open(path).await
    }
    pub async fn run() -> Result<()> {
        let temporary = tempfile::tempdir().map_err(Error::storage)?;
        let path = std::env::args()
            .nth(1)
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| temporary.path().join("workflow.sqlite"));
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
            typed_tool::<Value, Value, _, _>(
                spec("record", false, RetryPolicy::Never),
                move |_, _| {
                    let effects = record_effects.clone();
                    async move {
                        effects.fetch_add(1, Ordering::SeqCst);
                        Ok(ToolOutput::Complete(json!("record-123")))
                    }
                },
            ),
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
        let mut store = SqliteStore::open(&path).await?;
        let id = store
            .accept(
                &agent.erased(),
                "personal",
                "request-1",
                json!("Record a reminder"),
                None,
            )
            .await?;
        println!("Accepted {id} in {}", path.display());
        let mut state = store.claim(id).await?;
        agent
            .drive(&mut state, &store, RunControl::default())
            .await?;
        assert!(matches!(
            state.operations[0].status,
            OperationStatus::Approval
        ));
        println!("Approval wait committed; restart");

        store = reopen(&path, store).await?;
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

        store = reopen(&path, store).await?;
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

        store = reopen(&path, store).await?;
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

        store = reopen(&path, store).await?;
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
        store.close().await
    }
}
#[cfg(feature = "sqlite")]
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> yielded_agent::Result<()> {
    demo::run().await
}
#[cfg(not(feature = "sqlite"))]
fn main() {
    eprintln!("Enable the sqlite feature to run this example.");
}
