//! Deterministic consumer shared by the recurring example and its integration test.
use serde_json::{Value, json};
use std::{path::Path, sync::Arc, time::Duration};
use yielded_agent::{sqlite::SqliteStore, *};

pub async fn run(directory: &Path) -> Result<Vec<RunState>> {
    let tasks = json!(["Book the dentist", "Renew the passport"]);
    // The application supplies a cached inbox snapshot. This tool owns no child
    // work; real I/O adapters must join their resources in Tool::cleanup.
    let inbox = tasks.clone();
    let tool = typed_tool::<Value, Value, _, _>(
        ToolSpec::new("read_inbox", "1")
            .with_description("Read the personal task inbox snapshot")
            .with_retry(RetryPolicy::ReadOnly),
        move |_, _| {
            let tasks = inbox.clone();
            async move { Ok(ToolOutput::Complete(tasks)) }
        },
    );
    let agent = Agent::builder(
        "recurring-inbox-v1",
        Arc::new(ScriptedModel::new(vec![
            ModelResponse::tools(vec![ToolCall {
                id: "inbox".into(),
                name: "read_inbox".into(),
                arguments: json!({}),
            }]),
            ModelResponse::final_output(tasks.clone()),
        ])),
    )
    .instructions("Read the personal task inbox and return tasks needing attention.")
    .tool(tool)
    .input::<String>()
    .output::<Vec<String>>()
    .build()?
    .erased();
    let agents = [agent.clone()];
    let options = AutomationOptions::default();
    let path = directory.join("recurring.sqlite");
    let mut store = Arc::new(SqliteStore::open(&path).await?);
    // Provision once, with fixed values: a fresh clock reading as `first_at_ms` on every
    // restart would be a different definition.
    let hourly = IntervalAutomation::new(
        "personal-inbox",
        json!("Review my task inbox"),
        1_000,
        1_000,
    );
    store
        .create_automation(&agent, "personal-inbox", hourly)
        .await?;
    // An explicit demonstration clock stands in for real hourly intervals. A resident
    // application calls `Host::fire_automations` from its poll loop instead.
    let accepted = store.fire_due(&agents, 1_000, options).await?;
    let OccurrenceOutcome::Admitted { run_id: first } = accepted.occurrences[0].outcome else {
        return Err(Error::input("expected the first inbox review"));
    };

    // Lose the process after admission, before any model or tool work: the run is durable
    // and the repeated wakeup admits nothing new.
    store.close().await?;
    store = Arc::new(SqliteStore::open(&path).await?);
    assert!(
        store
            .fire_due(&agents, 1_000, options)
            .await?
            .occurrences
            .is_empty()
    );
    let mut host = Host::start(
        store.clone(),
        agent,
        HostOptions::default().with_poll_interval(Duration::from_millis(2)),
    )?;
    let work = async {
        let mut completed = vec![wait_for(&host, first).await?];
        // The second firing is on time; the third coalesces a downtime gap of 3.5 intervals.
        for now in [2_000, 5_500] {
            let report = host.store().fire_due(&agents, now, options).await?;
            let OccurrenceOutcome::Admitted { run_id } = report.occurrences[0].outcome else {
                return Err(Error::input("expected a recurring inbox review"));
            };
            completed.push(wait_for(&host, run_id).await?);
        }
        let current = store.automation("personal-inbox").await?;
        store
            .change_automation(&current.id, current.revision, AutomationChange::Pause)
            .await?;
        assert!(
            store
                .fire_due(&agents, 10_000, options)
                .await?
                .occurrences
                .is_empty()
        );
        for state in &completed {
            assert_eq!(
                state.phase,
                Phase::Settled(Settlement::Completed(tasks.clone()))
            );
            assert_eq!(state.usage.tool_calls, 1);
            let operations = store.operations(state.id).await?;
            assert_eq!(operations.len(), 1);
            assert!(matches!(&operations[0].status,
                OperationStatus::Complete(result) if result.value == tasks && !result.is_error));
        }
        let occurrences = store.occurrences("personal-inbox", 0, 10).await?;
        assert_eq!(occurrences.len(), 3);
        assert_eq!(occurrences[2].scheduled_at_ms, 5_000);
        assert_eq!(occurrences[2].coalesced, 2);
        Ok(completed)
    }
    .await;
    let shutdown = host.shutdown().await;
    let close = store.close().await;
    shutdown?;
    close?;
    work
}

async fn wait_for(host: &Host<SqliteStore>, id: RunId) -> Result<RunState> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            host.health()?;
            let state = host.status(id).await?;
            if state.is_terminal() {
                return Ok(state);
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .map_err(|_| Error::input("inbox review timed out"))?
}
