//! Own binary: installing a global subscriber is a once-per-process operation.
use agent_rs::*;
use serde_json::json;
use std::{sync::Arc, time::Duration};
#[path = "../src/tracing_test.rs"]
mod tracing_capture;

#[tokio::test]
async fn a_host_started_before_the_global_subscriber_still_emits_run_spans() {
    let agent = Agent::builder(
        "late-subscriber",
        Arc::new(ScriptedModel::new(vec![ModelResponse::final_output(
            json!("done"),
        )])),
    )
    .build()
    .unwrap();
    let mut host =
        Host::start(Arc::new(MemoryStore::new()), agent, HostOptions::default()).unwrap();
    tokio::task::yield_now().await;
    let capture = tracing_capture::Capture::at(tracing::Level::INFO);
    tracing::subscriber::set_global_default(capture.clone()).unwrap();
    let id = host
        .submit("conversation", "key", json!("input"), None)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !host.status(id).await.unwrap().is_terminal() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    host.shutdown().await.unwrap();
    let state = capture.0.lock().unwrap();
    let run = state
        .spans
        .iter()
        .find(|span| span.metadata.name() == "invoke_agent")
        .expect("host pinned the absent startup subscriber");
    assert_eq!(run.parent, None);
    assert_eq!(run.fields["run_id"], id.to_string());
}
