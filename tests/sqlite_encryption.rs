#![cfg(feature = "sqlite")]
//! What the SQLite store leaves on disk, with and without a key.
use agent_rs::{Host, HostOptions, sqlite::SqliteStore, *};
use serde_json::{Value, json};
use std::{path::Path, sync::Arc, time::Duration};
use tempfile::TempDir;

/// Text the run carries in its input and in a tool call's arguments.
const MARKER: &str = "cleartext-marker-51c9";
const SQLITE_HEADER: &[u8] = b"SQLite format 3\0";

/// A run that stops at an approval, so its input, history and pending operation are all stored.
fn agent() -> Agent {
    let record = typed_tool::<Value, Value, _, _>(
        ToolSpec::new("record", "1").with_requires_approval(true),
        |arguments, _| async move { Ok(ToolOutput::Complete(arguments)) },
    );
    let model = ScriptedModel::new(vec![
        ModelResponse::tools(vec![ToolCall {
            id: "call-1".into(),
            name: "record".into(),
            arguments: json!({"note": MARKER}),
        }]),
        ModelResponse::final_output(json!("recorded")),
    ]);
    Agent::builder("test", Arc::new(model))
        .tool(record)
        .build()
        .unwrap()
}
fn options() -> HostOptions {
    HostOptions::default().with_poll_interval(Duration::from_millis(2))
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
fn awaits_approval(state: &RunState) -> bool {
    state
        .operations
        .iter()
        .any(|operation| operation.status == OperationStatus::Approval)
}
/// Submits the marked run and returns once it waits for approval.
async fn run_to_approval(store: &Arc<SqliteStore>) -> (Host<SqliteStore>, RunId) {
    let host = Host::start(store.clone(), agent(), options()).unwrap();
    let id = host
        .submit("conversation", "request", json!(MARKER), None)
        .await
        .unwrap();
    wait_for(&host, id, awaits_approval).await;
    (host, id)
}
/// The database file followed by its write-ahead log, if there is one.
fn on_disk(path: &Path) -> Vec<u8> {
    let mut bytes = std::fs::read(path).unwrap();
    let mut wal = path.as_os_str().to_owned();
    wal.push("-wal");
    if let Ok(log) = std::fs::read(wal) {
        bytes.extend(log);
    }
    bytes
}
fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|window| window == needle)
}

/// The control for the encrypted cases below: it shows that these checks do find stored text.
#[tokio::test]
async fn without_a_key_the_file_is_plain_sqlite_holding_the_runs_text() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.sqlite");
    let store = Arc::new(SqliteStore::open(&path).await.unwrap());
    let (mut host, _) = run_to_approval(&store).await;
    assert!(contains(&on_disk(&path), MARKER.as_bytes()));
    host.shutdown().await.unwrap();
    store.close().await.unwrap();
    let closed = on_disk(&path);
    assert!(closed.starts_with(SQLITE_HEADER));
    assert!(contains(&closed, MARKER.as_bytes()));
}
