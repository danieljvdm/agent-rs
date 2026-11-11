#![cfg(feature = "sqlite")]
//! What the SQLite store leaves on disk, with and without a key.
use agent_rs::{
    Host, HostOptions,
    sqlite::{SqliteKey, SqliteOptions, SqliteStore},
    *,
};
use serde_json::{Value, json};
use std::{path::Path, sync::Arc, time::Duration};
use tempfile::TempDir;

/// Text the run carries in its input and in a tool call's arguments.
const MARKER: &str = "cleartext-marker-51c9";
const SQLITE_HEADER: &[u8] = b"SQLite format 3\0";

/// A run that stops at an approval, so its input, history and pending operation are all stored.
/// The scripted model answers by turn number, so the same agent serves before and after a restart.
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
fn keyed(byte: u8) -> SqliteOptions {
    SqliteOptions::default().with_key(SqliteKey::new([byte; 32]))
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
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
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

#[test]
fn a_key_is_not_printed() {
    let printed = format!("{:?}", keyed(7));
    assert!(printed.contains("SqliteKey(..)"), "{printed}");
    assert!(!printed.contains('7'), "{printed}");
}

/// A build whose SQLite has no encryption must refuse a key: SQLite itself would ignore it and
/// write the file unencrypted.
#[cfg(not(feature = "sqlcipher"))]
#[tokio::test]
async fn a_key_is_refused_when_sqlite_has_no_encryption() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.sqlite");
    let Err(error) = SqliteStore::open_with(&path, keyed(7)).await else {
        panic!("a key was accepted without encryption");
    };
    assert!(matches!(error, Error::Config(_)), "{error}");
    assert!(std::fs::read(&path).unwrap().is_empty());
    // The refusal leaves nothing behind that stops an ordinary open.
    SqliteStore::open(&path)
        .await
        .unwrap()
        .close()
        .await
        .unwrap();
}

#[cfg(feature = "sqlcipher")]
mod with_sqlcipher {
    use super::*;

    #[tokio::test]
    async fn with_a_key_neither_the_file_nor_its_log_holds_the_runs_text() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.sqlite");
        let store = Arc::new(SqliteStore::open_with(&path, keyed(7)).await.unwrap());
        let (mut host, _) = run_to_approval(&store).await;
        let open = on_disk(&path);
        assert!(!contains(&open, MARKER.as_bytes()));
        assert!(!contains(&open, SQLITE_HEADER));
        host.shutdown().await.unwrap();
        store.close().await.unwrap();
        let closed = on_disk(&path);
        assert!(!closed.is_empty());
        assert!(!contains(&closed, MARKER.as_bytes()));
        assert!(!contains(&closed, SQLITE_HEADER));
    }

    #[tokio::test]
    async fn a_waiting_run_resumes_after_reopening_with_the_same_key() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.sqlite");
        let store = Arc::new(SqliteStore::open_with(&path, keyed(7)).await.unwrap());
        let (mut host, id) = run_to_approval(&store).await;
        host.shutdown().await.unwrap();
        store.close().await.unwrap();

        let store = Arc::new(SqliteStore::open_with(&path, keyed(7)).await.unwrap());
        let mut host = Host::start(store.clone(), agent(), options()).unwrap();
        let state = wait_for(&host, id, awaits_approval).await;
        assert_eq!(state.input, json!(MARKER));
        host.resolve(id, "call-1", Resolution::Approve(true))
            .await
            .unwrap();
        let state = wait_for(&host, id, |state| matches!(state.phase, Phase::Settled(_))).await;
        assert_eq!(
            state.phase,
            Phase::Settled(Settlement::Completed(json!("recorded")))
        );
        host.shutdown().await.unwrap();
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_key_that_does_not_fit_the_file_is_a_configuration_error() {
        let dir = TempDir::new().unwrap();
        let encrypted = dir.path().join("encrypted.sqlite");
        let plain = dir.path().join("plain.sqlite");
        SqliteStore::open_with(&encrypted, keyed(7))
            .await
            .unwrap()
            .close()
            .await
            .unwrap();
        SqliteStore::open(&plain)
            .await
            .unwrap()
            .close()
            .await
            .unwrap();

        // Without a key, an encrypted file is like any file that is not a database.
        let Err(error) = SqliteStore::open(&encrypted).await else {
            panic!("an encrypted file opened without its key");
        };
        assert!(matches!(error, Error::Storage(_)), "{error}");

        let refused = [
            SqliteStore::open_with(&encrypted, keyed(8)).await,
            SqliteStore::open_with(&plain, keyed(7)).await,
        ];
        for attempt in refused {
            let Err(error) = attempt else {
                panic!("the file was opened");
            };
            assert!(matches!(error, Error::Config(_)), "{error}");
            assert!(
                std::error::Error::source(&error).is_some(),
                "the database's own error is kept as the cause"
            );
            // Neither key appears in the message, in the form SQLite was given.
            let message = format!("{error} {error:?}");
            assert!(
                !message.contains("0707") && !message.contains("0808"),
                "{message}"
            );
        }
        // A refused attempt changes nothing: each file still opens as it was created.
        SqliteStore::open_with(&encrypted, keyed(7))
            .await
            .unwrap()
            .close()
            .await
            .unwrap();
        SqliteStore::open(&plain)
            .await
            .unwrap()
            .close()
            .await
            .unwrap();
    }
}
