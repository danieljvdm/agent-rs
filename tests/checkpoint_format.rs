//! The SQLite checkpoint format rejects what it did not write: corrupt or foreign data is a
//! storage error, and a newer schema or state version is `Error::Incompatible`.
#![cfg(feature = "sqlite")]

use rusqlite::{Connection, params};
use serde_json::json;
use std::{path::PathBuf, sync::Arc};
use tempfile::TempDir;
use yielded_agent::{sqlite::SqliteStore, *};

struct Fixture {
    _directory: TempDir,
    path: PathBuf,
    expected: RunState,
}
impl Fixture {
    /// One accepted run with an input that exercises JSON number and string fidelity.
    async fn new() -> Self {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("checkpoint.sqlite");
        let store = SqliteStore::open(&path).await.unwrap();
        let agent = Agent::builder("format-test", Arc::new(ScriptedModel::new(vec![])))
            .instructions("Do not call the model")
            .build()
            .unwrap();
        let input = json!({
            "nested": [null, true, "quotes: \" and Unicode: λ", -0.0, 1.25, 9007199254740993u64],
            "object": {"answer": 42}
        });
        let id = store
            .accept(&agent, "conversation", "request", input.clone(), Some(1234))
            .await
            .unwrap();
        let expected = store.claim(id).await.unwrap();
        assert_eq!(expected.input, input);
        store.close().await.unwrap();
        Self {
            _directory: directory,
            path,
            expected,
        }
    }
    /// Apply `sql` to the closed database, then reopen it and load the run.
    async fn status_after(&self, sql: &str) -> Result<RunState> {
        Connection::open(&self.path)
            .unwrap()
            .execute(sql, params![self.expected.id.to_string()])
            .unwrap();
        let store = SqliteStore::open(&self.path).await?;
        let result = store.status(self.expected.id).await;
        store.close().await.unwrap();
        result
    }
}

#[tokio::test]
async fn reopened_runs_preserve_every_field_exactly() {
    let fixture = Fixture::new().await;
    let state = fixture
        .status_after("UPDATE runs SET id=id WHERE id=?1")
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_string(&state).unwrap(),
        serde_json::to_string(&fixture.expected).unwrap()
    );
    let zero = state.input["nested"][3].as_f64().unwrap();
    assert_eq!(zero.to_bits(), (-0.0f64).to_bits());
    assert_eq!(state.input["nested"][5].as_u64(), Some(9007199254740993));
}

/// What a run writes is pinned byte for byte, so a feature it does not use cannot grow it.
#[tokio::test]
async fn a_plain_run_keeps_its_stored_encoding() {
    let fixture = Fixture::new().await;
    let progress: String = Connection::open(&fixture.path)
        .unwrap()
        .query_row(
            "SELECT progress FROM checkpoints WHERE run_id=?1",
            [fixture.expected.id.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        progress,
        r#"{"cancel_requested":false,"phase":"Ready","usage":{"model_calls":0,"tool_calls":0,"input_tokens":0,"output_tokens":0,"active_ms":0},"pending_response":null,"operations":[]}"#
    );
    let encoded = serde_json::to_value(&fixture.expected).unwrap();
    let mut fields: Vec<&str> = encoded
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    fields.sort_unstable();
    assert_eq!(
        fields,
        [
            "agent_id",
            "binding",
            "cancel_requested",
            "conversation_id",
            "expires_at_ms",
            "generation",
            "history",
            "id",
            "input",
            "limits",
            "operations",
            "pending_response",
            "phase",
            "prior_context",
            "revision",
            "usage",
            "version",
        ]
    );
}

#[tokio::test]
async fn other_schema_versions_and_newer_state_versions_are_incompatible() {
    for version in [1, 3] {
        let fixture = Fixture::new().await;
        Connection::open(&fixture.path)
            .unwrap()
            .pragma_update(None, "user_version", version)
            .unwrap();
        let result = SqliteStore::open(&fixture.path).await;
        assert!(
            matches!(result, Err(Error::Incompatible(_))),
            "schema {version}: {:?}",
            result.err()
        );
    }
    let fixture = Fixture::new().await;
    let result = fixture
        .status_after("UPDATE runs SET version=version+1 WHERE id=?1")
        .await;
    assert!(matches!(result, Err(Error::Incompatible(_))), "{result:?}");
    let store = SqliteStore::open(&fixture.path).await.unwrap();
    assert!(matches!(
        store.claim(fixture.expected.id).await,
        Err(Error::Incompatible(_))
    ));
    store.close().await.unwrap();
}

#[tokio::test]
async fn corrupt_or_foreign_data_is_a_storage_error() {
    let fixture = Fixture::new().await;
    // The whole state where only progress belongs: an encoding this format never writes.
    let foreign = serde_json::to_string(&fixture.expected).unwrap();
    let cases = [
        ("malformed progress", "UPDATE checkpoints SET progress='{' WHERE run_id=?1".to_owned()),
        (
            "trailing content",
            "UPDATE checkpoints SET progress=progress||' trailing' WHERE run_id=?1".to_owned(),
        ),
        (
            "progress missing fields",
            "UPDATE checkpoints SET progress='{}' WHERE run_id=?1".to_owned(),
        ),
        (
            "foreign progress",
            format!(
                "UPDATE checkpoints SET progress='{}' WHERE run_id=?1",
                foreign.replace('\'', "''")
            ),
        ),
        (
            "invalid known field",
            "UPDATE checkpoints SET progress=json_set(progress,'$.cancel_requested','yes') WHERE run_id=?1"
                .to_owned(),
        ),
        (
            "malformed binding",
            "UPDATE runs SET binding='not json' WHERE id=?1".to_owned(),
        ),
        (
            "foreign prior context",
            "UPDATE runs SET prior_context='{\"entries\":[]}' WHERE id=?1".to_owned(),
        ),
        (
            "malformed history entry",
            "UPDATE history SET entry='[' WHERE run_id=?1".to_owned(),
        ),
        (
            "missing history",
            "DELETE FROM history WHERE run_id=?1".to_owned(),
        ),
        (
            "negative counter",
            "UPDATE checkpoints SET revision=-1 WHERE run_id=?1".to_owned(),
        ),
    ];
    for (case, sql) in cases {
        let fixture = Fixture::new().await;
        let result = fixture.status_after(&sql).await;
        assert!(
            matches!(result, Err(Error::Storage(_))),
            "{case}: {result:?}"
        );
        // Undecodable data keeps the decoder's error as the cause.
        if case == "malformed progress" {
            let error = result.unwrap_err();
            let cause = std::error::Error::source(&error).unwrap();
            assert!(cause.is::<serde_json::Error>(), "{case}: {cause:?}");
        }
    }
}
