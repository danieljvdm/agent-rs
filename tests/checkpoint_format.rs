#![cfg(feature = "sqlite")]

use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::sync::Arc;
use tempfile::TempDir;
use yielded_agent::{sqlite::SqliteStore, *};

struct CheckpointFixture {
    directory: TempDir,
    expected: RunState,
    encoded: String,
}

impl CheckpointFixture {
    async fn new(input: Value) -> Self {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("checkpoint.sqlite");
        let store = SqliteStore::open(&path).await.unwrap();
        let agent = Agent::new::<Value, Value>(
            "format-test",
            "Do not call the model",
            Arc::new(ScriptedModel::new(vec![])),
            ModelConfig::default(),
            vec![],
            Limits::default(),
        )
        .unwrap();
        let id = store
            .accept(&agent, "conversation", "request", input.clone(), Some(1234))
            .await
            .unwrap();
        let expected = store.status(id).await.unwrap();
        assert_eq!(expected.input, input);
        store.close().await.unwrap();
        let encoded = Connection::open(path)
            .unwrap()
            .query_row(
                "SELECT state FROM active WHERE run_id=?1",
                [id.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        Self {
            directory,
            expected,
            encoded,
        }
    }

    async fn status_after_reopen(&self, encoded: &str) -> Result<RunState> {
        let path = self.directory.path().join("checkpoint.sqlite");
        {
            let connection = Connection::open(&path).unwrap();
            assert_eq!(
                connection
                    .execute(
                        "UPDATE active SET state=?1 WHERE run_id=?2",
                        params![encoded, self.expected.id.to_string()],
                    )
                    .unwrap(),
                1
            );
        }
        let store = SqliteStore::open(path).await.unwrap();
        let result = store.status(self.expected.id).await;
        store.close().await.unwrap();
        result
    }
}

fn append_member(encoded: &str, member: &str) -> String {
    format!("{},{member}}}", encoded.strip_suffix('}').unwrap())
}

fn assert_state_eq(actual: &RunState, expected: &RunState, case: &str) {
    // Compare every public state field, retaining number types and signed zero.
    assert_eq!(
        serde_json::to_string(actual).unwrap(),
        serde_json::to_string(expected).unwrap(),
        "{case}"
    );
}

#[tokio::test]
async fn compatible_checkpoint_encodings_preserve_state_and_last_duplicate_values() {
    let fixture = CheckpointFixture::new(json!({
        "nested": [null, true, "quotes: \" and Unicode: λ", -0.0, 1.25],
        "object": {"answer": 42}
    }))
    .await;
    let mut cancelled = fixture.expected.clone();
    cancelled.cancel_requested = true;
    let mut changed_input = fixture.expected.clone();
    changed_input.input = json!({"choice": "last"});
    let cases = [
        (
            "writer encoding",
            fixture.encoded.clone(),
            &fixture.expected,
        ),
        (
            "pretty encoding",
            serde_json::to_string_pretty(&fixture.expected).unwrap(),
            &fixture.expected,
        ),
        (
            "reordered fields",
            serde_json::to_string(&serde_json::to_value(&fixture.expected).unwrap()).unwrap(),
            &fixture.expected,
        ),
        (
            "unknown field",
            append_member(
                &fixture.encoded,
                r#""future":{"version":99,"nested":[1,2]}"#,
            ),
            &fixture.expected,
        ),
        (
            "last supported version wins",
            format!("{{\"version\":99,{}", &fixture.encoded[1..]),
            &fixture.expected,
        ),
        (
            "last valid ordinary field wins over invalid type",
            format!(
                "{{\"cancel_requested\":\"invalid\",{}",
                &fixture.encoded[1..]
            ),
            &fixture.expected,
        ),
        (
            "last ordinary field changes state",
            append_member(&fixture.encoded, r#""cancel_requested":true"#),
            &cancelled,
        ),
        (
            "last nested and top-level input values win",
            append_member(
                &fixture.encoded,
                r#""input":{"choice":"first","choice":"last"}"#,
            ),
            &changed_input,
        ),
    ];
    for (case, encoded, expected) in cases {
        let actual = fixture
            .status_after_reopen(&encoded)
            .await
            .unwrap_or_else(|error| panic!("{case}: {error}"));
        assert_state_eq(&actual, expected, case);
    }
}

#[tokio::test]
async fn invalid_checkpoint_encodings_keep_version_and_storage_errors_distinct() {
    let fixture = CheckpointFixture::new(Value::Null).await;
    let mut unsupported = fixture.expected.clone();
    unsupported.version = 2;
    let mut missing_version = serde_json::to_value(&fixture.expected).unwrap();
    missing_version.as_object_mut().unwrap().remove("version");
    let state = &fixture.expected;
    // A complete positional struct encoding still has no named format version.
    let positional = json!([
        state.version,
        state.id,
        state.conversation_id,
        state.agent_id,
        state.binding,
        state.input,
        state.limits,
        state.expires_at_ms,
        state.generation,
        state.revision,
        state.cancel_requested,
        state.phase,
        state.usage,
        state.prior_context,
        state.history,
        state.pending_response,
        state.operations
    ]);
    let deep_unknown = format!("\"future\":{}0{}", "[".repeat(160), "]".repeat(160));
    let incompatible = [
        (
            "unsupported version",
            serde_json::to_string(&unsupported).unwrap(),
        ),
        ("missing version", missing_version.to_string()),
        (
            "floating version",
            append_member(&fixture.encoded, r#""version":1.0"#),
        ),
        (
            "fractional version",
            append_member(&fixture.encoded, r#""version":1.5"#),
        ),
        (
            "string version",
            append_member(&fixture.encoded, r#""version":"1""#),
        ),
        (
            "negative version",
            append_member(&fixture.encoded, r#""version":-1"#),
        ),
        (
            "null version",
            append_member(&fixture.encoded, r#""version":null"#),
        ),
        (
            "last unsupported version wins",
            append_member(&fixture.encoded, r#""version":2"#),
        ),
        (
            "version check precedes required fields",
            r#"{"version":2}"#.into(),
        ),
        ("top-level array", "[]".into()),
        ("positional struct array", positional.to_string()),
    ];
    for (case, encoded) in incompatible {
        let result = fixture.status_after_reopen(&encoded).await;
        assert!(
            matches!(result, Err(Error::Incompatible(_))),
            "{case}: {result:?}"
        );
    }
    let storage_errors = [
        ("malformed supported state", r#"{"version":1,"#.into()),
        ("malformed unsupported state", r#"{"version":2,"#.into()),
        ("trailing content", format!("{} trailing", fixture.encoded)),
        (
            "supported state missing required fields",
            r#"{"version":1}"#.into(),
        ),
        (
            "invalid known field",
            append_member(&fixture.encoded, r#""cancel_requested":"invalid""#),
        ),
        (
            "unknown field numeric overflow",
            append_member(&fixture.encoded, r#""future":1e400"#),
        ),
        (
            "unknown field excessive nesting",
            append_member(&fixture.encoded, &deep_unknown),
        ),
    ];
    for (case, encoded) in storage_errors {
        let result = fixture.status_after_reopen(&encoded).await;
        assert!(
            matches!(result, Err(Error::Storage(_))),
            "{case}: {result:?}"
        );
    }
}

#[tokio::test]
async fn nested_json_numbers_keep_their_types_and_negative_zero() {
    let fixture = CheckpointFixture::new(Value::Null).await;
    let cases = [
        (
            "integer-spelled negative zero",
            r#"{"nested":[-0,0,1]}"#,
            json!({"nested": [-0.0, 0, 1]}),
        ),
        (
            "canonical float values",
            r#"{"nested":[-0.0,0.0,1.25,125.0,9007199254740993]}"#,
            json!({"nested": [-0.0, 0.0, 1.25, 125.0, 9007199254740993u64]}),
        ),
        (
            "exponent-spelled float values",
            r#"{"nested":[-0e0,0e0,125e-2,1.25e2,1e0]}"#,
            json!({"nested": [-0.0, 0.0, 1.25, 125.0, 1.0]}),
        ),
    ];
    for (case, input, expected_input) in cases {
        let encoded = fixture
            .encoded
            .replacen(r#""input":null"#, &format!("\"input\":{input}"), 1);
        let mut expected = fixture.expected.clone();
        expected.input = expected_input;
        let actual = fixture
            .status_after_reopen(&encoded)
            .await
            .unwrap_or_else(|error| panic!("{case}: {error}"));
        assert_state_eq(&actual, &expected, case);
        assert_eq!(
            actual.input["nested"][0].as_f64().unwrap().to_bits(),
            (-0.0f64).to_bits(),
            "{case}"
        );
    }
}

#[tokio::test]
async fn completed_payload_respects_the_checkpoint_nesting_limit() {
    let fixture = CheckpointFixture::new(Value::Null).await;
    for (depth, accepted) in [(124, true), (125, false)] {
        let mut payload = Value::Null;
        for _ in 0..depth {
            payload = Value::Array(vec![payload]);
        }
        let mut expected = fixture.expected.clone();
        expected.phase = Phase::Settled(Settlement::Completed(payload));
        let encoded = serde_json::to_string(&expected).unwrap();
        let result = fixture.status_after_reopen(&encoded).await;
        if accepted {
            assert_state_eq(
                &result.expect("124 nested payload arrays remain readable"),
                &expected,
                "124 nested payload arrays",
            );
        } else {
            assert!(
                matches!(result, Err(Error::Storage(_))),
                "125 nested payload arrays exceed the checkpoint limit: {result:?}"
            );
        }
    }
}
