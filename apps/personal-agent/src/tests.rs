use super::*;
use axum::{
    Router,
    body::Body,
    http::{HeaderMap, Method, Request, StatusCode},
};
use serde_json::json;
use std::{
    fs::OpenOptions,
    io::Write,
    path::PathBuf,
    process::{Child, Command, Stdio},
};
use tower::ServiceExt;

const TOKEN: &str = "test-only-personal-agent-access-token";

/// Sends requests through the served router in-process: routing, authentication, extraction,
/// body limits, error mapping and response headers all run as they do behind a socket.
#[derive(Clone)]
struct Client(Router);
struct Reply {
    status: StatusCode,
    headers: HeaderMap,
    body: Value,
}
impl Client {
    async fn send(
        &self,
        method: Method,
        path: &str,
        token: Option<&str>,
        body: Option<Value>,
    ) -> Reply {
        let mut request = Request::builder().method(method).uri(path);
        if let Some(token) = token {
            request = request.header("authorization", format!("Bearer {token}"));
        }
        let request = match body {
            Some(body) => request
                .header("content-type", "application/json")
                .body(Body::from(body.to_string())),
            None => request.body(Body::empty()),
        }
        .unwrap();
        let response = self.0.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes)
                .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()))
        };
        Reply {
            status,
            headers,
            body,
        }
    }
    async fn get(&self, path: &str) -> Value {
        let reply = self.send(Method::GET, path, Some(TOKEN), None).await;
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
        reply.body
    }
    async fn post(&self, path: &str, body: Value) -> (StatusCode, Value) {
        let reply = self.send(Method::POST, path, Some(TOKEN), Some(body)).await;
        (reply.status, reply.body)
    }
    async fn wait(&self, id: Uuid, predicate: impl Fn(&Value) -> bool) -> Value {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let state = self.get(&format!("/api/runs/{id}")).await;
                if predicate(&state) {
                    return state;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("run did not reach its expected durable state")
    }
    async fn decide(&self, id: Uuid, decision: &Value, value: Value) -> StatusCode {
        self.post(&format!("/api/runs/{id}/resolve"), json!({
            "call_id": decision["call_id"], "generation": decision["generation"], "revision": decision["revision"], "decision": value,
        })).await.0
    }
    async fn create_conversation(&self, title: &str) -> Uuid {
        let conversation = Uuid::new_v4();
        let (status, response) = self
            .post(
                "/api/conversations",
                json!({"id": conversation, "title": title}),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{response}");
        conversation
    }
    async fn submit(&self, title: &str) -> (Uuid, Uuid) {
        let conversation = self.create_conversation(title).await;
        let (status, response) = self
            .post(
                &format!("/api/conversations/{conversation}/runs"),
                json!({"key": "request-1", "input": title}),
            )
            .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{response}");
        (
            conversation,
            serde_json::from_value(response["run_id"].clone()).unwrap(),
        )
    }
    async fn answer(&self, id: Uuid, original: &Value, value: Value) -> StatusCode {
        // A tool can publish its wait before its final usage-accounting checkpoint.
        // On a fenced conflict, re-review the exact same question before resubmitting.
        let mut current = original.clone();
        for _ in 0..10 {
            let status = self
                .decide(id, &current, json!({"kind": "input", "value": value}))
                .await;
            if status != StatusCode::CONFLICT {
                return status;
            }
            let run = self.get(&format!("/api/runs/{id}")).await;
            current = run["decisions"]
                .as_array()
                .unwrap()
                .iter()
                .find(|d| d["call_id"] == original["call_id"])
                .expect("the question must still be pending")
                .clone();
            for field in [
                "run_id",
                "call_id",
                "kind",
                "prompt",
                "arguments",
                "actions",
            ] {
                assert_eq!(
                    current[field], original[field],
                    "a changed question requires a different answer"
                );
            }
        }
        panic!("input wait never stabilized");
    }
}

struct Server {
    app: Arc<Application>,
    client: Client,
}
impl std::ops::Deref for Server {
    type Target = Client;
    fn deref(&self) -> &Client {
        &self.client
    }
}
impl Server {
    async fn start<I, O>(directory: &Path, agent: Agent<I, O>) -> Self {
        let app = Application::open(directory, agent).await.unwrap();
        let client = Client(router(app.clone(), AccessToken::new(TOKEN.into()).unwrap()));
        Self { app, client }
    }
    async fn demo(directory: &Path) -> Self {
        let notebook = notebook::Notebook::open(directory.join("notebook.sqlite"))
            .await
            .unwrap();
        Self::start(directory, notebook.demo_agent().unwrap()).await
    }
    async fn stop(self) {
        drop(self.client);
        self.app.shutdown().await.unwrap();
    }
}

fn decision(value: &Value, kind: &str) -> bool {
    value["decisions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|d| d["kind"] == kind)
}
fn settled(value: &Value) -> bool {
    value["phase"].get("Settled").is_some()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconnect_restart_and_denial_use_durable_history() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::demo(dir.path()).await;
    let (conversation, id) = server.submit("Prepare the travel checklist").await;
    let input = server.wait(id, |s| decision(s, "input")).await;
    let submit_path = format!("/api/conversations/{conversation}/runs");
    let original = json!({"key": "request-1", "input": "Prepare the travel checklist"});
    let duplicate = server.post(&submit_path, original.clone()).await;
    assert_eq!(duplicate.0, StatusCode::ACCEPTED);
    assert_eq!(duplicate.1["run_id"], json!(id));
    let changed = server
        .post(
            &submit_path,
            json!({"key": "request-1", "input": "changed"}),
        )
        .await;
    assert_eq!(changed.0, StatusCode::CONFLICT);
    assert_eq!(changed.1["error"]["code"], "idempotency_conflict");
    let busy = server
        .post(
            &submit_path,
            json!({"key": "request-2", "input": "more work"}),
        )
        .await;
    assert_eq!(busy.0, StatusCode::CONFLICT);
    assert_eq!(busy.1["error"]["code"], "conversation_busy");
    // A new client receives the same saved history without subscribing to any events.
    let reconnected = server.get(&format!("/api/runs/{id}")).await;
    assert_eq!(input["history"], reconnected["history"]);
    assert_eq!(input["decisions"], reconnected["decisions"]);
    server.stop().await;

    let server = Server::demo(dir.path()).await;
    let restored = server.wait(id, |s| decision(s, "input")).await;
    assert_eq!(input["usage"], restored["usage"]);
    assert_eq!(input["decisions"], restored["decisions"]);
    assert_eq!(
        server
            .decide(
                id,
                &restored["decisions"][0],
                json!({"kind": "input", "value": "Check passport and insurance"})
            )
            .await,
        StatusCode::NO_CONTENT
    );
    let stale = server
        .post(
            &format!("/api/runs/{id}/resolve"),
            json!({
                "call_id": restored["decisions"][0]["call_id"],
                "generation": restored["decisions"][0]["generation"],
                "revision": restored["decisions"][0]["revision"],
                "decision": {"kind": "input", "value": "stale reply"},
            }),
        )
        .await;
    assert_eq!(stale.0, StatusCode::CONFLICT);
    assert_eq!(stale.1["error"]["code"], "stale_decision");
    let approval = server.wait(id, |s| decision(s, "approval")).await;
    let notebook = notebook::Notebook::open(dir.path().join("notebook.sqlite"))
        .await
        .unwrap();
    assert!(notebook.list().await.unwrap().is_empty());
    server.stop().await;

    let server = Server::demo(dir.path()).await;
    let restored = server.wait(id, |s| decision(s, "approval")).await;
    assert_eq!(approval["decisions"], restored["decisions"]);
    assert_eq!(
        server
            .decide(id, &restored["decisions"][0], json!({"kind": "deny"}))
            .await,
        StatusCode::NO_CONTENT
    );
    let final_state = server.wait(id, settled).await;
    assert!(
        final_state["phase"]["Settled"]["Completed"]
            .as_str()
            .unwrap()
            .contains("not saved")
    );
    assert!(
        notebook.list().await.unwrap().is_empty(),
        "denial must prevent the effect"
    );
    assert_eq!(
        final_state["operations"][1]["status"]["Complete"]["is_error"],
        true
    );
    assert!(
        server.get("/api/inbox").await["items"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let history = server
        .get(&format!("/api/conversations/{conversation}"))
        .await;
    let accepted = history["submissions"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["admission"] == "accepted")
        .unwrap();
    assert_eq!(accepted["run"]["history"], final_state["history"]);
    // A prior rejected key is not silently admitted after the conversation becomes free.
    assert_eq!(
        server
            .post(
                &submit_path,
                json!({"key": "request-2", "input": "more work"})
            )
            .await
            .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        server.post(&submit_path, original).await.1["run_id"],
        json!(id)
    );
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn approval_saves_one_real_note_and_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::demo(dir.path()).await;
    let (conversation, id) = server.submit("Friday plan").await;
    let state = server.wait(id, |s| decision(s, "input")).await;
    assert_eq!(
        server
            .answer(id, &state["decisions"][0], json!("Book the dentist"))
            .await,
        StatusCode::NO_CONTENT
    );
    let state = server.wait(id, |s| decision(s, "approval")).await;
    assert_eq!(
        state["decisions"][0]["arguments"],
        json!({"title": "Friday plan", "body": "Book the dentist"})
    );
    assert_eq!(
        server
            .decide(id, &state["decisions"][0], json!({"kind": "approve"}))
            .await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        server
            .decide(id, &state["decisions"][0], json!({"kind": "approve"}))
            .await,
        StatusCode::CONFLICT
    );
    let completed = server.wait(id, settled).await;
    server.stop().await;
    let server = Server::demo(dir.path()).await;
    let restored = server.get(&format!("/api/runs/{id}")).await;
    assert_eq!(restored["history"], completed["history"]);
    let duplicate = server
        .post(
            &format!("/api/conversations/{conversation}/runs"),
            json!({"key": "request-1", "input": "Friday plan"}),
        )
        .await;
    assert_eq!(duplicate.1["run_id"], json!(id));
    let notebook = notebook::Notebook::open(dir.path().join("notebook.sqlite"))
        .await
        .unwrap();
    let notes = notebook.list().await.unwrap();
    assert_eq!(notes.len(), 1);
    assert_eq!(notes[0].body, "Book the dentist");
    server.stop().await;
}

/// The application sends no steering, but a run's durable history may contain an instruction
/// (accepted through the runtime's public API on this host's store). Snapshots expose it in
/// place for the UI to show as user input, and the demo workflow never mistakes it for its input.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn steering_entries_appear_in_history_without_changing_the_workflow() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::demo(dir.path()).await;
    let (_, id) = server.submit("Friday plan").await;
    let state = server.wait(id, |s| decision(s, "input")).await;
    let instruction = "Keep the note short.";
    let receipt = server
        .app
        .host
        .read()
        .await
        .steer(id, "steer-1", instruction)
        .await
        .unwrap();
    assert_eq!(
        server
            .answer(id, &state["decisions"][0], json!("Book the dentist"))
            .await,
        StatusCode::NO_CONTENT
    );
    let state = server.wait(id, |s| decision(s, "approval")).await;
    assert_eq!(
        state["decisions"][0]["arguments"],
        json!({"title": "Friday plan", "body": "Book the dentist"})
    );
    assert_eq!(
        server
            .decide(id, &state["decisions"][0], json!({"kind": "approve"}))
            .await,
        StatusCode::NO_CONTENT
    );
    let completed = server.wait(id, settled).await;
    let history = completed["history"].as_array().unwrap();
    let kinds: Vec<&str> = history
        .iter()
        .map(|entry| entry.as_object().unwrap().keys().next().unwrap().as_str())
        .collect();
    // Applied at the dispatch after the answered question, before the save turn.
    assert_eq!(
        kinds,
        ["Input", "Exchange", "Steering", "Exchange", "Exchange"]
    );
    assert_eq!(
        history[2]["Steering"],
        json!({"id": receipt.id, "instruction": instruction})
    );
    let late = server
        .app
        .host
        .read()
        .await
        .steer(id, "steer-2", "Too late.")
        .await;
    assert!(matches!(late, Err(Error::RunNotActive)), "{late:?}");
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_approvals_save_both_notes_without_uncertainty() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::demo(dir.path()).await;
    let (_, first) = server.submit("First note").await;
    let (_, second) = server.submit("Second note").await;
    let first_wait = server.wait(first, |s| decision(s, "input")).await;
    let second_wait = server.wait(second, |s| decision(s, "input")).await;
    assert_eq!(
        server
            .answer(first, &first_wait["decisions"][0], json!("First body"))
            .await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        server
            .answer(second, &second_wait["decisions"][0], json!("Second body"))
            .await,
        StatusCode::NO_CONTENT
    );
    let first_wait = server.wait(first, |s| decision(s, "approval")).await;
    let second_wait = server.wait(second, |s| decision(s, "approval")).await;
    let (first_status, second_status) = tokio::join!(
        server.decide(
            first,
            &first_wait["decisions"][0],
            json!({"kind": "approve"})
        ),
        server.decide(
            second,
            &second_wait["decisions"][0],
            json!({"kind": "approve"})
        ),
    );
    assert_eq!(first_status, StatusCode::NO_CONTENT);
    assert_eq!(second_status, StatusCode::NO_CONTENT);
    for id in [first, second] {
        let completed = server.wait(id, settled).await;
        assert!(
            completed["phase"]["Settled"]["Completed"]
                .as_str()
                .unwrap()
                .starts_with("Saved")
        );
        assert!(completed["decisions"].as_array().unwrap().is_empty());
    }
    server.stop().await;
    let notes = notebook::Notebook::open(dir.path().join("notebook.sqlite"))
        .await
        .unwrap()
        .list()
        .await
        .unwrap();
    assert_eq!(notes.len(), 2);
    assert_ne!(notes[0].note_id, notes[1].note_id);
}

fn uncertain_agent(path: PathBuf, hold_after_effect: bool) -> Agent<String, String> {
    let tool = typed_tool::<Value, Value, _, _>(
        ToolSpec::new("record", "1").with_description("Record a local test receipt"),
        move |_, _| {
            let path = path.clone();
            async move {
                let mut file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)
                    .map_err(Error::tool)?;
                file.write_all(b"receipt\n").map_err(Error::tool)?;
                file.sync_all().map_err(Error::tool)?;
                if hold_after_effect {
                    std::future::pending::<()>().await;
                }
                // The effect happened but its response was lost: the outcome is unknown.
                Err(Error::tool("response lost after the receipt was recorded"))
            }
        },
    );
    Agent::builder(
        "uncertain-fixture-v1",
        Arc::new(ScriptedModel::new(vec![
            ModelResponse::tools(vec![ToolCall {
                id: "receipt-op".into(),
                name: "record".into(),
                arguments: json!({}),
            }]),
            ModelResponse::final_output(json!("Outcome recorded.")),
        ])),
    )
    .instructions("Record once and reconcile.")
    .tool(tool)
    .input::<String>()
    .output::<String>()
    .build()
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uncertain_effects_remain_actionable_after_restart_and_cancellation() {
    for cancel in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let receipt = dir.path().join("receipts.txt");
        let server = Server::start(dir.path(), uncertain_agent(receipt.clone(), false)).await;
        let (_, id) = server.submit("Record receipt").await;
        let state = server.wait(id, |s| decision(s, "reconciliation")).await;
        let item = &state["decisions"][0];
        assert_eq!(item["run_id"], json!(id));
        assert_eq!(item["call_id"], "receipt-op");
        assert_eq!(item["actions"], json!(["reconcile"]));
        assert_eq!(
            server.decide(id, item, json!({"kind": "approve"})).await,
            StatusCode::CONFLICT
        );
        let mut wrong_operation = item.clone();
        wrong_operation["call_id"] = json!("other-operation");
        assert_eq!(
            server
                .decide(
                    id,
                    &wrong_operation,
                    json!({"kind": "reconcile", "value": "wrong", "is_error": false})
                )
                .await,
            StatusCode::CONFLICT
        );
        if cancel {
            assert_eq!(
                server
                    .post(&format!("/api/runs/{id}/cancel"), json!({}))
                    .await
                    .0,
                StatusCode::ACCEPTED
            );
            let cancelled = server.wait(id, settled).await;
            assert_eq!(cancelled["phase"]["Settled"], "Cancelled");
            assert!(decision(&cancelled, "reconciliation"));
        }
        server.stop().await;
        let server = Server::start(dir.path(), uncertain_agent(receipt.clone(), false)).await;
        let state = server.wait(id, |s| decision(s, "reconciliation")).await;
        assert_eq!(
            server.get("/api/inbox").await["items"][0]["run"]["id"],
            json!(id)
        );
        let outcome =
            json!({"kind": "reconcile", "value": {"receipt_id": "receipt-1"}, "is_error": false});
        assert_eq!(
            server
                .decide(id, &state["decisions"][0], outcome.clone())
                .await,
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            server.decide(id, &state["decisions"][0], outcome).await,
            StatusCode::CONFLICT
        );
        let state = server.wait(id, settled).await;
        assert!(state["decisions"].as_array().unwrap().is_empty());
        assert_eq!(
            state["operations"][0]["status"]["Complete"]["value"],
            json!({"receipt_id": "receipt-1"})
        );
        assert_eq!(
            std::fs::read_to_string(&receipt).unwrap(),
            "receipt\n",
            "reconciliation must not repeat the effect"
        );
        assert!(
            server.get("/api/inbox").await["items"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        server.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admission_journal_recovers_on_both_sides_of_host_acceptance() {
    for already_accepted in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let server = Server::demo(dir.path()).await;
        let conversation = server.create_conversation("Lost connection").await;
        // Stop only the recovery loop, then reproduce either crash boundary using
        // the actual durable stores; no catalogue confirmation is written.
        server.app.stopping.cancel();
        let recovery = server.app.recovery.lock().unwrap().take();
        if let Some(recovery) = recovery {
            recovery.await.unwrap();
        }
        let submission = Submission {
            key: "lost-response".into(),
            input: json!("Remember the passport"),
            expires_at_ms: None,
        };
        server
            .app
            .catalog
            .reserve(
                conversation,
                submission.clone(),
                server.app.agent.binding().clone(),
            )
            .await
            .unwrap();
        let old_id = if already_accepted {
            Some(
                server
                    .app
                    .host
                    .read()
                    .await
                    .submit(
                        &conversation.to_string(),
                        &submission.key,
                        submission.input.clone(),
                        None,
                    )
                    .await
                    .unwrap(),
            )
        } else {
            None
        };
        server.stop().await;

        let server = Server::demo(dir.path()).await;
        let recovered_id = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let snapshot = server
                    .get(&format!("/api/conversations/{conversation}"))
                    .await;
                if let Some(id) = snapshot["submissions"]["items"][0]["run"]["id"].as_str() {
                    break Uuid::parse_str(id).unwrap();
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("restart must discover an unconfirmed admission without a client retry");
        let response = server
            .post(
                &format!("/api/conversations/{conversation}/runs"),
                serde_json::to_value(submission).unwrap(),
            )
            .await;
        assert_eq!(response.0, StatusCode::ACCEPTED);
        let id = serde_json::from_value(response.1["run_id"].clone()).unwrap();
        assert_eq!(id, recovered_id);
        if let Some(old_id) = old_id {
            assert_eq!(id, old_id);
        }
        let state = server.wait(id, |s| decision(s, "input")).await;
        assert_eq!(
            state["history"],
            json!([{"Input": "Remember the passport"}])
        );
        assert_eq!(
            server
                .get(&format!("/api/conversations/{conversation}"))
                .await["submissions"]["items"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        server.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_access_control_bounds_and_pagination() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::demo(dir.path()).await;
    for path in [
        "/api/session",
        "/api/conversations",
        "/api/inbox",
        "/api/runs/anything",
        "/api/unknown",
    ] {
        // Query-string credentials are never accepted.
        let reply = server
            .send(Method::GET, &format!("{path}?token={TOKEN}"), None, None)
            .await;
        assert_eq!(reply.status, StatusCode::UNAUTHORIZED);
        assert_eq!(reply.headers["cache-control"], "no-store");
        assert_eq!(reply.headers["www-authenticate"], "Bearer");
        assert_eq!(reply.body["error"]["code"], "unauthorized");
    }
    let reply = server
        .send(
            Method::POST,
            "/api/conversations",
            Some("incorrect"),
            Some(json!({"id": Uuid::new_v4(), "title": "unauthorized"})),
        )
        .await;
    assert_eq!(reply.status, StatusCode::UNAUTHORIZED);
    // The public shell needs no token and carries the security headers.
    let shell = server.send(Method::GET, "/", None, None).await;
    assert_eq!(shell.status, StatusCode::OK);
    assert!(
        shell.headers["content-security-policy"]
            .to_str()
            .unwrap()
            .contains("default-src 'none'")
    );
    let session = server.get("/api/session").await;
    assert_eq!(
        session,
        json!({"agent_id": "notebook-demo-v1", "healthy": true})
    );
    assert!(
        server.get("/api/conversations").await["items"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let mut ids = Vec::new();
    for title in ["First", "Second", "Third"] {
        ids.push(server.create_conversation(title).await);
    }
    let page = server.get("/api/conversations?limit=2").await;
    assert_eq!(page["items"][0]["id"], json!(ids[2]));
    let next = server
        .get(&format!(
            "/api/conversations?limit=2&after={}",
            page["next_cursor"]
        ))
        .await;
    assert_eq!(next["items"].as_array().unwrap().len(), 1);
    assert_eq!(next["items"][0]["id"], json!(ids[0]));
    assert_eq!(next["next_cursor"], Value::Null);
    let reply = server
        .send(Method::GET, "/api/inbox?limit=101", Some(TOKEN), None)
        .await;
    assert_eq!(reply.status, StatusCode::BAD_REQUEST);
    assert_eq!(reply.body["error"]["code"], "invalid_input");
    assert_eq!(
        server
            .send(
                Method::GET,
                &format!("/api/conversations/{}", Uuid::new_v4()),
                Some(TOKEN),
                None
            )
            .await
            .status,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        server
            .post(
                "/api/conversations",
                json!({"id": ids[0], "title": "First"})
            )
            .await
            .0,
        StatusCode::CREATED
    );
    assert_eq!(
        server
            .post(
                "/api/conversations",
                json!({"id": ids[0], "title": "Different"})
            )
            .await
            .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        server
            .post(
                &format!("/api/conversations/{}/runs", ids[0]),
                json!({"key": "too-big", "input": "x".repeat(70_000)})
            )
            .await
            .0,
        StatusCode::PAYLOAD_TOO_LARGE
    );
    let reply = server
        .post(
            &format!("/api/conversations/{}/runs", ids[0]),
            json!({"key": "bad-type", "input": 42}),
        )
        .await;
    assert_eq!(reply.0, StatusCode::BAD_REQUEST);
    assert_eq!(reply.1["error"]["code"], "invalid_input");
    assert!(
        server.get(&format!("/api/conversations/{}", ids[0])).await["submissions"]["items"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(AccessToken::new("short".into()).is_err());
    let second = Application::open(
        dir.path(),
        notebook::Notebook::open(dir.path().join("notebook.sqlite"))
            .await
            .unwrap()
            .demo_agent()
            .unwrap(),
    )
    .await;
    assert!(
        matches!(second, Err(Error::StoreBusy)),
        "a second application must not own this directory"
    );
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_a_wait_releases_the_conversation_without_running_the_tool() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::demo(dir.path()).await;
    let (conversation, id) = server.submit("Cancel this request").await;
    let waiting = server.wait(id, |s| decision(s, "input")).await;
    assert_eq!(
        server
            .post(&format!("/api/runs/{id}/cancel"), json!({}))
            .await
            .0,
        StatusCode::ACCEPTED
    );
    assert_eq!(
        server.wait(id, settled).await["phase"]["Settled"],
        "Cancelled"
    );
    assert_eq!(
        server
            .decide(
                id,
                &waiting["decisions"][0],
                json!({"kind": "input", "value": "too late"})
            )
            .await,
        StatusCode::CONFLICT
    );
    assert_eq!(
        server
            .post(
                &format!("/api/conversations/{conversation}/runs"),
                json!({"key": "next-request", "input": "Try again"})
            )
            .await
            .0,
        StatusCode::ACCEPTED
    );
    server.stop().await;
    assert!(
        notebook::Notebook::open(dir.path().join("notebook.sqlite"))
            .await
            .unwrap()
            .list()
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_runs_remain_inspectable_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let agent = Agent::builder("failure-fixture-v1", Arc::new(ScriptedModel::new(vec![])))
        .instructions("Fail visibly.")
        .input::<String>()
        .output::<String>()
        .build()
        .unwrap();
    let server = Server::start(dir.path(), agent.clone()).await;
    let (conversation, id) = server.submit("Show the failure").await;
    let failed = server.wait(id, settled).await;
    assert!(
        failed["phase"]["Settled"]["Failed"]
            .as_str()
            .unwrap()
            .contains("script exhausted")
    );
    assert_eq!(failed["can_cancel"], false);
    server.stop().await;
    let server = Server::start(dir.path(), agent).await;
    let history = server
        .get(&format!("/api/conversations/{conversation}"))
        .await;
    assert_eq!(
        history["submissions"]["items"][0]["run"]["phase"],
        failed["phase"]
    );
    assert_eq!(
        history["submissions"]["items"][0]["run"]["history"],
        failed["history"]
    );
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rejected_note_is_a_known_failure_not_an_uncertain_effect() {
    // An invalid note changes nothing, so the run continues without reconciliation.
    let dir = tempfile::tempdir().unwrap();
    let notebook = notebook::Notebook::open(dir.path().join("notebook.sqlite"))
        .await
        .unwrap();
    let model = ScriptedModel::new(vec![
        ModelResponse::tools(vec![ToolCall {
            id: "empty-title".into(),
            name: "save_note".into(),
            arguments: json!({"title": " ", "body": "ignored"}),
        }]),
        ModelResponse::final_output(json!("The note was rejected.")),
    ]);
    let agent = notebook
        .agent(
            Arc::new(model),
            ModelConfig::default(),
            "rejection-fixture-v1",
        )
        .unwrap();
    let server = Server::start(dir.path(), agent).await;
    let (_, id) = server.submit("Reject this note").await;
    let state = server.wait(id, |s| decision(s, "approval")).await;
    assert_eq!(
        server
            .decide(id, &state["decisions"][0], json!({"kind": "approve"}))
            .await,
        StatusCode::NO_CONTENT
    );
    let settled = server.wait(id, settled).await;
    assert_eq!(
        settled["phase"]["Settled"]["Completed"],
        "The note was rejected."
    );
    let result = &settled["operations"][0]["status"]["Complete"];
    assert_eq!(result["is_error"], true);
    assert!(result["value"]["error"].as_str().unwrap().contains("title"));
    assert!(notebook.list().await.unwrap().is_empty());
    server.stop().await;
}

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn killed_process_restores_uncertainty_without_repeating_the_effect() {
    let dir = tempfile::tempdir().unwrap();
    let receipts = dir.path().join("receipts.txt");
    let run_file = dir.path().join("run-id");
    let child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "tests::process_fixture",
            "--ignored",
            "--nocapture",
        ])
        .env("PERSONAL_AGENT_TEST_DIRECTORY", dir.path())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let mut owner = Process(child);
    // The owner accepted the run over HTTP and recorded the effect, but never its outcome.
    let id: Uuid = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Ok(id) = std::fs::read_to_string(&run_file)
                && std::fs::read(&receipts).ok().as_deref() == Some(b"receipt\n")
            {
                break id.parse().unwrap();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    owner.0.kill().unwrap();
    owner.0.wait().unwrap();

    let server = Server::start(dir.path(), uncertain_agent(receipts.clone(), false)).await;
    let recovered = server.wait(id, |s| decision(s, "reconciliation")).await;
    assert_eq!(std::fs::read_to_string(&receipts).unwrap(), "receipt\n");
    assert_eq!(
        server
            .decide(
                id,
                &recovered["decisions"][0],
                json!({"kind": "reconcile", "value": "receipt confirmed", "is_error": false})
            )
            .await,
        StatusCode::NO_CONTENT
    );
    let completed = server.wait(id, settled).await;
    assert_eq!(
        completed["phase"]["Settled"]["Completed"],
        "Outcome recorded."
    );
    assert_eq!(std::fs::read_to_string(&receipts).unwrap(), "receipt\n");
    server.stop().await;
}

#[test]
#[ignore = "subprocess fixture, invoked by the process-recovery test"]
fn process_fixture() {
    let Ok(directory) = std::env::var("PERSONAL_AGENT_TEST_DIRECTORY") else {
        return;
    };
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let directory = PathBuf::from(directory);
            let server = Server::start(
                &directory,
                uncertain_agent(directory.join("receipts.txt"), true),
            )
            .await;
            let conversation = server.create_conversation("Process recovery").await;
            let (status, accepted) = server
                .post(
                    &format!("/api/conversations/{conversation}/runs"),
                    json!({"key": "process-request", "input": "Record one receipt"}),
                )
                .await;
            assert_eq!(status, StatusCode::ACCEPTED);
            let staged = directory.join("run-id.partial");
            std::fs::write(&staged, accepted["run_id"].as_str().unwrap()).unwrap();
            std::fs::rename(staged, directory.join("run-id")).unwrap();
            // Killed here by the parent, while the effect's outcome is still unrecorded.
            std::future::pending::<()>().await;
        });
}
