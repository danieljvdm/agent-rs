//! The Google Calendar tools against a stateful loopback HTTP fixture, plus the checks that
//! must hold before any request is sent. Tests that start `CalendarFixture` bind a loopback
//! socket.
#![cfg(all(feature = "google-calendar", feature = "sqlite"))]
#[path = "../examples/support/calendar_fixture.rs"]
mod calendar_fixture;
#[path = "../examples/support/personal_workflow.rs"]
mod personal_workflow;

use async_trait::async_trait;
use calendar_fixture::{CalendarFixture, Reply, TEST_TOKEN, scope, source_events};
use personal_workflow::{DATE, scenarios};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use yielded_agent::{google_calendar::GoogleCalendar, *};

/// A base URL nothing listens on: a test using it must finish before sending a request.
const UNREACHABLE: &str = "http://127.0.0.1:1/calendar/v3/";

fn context() -> ToolContext {
    ToolContext::new(RunId::nil(), "write", "run:3:write")
}
fn args() -> Value {
    json!({"title":"Book dentist appointment", "at":"2026-11-02T09:00:00-08:00"})
}
fn call() -> ToolCall {
    ToolCall {
        id: "write".into(),
        name: "create_reminder".into(),
        arguments: args(),
    }
}
fn uncertain(client: &GoogleCalendar) -> Operation {
    Operation::new(
        call(),
        client.tools()[1].spec().clone(),
        context().idempotency_key,
        OperationStatus::Unknown {
            reason: "response lost".into(),
        },
    )
}
fn offline() -> GoogleCalendar {
    GoogleCalendar::with_base_url(TEST_TOKEN, scope(), UNREACHABLE).unwrap()
}
async fn read(client: &GoogleCalendar) -> Result<ToolOutput<Value>> {
    client.tools()[0]
        .execute(json!({"date":DATE}), context())
        .await
}
fn complete(output: ToolOutput<Value>) -> Value {
    match output {
        ToolOutput::Complete(value) => value,
        other => panic!("expected a complete result, got {other:?}"),
    }
}
fn failed(output: ToolOutput<Value>) -> String {
    match output {
        ToolOutput::Failed(value) => value["error"].as_str().unwrap().to_owned(),
        other => panic!("expected a failed result, got {other:?}"),
    }
}

#[tokio::test]
async fn reads_every_page_expands_recurrences_and_filters_by_start_in_the_scoped_day() {
    let mut events = source_events(&scenarios()[1]);
    events.extend([
        json!({"id":"cancelled", "status":"cancelled"}),
        json!({"id":"all-day", "start":{"date":DATE}}),
        json!({"id":"overlap", "start":{"dateTime":"2026-11-01T23:30:00-08:00"}}),
        json!({"id":"next-day", "start":{"dateTime":"2026-11-03T00:00:00-08:00"}}),
    ]);
    let fixture = CalendarFixture::start(events);
    let day = complete(read(&fixture.client()).await.unwrap());
    let meetings = day["meetings"].as_array().unwrap();
    assert_eq!(meetings.len(), 3);
    assert_eq!(meetings[0]["start"], "2026-11-02T10:00:00-08:00");
    let state = fixture.state.lock().unwrap();
    assert_eq!(state.requests.len(), 4);
    for request in &state.requests {
        assert_eq!(request.query["singleEvents"], "true");
        assert_eq!(request.query["orderBy"], "startTime");
        assert_eq!(request.query["timeMin"], "2026-11-02T00:00:00-08:00");
        assert_eq!(request.query["timeMax"], "2026-11-03T00:00:00-08:00");
    }
}

#[tokio::test]
async fn private_popup_receipt_is_checked_against_the_actual_event_on_duplicate_ids() {
    let fixture = CalendarFixture::start(vec![]);
    let client = fixture.client();
    let writer = client.tools().remove(1);
    for _ in 0..2 {
        let receipt = complete(writer.execute(args(), context()).await.unwrap());
        assert_eq!(receipt["title"], args()["title"]);
    }
    let events = fixture.events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["start"]["dateTime"], "2026-11-02T09:00:00-08:00");
    assert_eq!(events[0]["end"]["dateTime"], "2026-11-02T09:01:00-08:00");
    assert_eq!(events[0]["visibility"], "private");
    assert_eq!(events[0]["transparency"], "transparent");
    assert_eq!(
        events[0]["reminders"],
        json!({"useDefault":false,"overrides":[{"method":"popup","minutes":0}]})
    );
    assert!(events[0].get("attendees").is_none());
    // The same identity with other content is an uncertain outcome, never a success.
    let mut conflict = args();
    conflict["title"] = json!("Changed after approval");
    assert!(matches!(
        writer.execute(conflict, context()).await,
        Err(Error::Tool(_))
    ));
    assert_eq!(
        fixture.events(),
        events,
        "ID collision must never update the event"
    );
    assert!(
        fixture
            .state
            .lock()
            .unwrap()
            .requests
            .iter()
            .filter(|r| r.method == "POST")
            .all(|r| r.query["sendUpdates"] == "none")
    );
}

#[tokio::test]
async fn refused_writes_are_failed_results_and_unknown_statuses_are_errors() {
    for status in [400, 401, 403, 404, 429] {
        let fixture = CalendarFixture::start(vec![]);
        fixture
            .state
            .lock()
            .unwrap()
            .replies
            .push_back(Reply::Json(status, json!({"error":TEST_TOKEN})));
        let output = fixture.client().tools()[1]
            .execute(args(), context())
            .await
            .unwrap();
        let message = failed(output);
        assert!(message.contains(&status.to_string()), "{message}");
        assert!(!message.contains(TEST_TOKEN));
        assert!(fixture.events().is_empty());
    }
    for status in [408, 500, 503] {
        let fixture = CalendarFixture::start(vec![]);
        fixture
            .state
            .lock()
            .unwrap()
            .replies
            .push_back(Reply::Json(status, json!({"error":TEST_TOKEN})));
        let error = fixture.client().tools()[1]
            .execute(args(), context())
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Tool(_)), "{error}");
        assert!(!error.to_string().contains(TEST_TOKEN));
        assert_eq!(fixture.state.lock().unwrap().requests.len(), 1);
    }
}

#[tokio::test]
async fn lost_http_write_response_reconciles_without_another_write() {
    let fixture = CalendarFixture::start(vec![]);
    fixture.state.lock().unwrap().drop_next_write_response = true;
    let client = fixture.client();
    let error = client.tools()[1]
        .execute(args(), context())
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Tool(_)), "{error}");
    let receipt = client
        .reconcile(&uncertain(&client), &CancellationToken::new())
        .await
        .unwrap()
        .unwrap();
    assert!(!receipt.is_error);
    assert_eq!(receipt.call_id, "write");
    assert_eq!(receipt.value["title"], args()["title"]);
    assert_eq!(receipt.value["reminder_id"], fixture.events()[0]["id"]);
    assert_eq!(
        client
            .reminders(&CancellationToken::new())
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(fixture.state.lock().unwrap().create_requests, 1);
}

#[tokio::test]
async fn absent_or_conflicting_receipts_cannot_resolve_uncertainty() {
    let fixture = CalendarFixture::start(vec![]);
    let client = fixture.client();
    let op = uncertain(&client);
    assert!(
        client
            .reconcile(&op, &CancellationToken::new())
            .await
            .unwrap()
            .is_none()
    );
    complete(client.tools()[1].execute(args(), context()).await.unwrap());
    fixture
        .state
        .lock()
        .unwrap()
        .reminders
        .values_mut()
        .next()
        .unwrap()["reminders"]["useDefault"] = json!(true);
    assert!(matches!(
        client.reconcile(&op, &CancellationToken::new()).await,
        Err(Error::Tool(_))
    ));
    assert_eq!(fixture.state.lock().unwrap().create_requests, 1);
}

#[tokio::test]
async fn reconciliation_requires_an_uncertain_write_of_the_same_scope() {
    let client = offline();
    let op = uncertain(&client);
    // The operation the engine recorded passes the checks, so the read is attempted, and
    // nothing listening means the outcome stays unknown.
    assert!(matches!(
        client.reconcile(&op, &CancellationToken::new()).await,
        Err(Error::Tool(_))
    ));
    let mut changed = scope();
    changed.reminder_calendar_id = "different-calendar".into();
    let other = GoogleCalendar::with_base_url(TEST_TOKEN, changed, UNREACHABLE).unwrap();
    assert!(matches!(
        other.reconcile(&op, &CancellationToken::new()).await,
        Err(Error::Input(_))
    ));
    let pending = Operation::new(
        call(),
        op.spec.clone(),
        op.idempotency_key.clone(),
        OperationStatus::Approval,
    );
    assert!(matches!(
        client.reconcile(&pending, &CancellationToken::new()).await,
        Err(Error::Input(_))
    ));
    let read = Operation::new(
        ToolCall {
            id: "read".into(),
            name: "read_calendar".into(),
            arguments: json!({"date":DATE}),
        },
        client.tools()[0].spec().clone(),
        "key",
        OperationStatus::Unknown {
            reason: "lost".into(),
        },
    );
    assert!(matches!(
        client.reconcile(&read, &CancellationToken::new()).await,
        Err(Error::Input(_))
    ));
}

#[tokio::test]
async fn recurring_event_receipts_cannot_reconcile_or_satisfy_a_duplicate_one_time_write() {
    for (field, value) in [
        ("recurrence", json!(["RRULE:FREQ=DAILY"])),
        ("recurringEventId", json!("another-recurring-series")),
        (
            "originalStartTime",
            json!({"dateTime":"2026-11-02T09:00:00-08:00"}),
        ),
    ] {
        let fixture = CalendarFixture::start(vec![]);
        let client = fixture.client();
        let writer = client.tools().remove(1);
        complete(writer.execute(args(), context()).await.unwrap());
        fixture
            .state
            .lock()
            .unwrap()
            .reminders
            .values_mut()
            .next()
            .unwrap()[field] = value;
        let changed_event = fixture.events()[0].clone();
        assert!(
            client
                .reconcile(&uncertain(&client), &CancellationToken::new())
                .await
                .is_err(),
            "recurring receipt accepted during reconciliation: {field}"
        );
        assert_eq!(
            fixture.state.lock().unwrap().create_requests,
            1,
            "reconciliation must only read"
        );
        assert!(
            writer.execute(args(), context()).await.is_err(),
            "recurring receipt accepted after duplicate-ID response: {field}"
        );
        // A successful POST response must pass the same one-time contract.
        fixture
            .state
            .lock()
            .unwrap()
            .replies
            .push_back(Reply::Json(200, changed_event.clone()));
        assert!(
            writer.execute(args(), context()).await.is_err(),
            "recurring receipt accepted in successful POST response: {field}"
        );
        assert_eq!(
            fixture.events(),
            vec![changed_event],
            "verification must not repair or overwrite the external event"
        );
        assert!(
            fixture
                .state
                .lock()
                .unwrap()
                .requests
                .iter()
                .all(|r| r.method == "GET" || r.method == "POST")
        );
    }
}

#[tokio::test]
async fn errors_bounds_and_redirects_never_retain_credentials_or_partial_reads() {
    // Statuses are the service's answer: a failed result for the model, without the body.
    for reply in [
        Reply::Json(401, json!({"error":TEST_TOKEN})),
        Reply::Json(429, json!({"error":TEST_TOKEN})),
        Reply::Json(500, json!({"error":TEST_TOKEN})),
        Reply::Redirect(format!("http://127.0.0.1:9/{TEST_TOKEN}")),
    ] {
        let fixture = CalendarFixture::start(vec![]);
        fixture.state.lock().unwrap().replies.push_back(reply);
        let message = failed(read(&fixture.client()).await.unwrap());
        assert!(message.starts_with("Google Calendar refused the read with HTTP status"));
        assert!(!message.contains(TEST_TOKEN));
        assert_eq!(fixture.state.lock().unwrap().requests.len(), 1);
    }
    // A response that cannot be trusted is an error, which quotes neither the body nor a URL.
    for reply in [
        Reply::Raw(200, format!("bad JSON {TEST_TOKEN}")),
        Reply::Raw(200, " ".repeat(1_048_577)),
        Reply::Json(200, json!({"items":[{"id":"broken", "start":{}}]})),
        Reply::Json(
            200,
            json!({"items":[{"id":"broken", "start":{"dateTime":TEST_TOKEN}}]}),
        ),
    ] {
        let fixture = CalendarFixture::start(vec![]);
        fixture.state.lock().unwrap().replies.push_back(reply);
        let error = read(&fixture.client()).await.unwrap_err();
        assert!(matches!(error, Error::Tool(_)), "{error}");
        let error = error.to_string();
        assert!(!error.contains(TEST_TOKEN), "{error}");
        assert!(!error.contains("http://"), "{error}");
        assert_eq!(fixture.state.lock().unwrap().requests.len(), 1);
    }
    let fixture = CalendarFixture::start(vec![]);
    for id in ["first", "second"] {
        fixture.state.lock().unwrap().replies.push_back(Reply::Json(
            200,
            json!({"items":[{"id":id}],"nextPageToken":"repeat"}),
        ));
    }
    assert!(
        read(&fixture.client())
            .await
            .unwrap_err()
            .to_string()
            .contains("page token")
    );
}

#[tokio::test]
async fn invalid_requests_are_failed_results_without_a_request() {
    let client = offline();
    let tools = client.tools();
    let message = failed(
        tools[0]
            .execute(json!({"date":"2026-11-03"}), context())
            .await
            .unwrap(),
    );
    assert!(message.contains("outside"), "{message}");
    for arguments in [
        json!({"title":"", "at":"2026-11-02T09:00:00-08:00"}),
        json!({"title":"x".repeat(257), "at":"2026-11-02T09:00:00-08:00"}),
        json!({"title":"Reminder", "at":"not-a-time"}),
        json!({"title":"Reminder", "at":"2026-11-02T09:00:00"}),
    ] {
        failed(tools[1].execute(arguments, context()).await.unwrap());
    }
    // Unknown fields fail validation, so credentials cannot ride in as arguments.
    assert!(matches!(
        tools[1].validate(&json!({"title":"test","at":"test","access_token":TEST_TOKEN})),
        Err(Error::ModelOutput(_))
    ));
}

#[tokio::test]
async fn specs_bind_the_scope_and_gate_writes() {
    let client = offline();
    let tools = client.tools();
    let (reader, writer) = (tools[0].spec(), tools[1].spec());
    assert_eq!(reader.name, "read_calendar");
    assert_eq!(reader.retry, RetryPolicy::ReadOnly);
    assert!(!reader.requires_approval);
    assert_eq!(writer.name, "create_reminder");
    assert_eq!(writer.retry, RetryPolicy::Never);
    assert_eq!(writer.execution, Execution::Sequential);
    assert!(writer.requires_approval);
    // The scope is in the version; the token is not.
    let rotated = GoogleCalendar::with_base_url("another-token", scope(), UNREACHABLE).unwrap();
    assert_eq!(rotated.tools()[1].spec(), writer);
    let mut changed = scope();
    changed.source_calendar_id = "other-source".into();
    let other = GoogleCalendar::with_base_url(TEST_TOKEN, changed, UNREACHABLE).unwrap();
    assert_ne!(other.tools()[0].spec().version, reader.version);
    assert!(
        !serde_json::to_string(&(reader, writer))
            .unwrap()
            .contains(TEST_TOKEN)
    );
}

#[tokio::test]
async fn scope_validation_and_cancellation_send_no_requests() {
    let client = offline();
    let ctx = context();
    ctx.cancel.cancel();
    assert!(matches!(
        client.tools()[0].execute(json!({"date":DATE}), ctx).await,
        Err(Error::Interrupted)
    ));
    let config = |result: Result<GoogleCalendar>| matches!(result, Err(Error::Config(_)));
    let mut alias = scope();
    alias.source_calendar_id = "primary".into();
    assert!(config(GoogleCalendar::with_base_url(
        TEST_TOKEN,
        alias,
        UNREACHABLE
    )));
    let mut not_midnight = scope();
    not_midnight.day_start = "2026-11-02T01:00:00-08:00".into();
    assert!(config(GoogleCalendar::with_base_url(
        TEST_TOKEN,
        not_midnight,
        UNREACHABLE
    )));
    let mut dst = scope();
    dst.day_start = "2026-11-01T00:00:00-07:00".into();
    dst.day_end = "2026-11-02T00:00:00-08:00".into();
    assert!(GoogleCalendar::with_base_url(TEST_TOKEN, dst, UNREACHABLE).is_ok());
    assert!(config(GoogleCalendar::with_base_url(
        TEST_TOKEN,
        scope(),
        "http://example.com/"
    )));
    assert!(config(GoogleCalendar::with_base_url(
        " ",
        scope(),
        UNREACHABLE
    )));
    assert!(config(GoogleCalendar::new(TEST_TOKEN, {
        let mut bad = scope();
        bad.day_end = "tomorrow".into();
        bad
    })));
}

struct DenyWrites;
#[async_trait]
impl Authorizer for DenyWrites {
    async fn authorize(&self, _: &RunState, call: &ToolCall) -> Result<bool> {
        Ok(call.name != "create_reminder")
    }
}
#[tokio::test]
async fn approval_and_fresh_application_authorization_both_precede_network_writes() {
    // Nothing listens at the client's base URL: a dispatched write would fail with an unknown
    // outcome and leave the run waiting for reconciliation instead of failing it.
    let model = Arc::new(ScriptedModel::new(vec![ModelResponse::tools(vec![call()])]));
    let agent = Agent::builder("service-test", model)
        .tools(offline().tools())
        .authorizer(Arc::new(DenyWrites))
        .build()
        .unwrap();
    let mut conversation = Conversation::default();
    assert!(matches!(
        agent.run(&mut conversation, json!("remind me")).await,
        Err(Error::Waiting)
    ));
    let run = conversation.last_run.as_mut().unwrap();
    assert!(matches!(
        run.operations[0].status,
        OperationStatus::Approval
    ));
    assert_eq!(run.usage.tool_calls, 0);
    run.resolve("write", Resolution::Approve(true)).unwrap();
    let outcome = agent.resume(&mut conversation, RunControl::default()).await;
    let Err(Error::Failed(message)) = outcome else {
        panic!("{outcome:?}")
    };
    assert!(message.to_string().contains("authoriz"), "{message}");
    let run = conversation.last_run.as_ref().unwrap();
    assert!(
        !run.operations
            .iter()
            .any(|op| matches!(op.status, OperationStatus::Unknown { .. })),
        "the denied write must not have been dispatched"
    );
}
