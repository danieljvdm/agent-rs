//! How `Agent::drive` treats each kind of failure: either the run's own work went wrong and the
//! run settles as `Failed` with the error's message, or the run could not be advanced and `drive`
//! returns the error, leaving the run as of its last checkpoint so it can be driven again.
use agent_rs::*;
use async_trait::async_trait;
use serde_json::{Value, json};
use std::{error::Error as _, sync::Arc};
use tokio_util::sync::CancellationToken;

fn spec(name: &str) -> ToolSpec {
    ToolSpec::new(name, "1")
        .with_description("test")
        .with_retry(RetryPolicy::ReadOnly)
}
fn call(id: &str, name: &str, arguments: Value) -> ToolCall {
    ToolCall {
        id: id.into(),
        name: name.into(),
        arguments,
    }
}
fn echo() -> Arc<dyn Tool> {
    typed_tool::<u32, u32, _, _>(
        spec("echo"),
        |n, _| async move { Ok(ToolOutput::Complete(n)) },
    )
}
fn scripted(responses: Vec<ModelResponse>) -> Arc<dyn Model> {
    Arc::new(ScriptedModel::new(responses))
}
/// Calls `echo` once, then answers "done".
fn echo_then_done() -> Arc<dyn Model> {
    scripted(vec![
        ModelResponse::tools(vec![call("a", "echo", json!(1))]),
        ModelResponse::final_output(json!("done")),
    ])
}
fn done() -> Arc<dyn Model> {
    scripted(vec![ModelResponse::final_output(json!("done"))])
}

struct FailingModel;
#[async_trait]
impl Model for FailingModel {
    async fn stream(&self, _: ModelRequest, _: CancellationToken) -> Result<ModelStream> {
        Err(Error::provider("connection reset"))
    }
}
struct DenyAll;
#[async_trait]
impl Authorizer for DenyAll {
    async fn authorize(&self, _: &RunState, _: &ToolCall) -> Result<bool> {
        Ok(false)
    }
}
/// An authorizer whose own policy store fails. Its error must fail the run, not be mistaken for
/// the run's checkpoint storage failing.
struct BrokenAuthorizer;
#[async_trait]
impl Authorizer for BrokenAuthorizer {
    async fn authorize(&self, _: &RunState, _: &ToolCall) -> Result<bool> {
        Err(Error::storage("policy store down"))
    }
}
/// Every commit fails with the error `fail` makes.
struct FailingCheckpoint(fn() -> Error);
#[async_trait]
impl Checkpoint for FailingCheckpoint {
    async fn commit(&self, _: &mut RunState) -> Result<()> {
        Err((self.0)())
    }
}

/// What `drive` does with a failure.
enum Expect {
    /// `drive` returns `Ok` and the run settles as `Failed` with a message containing each part.
    Fails(&'static [&'static str]),
    /// `drive` returns `Ok` and the run settles as `Cancelled`.
    Cancelled,
    /// `drive` returns an error the predicate accepts, whose message contains each part, and
    /// leaves the run unsettled.
    Returns(fn(&Error) -> bool, &'static [&'static str]),
}
struct Case {
    name: &'static str,
    agent: Agent,
    /// The agent that resumes the run once the cause is fixed; `agent` when `None`.
    resume: Option<Agent>,
    checkpoint: Box<dyn Checkpoint>,
    control: RunControl,
    expect: Expect,
}
fn case(name: &'static str, agent: AgentBuilder, expect: Expect) -> Case {
    Case {
        name,
        agent: agent.build().unwrap(),
        resume: None,
        checkpoint: Box::new(MemoryCheckpoint),
        control: RunControl::default(),
        expect,
    }
}
fn builder(model: Arc<dyn Model>) -> AgentBuilder {
    Agent::builder("classification", model).instructions("test")
}

fn cases() -> Vec<Case> {
    let shutdown = RunControl::default();
    shutdown.shutdown.cancel();
    let cancel = RunControl::default();
    cancel.cancel.cancel();
    let blob = typed_tool::<usize, String, _, _>(spec("blob"), |n, _| async move {
        Ok(ToolOutput::Complete("x".repeat(n)))
    });
    vec![
        case(
            "provider error",
            builder(Arc::new(FailingModel)),
            Expect::Fails(&["provider: ", "connection reset"]),
        ),
        Case {
            agent: builder(scripted(vec![ModelResponse::final_output(json!(42))]))
                .output::<String>()
                .build()
                .unwrap()
                .erased(),
            ..case(
                "model output failing validation",
                builder(done()),
                Expect::Fails(&["invalid type: integer `42`, expected a string"]),
            )
        },
        case(
            "unknown tool",
            builder(scripted(vec![ModelResponse::tools(vec![call(
                "a",
                "missing",
                json!(1),
            )])])),
            Expect::Fails(&["unknown tool missing"]),
        ),
        case(
            "authorization denied",
            builder(echo_then_done())
                .tool(echo())
                .authorizer(Arc::new(DenyAll)),
            Expect::Fails(&["authorization denied for echo"]),
        ),
        case(
            "authorizer error",
            builder(echo_then_done())
                .tool(echo())
                .authorizer(Arc::new(BrokenAuthorizer)),
            Expect::Fails(&["authorization failed", "policy store down"]),
        ),
        case(
            "context that cannot fit",
            builder(scripted(vec![
                ModelResponse::tools(vec![call("big", "blob", json!(1_000))]),
                ModelResponse::final_output(json!("done")),
            ]))
            .tool(blob)
            .limits(Limits::default().with_max_context_bytes(512)),
            Expect::Fails(&["current input and pinned history exceed context limit"]),
        ),
        Case {
            checkpoint: Box::new(FailingCheckpoint(|| Error::storage("disk full"))),
            ..case(
                "checkpoint storage failure",
                builder(done()),
                Expect::Returns(|e| matches!(e, Error::Storage(_)), &["disk full"]),
            )
        },
        Case {
            // A checkpoint's error never fails the run, whatever its kind: it is storage.
            checkpoint: Box::new(FailingCheckpoint(|| Error::input("history is append-only"))),
            ..case(
                "checkpoint error of another kind",
                builder(done()),
                Expect::Returns(
                    |e| matches!(e, Error::Storage(_)),
                    &["storage: ", "history is append-only"],
                ),
            )
        },
        Case {
            checkpoint: Box::new(FailingCheckpoint(|| Error::Stale)),
            ..case(
                "stale checkpoint",
                builder(done()),
                Expect::Returns(|e| matches!(e, Error::Stale), &[]),
            )
        },
        Case {
            // The run was accepted by a definition with other instructions.
            resume: Some(builder(done()).instructions("other").build().unwrap()),
            ..case(
                "incompatible binding",
                builder(done()),
                Expect::Returns(|e| matches!(e, Error::Incompatible(_)), &["differs"]),
            )
        },
        Case {
            control: shutdown,
            ..case(
                "interruption",
                builder(done()),
                Expect::Returns(|e| matches!(e, Error::Interrupted), &[]),
            )
        },
        Case {
            control: cancel,
            ..case(
                "interruption by cancellation",
                builder(done()),
                Expect::Cancelled,
            )
        },
    ]
}

#[tokio::test]
async fn drive_fails_the_run_or_returns_the_error_by_kind_of_failure() {
    for case in cases() {
        let name = case.name;
        let resume = case.resume.unwrap_or_else(|| case.agent.clone());
        let mut state = resume.new_run("c", json!("go"), &[], None).unwrap();
        // Nothing commits before these failures, so this is the run's last checkpoint.
        let checkpointed = state.clone();
        let result = case
            .agent
            .drive(&mut state, &*case.checkpoint, case.control)
            .await;
        match case.expect {
            Expect::Fails(parts) => {
                assert!(result.is_ok(), "{name}: {result:?}");
                let Phase::Settled(Settlement::Failed(reason)) = &state.phase else {
                    panic!("{name}: {:?}", state.phase);
                };
                for part in parts {
                    assert!(reason.contains(part), "{name}: {reason:?} lacks {part:?}");
                }
            }
            Expect::Cancelled => {
                assert!(result.is_ok(), "{name}: {result:?}");
                assert_eq!(state.phase, Phase::Settled(Settlement::Cancelled), "{name}");
            }
            Expect::Returns(kind, parts) => {
                let error = result.expect_err(name);
                assert!(kind(&error), "{name}: {error:?}");
                let message = error.to_string();
                for part in parts {
                    assert!(message.contains(part), "{name}: {message:?} lacks {part:?}");
                }
                assert!(!state.is_terminal(), "{name}: {:?}", state.phase);
                // Once the cause is fixed the run resumes from its checkpoint and completes.
                let mut state = checkpointed;
                resume
                    .drive(&mut state, &MemoryCheckpoint, RunControl::default())
                    .await
                    .unwrap();
                assert_eq!(
                    state.phase,
                    Phase::Settled(Settlement::Completed(json!("done"))),
                    "{name}"
                );
            }
        }
    }
}

#[test]
fn fails_run_separates_failed_runs_from_resumable_ones() {
    for error in [
        Error::storage("disk full"),
        Error::Stale,
        Error::incompatible("binding differs"),
        Error::Interrupted,
    ] {
        assert!(!error.fails_run(), "{error:?}");
    }
    for error in [
        Error::config("bad"),
        Error::input("bad"),
        Error::model_output("bad"),
        Error::provider("bad"),
        Error::tool("bad"),
        Error::context("bad"),
        Error::IdempotencyConflict,
        Error::ConversationBusy,
        Error::StoreBusy,
        Error::NotFound,
        Error::NotWaiting,
        Error::RunNotActive,
        Error::Waiting,
        Error::Stopped(StopReason::Tokens),
        Error::Cancelled,
    ] {
        assert!(error.fails_run(), "{error:?}");
    }
}

#[test]
fn only_stale_and_interrupted_runs_resume_without_a_retry() {
    for error in [Error::Stale, Error::Interrupted] {
        assert!(!error.needs_retry(), "{error:?}");
    }
    for error in [
        Error::storage("disk full"),
        Error::incompatible("agent is not registered"),
        Error::NotFound,
        Error::RunNotActive,
    ] {
        assert!(error.needs_retry(), "{error:?}");
    }
}

// ---------------------------------------------------------------------------------------------
// How an in-memory run ended, as `run` and `resume` report it.
// ---------------------------------------------------------------------------------------------

fn typed(
    model: Arc<dyn Model>,
    tools: Vec<Arc<dyn Tool>>,
    limits: Limits,
) -> Agent<String, String> {
    builder(model)
        .tools(tools)
        .limits(limits)
        .input::<String>()
        .output::<String>()
        .build()
        .unwrap()
}

#[tokio::test]
async fn run_reports_a_waiting_run_and_resume_completes_it() {
    let mut contract = spec("ask");
    contract.requires_approval = true;
    let ask = typed_tool::<u32, Value, _, _>(contract, |_, _| async {
        Ok(ToolOutput::Wait(WaitRequest::Input {
            prompt: "Where?".into(),
        }))
    });
    let model = scripted(vec![
        ModelResponse::tools(vec![call("ask", "ask", json!(1))]),
        ModelResponse::final_output(json!("done")),
    ]);
    let agent = typed(model, vec![ask], Limits::default());
    let mut conversation = Conversation::default();
    // Waiting for approval, then for the tool's input request.
    assert!(matches!(
        agent.run(&mut conversation, "go").await,
        Err(Error::Waiting)
    ));
    let run = conversation.last_run.as_mut().unwrap();
    run.resolve("ask", Resolution::Approve(true)).unwrap();
    assert!(matches!(
        agent.resume(&mut conversation, RunControl::default()).await,
        Err(Error::Waiting)
    ));
    let run = conversation.last_run.as_mut().unwrap();
    run.resolve("ask", Resolution::Input(json!("here")))
        .unwrap();
    let reply = agent
        .resume(&mut conversation, RunControl::default())
        .await
        .unwrap();
    assert_eq!(reply, "done");
}

#[tokio::test]
async fn run_reports_a_stopped_run_with_its_reason() {
    let model = scripted(vec![
        ModelResponse::final_output(json!("too long")).with_usage(TokenUsage::new(8, 8)),
    ]);
    let limits = Limits::default().with_max_tokens(10);
    let agent = typed(model, vec![], limits);
    let mut conversation = Conversation::default();
    let error = agent.run(&mut conversation, "go").await.unwrap_err();
    assert!(
        matches!(error, Error::Stopped(StopReason::Tokens)),
        "{error:?}"
    );
    assert_eq!(error.to_string(), "run stopped: token limit reached");
    assert_eq!(
        conversation.last_run.unwrap().phase,
        Phase::Settled(Settlement::Stopped(StopReason::Tokens))
    );
}

#[tokio::test]
async fn run_reports_a_failed_run_with_its_settled_message() {
    let model = scripted(vec![ModelResponse::tools(vec![call(
        "a",
        "missing",
        json!(1),
    )])]);
    let agent = typed(model, vec![], Limits::default());
    let mut conversation = Conversation::default();
    let error = agent.run(&mut conversation, "go").await.unwrap_err();
    let Error::Failed(reason) = &error else {
        panic!("{error:?}");
    };
    assert_eq!(
        reason.to_string(),
        "invalid model output: unknown tool missing"
    );
    assert_eq!(
        error.to_string(),
        "run failed: invalid model output: unknown tool missing"
    );
    assert_eq!(
        conversation.last_run.unwrap().phase,
        Phase::Settled(Settlement::Failed(reason.to_string()))
    );
}

#[tokio::test]
async fn resume_reports_a_cancelled_run() {
    let wait = typed_tool::<u32, Value, _, _>(spec("wait"), |_, _| async {
        Ok(ToolOutput::Wait(WaitRequest::Input {
            prompt: "Continue?".into(),
        }))
    });
    let model = scripted(vec![ModelResponse::tools(vec![call(
        "w",
        "wait",
        json!(1),
    )])]);
    let agent = typed(model, vec![wait], Limits::default());
    let mut conversation = Conversation::default();
    assert!(matches!(
        agent.run(&mut conversation, "go").await,
        Err(Error::Waiting)
    ));
    let control = RunControl::default();
    control.cancel.cancel();
    assert!(matches!(
        agent.resume(&mut conversation, control).await,
        Err(Error::Cancelled)
    ));
    assert_eq!(
        conversation.last_run.unwrap().phase,
        Phase::Settled(Settlement::Cancelled)
    );
}

// ---------------------------------------------------------------------------------------------
// Requests rejected before they run, and the causes errors keep.
// ---------------------------------------------------------------------------------------------

#[test]
fn rejected_requests_name_their_kind() {
    let erased = typed(done(), vec![], Limits::default()).erased();
    // Input that fails the agent's type keeps the decoding error.
    let error = erased.new_run("c", json!(42), &[], None).unwrap_err();
    assert!(matches!(error, Error::Input(_)), "{error:?}");
    assert!(error.source().unwrap().is::<serde_json::Error>());
    assert!(matches!(
        erased.new_run("", json!("go"), &[], None),
        Err(Error::Input(_))
    ));
    assert!(matches!(
        Agent::builder("", done()).build(),
        Err(Error::Config(_))
    ));
    // The conversation's newest entry cannot fit the context limit beside the new input.
    let small = Limits::default().with_max_context_bytes(16);
    let entry = HistoryEntry::Input(json!("x".repeat(64)));
    assert!(matches!(
        typed(done(), vec![], small).new_run("c", json!("go"), &[entry], None),
        Err(Error::Context(_))
    ));
}

#[tokio::test]
async fn tool_errors_keep_their_cause() {
    struct Unencodable;
    impl serde::Serialize for Unencodable {
        fn serialize<S: serde::Serializer>(&self, _: S) -> std::result::Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("cannot encode"))
        }
    }
    let tool = typed_tool::<u32, _, _, _>(spec("bad"), |_, _| async {
        Ok(ToolOutput::Complete(Unencodable))
    });
    // The model's arguments fail the tool's type.
    let error = tool.validate(&json!("one")).unwrap_err();
    assert!(matches!(error, Error::ModelOutput(_)), "{error:?}");
    assert!(error.source().unwrap().is::<serde_json::Error>());
    let context = ToolContext::new(RunId::nil(), "call", "key");
    let error = tool.execute(json!(1), context).await.unwrap_err();
    assert!(matches!(error, Error::Tool(_)), "{error:?}");
    assert_eq!(error.to_string(), "tool: cannot encode");
    assert!(error.source().unwrap().is::<serde_json::Error>());
}

#[test]
fn implementor_errors_keep_their_cause() {
    let reset = || std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset by peer");
    for (error, prefix) in [
        (Error::provider(reset()), "provider: "),
        (Error::tool(reset()), "tool: "),
        (Error::storage(reset()), "storage: "),
    ] {
        assert_eq!(error.to_string(), format!("{prefix}reset by peer"));
        let cause = error.source().unwrap().downcast_ref::<std::io::Error>();
        assert_eq!(cause.unwrap().kind(), std::io::ErrorKind::ConnectionReset);
    }
    // A message is not a cause.
    assert!(Error::storage("revision exhausted").source().is_none());
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_storage_errors_keep_the_database_error() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("not-a-database.sqlite");
    std::fs::write(&path, vec![b'x'; 4096]).unwrap();
    let error = match sqlite::SqliteStore::open(&path).await {
        Err(error) => error,
        Ok(_) => panic!("a file that is not a database opened"),
    };
    assert!(matches!(error, Error::Storage(_)), "{error:?}");
    assert!(error.source().unwrap().is::<rusqlite::Error>(), "{error:?}");
}
