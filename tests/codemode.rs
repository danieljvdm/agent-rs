//! Code mode: the replay driver against a fake runtime, the QuickJS runtime against real scripts,
//! and scripts run end to end as attached children of a parent run.
#![cfg(feature = "codemode")]
use agent_rs::codemode::{
    Answer, CodeMode, QuickJs, ScriptCall, ScriptFunction, ScriptLimits, ScriptModel,
    ScriptRuntime, ScriptSession, Step, Subprocess, default_limits, description, input_type,
};
use agent_rs::*;
use async_trait::async_trait;
use futures::StreamExt;
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------------------------
// Shared fixtures.

fn spec(name: &str) -> ToolSpec {
    ToolSpec::new(name, "1")
        .with_description(format!("The {name} tool."))
        .with_retry(RetryPolicy::ReadOnly)
}
#[derive(Deserialize, JsonSchema)]
struct Query {
    q: String,
}
/// `search({q})` answers `{hits: ["hit:<q>"]}` and counts its executions.
fn search(executions: Arc<AtomicUsize>) -> Arc<dyn Tool> {
    typed_tool::<Query, Value, _, _>(spec("search"), move |query, _| {
        let executions = executions.clone();
        async move {
            executions.fetch_add(1, Ordering::SeqCst);
            Ok(ToolOutput::Complete(
                json!({"hits": [format!("hit:{}", query.q)]}),
            ))
        }
    })
}
fn counter() -> Arc<AtomicUsize> {
    Arc::new(AtomicUsize::new(0))
}
/// The parent's call of the code mode tool.
fn delegate(code: &str) -> ToolCall {
    ToolCall {
        id: "run".into(),
        name: "code".into(),
        arguments: json!({ "code": code }),
    }
}
/// Delegates `code` until its history holds the result, then answers "done". Unlike
/// [`ScriptedModel`], a turn made again after a failed commit gives the same response.
struct Parent(String);
#[async_trait]
impl Model for Parent {
    async fn stream(&self, request: ModelRequest, _: CancellationToken) -> Result<ModelStream> {
        let delegated =
            (request.history.iter()).any(|entry| matches!(entry, HistoryEntry::Exchange { .. }));
        let response = if delegated {
            ModelResponse::final_output(json!("done"))
        } else {
            ModelResponse::tools(vec![delegate(&self.0)])
        };
        Ok(Box::pin(futures::stream::iter([Ok(ModelEvent::Response(
            response,
        ))])))
    }
}
/// A parent that runs `code` once through `code_mode`, then answers "done".
fn parent(code_mode: Subagent, code: &str) -> Agent {
    Agent::builder("parent", Arc::new(Parent(code.into())))
        .instructions("Write code.")
        .subagents(1, [code_mode])
        .build()
        .unwrap()
}
fn code_mode(tools: Vec<Arc<dyn Tool>>) -> CodeMode {
    CodeMode::new("code").tools(tools)
}
/// Runs `agent` in memory to its end and returns the parent's run.
async fn run(agent: &Agent) -> (Result<Value>, RunState) {
    let mut conversation = Conversation::default();
    let result = agent.run(&mut conversation, json!("go")).await;
    (result, conversation.last_run.unwrap())
}
/// The delegation's result, as the parent's model received it.
fn delegated(state: &RunState) -> &ToolResult {
    match &state.history[1] {
        HistoryEntry::Exchange { results, .. } => &results[0],
        other => panic!("expected the delegated exchange: {other:?}"),
    }
}
fn child(state: &RunState) -> &RunState {
    &state.children[0].state
}

// ---------------------------------------------------------------------------------------------
// Step 1: the replay driver, against a runtime that plays a fixed list of steps.

/// Plays `steps` in order, whatever it is answered, and records the answers of each advance.
struct Fake {
    steps: Vec<Step>,
    answers: Arc<Mutex<Vec<Vec<Answer>>>>,
}
impl Fake {
    fn new(steps: Vec<Step>) -> Self {
        Self {
            steps,
            answers: Arc::default(),
        }
    }
}
struct FakeSession {
    steps: std::vec::IntoIter<Step>,
    answers: Arc<Mutex<Vec<Vec<Answer>>>>,
}
impl ScriptRuntime for Fake {
    fn binding(&self) -> Value {
        json!("fake")
    }
    fn start(
        &self,
        _: &str,
        _: &[ScriptFunction],
        _: &ScriptLimits,
        _: CancellationToken,
    ) -> Result<Box<dyn ScriptSession>> {
        Ok(Box::new(FakeSession {
            steps: self.steps.clone().into_iter(),
            answers: self.answers.clone(),
        }))
    }
}
impl ScriptSession for FakeSession {
    fn advance(&mut self, answers: Vec<Answer>) -> Result<Step> {
        self.answers.lock().unwrap().push(answers);
        self.steps
            .next()
            .ok_or_else(|| Error::provider("fake exhausted"))
    }
    fn logs(&mut self) -> Vec<String> {
        vec!["logged".into()]
    }
}
fn script_call(tool: &str, arguments: Value) -> ScriptCall {
    ScriptCall {
        tool: tool.into(),
        arguments,
    }
}
fn input(code: &str) -> HistoryEntry {
    HistoryEntry::Input(json!({ "code": code }))
}
/// A recorded round of `search` calls with these queries, numbered from `first`.
fn round(first: usize, queries: &[&str]) -> HistoryEntry {
    let calls: Vec<ToolCall> = (queries.iter().enumerate())
        .map(|(offset, q)| ToolCall {
            id: format!("c{}", first + offset),
            name: "search".into(),
            arguments: json!({ "q": q }),
        })
        .collect();
    let results = (calls.iter())
        .map(|call| ToolResult {
            call_id: call.id.clone(),
            value: json!({"hits": [call.arguments["q"]]}),
            is_error: false,
        })
        .collect();
    HistoryEntry::Exchange {
        response: ModelResponse::tools(calls),
        results,
    }
}
async fn turn(model: &ScriptModel, history: Vec<HistoryEntry>) -> Result<ModelResponse> {
    let request = ModelRequest::new(RunId::new_v4(), history);
    let mut stream = model.stream(request, CancellationToken::new()).await?;
    match stream.next().await {
        Some(Ok(ModelEvent::Response(response))) => Ok(response),
        Some(Err(error)) => Err(error),
        other => panic!("unexpected event: {other:?}"),
    }
}
fn fake_model(fake: Fake) -> ScriptModel {
    ScriptModel::new(Arc::new(fake), [search(counter())], ScriptLimits::default()).unwrap()
}
fn blocked(calls: &[(&str, Value)]) -> Step {
    Step::Blocked(
        (calls.iter())
            .map(|(tool, arguments)| script_call(tool, arguments.clone()))
            .collect(),
    )
}

#[tokio::test]
async fn a_replay_answers_recorded_rounds_and_numbers_new_calls_on_from_them() {
    let fake = Fake::new(vec![
        blocked(&[("search", json!({"q": "a"}))]),
        blocked(&[("search", json!({"q": "b"})), ("search", json!({"q": "c"}))]),
    ]);
    let answers = fake.answers.clone();
    let model = fake_model(fake);
    let response = turn(&model, vec![input("x"), round(0, &["a"])])
        .await
        .unwrap();
    let ids: Vec<&str> = response.tools.iter().map(|c| c.id.as_str()).collect();
    assert_eq!(ids, ["c1", "c2"]);
    assert_eq!(response.tools[1].arguments, json!({"q": "c"}));
    let answers = answers.lock().unwrap();
    assert_eq!(answers[0], vec![]);
    assert_eq!(
        answers[1],
        vec![Answer {
            call: 0,
            value: json!({"hits": ["a"]}),
            is_error: false
        }]
    );
}

#[tokio::test]
async fn a_replay_that_diverges_in_tool_arguments_or_count_is_refused() {
    let history = || vec![input("x"), round(0, &["a", "b"])];
    for (name, step) in [
        (
            "tool",
            blocked(&[("search", json!({"q": "a"})), ("other", json!({"q": "b"}))]),
        ),
        (
            "arguments",
            blocked(&[("search", json!({"q": "a"})), ("search", json!({"q": "z"}))]),
        ),
        ("count", blocked(&[("search", json!({"q": "a"}))])),
    ] {
        // An unknown tool is refused before the round is compared: make it known for this case.
        let tools: Vec<Arc<dyn Tool>> = vec![
            search(counter()),
            typed_tool::<Query, Value, _, _>(spec("other"), |_, _| async {
                Ok(ToolOutput::Complete(json!(null)))
            }),
        ];
        let model = ScriptModel::new(
            Arc::new(Fake::new(vec![step])),
            tools,
            ScriptLimits::default(),
        )
        .unwrap();
        let result = turn(&model, history()).await;
        assert!(
            matches!(&result, Err(Error::Incompatible(_))),
            "{name}: {result:?}"
        );
    }
}

#[tokio::test]
async fn returning_or_throwing_while_rounds_remain_is_divergence() {
    for end in [Step::Returned(json!(1)), threw("no")] {
        let model = fake_model(Fake::new(vec![end]));
        let result = turn(&model, vec![input("x"), round(0, &["a"])]).await;
        assert!(matches!(result, Err(Error::Incompatible(_))), "{result:?}");
    }
}

#[tokio::test]
async fn a_gap_in_the_recorded_call_ids_is_refused() {
    let model = fake_model(Fake::new(vec![blocked(&[("search", json!({"q": "a"}))])]));
    // The first round, c0, was dropped.
    let result = turn(&model, vec![input("x"), round(1, &["a"])]).await;
    let Err(Error::Incompatible(detail)) = result else {
        panic!("{result:?}")
    };
    assert!(detail.to_string().contains("gap"), "{detail}");
}

#[tokio::test]
async fn an_invalid_call_is_answered_as_an_error_and_never_dispatched() {
    let fake = Fake::new(vec![
        blocked(&[
            ("search", json!({"q": "valid"})),
            ("search", json!({"wrong": 1})),
            ("missing", json!({})),
        ]),
        // Answered the two refusals, the script issues one more call.
        blocked(&[("search", json!({"q": "after"}))]),
    ]);
    let answers = fake.answers.clone();
    let model = fake_model(fake);
    let response = turn(&model, vec![input("x")]).await.unwrap();
    let calls: Vec<(&str, &Value)> = (response.tools.iter())
        .map(|call| (call.id.as_str(), &call.arguments))
        .collect();
    assert_eq!(
        calls,
        [
            ("c0", &json!({"q": "valid"})),
            ("c1", &json!({"q": "after"}))
        ]
    );
    let answers = answers.lock().unwrap();
    let refused: Vec<(usize, bool)> = answers[1].iter().map(|a| (a.call, a.is_error)).collect();
    assert_eq!(refused, [(1, true), (2, true)]);
    assert!(answers[1][0].value.as_str().unwrap().contains("invalid"));
    assert!(
        answers[1][1]
            .value
            .as_str()
            .unwrap()
            .contains("unknown tool")
    );
}

#[tokio::test]
async fn a_script_that_ends_is_the_final_output() {
    let model = fake_model(Fake::new(vec![Step::Returned(json!({"n": 1}))]));
    let response = turn(&model, vec![input("x")]).await.unwrap();
    assert_eq!(
        response.output,
        Some(json!({"result": {"n": 1}, "logs": ["logged"]}))
    );
    let model = fake_model(Fake::new(vec![
        blocked(&[("search", json!({"q": "a"})), ("search", json!(3))]),
        // Answered the refusal, the script still waits on the first call.
        Step::Blocked(vec![]),
        Step::Threw {
            error: "boom".into(),
            issued: vec![script_call("search", json!({"q": "late"}))],
        },
    ]));
    let response = turn(&model, vec![input("x"), round(0, &["a"])])
        .await
        .unwrap();
    assert_eq!(
        response.output,
        Some(json!({
            "error": "boom",
            "logs": ["logged"],
            "calls": [
                {"tool": "search", "status": "rejected"},
                {"tool": "search", "id": "c0", "status": "succeeded"},
                {"tool": "search", "status": "not_dispatched"},
            ],
        }))
    );
}

#[tokio::test]
async fn a_long_error_is_bounded() {
    let long = format!("{}é", "x".repeat(10_000));
    let model = fake_model(Fake::new(vec![threw(&long)]));
    let response = turn(&model, vec![input("x")]).await.unwrap();
    let error = response.output.unwrap()["error"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(
        error.len() <= 4_096 + "… (truncated)".len(),
        "{}",
        error.len()
    );
    assert!(error.ends_with("… (truncated)"));
}

#[test]
fn tools_whose_names_map_to_one_function_are_a_config_error() {
    let tools: Vec<Arc<dyn Tool>> = vec![search_named("read-file"), search_named("read.file")];
    let result = ScriptModel::new(Arc::new(QuickJs), tools.clone(), ScriptLimits::default());
    assert!(matches!(result, Err(Error::Config(_))));
    assert!(matches!(
        CodeMode::new("code").tools(tools).build(),
        Err(Error::Config(_))
    ));
}
fn search_named(name: &str) -> Arc<dyn Tool> {
    typed_tool::<Query, Value, _, _>(spec(name), |_, _| async {
        Ok(ToolOutput::Complete(json!(null)))
    })
}

// ---------------------------------------------------------------------------------------------
// Steps 2 and 6: what every `ScriptRuntime` must do, against real scripts, without an agent. Each
// check takes the runtime; `conformance!` runs every check against one runtime.

macro_rules! conformance {
    ($module:ident, $runtime:expr) => {
        mod $module {
            use super::*;
            conformance!(@checks $runtime;
                sequential_awaits_issue_one_call_per_step,
                promise_all_issues_its_calls_in_one_step,
                a_failed_call_throws_a_tool_error_the_script_can_catch,
                an_uncaught_failure_or_throw_ends_the_script,
                an_unawaited_call_and_a_never_settling_await_are_failures,
                the_same_script_and_answers_issue_the_same_calls,
                nondeterministic_builtins_are_absent,
                console_log_is_returned_line_by_line,
                calls_take_one_json_argument_and_an_absent_one_is_an_empty_object,
                a_result_that_is_not_json_or_too_large_is_a_failure,
                cancelling_a_session_interrupts_a_running_script,
                a_memory_bomb_deep_recursion_and_a_log_flood_are_contained,
                a_script_cannot_replace_a_tool_function_or_reach_the_bridge,
            );
        }
    };
    (@checks $runtime:expr; $($check:ident),* $(,)?) => {
        $(
            #[test]
            fn $check() {
                super::$check(&$runtime);
            }
        )*
    };
}
conformance!(quickjs, QuickJs);
conformance!(subprocess, helper());

/// The `agent-rs-quickjs` binary this package builds.
fn helper() -> Subprocess {
    Subprocess::new(env!("CARGO_BIN_EXE_agent-rs-quickjs"))
}

#[test]
fn a_process_that_cannot_start_or_runs_another_interpreter_is_refused() {
    let start = |runtime: Subprocess| {
        runtime
            .start(
                "return 1;",
                &functions(),
                &ScriptLimits::default(),
                CancellationToken::new(),
            )
            .map(|_| ())
    };
    let missing = start(Subprocess::new("/nonexistent/agent-rs-quickjs"));
    assert!(
        matches!(&missing, Err(Error::Provider(d)) if d.to_string().contains("cannot start")),
        "{missing:?}"
    );
    #[cfg(unix)]
    {
        let impostor = Subprocess::new("/bin/sh")
            .arg("-c")
            .arg(r#"read line; echo '{"ready":{"binding":"another"}}'"#);
        let result = start(impostor);
        assert!(
            matches!(&result, Err(Error::Provider(d)) if d.to_string().contains("runs \"another\"")),
            "{result:?}"
        );
        // A process that exits mid-session fails the step, not the host.
        let quitter = Subprocess::new("/bin/sh").arg("-c").arg(format!(
            "read line; echo '{{\"ready\":{{\"binding\":{}}}}}'; exit 0",
            serde_json::to_string(&helper().binding()["process"]).unwrap()
        ));
        let mut session = quitter
            .start(
                "return 1;",
                &functions(),
                &ScriptLimits::default(),
                CancellationToken::new(),
            )
            .unwrap();
        let result = session.advance(vec![]);
        assert!(
            matches!(&result, Err(Error::Provider(d)) if d.to_string().contains("exited")),
            "{result:?}"
        );
    }
}

#[test]
fn the_process_binding_names_the_interpreter_not_the_path() {
    let here = helper().binding();
    let elsewhere = Subprocess::new("/opt/elsewhere/agent-rs-quickjs").binding();
    assert_eq!(here, elsewhere);
    assert_eq!(here["process"]["interpreter"], QuickJs.binding());
    assert_ne!(here, QuickJs.binding());
}

#[tokio::test]
async fn a_script_runs_end_to_end_in_a_subprocess() {
    let executions = counter();
    let code = code_mode(vec![search(executions.clone())])
        .runtime(Arc::new(helper()))
        .build()
        .unwrap();
    let agent = parent(
        code,
        r#"
        const [a, b] = await Promise.all([search({ q: "a" }), search({ q: "b" })]);
        console.log("two");
        return (await search({ q: a.hits[0] + b.hits[0] })).hits;
        "#,
    );
    let (result, state) = run(&agent).await;
    result.unwrap();
    assert_eq!(
        delegated(&state).value,
        json!({"result": ["hit:hit:ahit:b"], "logs": ["two"]})
    );
    assert_eq!(executions.load(Ordering::SeqCst), 3);
    // An infinite loop in the process ends at the deadline, and the process with it.
    let code = code_mode(vec![search(counter())])
        .runtime(Arc::new(helper()))
        .limits(default_limits().with_operation_timeout(Duration::from_millis(200)))
        .build()
        .unwrap();
    let (result, state) = run(&parent(code, "while (true) {}")).await;
    result.unwrap();
    let error = delegated(&state).value["error"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(error.contains("timed out"), "{error}");
}

fn functions() -> Vec<ScriptFunction> {
    ["search", "write"]
        .into_iter()
        .map(|name| ScriptFunction {
            identifier: name.into(),
            tool: name.into(),
        })
        .collect()
}
/// Runs `code`, answering every call with `answer`, and returns each step and the logs.
fn script(
    runtime: &dyn ScriptRuntime,
    code: &str,
    limits: &ScriptLimits,
    answer: impl Fn(&ScriptCall) -> (Value, bool),
) -> (Vec<Step>, Vec<String>) {
    let mut session = runtime
        .start(code, &functions(), limits, CancellationToken::new())
        .unwrap();
    let mut steps = Vec::new();
    let mut answers = Vec::new();
    let mut issued = 0;
    loop {
        let step = session.advance(std::mem::take(&mut answers)).unwrap();
        steps.push(step.clone());
        let Step::Blocked(calls) = step else {
            return (steps, session.logs());
        };
        assert!(!calls.is_empty(), "this harness answers every call at once");
        for call in calls {
            let (value, is_error) = answer(&call);
            answers.push(Answer {
                call: issued,
                value,
                is_error,
            });
            issued += 1;
        }
    }
}
fn echo(call: &ScriptCall) -> (Value, bool) {
    (json!({"echo": call.arguments}), false)
}
fn threw(error: &str) -> Step {
    Step::Threw {
        error: error.into(),
        issued: vec![],
    }
}
/// The error of a script that threw.
fn error_of(step: &Step) -> &str {
    match step {
        Step::Threw { error, .. } => error,
        other => panic!("the script did not throw: {other:?}"),
    }
}
fn ended(steps: &[Step]) -> &Step {
    steps.last().unwrap()
}

fn sequential_awaits_issue_one_call_per_step(runtime: &dyn ScriptRuntime) {
    let (steps, _) = script(
        runtime,
        r#"
        const a = await search({ q: "a" });
        const b = await search({ q: a.echo.q + "b" });
        return b.echo.q;
        "#,
        &ScriptLimits::default(),
        echo,
    );
    assert_eq!(
        steps,
        [
            Step::Blocked(vec![script_call("search", json!({"q": "a"}))]),
            Step::Blocked(vec![script_call("search", json!({"q": "ab"}))]),
            Step::Returned(json!("ab")),
        ]
    );
}

fn promise_all_issues_its_calls_in_one_step(runtime: &dyn ScriptRuntime) {
    let (steps, _) = script(
        runtime,
        r#"
        const results = await Promise.all([1, 2, 3].map((n) => search({ q: String(n) })));
        return results.map((r) => r.echo.q);
        "#,
        &ScriptLimits::default(),
        echo,
    );
    assert_eq!(steps.len(), 2);
    let Step::Blocked(calls) = &steps[0] else {
        panic!()
    };
    assert_eq!(calls.len(), 3);
    assert_eq!(steps[1], Step::Returned(json!(["1", "2", "3"])));
}

fn a_failed_call_throws_a_tool_error_the_script_can_catch(runtime: &dyn ScriptRuntime) {
    let (steps, _) = script(
        runtime,
        r#"
        try {
          await write({ path: "x" });
          return "unreachable";
        } catch (error) {
          return [error instanceof Error, error.name, error.tool, error.value, error.message];
        }
        "#,
        &ScriptLimits::default(),
        |_| (json!({"reason": "denied"}), true),
    );
    assert_eq!(
        ended(&steps),
        &Step::Returned(json!([
            true,
            "ToolError",
            "write",
            {"reason": "denied"},
            "{\"reason\":\"denied\"}"
        ]))
    );
}

fn an_uncaught_failure_or_throw_ends_the_script(runtime: &dyn ScriptRuntime) {
    let (steps, _) = script(
        runtime,
        "await write({});",
        &ScriptLimits::default(),
        |_| (json!("disk full"), true),
    );
    let message = error_of(ended(&steps));
    assert!(message.starts_with("ToolError: disk full"), "{message}");
    let (steps, _) = script(
        runtime,
        "throw new RangeError('too far');",
        &ScriptLimits::default(),
        echo,
    );
    let message = error_of(ended(&steps));
    assert!(message.starts_with("RangeError: too far"), "{message}");
    assert!(
        message.contains("code.js"),
        "the stack names the script: {message}"
    );
    let (steps, _) = script(runtime, "return (;", &ScriptLimits::default(), echo);
    let message = error_of(ended(&steps));
    assert!(message.starts_with("SyntaxError"), "{message}");
}

fn an_unawaited_call_and_a_never_settling_await_are_failures(runtime: &dyn ScriptRuntime) {
    let (steps, _) = script(
        runtime,
        "search({ q: 'lost' }); return 1;",
        &ScriptLimits::default(),
        echo,
    );
    assert_eq!(
        steps,
        [Step::Threw {
            error: "1 calls were not awaited; none of them was dispatched".into(),
            issued: vec![script_call("search", json!({"q": "lost"}))],
        }]
    );
    let (steps, _) = script(
        runtime,
        "await new Promise(() => {}); return 1;",
        &ScriptLimits::default(),
        echo,
    );
    assert_eq!(
        steps,
        [threw(
            "the script is waiting on a promise that never settles"
        )]
    );
}

fn the_same_script_and_answers_issue_the_same_calls(runtime: &dyn ScriptRuntime) {
    let code = r#"
        const seen = new Map();
        const object = { z: 1, a: 2, [Symbol.iterator]: null, 10: 3, 2: 4 };
        for (const key of Object.keys(object)) seen.set(key, key.length);
        const first = await Promise.all([search({ q: JSON.stringify([...seen]) }), write({ n: 1 })]);
        const matches = "a1b22c333".match(/\d+/g);
        let total = 0;
        for (const m of matches) total += (await search({ q: m })).echo.q.length;
        console.log("total", total, { first });
        return { total, keys: Object.keys(object) };
    "#;
    let first = script(runtime, code, &ScriptLimits::default(), echo);
    let second = script(runtime, code, &ScriptLimits::default(), echo);
    assert_eq!(first, second);
    assert_eq!(first.0.len(), 5);
}

fn nondeterministic_builtins_are_absent(runtime: &dyn ScriptRuntime) {
    let (steps, _) = script(
        runtime,
        r#"
        return ["Date", "WeakRef", "FinalizationRegistry", "performance", "Atomics",
                "SharedArrayBuffer", "setTimeout", "fetch", "require", "process"]
          .map((name) => typeof globalThis[name])
          .concat([typeof Math.random]);
        "#,
        &ScriptLimits::default(),
        echo,
    );
    assert_eq!(ended(&steps), &Step::Returned(json!(vec!["undefined"; 11])));
}

fn console_log_is_returned_line_by_line(runtime: &dyn ScriptRuntime) {
    let (_, logs) = script(
        runtime,
        "console.log('a', 1, { b: [2] }); console.error('c'); return null;",
        &ScriptLimits::default(),
        echo,
    );
    assert_eq!(logs, ["a 1 {\"b\":[2]}", "c"]);
}

fn calls_take_one_json_argument_and_an_absent_one_is_an_empty_object(runtime: &dyn ScriptRuntime) {
    let (steps, _) = script(
        runtime,
        r#"
        await search();
        const errors = [];
        for (const bad of [() => search(1, 2), () => search(() => 1), () => search(1n)]) {
          try { await bad(); } catch (error) { errors.push(error.name); }
        }
        return errors;
        "#,
        &ScriptLimits::default(),
        echo,
    );
    assert_eq!(
        steps,
        [
            Step::Blocked(vec![script_call("search", json!({}))]),
            Step::Returned(json!(["TypeError", "TypeError", "TypeError"])),
        ]
    );
}

fn a_result_that_is_not_json_or_too_large_is_a_failure(runtime: &dyn ScriptRuntime) {
    let (steps, _) = script(runtime, "return 1n;", &ScriptLimits::default(), echo);
    assert!(error_of(ended(&steps)).contains("cannot be encoded"));
    let (steps, _) = script(runtime, "return undefined;", &ScriptLimits::default(), echo);
    assert_eq!(ended(&steps), &Step::Returned(Value::Null));
    let limits = ScriptLimits::default().with_max_result_bytes(16);
    let (steps, _) = script(runtime, "return 'x'.repeat(100);", &limits, echo);
    assert!(error_of(ended(&steps)).contains("over the limit"));
}

// ---------------------------------------------------------------------------------------------
// Step 3: end to end through `Subagent`.

#[tokio::test]
async fn sequential_calls_and_a_parallel_batch_settle_with_the_script_result() {
    let executions = counter();
    let code = code_mode(vec![search(executions.clone())]).build().unwrap();
    let agent = parent(
        code,
        r#"
        const [a, b] = await Promise.all([search({ q: "a" }), search({ q: "b" })]);
        const c = await search({ q: a.hits[0] + "+" + b.hits[0] });
        console.log("joined", c.hits[0]);
        return c.hits[0];
        "#,
    );
    let (result, state) = run(&agent).await;
    assert_eq!(result.unwrap(), json!("done"));
    let delegated = delegated(&state);
    assert!(!delegated.is_error);
    assert_eq!(
        delegated.value,
        json!({"result": "hit:hit:a+hit:b", "logs": ["joined hit:hit:a+hit:b"]})
    );
    assert_eq!(executions.load(Ordering::SeqCst), 3);
    let child = child(&state);
    // Two rounds and the final turn; the parent's call and the child's three.
    assert_eq!(child.usage.model_calls, 3);
    assert_eq!(child.usage.tool_calls, 3);
    assert_eq!(state.usage.tool_calls, 1 + 3);
    assert_eq!(child.usage.input_tokens + child.usage.output_tokens, 0);
    let rounds: Vec<Vec<&str>> = (child.history.iter())
        .filter_map(|entry| match entry {
            HistoryEntry::Exchange { response, .. } if response.output.is_none() => {
                Some(response.tools.iter().map(|c| c.id.as_str()).collect())
            }
            _ => None,
        })
        .collect();
    assert_eq!(rounds, [vec!["c0", "c1"], vec!["c2"]]);
}

#[tokio::test]
async fn an_inner_failure_the_script_catches_does_not_fail_it() {
    let failing = typed_tool::<Value, Value, _, _>(spec("write"), |_, _| async {
        Ok(ToolOutput::Failed(json!("read-only file system")))
    });
    let code = code_mode(vec![failing]).build().unwrap();
    let agent = parent(
        code,
        "try { await write({}); } catch (e) { return `caught: ${e.value}`; }",
    );
    let (result, state) = run(&agent).await;
    result.unwrap();
    assert_eq!(
        delegated(&state).value,
        json!({"result": "caught: read-only file system", "logs": []})
    );
    assert!(!delegated(&state).is_error);
}

#[tokio::test]
async fn a_script_that_throws_is_an_error_result_listing_its_calls() {
    let code = code_mode(vec![search(counter())]).build().unwrap();
    let agent = parent(
        code,
        r#"
        await search({ q: "a" });
        try { await search({ nope: true }); } catch {}
        search({ q: "never" });
        throw new Error("gave up");
        "#,
    );
    let (result, state) = run(&agent).await;
    result.unwrap();
    let delegated = delegated(&state);
    assert!(delegated.is_error);
    let value = &delegated.value;
    assert!(
        value["error"]
            .as_str()
            .unwrap()
            .starts_with("Error: gave up")
    );
    assert_eq!(
        value["calls"],
        json!([
            {"tool": "search", "id": "c0", "status": "succeeded"},
            {"tool": "search", "status": "rejected"},
            {"tool": "search", "status": "not_dispatched"},
        ])
    );
    assert_eq!(child(&state).usage.tool_calls, 1);
}

#[tokio::test]
async fn an_inner_call_that_requires_approval_waits_for_the_host() {
    let executions = counter();
    let approved = typed_tool::<Query, Value, _, _>(spec("search").with_requires_approval(true), {
        let executions = executions.clone();
        move |query, _| {
            let executions = executions.clone();
            async move {
                executions.fetch_add(1, Ordering::SeqCst);
                Ok(ToolOutput::Complete(json!(query.q)))
            }
        }
    });
    let code = code_mode(vec![approved]).build().unwrap();
    let agent = parent(code, "return await search({ q: 'approved' });");
    let store = Arc::new(MemoryStore::new());
    let options = HostOptions::default().with_poll_interval(Duration::from_millis(2));
    let mut host = Host::start(store, agent, options).unwrap();
    let id = host
        .submit("conversation", "key", json!("go"), None)
        .await
        .unwrap();
    let waiting = observe(&host, id, |state| {
        state.children.first().is_some_and(|child| {
            (child.state.operations.iter()).any(|op| op.status == OperationStatus::Approval)
        })
    })
    .await;
    assert_eq!(executions.load(Ordering::SeqCst), 0);
    let child_id = child(&waiting).id;
    host.resolve_child(id, child_id, "c0", Resolution::Approve(true))
        .await
        .unwrap();
    let done = observe(&host, id, RunState::is_terminal).await;
    assert_eq!(
        delegated(&done).value,
        json!({"result": "approved", "logs": []})
    );
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    host.shutdown().await.unwrap();
}
async fn observe<S: DurableStore + 'static>(
    host: &Host<S>,
    id: RunId,
    ready: impl Fn(&RunState) -> bool,
) -> RunState {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            host.health().unwrap();
            let state = host.status(id).await.unwrap();
            if ready(&state) {
                return state;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("host did not reach the expected state")
}

#[tokio::test]
async fn an_unknown_inner_outcome_waits_for_reconciliation_then_the_script_goes_on() {
    let attempts = counter();
    let never = typed_tool::<Query, Value, _, _>(spec("write").with_retry(RetryPolicy::Never), {
        let attempts = attempts.clone();
        move |_, _| {
            let attempts = attempts.clone();
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err(Error::tool("connection reset"))
            }
        }
    });
    let code = code_mode(vec![never]).build().unwrap();
    let agent = parent(code, "const r = await write({ q: 'x' }); return r;");
    let mut conversation = Conversation::default();
    assert!(matches!(
        agent.run(&mut conversation, json!("go")).await,
        Err(Error::Waiting)
    ));
    let run = conversation.last_run.as_mut().unwrap();
    let child_id = child(run).id;
    assert!(matches!(
        child(run).operations[0].status,
        OperationStatus::Unknown { .. }
    ));
    run.resolve_child(
        child_id,
        "c0",
        Resolution::Reconcile(ToolResult {
            call_id: "c0".into(),
            value: json!("reconciled"),
            is_error: false,
        }),
    )
    .unwrap();
    agent
        .resume(&mut conversation, RunControl::default())
        .await
        .unwrap();
    let state = conversation.last_run.as_ref().unwrap();
    assert_eq!(
        delegated(state).value,
        json!({"result": "reconciled", "logs": []})
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

/// Fails the `fail_at`th commit, and keeps a copy of the state as of the last that succeeded.
struct Flaky {
    fail_at: usize,
    commits: AtomicUsize,
    durable: Mutex<Option<RunState>>,
}
impl Flaky {
    fn new(fail_at: usize) -> Self {
        Self {
            fail_at,
            commits: AtomicUsize::new(0),
            durable: Mutex::new(None),
        }
    }
}
#[async_trait]
impl Checkpoint for Flaky {
    async fn commit(&self, state: &mut RunState) -> Result<()> {
        if self.commits.fetch_add(1, Ordering::SeqCst) + 1 == self.fail_at {
            return Err(Error::storage("injected commit failure"));
        }
        state.revision += 1;
        *self.durable.lock().unwrap() = Some(state.clone());
        Ok(())
    }
}

#[tokio::test]
async fn a_failed_commit_at_any_point_resumes_without_repeating_an_effect() {
    // Each idempotency key's effect, applied once however often its call executes.
    let effects: Arc<Mutex<HashMap<String, Value>>> = Arc::default();
    let write = typed_tool::<Query, Value, _, _>(
        spec("write").with_retry(RetryPolicy::Idempotent {
            contract: "keyed-v1".into(),
        }),
        {
            let effects = effects.clone();
            move |query, context| {
                let effects = effects.clone();
                async move {
                    let mut effects = effects.lock().unwrap();
                    let value = effects
                        .entry(context.idempotency_key)
                        .or_insert_with(|| json!(format!("wrote {}", query.q)));
                    Ok(ToolOutput::Complete(value.clone()))
                }
            }
        },
    );
    let code = code_mode(vec![write]).build().unwrap();
    let agent = parent(
        code,
        "const a = await write({ q: 'a' }); const b = await write({ q: 'b' }); return [a, b];",
    );
    let expected = json!({"result": ["wrote a", "wrote b"], "logs": []});
    // Count the commits of an undisturbed run.
    let counting = Flaky::new(usize::MAX);
    let mut state = agent.new_run("c", json!("go"), &[], None).unwrap();
    agent
        .drive(&mut state, &counting, RunControl::default())
        .await
        .unwrap();
    assert_eq!(delegated(&state).value, expected);
    let commits = counting.commits.load(Ordering::SeqCst);
    assert!(commits > 10, "{commits}");
    for fail_at in 1..=commits {
        effects.lock().unwrap().clear();
        let initial = agent.new_run("c", json!("go"), &[], None).unwrap();
        let mut state = initial.clone();
        let flaky = Flaky::new(fail_at);
        let result = agent.drive(&mut state, &flaky, RunControl::default()).await;
        assert!(
            matches!(result, Err(Error::Storage(_))),
            "{fail_at}: {result:?}"
        );
        // Resume from what was durable.
        let mut state = flaky.durable.lock().unwrap().clone().unwrap_or(initial);
        agent
            .drive(&mut state, &MemoryCheckpoint, RunControl::default())
            .await
            .unwrap();
        assert_eq!(delegated(&state).value, expected, "commit {fail_at}");
        assert_eq!(effects.lock().unwrap().len(), 2, "commit {fail_at}");
    }
}

#[tokio::test]
async fn a_journal_too_large_for_the_context_fails_the_script() {
    let large = typed_tool::<Query, Value, _, _>(spec("search"), |_, _| async {
        Ok(ToolOutput::Complete(json!("x".repeat(300))))
    });
    let code = code_mode(vec![large])
        .limits(default_limits().with_max_context_bytes(1_000))
        .build()
        .unwrap();
    let agent = parent(
        code,
        "for (let i = 0; i < 6; i++) await search({ q: String(i) }); return 'all';",
    );
    let (result, state) = run(&agent).await;
    result.unwrap();
    let delegated = delegated(&state);
    assert!(delegated.is_error);
    let error = delegated.value["error"].as_str().unwrap();
    assert!(error.starts_with("failed: "), "{error}");
    assert!(error.contains("context"), "{error}");
    // Every round before the one that did not fit was dispatched; none was dropped.
    assert!(matches!(
        child(&state).phase,
        Phase::Settled(Settlement::Failed(_))
    ));
}

#[tokio::test]
async fn model_and_tool_call_limits_stop_the_script() {
    let code = "await search({ q: 'a' }); await search({ q: 'b' }); return 1;";
    for (limits, reason) in [
        (
            default_limits().with_max_model_calls(2),
            StopReason::ModelCalls,
        ),
        (
            default_limits().with_max_tool_calls(1),
            StopReason::ToolCalls,
        ),
    ] {
        let mode = code_mode(vec![search(counter())])
            .limits(limits)
            .build()
            .unwrap();
        let (result, state) = run(&parent(mode, code)).await;
        result.unwrap();
        assert_eq!(
            child(&state).phase,
            Phase::Settled(Settlement::Stopped(reason.clone()))
        );
        assert_eq!(
            delegated(&state).value,
            json!({"error": format!("stopped: {reason}")})
        );
        assert!(delegated(&state).is_error);
    }
}

#[tokio::test]
async fn a_sequential_tool_inside_promise_all_still_runs_alone() {
    let active = counter();
    let overlapped = Arc::new(AtomicBool::new(false));
    let tool = |name: &str, execution: Execution| {
        let (active, overlapped) = (active.clone(), overlapped.clone());
        let sequential = execution == Execution::Sequential;
        typed_tool::<Value, Value, _, _>(spec(name).with_execution(execution), move |_, _| {
            let (active, overlapped) = (active.clone(), overlapped.clone());
            async move {
                let others = active.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(20)).await;
                let after = active.fetch_sub(1, Ordering::SeqCst) - 1;
                if sequential && (others > 0 || after > 0) {
                    overlapped.store(true, Ordering::SeqCst);
                }
                Ok(ToolOutput::Complete(json!(null)))
            }
        })
    };
    let mode = code_mode(vec![
        tool("parallel", Execution::Parallel),
        tool("alone", Execution::Sequential),
    ])
    .build()
    .unwrap();
    let agent = parent(
        mode,
        "await Promise.all([parallel(), parallel(), alone(), parallel()]); return 'ok';",
    );
    let (result, state) = run(&agent).await;
    result.unwrap();
    assert_eq!(delegated(&state).value["result"], json!("ok"));
    assert_eq!(child(&state).usage.tool_calls, 4);
    assert!(!overlapped.load(Ordering::SeqCst));
}

// ---------------------------------------------------------------------------------------------
// Step 4: the model-facing surface.

#[test]
fn declarations_render_each_kind_of_schema() {
    let cases = [
        (json!({"type": "object", "properties": {}}), "{}"),
        (json!(true), "unknown"),
        (json!(null), "unknown"),
        (json!({"type": "string"}), "string"),
        (json!({"type": ["integer", "null"]}), "number | null"),
        (json!({"enum": ["a", 1, null]}), "\"a\" | 1 | null"),
        (json!({"const": "only"}), "\"only\""),
        (
            json!({"type": "array", "items": {"anyOf": [{"type": "string"}, {"type": "number"}]}}),
            "(string | number)[]",
        ),
        (
            json!({"type": "array", "prefixItems": [{"type": "string"}, {"type": "boolean"}]}),
            "[string, boolean]",
        ),
        (
            json!({"type": "object", "additionalProperties": {"type": "number"}}),
            "Record<string, number>",
        ),
        (
            json!({
                "type": "object",
                "required": ["path", "mode"],
                "properties": {
                    "path": {"type": "string", "description": "Where to write."},
                    "mode": {"$ref": "#/$defs/Mode"},
                    "if-exists": {"type": "boolean"},
                    "tags": {"type": "array", "items": {"type": "string"}},
                    "nested": {"type": "object", "properties": {"n": {"type": "integer"}}}
                },
                "$defs": {"Mode": {"enum": ["create", "append"]}}
            }),
            "{\n  \"if-exists\"?: boolean;\n  mode: \"create\" | \"append\";\n  nested?: {\n    n?: number;\n  };\n  /** Where to write. */\n  path: string;\n  tags?: string[];\n}",
        ),
        // Malformed or unrenderable schemas are `unknown`, never an error.
        (json!({"type": "object", "properties": 3}), "unknown"),
        (json!({"type": "tuple"}), "unknown"),
        (json!({"$ref": "#/$defs/Missing"}), "unknown"),
        (json!({"$ref": "https://example.com/schema"}), "unknown"),
        (json!({"$ref": "#", "type": "object"}), "unknown"),
    ];
    for (schema, expected) in cases {
        assert_eq!(input_type(&schema), expected, "{schema}");
    }
    let wide: serde_json::Map<String, Value> = (0..400)
        .map(|n| (format!("field_{n}"), json!({"type": "string"})))
        .collect();
    assert_eq!(
        input_type(&json!({"type": "object", "properties": wide})),
        "unknown",
        "a type over the byte cap"
    );
}

#[test]
fn the_description_declares_each_tool_and_is_byte_stable() {
    let tools = || -> Vec<Arc<dyn Tool>> {
        vec![
            search(counter()),
            typed_tool::<Value, Value, _, _>(
                ToolSpec::new("list-files", "1")
                    .with_description("List files.\nOne per line.")
                    .with_input_schema(json!({"type": "object"})),
                |_, _| async { Ok(ToolOutput::Complete(json!([]))) },
            ),
        ]
    };
    let specs: Vec<ToolSpec> = tools().iter().map(|t| t.spec().clone()).collect();
    let text = description(&specs);
    assert!(text.contains(
        "/** The search tool. */\ndeclare function search(input: {\n  q: string;\n}): Promise<unknown>;\n"
    ), "{text}");
    // `typed_tool` replaced the schema with that of `Value`.
    assert!(
        text.contains(
            "/**\n * List files.\n * One per line.\n */\ndeclare function list_files(input: unknown): Promise<unknown>;\n"
        ),
        "{text}"
    );
    for rule in [
        "async function",
        "`return`",
        "Promise.all",
        "not undone",
        "no clock",
    ] {
        assert!(text.contains(rule), "{rule}");
    }
    let empty = ToolSpec::new("now", "1").with_input_schema(json!({"type": "object"}));
    assert!(description(&[empty]).contains("declare function now(): Promise<unknown>;"));
    let first = code_mode(tools()).build().unwrap();
    let second = code_mode(tools()).build().unwrap();
    assert_eq!(
        first.spec().description.as_bytes(),
        second.spec().description.as_bytes()
    );
    assert_eq!(first.spec().description, text);
    assert_eq!(first.agent().binding(), second.agent().binding());
    assert_eq!(
        first.spec().input_schema["properties"]["code"]["type"],
        json!("string")
    );
}

// ---------------------------------------------------------------------------------------------
// Step 6: limits and hardening.

#[tokio::test]
async fn an_infinite_loop_ends_at_the_deadline_and_the_script_fails_cleanly() {
    let code = code_mode(vec![search(counter())])
        .limits(default_limits().with_operation_timeout(Duration::from_millis(200)))
        .build()
        .unwrap();
    let agent = parent(code, "await search({ q: 'a' }); while (true) {}");
    let (result, state) = run(&agent).await;
    result.unwrap();
    let error = delegated(&state).value["error"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(error.contains("timed out"), "{error}");
}

fn cancelling_a_session_interrupts_a_running_script(runtime: &dyn ScriptRuntime) {
    let cancel = CancellationToken::new();
    let mut session = runtime
        .start(
            "while (true) {}",
            &functions(),
            &ScriptLimits::default(),
            cancel.clone(),
        )
        .unwrap();
    let canceller = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(50));
        cancel.cancel();
    });
    assert!(matches!(session.advance(vec![]), Err(Error::Provider(_))));
    canceller.join().unwrap();
}

fn a_memory_bomb_deep_recursion_and_a_log_flood_are_contained(runtime: &dyn ScriptRuntime) {
    let limits = ScriptLimits::default()
        .with_max_memory_bytes(8 * 1024 * 1024)
        .with_max_log_bytes(64);
    let (steps, _) = script(
        runtime,
        "const hoard = []; while (true) hoard.push('x'.repeat(1 << 16) + hoard.length); ",
        &limits,
        echo,
    );
    let message = error_of(ended(&steps));
    assert!(message.contains("out of memory"), "{message}");
    let (steps, _) = script(
        runtime,
        "const deeper = (n) => deeper(n + 1) + 1; return deeper(0);",
        &limits,
        echo,
    );
    let message = error_of(ended(&steps));
    assert!(
        message.starts_with("RangeError: Maximum call stack size exceeded"),
        "{message}"
    );
    let (steps, logs) = script(
        runtime,
        "for (let i = 0; i < 10000; i++) console.log(`line ${i}`); return 'flooded';",
        &limits,
        echo,
    );
    assert_eq!(ended(&steps), &Step::Returned(json!("flooded")));
    assert_eq!(logs.len(), 11, "{logs:?}");
    assert_eq!(logs[10], "[9990 more log lines dropped]");
}

fn a_script_cannot_replace_a_tool_function_or_reach_the_bridge(runtime: &dyn ScriptRuntime) {
    let (steps, _) = script(
        runtime,
        r#"
        const refused = [];
        const attempts = {
          assign: () => { search = () => "fake"; },
          property: () => { globalThis.search = () => "fake"; },
          define: () => Object.defineProperty(globalThis, "search", { value: 1 }),
          remove: () => { delete globalThis.search; },
          mutate: () => { search.call = () => "fake"; },
          console: () => { console.log = () => {}; },
        };
        for (const [name, attempt] of Object.entries(attempts)) {
          try { attempt(); } catch (error) { refused.push(name); }
        }
        // Replacing the built-ins the bridge used does not change what it reports.
        JSON.stringify = () => "[]";
        JSON.parse = () => "forged";
        Array.prototype.push = () => { throw new Error("push"); };
        Map.prototype.set = () => { throw new Error("set"); };
        Map.prototype.get = () => ["forged"];
        const answer = await search({ q: "real" });
        const globals = Object.getOwnPropertyNames(globalThis)
          .filter((name) => /settle|take|host|bridge|appendLog|functions/i.test(name));
        return { refused, answer, globals };
        "#,
        &ScriptLimits::default(),
        echo,
    );
    assert_eq!(
        steps,
        [
            Step::Blocked(vec![script_call("search", json!({"q": "real"}))]),
            Step::Returned(json!({
                "refused": ["assign", "property", "define", "remove", "mutate", "console"],
                "answer": {"echo": {"q": "real"}},
                "globals": [],
            })),
        ]
    );
}

/// QuickJS that announces each session it starts and holds it until given a permit or cancelled.
struct Gated {
    started: std::sync::mpsc::Sender<()>,
    permits: Arc<AtomicUsize>,
}
impl ScriptRuntime for Gated {
    fn binding(&self) -> Value {
        json!("gated")
    }
    fn start(
        &self,
        code: &str,
        functions: &[ScriptFunction],
        limits: &ScriptLimits,
        cancel: CancellationToken,
    ) -> Result<Box<dyn ScriptSession>> {
        let _ = self.started.send(());
        loop {
            if cancel.is_cancelled() {
                return Err(Error::provider("cancelled while gated"));
            }
            let permits = self.permits.load(Ordering::SeqCst);
            if permits > 0
                && (self.permits)
                    .compare_exchange(permits, permits - 1, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
            {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        QuickJs.start(code, functions, limits, cancel)
    }
}

#[tokio::test]
async fn shutdown_mid_replay_leaves_the_child_resumable_and_cancellation_settles_it() {
    for cancel in [false, true] {
        let executions = counter();
        let (started, starts) = std::sync::mpsc::channel();
        let permits = Arc::new(AtomicUsize::new(0));
        let gated = Arc::new(Gated {
            started,
            permits: permits.clone(),
        });
        let code = code_mode(vec![search(executions.clone())])
            .runtime(gated)
            .build()
            .unwrap();
        let agent = parent(
            code,
            "const a = await search({ q: 'a' }); return (await search({ q: a.hits[0] })).hits;",
        );
        let token = CancellationToken::new();
        let control = if cancel {
            RunControl::default().with_cancel(token.clone())
        } else {
            RunControl::default().with_shutdown(token.clone())
        };
        let mut state = agent.new_run("c", json!("go"), &[], None).unwrap();
        let stopper = {
            let permits = permits.clone();
            tokio::task::spawn_blocking(move || {
                // Let the first turn through; stop the second, which replays the first round.
                starts.recv().unwrap();
                permits.fetch_add(1, Ordering::SeqCst);
                starts.recv().unwrap();
                token.cancel();
            })
        };
        let result = agent.drive(&mut state, &MemoryCheckpoint, control).await;
        stopper.await.unwrap();
        assert_eq!(executions.load(Ordering::SeqCst), 1);
        if cancel {
            result.unwrap();
            assert_eq!(state.phase, Phase::Settled(Settlement::Cancelled));
            assert_eq!(child(&state).phase, Phase::Settled(Settlement::Cancelled));
            assert_eq!(delegated(&state).value, json!({"error": "cancelled"}));
            assert!(delegated(&state).is_error);
            continue;
        }
        assert!(matches!(result, Err(Error::Interrupted)), "{result:?}");
        assert!(!child(&state).is_terminal());
        permits.fetch_add(100, Ordering::SeqCst);
        agent
            .drive(&mut state, &MemoryCheckpoint, RunControl::default())
            .await
            .unwrap();
        assert_eq!(
            delegated(&state).value,
            json!({"result": ["hit:hit:a"], "logs": []})
        );
        // The first round was durable, so resuming replayed it instead of calling again.
        assert_eq!(executions.load(Ordering::SeqCst), 2);
    }
}
