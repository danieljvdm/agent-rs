//! `LlamaModel` over real HTTP to a loopback server, replaying streams recorded from llama-server
//! 0.9.0 (build 12110) serving Gemma 4 26B-A4B with reasoning on. The fixtures are unedited
//! apart from the model path, and `length.sse`, which keeps the first and last events of a
//! reply that ran out of tokens while reasoning.
#![cfg(feature = "openai")]
#[path = "support/openai_http.rs"]
mod openai_http;
#[path = "support/openai_wire.rs"]
mod openai_wire;
use agent_rs::{
    Agent, MemoryCheckpoint, Model, ModelConfig, Phase, ProgressKind, RetryPolicy, RunControl,
    RunState, Settlement, Tool, ToolOutput, ToolSpec,
    openai::{LlamaModel, OpenAiModel},
    typed_tool,
};
use openai_http::{serve, serve_status, serve_with_header_names};
use openai_wire::Api;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

const TEXT: &str = include_str!("fixtures/llama/text.sse");
const TOOLS: &str = include_str!("fixtures/llama/tools.sse");
const JSON: &str = include_str!("fixtures/llama/json.sse");
const LENGTH: &str = include_str!("fixtures/llama/length.sse");
const CONTEXT_EXCEEDED: &str = include_str!("fixtures/llama/context-exceeded.json");

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap()
}
fn model(base: &str) -> LlamaModel {
    LlamaModel::with_client(client(), base).unwrap()
}
fn agent<I, O>(model: LlamaModel, tools: Vec<Arc<dyn Tool>>) -> Agent<I, O>
where
    I: Serialize + DeserializeOwned + schemars::JsonSchema + Send + Sync + 'static,
    O: Serialize + DeserializeOwned + schemars::JsonSchema + Send + Sync + 'static,
{
    Agent::builder("local", Arc::new(model))
        .instructions("Answer in one short sentence.")
        .config(ModelConfig::new("gemma-4-26B-A4B-it-Q4_K_M"))
        .tools(tools)
        .input::<I>()
        .output::<O>()
        .build()
        .unwrap()
}
/// Drives a run of `input` to settlement, returning it with its streamed reasoning and text.
async fn drive<O>(agent: &Agent<String, O>, input: &str) -> (RunState, String, String)
where
    O: Serialize + DeserializeOwned + schemars::JsonSchema + Send + Sync + 'static,
{
    let mut state = agent.new_run("c", json!(input), &[], None).unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::channel(1024);
    agent
        .drive(
            &mut state,
            &MemoryCheckpoint,
            RunControl::default().with_events(tx),
        )
        .await
        .unwrap();
    let (mut reasoning, mut text) = (String::new(), String::new());
    while let Some(event) = rx.recv().await {
        match event.kind {
            ProgressKind::ReasoningDelta(delta) => reasoning.push_str(&delta),
            ProgressKind::TextDelta(delta) => text.push_str(&delta),
            _ => {}
        }
    }
    (state, reasoning, text)
}
fn failure(state: &RunState) -> &str {
    match &state.phase {
        Phase::Settled(Settlement::Failed(error)) => error,
        other => panic!("not failed: {other:?}"),
    }
}

#[tokio::test]
async fn a_plain_text_chat_streams_reasoning_and_answers_in_text_without_a_key() {
    let (base, server) =
        serve_with_header_names(Api::Chat, vec![TEXT.into()], "200 OK", "text/event-stream");
    let agent = agent::<String, String>(model(&base), vec![]);
    let (state, reasoning, text) = drive(&agent, "What is 2+2?").await;

    assert_eq!(
        state.phase,
        Phase::Settled(Settlement::Completed(json!("2 + 2 equals 4.")))
    );
    assert_eq!(text, "2 + 2 equals 4.");
    assert!(reasoning.starts_with("The user is asking"), "{reasoning}");
    assert_eq!(
        (state.usage.input_tokens, state.usage.output_tokens),
        (28, 78)
    );
    // Reasoning is streamed, never retained.
    let history = serde_json::to_string(&state.history).unwrap();
    assert!(!history.contains("The user is asking"));

    let (headers, body) = server.join().unwrap().remove(0);
    assert!(
        !headers.iter().any(|name| name == "authorization"),
        "{headers:?}"
    );
    assert_eq!(body["model"], "gemma-4-26B-A4B-it-Q4_K_M");
    assert_eq!(
        body["messages"],
        json!([
            {"role":"system","content":"Answer in one short sentence."},
            {"role":"user","content":"What is 2+2?"},
        ])
    );
    assert!(body.get("response_format").is_none());
    assert_eq!(body["stream_options"]["include_usage"], true);
}

#[tokio::test]
async fn an_api_key_is_sent_as_a_bearer_token() {
    let (base, server) =
        serve_with_header_names(Api::Chat, vec![TEXT.into()], "200 OK", "text/event-stream");
    let model = model(&base).with_api_key("local-test-key").unwrap();
    let agent = agent::<String, String>(model, vec![]);
    drive(&agent, "What is 2+2?").await;
    let (headers, _) = server.join().unwrap().remove(0);
    assert!(
        headers.iter().any(|name| name == "authorization"),
        "{headers:?}"
    );
}

#[tokio::test]
async fn a_recorded_tool_call_runs_and_its_result_replays_as_plain_text() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let lookup = typed_tool::<Value, String, _, _>(
        ToolSpec::new("lookup", "1")
            .with_description("Look up a stored number by key")
            .with_input_schema(
                json!({"type":"object","properties":{"key":{"type":"string"}},"required":["key"]}),
            )
            .with_retry(RetryPolicy::ReadOnly),
        move |arguments, _| {
            counted.fetch_add(1, Ordering::SeqCst);
            async move {
                assert_eq!(arguments, json!({"key":"alpha"}));
                Ok(ToolOutput::Complete("seventeen".to_owned()))
            }
        },
    );
    let (base, server) = serve(Api::Chat, vec![TOOLS.into(), TEXT.into()]);
    let agent = agent::<String, String>(model(&base), vec![lookup]);
    let (state, _, _) = drive(&agent, "What number is stored under key alpha?").await;

    assert!(
        matches!(state.phase, Phase::Settled(Settlement::Completed(_))),
        "{:?}",
        state.phase
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        (state.usage.input_tokens, state.usage.output_tokens),
        (70 + 28, 17 + 78)
    );
    let requests = server.join().unwrap();
    assert_eq!(requests[0]["tools"][0]["function"]["name"], "lookup");
    let messages = &requests[1]["messages"];
    assert_eq!(
        messages[2]["tool_calls"][0]["id"],
        "pHwVpTkr5SIyHCmlrRlHyj6iUubJ2OpG"
    );
    assert_eq!(
        messages[3],
        json!({"role":"tool","tool_call_id":"pHwVpTkr5SIyHCmlrRlHyj6iUubJ2OpG","content":"seventeen"})
    );
}

#[tokio::test]
async fn a_typed_output_is_grammar_constrained_in_the_envelope() {
    let (base, server) = serve(Api::Chat, vec![JSON.into()]);
    let agent = agent::<String, u64>(model(&base), vec![]);
    let (state, reasoning, text) = drive(&agent, "What is 6 times 7?").await;
    assert_eq!(
        state.phase,
        Phase::Settled(Settlement::Completed(json!(42)))
    );
    assert_eq!(text, "{\n  \"value\": 42\n}");
    assert!(!reasoning.is_empty());
    let body = server.join().unwrap().remove(0);
    let format = &body["response_format"];
    assert_eq!(format["type"], "json_schema");
    assert_eq!(
        format["json_schema"]["schema"]["properties"]["value"]["type"],
        "integer"
    );
    assert_eq!(
        format["json_schema"]["schema"]["required"],
        json!(["value"])
    );
}

#[tokio::test]
async fn running_out_of_tokens_fails_the_run_and_says_so() {
    let (base, server) = serve(Api::Chat, vec![LENGTH.into()]);
    let agent = agent::<String, String>(model(&base), vec![]);
    let (state, reasoning, text) = drive(&agent, "Classify my mood.").await;
    assert_eq!(
        failure(&state),
        "provider: llama-server: output reached the token limit before it was complete"
    );
    assert!(!reasoning.is_empty());
    assert!(text.is_empty());
    server.join().unwrap();
}

#[tokio::test]
async fn http_errors_report_the_servers_message_but_never_the_url_or_key() {
    let (base, server) = serve_status(
        Api::Chat,
        vec![CONTEXT_EXCEEDED.into()],
        "400 Bad Request",
        "application/json; charset=utf-8",
    );
    let keyed = model(&base).with_api_key("local-test-key").unwrap();
    let (state, _, _) = drive(
        &agent::<String, String>(keyed, vec![]),
        "A very long message",
    )
    .await;
    let error = failure(&state);
    // llama-server names this failure, so the engine can tell it from any other.
    assert_eq!(
        error,
        "context window exceeded: llama-server: HTTP status 400: request (12015 tokens) exceeds \
         the available context size (8192 tokens), try increasing it"
    );
    assert!(!error.contains(&base) && !error.contains("local-test-key"));
    assert_eq!(server.join().unwrap().len(), 1);

    // A body without an error message reports the status alone, and is not retried.
    for body in ["<html>Bad gateway</html>", "{\"error\":{\"message\":\"\"}}"] {
        let (base, server) =
            serve_status(Api::Chat, vec![body.into()], "502 Bad Gateway", "text/html");
        let (state, _, _) = drive(&agent::<String, String>(model(&base), vec![]), "Hi").await;
        assert_eq!(failure(&state), "provider: llama-server: HTTP status 502");
        assert_eq!(server.join().unwrap().len(), 1);
    }
}

/// Serves `body` once to a `GET` of `path`, and returns the request's first line.
fn serve_get(path: &'static str, body: String) -> (String, std::thread::JoinHandle<String>) {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let thread = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        let mut bytes = Vec::new();
        while !bytes.windows(4).any(|w| w == b"\r\n\r\n") {
            let mut buffer = [0; 4096];
            let n = socket.read(&mut buffer).unwrap();
            assert!(n > 0);
            bytes.extend_from_slice(&buffer[..n]);
        }
        let request = String::from_utf8_lossy(&bytes);
        let line = request.lines().next().unwrap().to_owned();
        let status = if line.starts_with(&format!("GET {path} ")) {
            "200 OK"
        } else {
            "404 Not Found"
        };
        write!(socket, "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
        line
    });
    (format!("http://{address}/v1"), thread)
}

#[tokio::test]
async fn the_context_window_is_read_from_the_servers_model_list() {
    let models = json!({"object":"list","data":[
        {"id":"gemma-4-26B-A4B-it-Q4_K_M","aliases":[],"object":"model",
            "meta":{"n_ctx":32768,"n_ctx_train":262144}},
        {"id":"qwen","object":"model","status":{"value":"unloaded"}},
    ]});
    let (base, server) = serve_get("/v1/models", models.to_string());
    let window = model(&base)
        .context_window("gemma-4-26B-A4B-it-Q4_K_M")
        .await;
    assert_eq!(window.unwrap(), Some(32_768));
    assert_eq!(server.join().unwrap(), "GET /v1/models HTTP/1.1");
    // A model that is listed but not loaded has no context yet.
    let (base, server) = serve_get("/v1/models", models.to_string());
    assert_eq!(model(&base).context_window("qwen").await.unwrap(), None);
    server.join().unwrap();
    // A server that does not answer with a model list is an error, without its URL.
    let (base, server) = serve_get("/v1/models", "not json".into());
    let error = model(&base).context_window("qwen").await.unwrap_err();
    assert_eq!(
        error.to_string(),
        "provider: llama-server: malformed JSON response"
    );
    server.join().unwrap();
}

#[test]
fn the_binding_tells_llama_server_apart_from_openai_at_the_same_endpoint() {
    let base = "http://127.0.0.1:8080/v1";
    let llama = model(base).binding();
    let openai = OpenAiModel::with_client(client(), "key", base)
        .unwrap()
        .binding();
    assert_eq!(llama["endpoint"], openai["endpoint"]);
    assert_eq!(llama["api"], openai["api"]);
    assert_ne!(llama, openai);
    assert_eq!(llama["provider"], "llama.cpp");
}

/// Against a live server: `LLAMA_SERVER_URL=http://127.0.0.1:8080/v1 cargo test --test llama
/// -- --ignored`, with `LLAMA_MODEL` naming the model for a router-mode server. Checks a plain
/// chat, a typed answer and a tool round, not the quality of the answers.
#[tokio::test]
#[ignore = "needs a running llama-server; set LLAMA_SERVER_URL"]
async fn live_server_chats_answers_typed_and_calls_a_tool() {
    let base = std::env::var("LLAMA_SERVER_URL").expect("LLAMA_SERVER_URL");
    let name = std::env::var("LLAMA_MODEL").unwrap_or_else(|_| "default".into());
    let live = |instructions: &str, tools: Vec<Arc<dyn Tool>>| {
        Agent::builder("live", Arc::new(LlamaModel::new(&base).unwrap()))
            .instructions(instructions)
            .config(ModelConfig::new(name.clone()))
            .tools(tools)
            .limits(
                agent_rs::Limits::default()
                    .with_max_tokens(20_000)
                    .with_operation_timeout(std::time::Duration::from_secs(300)),
            )
    };

    let chat: Agent<String, String> = live("Answer in one short sentence.", vec![])
        .input::<String>()
        .output::<String>()
        .build()
        .unwrap();
    let (state, reasoning, text) = drive(&chat, "What is the capital of France?").await;
    let Phase::Settled(Settlement::Completed(answer)) = &state.phase else {
        panic!("{:?}", state.phase)
    };
    println!("chat: {answer} (reasoning {} bytes)", reasoning.len());
    assert_eq!(answer.as_str(), Some(text.as_str()));
    assert!(text.contains("Paris"), "{text}");
    assert!(state.usage.output_tokens > 0);

    let typed: Agent<String, u64> = live("Answer with the number only.", vec![])
        .input::<String>()
        .output::<u64>()
        .build()
        .unwrap();
    let (state, _, text) = drive(&typed, "What is 6 times 7?").await;
    println!("typed: {text}");
    assert_eq!(
        state.phase,
        Phase::Settled(Settlement::Completed(json!(42)))
    );

    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let lookup = typed_tool::<Value, String, _, _>(
        ToolSpec::new("lookup", "1")
            .with_description("Look up the number stored under a key")
            .with_input_schema(
                json!({"type":"object","properties":{"key":{"type":"string"}},"required":["key"]}),
            )
            .with_retry(RetryPolicy::ReadOnly),
        move |_, _| {
            counted.fetch_add(1, Ordering::SeqCst);
            async { Ok(ToolOutput::Complete("seventeen".to_owned())) }
        },
    );
    let tooled: Agent<String, String> = live("Use the lookup tool to answer.", vec![lookup])
        .input::<String>()
        .output::<String>()
        .build()
        .unwrap();
    let (state, _, text) = drive(&tooled, "What number is stored under the key alpha?").await;
    println!("tool: {text}");
    assert!(
        matches!(state.phase, Phase::Settled(Settlement::Completed(_))),
        "{:?}",
        state.phase
    );
    assert!(calls.load(Ordering::SeqCst) >= 1);
    assert!(
        text.contains("17") || text.to_lowercase().contains("seventeen"),
        "{text}"
    );
}
