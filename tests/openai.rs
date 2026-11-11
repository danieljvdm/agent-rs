//! The contract both OpenAI adapters share, over real HTTP to a loopback server. Each test runs
//! once per API with the same fixtures.
#![cfg(feature = "openai")]
#[path = "support/openai_http.rs"]
mod openai_http;
#[path = "support/openai_wire.rs"]
mod openai_wire;
use futures::StreamExt;
use openai_http::{serve, serve_status};
use openai_wire::Api;
use serde_json::{Value, json};
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
};
use tokio_util::sync::CancellationToken;
use yielded_agent::{
    Agent, Error, HistoryEntry, Limits, MemoryCheckpoint, Model, ModelConfig, ModelEvent,
    ModelRequest, Phase, RetryPolicy, RunControl, Settlement, TokenUsage, Tool, ToolOutput,
    ToolSpec,
    openai::{OpenAiModel, OpenAiResponsesModel},
    typed_tool,
};

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap()
}
fn model(api: Api, base: &str) -> Arc<dyn Model> {
    match api {
        Api::Chat => Arc::new(OpenAiModel::with_client(client(), "local-test-key", base).unwrap()),
        Api::Responses => {
            Arc::new(OpenAiResponsesModel::with_client(client(), "local-test-key", base).unwrap())
        }
    }
}
fn other(api: Api) -> Api {
    match api {
        Api::Chat => Api::Responses,
        Api::Responses => Api::Chat,
    }
}
fn config() -> ModelConfig {
    ModelConfig::new("exact-test-model")
}
fn request() -> ModelRequest {
    let mut request =
        ModelRequest::new(uuid::Uuid::nil(), vec![HistoryEntry::Input(json!("hello"))]);
    request.instructions = "Return the number 42".into();
    request.config = Arc::new(config());
    request.output_schema =
        Arc::new(json!({"$defs":{"Answer":{"type":"integer"}},"$ref":"#/$defs/Answer"}));
    request.max_response_bytes = 4096;
    request.remaining_tokens = 256;
    request
}
fn echo(calls: Arc<AtomicUsize>) -> Arc<dyn Tool> {
    let spec = ToolSpec::new("echo", "1")
        .with_description("Echo an integer")
        .with_retry(RetryPolicy::ReadOnly);
    typed_tool::<u64, u64, _, _>(spec, move |value, _| {
        calls.fetch_add(1, Ordering::SeqCst);
        async move { Ok(ToolOutput::Complete(value)) }
    })
}
fn agent(api: Api, base: &str, tools: Vec<Arc<dyn Tool>>, limits: Limits) -> Agent<String, u64> {
    Agent::builder("contract", model(api, base))
        .instructions("Echo")
        .config(config())
        .tools(tools)
        .limits(limits)
        .input::<String>()
        .output::<u64>()
        .build()
        .unwrap()
}
async fn drive(agent: &Agent<String, u64>) -> yielded_agent::RunState {
    let mut state = agent.new_run("c", json!("hello"), &[], None).unwrap();
    agent
        .drive(&mut state, &MemoryCheckpoint, RunControl::default())
        .await
        .unwrap();
    state
}

#[tokio::test]
async fn adapters_send_exact_model_and_typed_schema_and_report_usage_once() {
    for api in Api::ALL {
        let (base, server) = serve(api, vec![api.text("{\"value\":42}")]);
        let mut stream = model(api, &base)
            .stream(request(), CancellationToken::new())
            .await
            .unwrap();
        let mut responses = vec![];
        let mut usage = vec![];
        while let Some(event) = stream.next().await {
            match event.unwrap() {
                ModelEvent::Response(response) => responses.push(response),
                ModelEvent::Usage(snapshot) => usage.push(snapshot),
                _ => {}
            }
        }
        assert_eq!(responses.len(), 1, "{api:?}");
        assert_eq!(responses[0].output, Some(json!(42)));
        assert_eq!(usage, [TokenUsage::new(9, 4)]);
        assert_eq!(responses[0].usage, usage[0]);
        let body = server.join().unwrap().remove(0);
        assert_eq!(body["model"], "exact-test-model");
        assert_eq!(body["store"], false);
        assert_eq!(body["stream"], true);
        let schema = match api {
            Api::Chat => {
                assert_eq!(body["max_completion_tokens"], 256);
                assert_eq!(body["stream_options"]["include_usage"], true);
                &body["response_format"]["json_schema"]["schema"]
            }
            Api::Responses => {
                assert_eq!(body["max_output_tokens"], 256);
                assert_eq!(body["truncation"], "disabled");
                assert_eq!(body["background"], false);
                assert_eq!(body["instructions"], "Return the number 42");
                assert_eq!(body["input"][0]["role"], "user");
                assert_eq!(body["text"]["format"]["type"], "json_schema");
                &body["text"]["format"]["schema"]
            }
        };
        assert_eq!(
            schema["properties"]["value"]["$ref"],
            "#/properties/value/$defs/Answer"
        );
    }
}

#[tokio::test]
async fn adapters_execute_tools_then_replay_results_with_cumulative_budget() {
    for api in Api::ALL {
        let calls = Arc::new(AtomicUsize::new(0));
        let (base, server) = serve(
            api,
            vec![
                api.encode(&api.frames(
                    Some("Checking."),
                    &[("call-a", "echo", "42"), ("call-b", "echo", "7")],
                )),
                api.text("{\"value\":42}"),
            ],
        );
        let agent = agent(
            api,
            &base,
            vec![echo(calls.clone())],
            Limits::default().with_max_tokens(100),
        );
        let state = drive(&agent).await;
        assert_eq!(
            state.phase,
            Phase::Settled(Settlement::Completed(json!(42))),
            "{api:?}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(state.usage.model_calls, 2);
        // Each turn reports 9 + 4 once, though it also repeats them in its response.
        assert_eq!(state.usage.input_tokens, 18);
        assert_eq!(state.usage.output_tokens, 8);
        let requests = server.join().unwrap();
        match api {
            Api::Chat => {
                assert_eq!(requests[1]["max_completion_tokens"], 87);
                assert_eq!(requests[1]["messages"][2]["tool_calls"][0]["id"], "call-a");
                assert_eq!(requests[1]["messages"][3]["tool_call_id"], "call-a");
                assert_eq!(requests[1]["messages"][3]["content"], "42");
            }
            Api::Responses => {
                assert_eq!(requests[1]["max_output_tokens"], 87);
                assert_eq!(requests[0]["tools"][0]["type"], "function");
                assert_eq!(requests[0]["tools"][0]["name"], "echo");
                assert_eq!(requests[1]["input"][1]["call_id"], "call-a");
                assert_eq!(requests[1]["input"][2]["call_id"], "call-b");
                assert_eq!(requests[1]["input"][3]["type"], "function_call_output");
                assert_eq!(requests[1]["input"][3]["call_id"], "call-a");
                assert_eq!(requests[1]["input"][3]["output"], "42");
            }
        }
    }
}

#[tokio::test]
async fn adapters_never_dispatch_invalid_or_truncated_tool_batches() {
    for api in Api::ALL {
        let complete = api.encode(&api.frames(None, &[("a", "echo", "42"), ("b", "echo", "{")]));
        let valid = api.encode(&api.frames(None, &[("a", "echo", "42")]));
        for body in [
            complete,
            api.truncate(&valid),
            valid.replace("\"echo\"", "\"unknown\""),
        ] {
            let calls = Arc::new(AtomicUsize::new(0));
            let (base, server) = serve(api, vec![body]);
            let agent = agent(api, &base, vec![echo(calls.clone())], Limits::default());
            let state = drive(&agent).await;
            assert!(
                matches!(state.phase, Phase::Settled(Settlement::Failed(_))),
                "{api:?}: {:?}",
                state.phase
            );
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            assert_eq!(state.usage.model_calls, 1);
            server.join().unwrap();
        }
    }
}

#[tokio::test]
async fn adapters_reject_bad_output_and_preserve_available_usage() {
    for api in Api::ALL {
        for output in ["{\"value\":\"wrong type\"}", "{\"other\":42}", "{"] {
            let (base, server) = serve(api, vec![api.text(output)]);
            let state = drive(&agent(api, &base, vec![], Limits::default())).await;
            assert!(
                matches!(state.phase, Phase::Settled(Settlement::Failed(_))),
                "{api:?}"
            );
            assert_eq!(
                (state.usage.input_tokens, state.usage.output_tokens),
                (9, 4),
                "{api:?}"
            );
            server.join().unwrap();
        }
    }
}

#[tokio::test]
async fn adapters_bound_payloads_and_reject_malformed_streams() {
    for api in Api::ALL {
        let mut no_usage = api.frames(Some("{\"value\":42}"), &[]);
        match api {
            Api::Chat => {
                no_usage.pop();
            }
            Api::Responses => {
                no_usage.last_mut().unwrap()["response"]["usage"] = Value::Null;
            }
        }
        let bodies = [
            "data: invalid JSON\n\n".into(),
            api.encode(&no_usage),
            api.text(&format!("{{\"value\":\"{}\"}}", "x".repeat(5000))),
            "x".repeat(50_000),
            "\n".repeat(220_000),
        ];
        for body in bodies {
            let (base, server) = serve(api, vec![body]);
            let mut stream = model(api, &base)
                .stream(request(), CancellationToken::new())
                .await
                .unwrap();
            let mut error = false;
            while let Some(event) = stream.next().await {
                match event {
                    Ok(ModelEvent::Response(_)) => {
                        panic!("{api:?} accepted malformed/unbounded data")
                    }
                    Err(_) => {
                        error = true;
                        break;
                    }
                    _ => {}
                }
            }
            assert!(error);
            server.join().unwrap();
        }
    }
}

#[tokio::test]
async fn adapters_do_not_retry_http_errors_or_expose_server_bodies() {
    for api in Api::ALL {
        for status in [
            "400 Bad Request",
            "401 Unauthorized",
            "429 Too Many Requests",
            "503 Service Unavailable",
        ] {
            let (base, server) = serve_status(
                api,
                vec!["sensitive-provider-body".into()],
                status,
                "application/json",
            );
            let error = match model(api, &base)
                .stream(request(), CancellationToken::new())
                .await
            {
                Err(error) => error.to_string(),
                Ok(_) => panic!("accepted {status}"),
            };
            assert_eq!(
                error,
                format!("provider: OpenAI: HTTP status {}", &status[..3])
            );
            assert!(!error.contains("sensitive-provider-body"));
            assert!(!error.contains("local-test-key"));
            assert!(!error.contains(&base));
            assert_eq!(server.join().unwrap().len(), 1);
        }
        let (base, server) = serve_status(api, vec!["{}".into()], "200 OK", "application/json");
        assert!(
            model(api, &base)
                .stream(request(), CancellationToken::new())
                .await
                .is_err()
        );
        server.join().unwrap();
    }
}

#[tokio::test]
async fn adapters_pre_cancel_without_connecting_and_drop_active_connections() {
    for api in Api::ALL {
        let cancel = CancellationToken::new();
        cancel.cancel();
        assert!(matches!(
            model(api, "http://127.0.0.1:1/v1")
                .stream(request(), cancel)
                .await,
            Err(Error::Interrupted)
        ));

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}/v1", listener.local_addr().unwrap());
        let server = thread::spawn(move || {
            let mut socket = openai_http::accept(&listener);
            openai_http::request(&mut socket, api);
            let body = api.truncate(&api.text("{\"value\":42}"));
            write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n{:x}\r\n{}\r\n", body.len(), body).unwrap();
            let mut byte = [0];
            assert_eq!(
                socket.read(&mut byte).unwrap(),
                0,
                "cancelled stream retained its socket"
            );
        });
        let cancel = CancellationToken::new();
        let model = model(api, &base);
        let mut stream = model.stream(request(), cancel.clone()).await.unwrap();
        assert!(matches!(
            stream.next().await,
            Some(Ok(ModelEvent::TextDelta(_)))
        ));
        cancel.cancel();
        assert!(matches!(stream.next().await, Some(Err(Error::Interrupted))));
        drop(stream);
        model.cleanup(uuid::Uuid::nil()).await.unwrap();
        tokio::task::spawn_blocking(move || server.join().unwrap())
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn cancellation_discards_buffered_completed_output() {
    for api in Api::ALL {
        let (base, server) = serve(api, vec![api.text("{\"value\":42}")]);
        let cancel = CancellationToken::new();
        let mut stream = model(api, &base)
            .stream(request(), cancel.clone())
            .await
            .unwrap();
        assert!(matches!(
            stream.next().await,
            Some(Ok(ModelEvent::TextDelta(_)))
        ));
        cancel.cancel();
        assert!(matches!(stream.next().await, Some(Err(Error::Interrupted))));
        server.join().unwrap();
    }
}

/// No socket: bindings and URL checks are local.
#[test]
fn bindings_pin_api_and_endpoint_but_allow_key_rotation() {
    let base = "http://127.0.0.1:1/v1";
    for api in Api::ALL {
        let original = model(api, base);
        let rotated: Arc<dyn Model> = match api {
            Api::Chat => Arc::new(OpenAiModel::with_client(client(), "rotated-key", base).unwrap()),
            Api::Responses => {
                Arc::new(OpenAiResponsesModel::with_client(client(), "rotated-key", base).unwrap())
            }
        };
        assert_eq!(original.binding(), rotated.binding());
        assert_ne!(
            original.binding(),
            model(api, "http://127.0.0.1:2/v1").binding()
        );
        assert_ne!(original.binding(), model(other(api), base).binding());
        assert!(!original.binding().to_string().contains("key"));
        // The identity is part of the agent's durable binding.
        let agent = agent(api, base, vec![], Limits::default());
        assert_eq!(agent.binding()["model_adapter"], original.binding());
    }
    assert_eq!(
        model(Api::Responses, "https://api.example.test/v1/").binding(),
        json!({"provider":"openai","api":"responses","version":1,
            "endpoint":"https://api.example.test/v1/responses"})
    );
    assert_eq!(
        model(Api::Chat, "https://api.example.test/v1").binding()["api"],
        "chat_completions"
    );
    for base in [
        "http://example.com/v1",
        "https://user:pass@example.com/v1",
        "https://example.com/v1?key=x",
        "https://example.com/v1#key",
    ] {
        assert!(matches!(
            OpenAiModel::with_client(client(), "key", base),
            Err(Error::Config(_))
        ));
        assert!(matches!(
            OpenAiResponsesModel::with_client(client(), "key", base),
            Err(Error::Config(_))
        ));
    }
}

/// A run accepted through one API is not driven through the other, even under the same agent ID
/// and model name, until the original route is restored and the run explicitly retried.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn durable_recovery_rejects_a_changed_api_until_the_original_route_is_restored() {
    use std::time::Duration;
    use yielded_agent::{DurableStore, Host, HostOptions, sqlite::SqliteStore};
    for api in Api::ALL {
        let (base, server) = serve(api, vec![api.text("{\"value\":42}")]);
        let original = agent(api, &base, vec![], Limits::default());
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("route.sqlite");
        let store = SqliteStore::open(&path).await.unwrap();
        let id = store
            .accept(
                &original.erased(),
                "conversation",
                "key",
                json!("hello"),
                None,
            )
            .await
            .unwrap();
        store.close().await.unwrap();
        let store = Arc::new(SqliteStore::open(&path).await.unwrap());
        let options = || {
            HostOptions::default()
                .with_max_concurrent_runs(1)
                .with_poll_interval(Duration::from_millis(2))
        };
        let changed = agent(other(api), &base, vec![], Limits::default());
        let mut host = Host::start(store.clone(), changed, options()).unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while store.last_error(id).await.unwrap().is_none() {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
        assert!(
            store
                .last_error(id)
                .await
                .unwrap()
                .unwrap()
                .contains("incompatible")
        );
        let rejected = store.status(id).await.unwrap();
        assert!(!rejected.is_terminal());
        assert_eq!(rejected.usage.model_calls, 0);
        host.shutdown().await.unwrap();
        let mut host = Host::start(store.clone(), original, options()).unwrap();
        // Let the initial pass exclude the run with a retained binding error.
        tokio::time::sleep(Duration::from_millis(10)).await;
        host.retry(id).await.unwrap();
        let completed = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let state = host.status(id).await.unwrap();
                if state.is_terminal() {
                    break state;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            completed.phase,
            Phase::Settled(Settlement::Completed(json!(42)))
        );
        assert_eq!(completed.usage.model_calls, 1);
        assert_eq!(
            (completed.usage.input_tokens, completed.usage.output_tokens),
            (9, 4)
        );
        host.shutdown().await.unwrap();
        store.close().await.unwrap();
        assert_eq!(server.join().unwrap().len(), 1);
    }
}
