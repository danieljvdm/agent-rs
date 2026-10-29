#![cfg(feature = "openai")]
use futures::StreamExt;
use serde_json::{Value, json};
use std::{
    io::{Read, Write},
    net::TcpListener,
    thread,
};
use tokio_util::sync::CancellationToken;
use yielded_agent::{Model, ModelConfig, ModelEvent, ModelRequest, openai::OpenAiModel};

fn request() -> ModelRequest {
    ModelRequest {
        run_id: uuid::Uuid::nil(),
        instructions: "Return the number 42".into(),
        config: ModelConfig {
            model: "test-model".into(),
            temperature: None,
        },
        history: vec![],
        tools: vec![],
        output_schema: json!({"type":"integer"}),
        turn: 1,
        max_response_bytes: 1024,
        remaining_tokens: 256,
    }
}
fn serve(body: String) -> (String, thread::JoinHandle<Value>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let thread = thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        let mut bytes = Vec::new();
        let (header_end, length) = loop {
            let mut buffer = [0; 4096];
            let n = socket.read(&mut buffer).unwrap();
            assert!(n > 0);
            bytes.extend_from_slice(&buffer[..n]);
            if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&bytes[..end]);
                assert!(headers.starts_with("POST /v1/chat/completions HTTP/1.1"));
                let length: usize = headers
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .map(|n| n.trim().parse().unwrap())
                    })
                    .unwrap();
                break (end + 4, length);
            }
        };
        while bytes.len() < header_end + length {
            let mut buffer = [0; 4096];
            let n = socket.read(&mut buffer).unwrap();
            assert!(n > 0);
            bytes.extend_from_slice(&buffer[..n]);
        }
        let request = serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap();
        write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).unwrap();
        // Split at every wire byte; the TCP stack may coalesce independently.
        for byte in body.as_bytes() {
            socket.write_all(&[*byte]).unwrap();
        }
        request
    });
    (format!("http://{address}/v1"), thread)
}
fn source() -> String {
    "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"{\\\"value\\\":42}\"},\"finish_reason\":null}]}\n\n\
     data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n\
     data: {\"choices\":[],\"usage\":{\"prompt_tokens\":9,\"completion_tokens\":4}}\n\n\
     data: [DONE]\n\n".into()
}
#[tokio::test]
async fn sends_request_and_completes_through_real_http_transport() {
    let (base, server) = serve(source());
    let model = OpenAiModel::with_client(
        reqwest::Client::builder().no_proxy().build().unwrap(),
        "local-test-key",
        base,
    )
    .unwrap();
    let mut stream = model
        .stream(request(), CancellationToken::new())
        .await
        .unwrap();
    let mut responses = Vec::new();
    while let Some(event) = stream.next().await {
        if let ModelEvent::Response(response) = event.unwrap() {
            responses.push(response);
        }
    }
    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0].output, Some(json!(42)));
    assert_eq!(responses[0].usage.input, 9);
    let request = server.join().unwrap();
    assert_eq!(request["model"], "test-model");
    assert_eq!(request["max_completion_tokens"], 256);
    assert_eq!(request["stream_options"]["include_usage"], true);
    assert_eq!(request["response_format"]["type"], "json_schema");
}
#[tokio::test]
async fn truncated_http_body_cannot_publish_a_response() {
    let (base, server) = serve(source().replace("data: [DONE]\n\n", ""));
    let model = OpenAiModel::with_client(
        reqwest::Client::builder().no_proxy().build().unwrap(),
        "local-test-key",
        base,
    )
    .unwrap();
    let mut stream = model
        .stream(request(), CancellationToken::new())
        .await
        .unwrap();
    let mut saw_error = false;
    while let Some(event) = stream.next().await {
        match event {
            Ok(ModelEvent::Response(_)) => panic!("truncated output completed"),
            Err(_) => {
                saw_error = true;
                break;
            }
            _ => {}
        }
    }
    assert!(saw_error);
    server.join().unwrap();
}
#[tokio::test]
async fn pre_cancelled_request_never_connects() {
    let cancel = CancellationToken::new();
    cancel.cancel();
    let model = OpenAiModel::with_client(
        reqwest::Client::builder().no_proxy().build().unwrap(),
        "local-test-key",
        "http://127.0.0.1:1/v1",
    )
    .unwrap();
    assert!(matches!(
        model.stream(request(), cancel).await,
        Err(yielded_agent::Error::Interrupted)
    ));
}

#[tokio::test]
async fn cancellation_discards_buffered_completed_output() {
    let (base, server) = serve(source());
    let model = OpenAiModel::with_client(
        reqwest::Client::builder().no_proxy().build().unwrap(),
        "local-test-key",
        base,
    )
    .unwrap();
    let cancel = CancellationToken::new();
    let mut stream = model.stream(request(), cancel.clone()).await.unwrap();
    assert!(matches!(
        stream.next().await.unwrap().unwrap(),
        ModelEvent::TextDelta(_)
    ));
    cancel.cancel();
    assert!(matches!(
        stream.next().await,
        Some(Err(yielded_agent::Error::Interrupted))
    ));
    server.join().unwrap();
}
