//! The OpenAI adapters' own CPU cost: encoding a request body and decoding a streamed answer,
//! for Chat Completions and Responses, through the hidden `openai::cost_probe`. No network, so
//! HTTP and connection setup (which dominate a loopback round trip) are excluded; the inputs are
//! the contract tests' wire fixtures. Every iteration checks its result. Live latency is out of
//! scope.
#[path = "../tests/support/openai_wire.rs"]
mod openai_wire;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use openai_wire::Api as Wire;
use serde_json::json;
use std::{hint::black_box, sync::Arc, time::Duration};
use yielded_agent::{
    HistoryEntry, ModelConfig, ModelEvent, ModelRequest, ModelResponse, RunId, ToolCall,
    ToolResult, ToolSpec,
    openai::cost_probe::{self, Api},
};

const APIS: [(&str, Api, Wire); 2] = [
    ("chat", Api::ChatCompletions, Wire::Chat),
    ("responses", Api::Responses, Wire::Responses),
];
const ANSWER_BYTES: usize = 1024;

fn configured() -> Criterion {
    let quick = std::env::var_os("AGENT_BENCH_QUICK").is_some();
    Criterion::default()
        .sample_size(if quick { 10 } else { 30 })
        .warm_up_time(Duration::from_millis(if quick { 100 } else { 1_000 }))
        .measurement_time(Duration::from_millis(if quick { 250 } else { 2_000 }))
}

/// A 1,024-byte answer streamed in 1, 64 and 1,024 deltas, one SSE event per network read.
fn decode(c: &mut Criterion) {
    let text = format!("{{\"value\":\"{}\"}}", "x".repeat(ANSWER_BYTES - 12));
    let mut group = c.benchmark_group("adapter_decode");
    group.throughput(Throughput::Bytes(ANSWER_BYTES as u64));
    for (name, api, wire) in APIS {
        for deltas in [1, 64, 1024] {
            let chunks = wire.streamed_text(&text, ANSWER_BYTES / deltas);
            let chunks: Vec<&[u8]> = chunks.iter().map(Vec::as_slice).collect();
            group.bench_with_input(BenchmarkId::new(name, deltas), &chunks, |b, chunks| {
                b.iter(|| {
                    let (mut seen, mut output) = (0, None);
                    cost_probe::decode(api, 4096, black_box(chunks), |event| match event {
                        ModelEvent::TextDelta(_) => seen += 1,
                        ModelEvent::Response(response) => output = response.output,
                        _ => {}
                    })
                    .unwrap();
                    assert_eq!(seen, deltas);
                    assert_eq!(output.unwrap().as_str().unwrap().len(), ANSWER_BYTES - 12);
                })
            });
        }
    }
    group.finish();
}

/// A turn with four tools and one completed tool exchange.
fn request() -> ModelRequest {
    let mut request = ModelRequest::new(
        RunId::nil(),
        vec![
            HistoryEntry::Input(json!("Look up the key, then answer.")),
            HistoryEntry::Exchange {
                response: ModelResponse::tools(vec![ToolCall {
                    id: "call-0".into(),
                    name: "search".into(),
                    arguments: json!({"key": "a key", "limit": 3}),
                }]),
                results: vec![ToolResult {
                    call_id: "call-0".into(),
                    value: json!("a key:3"),
                    is_error: false,
                }],
            },
        ],
    );
    request.instructions = "Answer with the result.".into();
    request.config = Arc::new(ModelConfig::new("bench-model"));
    request.tools = ["echo", "search", "fetch", "summarize"]
        .map(|name| {
            ToolSpec::new(name, "1")
                .with_description(format!("Deterministic {name} tool."))
                .with_input_schema(json!({"type":"object","properties":{"key":{"type":"string"}}}))
        })
        .into();
    request.output_schema = Arc::new(json!({"type":"object","properties":{
        "text":{"type":"string"},"confidence":{"type":"integer"}},"required":["text"]}));
    request
}

fn encode(c: &mut Criterion) {
    let request = request();
    let mut group = c.benchmark_group("adapter_request_body");
    group.throughput(Throughput::Elements(1));
    for (name, api, _) in APIS {
        group.bench_function(name, |b| {
            b.iter_batched(
                || request.clone(),
                |request| {
                    let body = cost_probe::request_body(api, black_box(request)).unwrap();
                    assert!(body.len() > 1_000);
                    body
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

criterion_group! { name = benches; config = configured(); targets = decode, encode }
criterion_main!(benches);
