//! What a long conversation costs a local model, measured on the bytes `LlamaModel` would send.
//!
//! llama-server reuses its cache for the longest prefix a request shares with the one before it
//! and evaluates the rest again, so the cost of a turn is the part of its prompt that is not a
//! prefix of the previous prompt. These guards hold that cost for one scripted conversation, with
//! exact byte counts: the scripted model does no inference, and nothing here is a timing.
#![cfg(feature = "openai")]

use agent_rs::openai::cost_probe::{self, Api};
use agent_rs::*;
use async_trait::async_trait;
use futures::stream;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

/// Runs of the conversation: each is a question, one tool round and an answer.
const RUNS: usize = 24;
/// Bytes of text the `read` tool returns.
const TOOL_RESULT_BYTES: usize = 1_200;
/// The byte limit of the conversation without a context window: 4,096 tokens at four bytes each.
const MAX_CONTEXT_BYTES: usize = 16_384;

// Without a context window the engine drops the oldest entries that do not fit
// `max_context_bytes`. Once the conversation is over the limit, each new run starts later in the
// conversation than the one before it, so its first prompt is evaluated from its first message:
// 15 of the 24 runs, and two fifths of every byte sent.
const SLIDING_REQUESTS: usize = 48;
const SLIDING_SENT_BYTES: usize = 598_867;
const SLIDING_COLD_BYTES: usize = 244_387;
const SLIDING_LARGEST_REQUEST_BYTES: usize = 15_582;
/// Requests whose prompt does not extend the previous one.
const SLIDING_COLD_REQUESTS: usize = 15;

/// Answers a question by reading once, and records the messages of every request as
/// `LlamaModel` would send them.
#[derive(Default)]
struct Reader {
    messages: Mutex<Vec<Vec<u8>>>,
}
#[async_trait]
impl Model for Reader {
    async fn stream(&self, request: ModelRequest, _: CancellationToken) -> Result<ModelStream> {
        let body = cost_probe::request_body(Api::Llama, request.clone())?;
        let body: Value = serde_json::from_slice(&body).expect("a JSON body");
        let mut messages = self.messages.lock().unwrap();
        messages.push(serde_json::to_vec(&body["messages"]).unwrap());
        let response = match request.history.last() {
            Some(HistoryEntry::Exchange { .. }) => {
                ModelResponse::final_output(json!(format!("Answer: {}", "a".repeat(180))))
            }
            _ => ModelResponse::tools(vec![ToolCall {
                id: format!("read-{}", messages.len()),
                name: "read".into(),
                arguments: json!(TOOL_RESULT_BYTES),
            }]),
        };
        Ok(Box::pin(stream::iter([Ok(ModelEvent::Response(response))])))
    }
}

/// What the conversation cost.
#[derive(Debug, PartialEq, Eq)]
struct Cost {
    requests: usize,
    /// Bytes of messages sent, over every request.
    sent_bytes: usize,
    /// Bytes of messages after the prefix each request shares with the one before it.
    cold_bytes: usize,
    largest_request_bytes: usize,
    /// Requests that changed or dropped something the previous request sent.
    cold_requests: usize,
}

fn cost(messages: &[Vec<u8>]) -> Cost {
    let mut cost = Cost {
        requests: messages.len(),
        sent_bytes: 0,
        cold_bytes: 0,
        largest_request_bytes: 0,
        cold_requests: 0,
    };
    let mut previous: &[u8] = b"[]";
    for request in messages {
        // The array's closing bracket is not part of what the next request extends.
        let extendable = previous.len() - 1;
        let shared = (previous.iter().zip(request))
            .take_while(|(a, b)| a == b)
            .count();
        cost.sent_bytes += request.len();
        cost.cold_bytes += request.len() - shared;
        cost.largest_request_bytes = cost.largest_request_bytes.max(request.len());
        cost.cold_requests += usize::from(shared < extendable);
        previous = request;
    }
    cost
}

fn builder(model: Arc<dyn Model>) -> AgentBuilder<String, String> {
    let read = typed_tool::<usize, String, _, _>(
        ToolSpec::new("read", "1")
            .with_description("Read the next page.")
            .with_retry(RetryPolicy::ReadOnly),
        |bytes, _| async move { Ok(ToolOutput::Complete("r".repeat(bytes))) },
    );
    Agent::builder("reader", model)
        .instructions("Answer from what you read.")
        .tool(read)
        .input::<String>()
        .output::<String>()
}

/// Holds the conversation's `RUNS` runs with the agent `configure` finishes.
async fn converse(
    configure: impl FnOnce(AgentBuilder<String, String>) -> AgentBuilder<String, String>,
) -> (Cost, Conversation) {
    let model = Arc::new(Reader::default());
    let agent = configure(builder(model.clone())).build().unwrap();
    let mut conversation = Conversation::default();
    for run in 0..RUNS {
        let question = format!("Question {run}: what does page {run} say about the schedule?");
        let answer = agent.run(&mut conversation, question).await.unwrap();
        assert!(answer.starts_with("Answer: "));
    }
    let cost = cost(&model.messages.lock().unwrap());
    (cost, conversation)
}

#[tokio::test]
async fn a_byte_limit_alone_starts_every_later_run_cold() {
    let (cost, _) = converse(|agent| {
        agent.limits(Limits::default().with_max_context_bytes(MAX_CONTEXT_BYTES))
    })
    .await;
    assert_eq!(
        cost,
        Cost {
            requests: SLIDING_REQUESTS,
            sent_bytes: SLIDING_SENT_BYTES,
            cold_bytes: SLIDING_COLD_BYTES,
            largest_request_bytes: SLIDING_LARGEST_REQUEST_BYTES,
            cold_requests: SLIDING_COLD_REQUESTS,
        }
    );
}
