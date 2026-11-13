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
/// The byte limit of the conversation without a context window.
const MAX_CONTEXT_BYTES: usize = 16_384;
/// Bytes of context the engine takes for a token until a provider reports usage.
const BYTES_PER_TOKEN: usize = 3;

// Without a context window the engine drops the oldest entries that do not fit
// `max_context_bytes`. Once the conversation is over the limit, each new run starts later in the
// conversation than the one before it, so its first prompt is evaluated from its first message:
// 15 of the 24 runs, and two fifths of every byte sent.
const SLIDING_REQUESTS: usize = 48;
const SLIDING_SENT_BYTES: usize = 598_867;
const SLIDING_COLD_BYTES: usize = 244_387;
const SLIDING_LARGEST_REQUEST_BYTES: usize = 15_582;
/// Requests that reuse less than a quarter of the prompt before them.
const SLIDING_COLD_REQUESTS: usize = 15;

/// The same conversation's context window, in tokens: the byte limit above at three bytes each.
const WINDOW_TOKENS: u64 = (MAX_CONTEXT_BYTES / BYTES_PER_TOKEN) as u64;
// With a context window the conversation is compacted when a prompt would leave less than the
// window's reserve. Every prompt extends the one before it except the first after a compaction,
// which starts from the summary: three summary requests buy an 81% cut in bytes evaluated again.
// (Keeping 30% of the window through a compaction instead of 15% made it five, and 62,791 bytes.)
const COMPACTIONS: usize = 3;
const COMPACTED_REQUESTS: usize = 51;
const COMPACTED_SENT_BYTES: usize = 338_053;
const COMPACTED_COLD_BYTES: usize = 45_854;
const COMPACTED_LARGEST_REQUEST_BYTES: usize = 11_675;

/// The summary the scripted model writes when it is asked for one.
const SUMMARY: &str = "## Goal\nRead the schedule.\n\n## Requirements\n(none)\n\n## Progress\nPages were read and answered.\n\n## Open items\n(none)\n\n## Key details\nThe schedule is on the pages.";

/// What the scripted model answers to its `n`th request, counted from one.
type Respond = dyn Fn(&ModelRequest, usize) -> Result<ModelResponse> + Send + Sync;

/// Plays `respond`, streaming each text answer as one delta first, and records every request
/// with its messages as `LlamaModel` would send them.
struct Script {
    respond: Box<Respond>,
    requests: Mutex<Vec<ModelRequest>>,
    messages: Mutex<Vec<Vec<u8>>>,
}
impl Script {
    fn new(
        respond: impl Fn(&ModelRequest, usize) -> Result<ModelResponse> + Send + Sync + 'static,
    ) -> Arc<Self> {
        Arc::new(Self {
            respond: Box::new(respond),
            requests: Mutex::default(),
            messages: Mutex::default(),
        })
    }
    fn requests(&self) -> Vec<ModelRequest> {
        self.requests.lock().unwrap().clone()
    }
}
#[async_trait]
impl Model for Script {
    async fn stream(&self, request: ModelRequest, _: CancellationToken) -> Result<ModelStream> {
        let body = cost_probe::request_body(Api::Llama, request.clone())?;
        let body: Value = serde_json::from_slice(&body).expect("a JSON body");
        let messages = serde_json::to_vec(&body["messages"]).unwrap();
        self.messages.lock().unwrap().push(messages);
        let count = {
            let mut requests = self.requests.lock().unwrap();
            requests.push(request.clone());
            requests.len()
        };
        let response = (self.respond)(&request, count)?;
        let mut events = Vec::new();
        if let Some(Value::String(text)) = &response.output {
            events.push(Ok(ModelEvent::TextDelta(text.clone())));
        }
        events.push(Ok(ModelEvent::Response(response)));
        Ok(Box::pin(stream::iter(events)))
    }
}

/// Whether the request asks for a compaction's summary.
fn asks_for_summary(request: &ModelRequest) -> bool {
    !request.allow_tool_calls
}
fn read(n: usize) -> ModelResponse {
    ModelResponse::tools(vec![ToolCall {
        id: format!("read-{n}"),
        name: "read".into(),
        arguments: json!(TOOL_RESULT_BYTES),
    }])
}
fn answer() -> ModelResponse {
    ModelResponse::final_output(json!(format!("Answer: {}", "a".repeat(180))))
}
/// Summarizes when asked, and otherwise answers each question after reading once.
fn reader(request: &ModelRequest, n: usize) -> Result<ModelResponse> {
    Ok(match request.history.last() {
        _ if asks_for_summary(request) => ModelResponse::final_output(json!(SUMMARY)),
        Some(HistoryEntry::Exchange { .. }) => answer(),
        _ => read(n),
    })
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
    /// Requests that reuse less than a quarter of the prompt before them.
    cold_requests: usize,
}
/// The requests, counted from zero, that lose the prompt before them: less than a quarter of it
/// is still a prefix of theirs. A summary request is not one of them, since it is the start of
/// the prompt before it with one instruction added.
fn cold_requests(messages: &[Vec<u8>]) -> Vec<usize> {
    let shared = |pair: &[Vec<u8>]| {
        (pair[0].iter().zip(&pair[1]))
            .take_while(|(a, b)| a == b)
            .count()
    };
    (messages.windows(2).enumerate())
        .filter(|(_, pair)| shared(pair) * 4 < pair[0].len())
        .map(|(index, _)| index + 1)
        .collect()
}

fn cost(messages: &[Vec<u8>]) -> Cost {
    let mut cost = Cost {
        requests: messages.len(),
        sent_bytes: 0,
        cold_bytes: 0,
        largest_request_bytes: 0,
        cold_requests: cold_requests(messages).len(),
    };
    let mut previous: &[u8] = b"[]";
    for request in messages {
        let shared = (previous.iter().zip(request))
            .take_while(|(a, b)| a == b)
            .count();
        cost.sent_bytes += request.len();
        cost.cold_bytes += request.len() - shared;
        cost.largest_request_bytes = cost.largest_request_bytes.max(request.len());
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
) -> (Cost, Conversation, Arc<Script>) {
    let model = Script::new(reader);
    let agent = configure(builder(model.clone())).build().unwrap();
    let mut conversation = Conversation::default();
    for run in 0..RUNS {
        let question = format!("Question {run}: what does page {run} say about the schedule?");
        let answer = agent.run(&mut conversation, question).await.unwrap();
        assert!(answer.starts_with("Answer: "));
    }
    let cost = cost(&model.messages.lock().unwrap());
    (cost, conversation, model)
}
fn compactions(history: &[HistoryEntry]) -> usize {
    (history.iter())
        .filter(|entry| matches!(entry, HistoryEntry::Compaction { .. }))
        .count()
}

#[tokio::test]
async fn a_byte_limit_alone_starts_every_later_run_cold() {
    let (cost, conversation, _) =
        converse(|agent| agent.limits(Limits::default().with_max_context_bytes(MAX_CONTEXT_BYTES)))
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
    assert_eq!(compactions(&conversation.history), 0);
}

#[tokio::test]
async fn a_context_window_makes_only_the_prompt_after_a_compaction_cold() {
    let (cost, conversation, model) =
        converse(|agent| agent.context_window(ContextWindow::new(WINDOW_TOKENS))).await;
    assert_eq!(
        cost,
        Cost {
            requests: COMPACTED_REQUESTS,
            sent_bytes: COMPACTED_SENT_BYTES,
            cold_bytes: COMPACTED_COLD_BYTES,
            largest_request_bytes: COMPACTED_LARGEST_REQUEST_BYTES,
            cold_requests: COMPACTIONS,
        }
    );
    assert_eq!(compactions(&conversation.history), COMPACTIONS);
    assert_eq!(cost.requests, SLIDING_REQUESTS + COMPACTIONS);
    // Each summary request extends the prompt before it, so the provider's cache serves it, and
    // the only cold prompts are the ones that follow a summary.
    let requests = model.requests();
    let cold = cold_requests(&model.messages.lock().unwrap());
    assert_eq!(cold.len(), COMPACTIONS);
    for index in cold {
        assert!(asks_for_summary(&requests[index - 1]), "request {index}");
        assert!(matches!(
            requests[index].history[0],
            HistoryEntry::Compaction { .. }
        ));
    }
    // No request was larger than the window.
    assert!(cost.largest_request_bytes < MAX_CONTEXT_BYTES);
}

fn window() -> ContextWindow {
    ContextWindow::new(WINDOW_TOKENS)
}
/// A recorded read and its result, as an earlier run left it.
fn exchange(id: &str) -> HistoryEntry {
    HistoryEntry::Exchange {
        response: ModelResponse::tools(vec![ToolCall {
            id: id.into(),
            name: "read".into(),
            arguments: json!(TOOL_RESULT_BYTES),
        }]),
        results: vec![ToolResult {
            call_id: id.into(),
            value: json!("r".repeat(TOOL_RESULT_BYTES)),
            is_error: false,
        }],
    }
}
/// An earlier run of the conversation: a question, a read and an answer.
fn earlier_run(n: usize) -> [HistoryEntry; 3] {
    [
        HistoryEntry::Input(json!(format!("Earlier question {n}"))),
        exchange(&format!("earlier-{n}")),
        HistoryEntry::Exchange {
            response: answer(),
            results: vec![],
        },
    ]
}
fn earlier_runs(count: usize) -> Vec<HistoryEntry> {
    (0..count).flat_map(earlier_run).collect()
}
async fn drive(agent: &Agent<String, String>, state: &mut RunState) -> Vec<ProgressKind> {
    let (tx, mut rx) = tokio::sync::mpsc::channel(1024);
    let control = RunControl::default().with_events(tx);
    agent
        .drive(state, &MemoryCheckpoint, control)
        .await
        .unwrap();
    let mut events = Vec::new();
    while let Some(event) = rx.recv().await {
        events.push(event.kind);
    }
    events
}
fn summary_of(entry: &HistoryEntry) -> &str {
    match entry {
        HistoryEntry::Compaction { summary, .. } => summary,
        other => panic!("not a compaction: {other:?}"),
    }
}

#[tokio::test]
async fn a_long_run_is_compacted_and_keeps_its_input_and_recent_entries() {
    // One run that reads eleven pages before it answers.
    let model = Script::new(|request, n| {
        Ok(match n {
            _ if asks_for_summary(request) => ModelResponse::final_output(json!(SUMMARY)),
            n if n > 12 => answer(),
            n => read(n),
        })
    });
    let agent = builder(model.clone())
        .context_window(window())
        .build()
        .unwrap();
    let mut state = agent.new_run("c", json!("Read it all"), &[], None).unwrap();
    let events = drive(&agent, &mut state).await;
    assert!(matches!(
        state.phase,
        Phase::Settled(Settlement::Completed(_))
    ));

    let requests = model.requests();
    let asked = (requests.iter().position(asks_for_summary)).expect("a summary request");
    // The summary request is the turn's own context, then the instruction, with the tools
    // still listed and a text answer.
    let summary_request = &requests[asked];
    // It sends what the compaction covers, which is the start of the turn's context, and not
    // the entries it keeps.
    let context = &summary_request.history[..summary_request.history.len() - 1];
    assert!(!context.is_empty() && requests[asked - 1].history.starts_with(context));
    assert!(matches!(summary_request.history.last(),
        Some(HistoryEntry::Steering { instruction, .. }) if instruction.contains("checkpoint summary")));
    assert_eq!(summary_request.tools.len(), 1);
    // The agent answers in text, so its own schema is kept and the request differs from the
    // turn before it only at its end.
    assert_eq!(
        summary_request.output_schema,
        requests[asked - 1].output_schema
    );
    assert!(
        requests
            .iter()
            .filter(|r| !asks_for_summary(r))
            .all(|r| r.allow_tool_calls)
    );

    // The compaction is recorded after the entries it kept; nothing recorded is rewritten.
    let at = (state.history.iter())
        .position(|entry| matches!(entry, HistoryEntry::Compaction { .. }))
        .unwrap();
    let HistoryEntry::Compaction { summary, kept } = &state.history[at] else {
        unreachable!()
    };
    assert_eq!(summary, SUMMARY);
    let kept = *kept as usize;
    assert!(kept >= 1 && kept < at - 1, "kept {kept} of {at}");
    assert_eq!(state.history[0], HistoryEntry::Input(json!("Read it all")));

    // The next turn sends the summary, the run's input, and the kept entries.
    let next = &requests[asked + 1].history;
    assert_eq!(next[0], state.history[at]);
    assert_eq!(next[1], state.history[0]);
    assert_eq!(next[2..], state.history[at - kept..at]);
    // About the newest `keep_recent_tokens` are kept.
    let kept_bytes: usize = (next[2..].iter())
        .map(|entry| serde_json::to_vec(entry).unwrap().len())
        .sum();
    let keep_bytes = BYTES_PER_TOKEN * window().keep_recent_tokens as usize;
    assert!(kept_bytes <= keep_bytes);
    assert!(kept_bytes + TOOL_RESULT_BYTES + 200 > keep_bytes);
    // The summary request left exactly those out.
    assert_eq!(context.len() + kept, requests[asked - 1].history.len() + 1);

    // The summary's text is not progress; the compaction is.
    let summarized = (events.iter())
        .filter(|kind| {
            matches!(
                kind,
                ProgressKind::Compacted {
                    summarized: true,
                    ..
                }
            )
        })
        .count();
    let started = (events.iter())
        .filter(|kind| matches!(kind, ProgressKind::CompactionStarted))
        .count();
    assert_eq!(
        (started, summarized),
        (compactions(&state.history), compactions(&state.history))
    );
    assert!(
        events
            .iter()
            .all(|kind| !matches!(kind, ProgressKind::TextDelta(text) if text == SUMMARY))
    );
    assert!(
        events.iter().any(
            |kind| matches!(kind, ProgressKind::TextDelta(text) if text.starts_with("Answer"))
        )
    );
    // A compaction is a model call like any other.
    assert_eq!(state.usage.model_calls as usize, requests.len());
}

#[tokio::test]
async fn a_later_run_is_accepted_with_the_latest_summary_and_what_it_kept() {
    let (_, conversation, _) = converse(|agent| agent.context_window(window())).await;
    let history = &conversation.history;
    let at = (history.iter())
        .rposition(|entry| matches!(entry, HistoryEntry::Compaction { .. }))
        .unwrap();
    let HistoryEntry::Compaction { kept, .. } = &history[at] else {
        unreachable!()
    };
    let expected = &history[at - *kept as usize..];
    assert_eq!(compactions(expected), 1, "it kept no earlier compaction");

    // With or without a context window or policy, acceptance stops at what the summary covers.
    struct NoPolicy;
    impl ContextPolicy for NoPolicy {
        fn binding(&self) -> Value {
            json!("none")
        }
    }
    let plain = builder(Script::new(reader)).build().unwrap();
    let windowed = builder(Script::new(reader))
        .context_window(window())
        .build()
        .unwrap();
    let with_policy = builder(Script::new(reader))
        .context_policy(Arc::new(NoPolicy), 0)
        .build()
        .unwrap();
    for agent in [plain, windowed, with_policy] {
        let state = agent.new_run("c", json!("Next"), history, None).unwrap();
        assert_eq!(state.prior_context, expected);
    }
}

#[tokio::test]
async fn a_summary_the_model_cannot_write_is_built_from_the_conversation() {
    // Each of these is an answer that cannot be used as a summary.
    let unusable: [fn() -> Result<ModelResponse>; 4] = [
        || Ok(read(99)),
        || Ok(ModelResponse::final_output(json!("  "))),
        || Ok(ModelResponse::final_output(json!("s".repeat(4_000)))),
        || Err(Error::context_overflow("the summary request is too large")),
    ];
    for unusable in unusable {
        let model = Script::new(move |request, n| match request.history.last() {
            _ if asks_for_summary(request) => unusable(),
            Some(HistoryEntry::Exchange { .. }) => Ok(answer()),
            _ => Ok(read(n)),
        });
        let agent = builder(model.clone())
            .context_window(window())
            .build()
            .unwrap();
        let prior = earlier_runs(8);
        let mut state = agent.new_run("c", json!("And now?"), &prior, None).unwrap();
        let events = drive(&agent, &mut state).await;
        // The run goes on, with a record of what the summary would have covered.
        assert!(matches!(
            state.phase,
            Phase::Settled(Settlement::Completed(_))
        ));
        assert!(events.iter().any(|kind| matches!(
            kind,
            ProgressKind::Compacted {
                summarized: false,
                ..
            }
        )));
        let summary = summary_of(&state.history[1]);
        assert!(summary.starts_with("(The model's summary"), "{summary}");
        assert!(
            summary.contains("- User: Earlier question 0\n"),
            "{summary}"
        );
        assert!(summary.contains("- Assistant called: read\n"), "{summary}");
        assert!(summary.contains("- Assistant: Answer: aaa"), "{summary}");
        assert!(!summary.contains("rrrr"), "tool results are left out");
        assert!(summary.len() <= 8 * window().summary_tokens as usize);
        let requests = model.requests();
        assert_eq!(requests[1].history[0], state.history[1]);
    }
}

#[tokio::test]
async fn a_second_compaction_carries_the_first_summary_forward() {
    let (_, conversation, model) = converse(|agent| agent.context_window(window())).await;
    let requests = model.requests();
    let asked: Vec<_> = (requests.iter().enumerate())
        .filter(|(_, request)| asks_for_summary(request))
        .map(|(index, _)| index)
        .collect();
    // The second summary request starts with the first summary, and sends no other.
    let second = &requests[asked[1]].history;
    assert_eq!(summary_of(&second[0]), SUMMARY);
    assert_eq!(compactions(second), 1);
    // A summary the engine builds itself quotes the one before it.
    let at = (conversation.history.iter())
        .rposition(|entry| matches!(entry, HistoryEntry::Compaction { .. }))
        .unwrap();
    let model = Script::new(|request, n| match request.history.last() {
        _ if asks_for_summary(request) => Ok(read(n)),
        Some(HistoryEntry::Exchange { .. }) => Ok(answer()),
        _ => Ok(read(n)),
    });
    let agent = builder(model).context_window(window()).build().unwrap();
    let mut history = conversation.history[..=at].to_vec();
    history.extend(earlier_runs(8));
    let mut state = agent.new_run("c", json!("More"), &history, None).unwrap();
    drive(&agent, &mut state).await;
    let summary = summary_of(&state.history[1]);
    assert!(
        summary.contains(&format!("Earlier summary:\n{SUMMARY}\n")),
        "{summary}"
    );
}

#[tokio::test]
async fn reported_usage_decides_when_to_compact() {
    // Six earlier runs are about 3,100 tokens by their bytes, under the 4,096 that compacts.
    for reported in [None, Some(TokenUsage::new(4_000, 20))] {
        let usage = reported.clone();
        let model = Script::new(move |request, n| {
            let response = match request.history.last() {
                _ if asks_for_summary(request) => ModelResponse::final_output(json!(SUMMARY)),
                Some(HistoryEntry::Exchange { .. }) => answer(),
                _ => read(n),
            };
            Ok(response.with_usage(usage.clone().unwrap_or_default()))
        });
        let agent = builder(model.clone())
            .context_window(window())
            .limits(Limits::default().with_max_tokens(1_000_000))
            .build()
            .unwrap();
        let prior = earlier_runs(6);
        let mut state = agent.new_run("c", json!("And now?"), &prior, None).unwrap();
        drive(&agent, &mut state).await;
        assert!(matches!(
            state.phase,
            Phase::Settled(Settlement::Completed(_))
        ));
        let asked: Vec<_> = model.requests().iter().map(asks_for_summary).collect();
        match reported {
            // By bytes alone the run never comes near the window.
            None => assert_eq!(asked, [false, false]),
            // The provider's count for the first turn, and the result recorded since, do.
            Some(_) => assert_eq!(asked, [false, true, false]),
        }
    }
}

#[tokio::test]
async fn a_rejected_turn_is_compacted_and_made_again_once() {
    let overflow = || Error::context_overflow("request (9000 tokens) exceeds the context size");
    // The provider rejects the first turn, whatever the estimate said.
    let model = Script::new(move |request, n| match n {
        1 => Err(overflow()),
        _ if asks_for_summary(request) => Ok(ModelResponse::final_output(json!(SUMMARY))),
        _ => Ok(answer()),
    });
    let agent = builder(model.clone())
        .context_window(window())
        .build()
        .unwrap();
    let prior = earlier_runs(6);
    let mut state = agent.new_run("c", json!("And now?"), &prior, None).unwrap();
    drive(&agent, &mut state).await;
    assert!(matches!(
        state.phase,
        Phase::Settled(Settlement::Completed(_))
    ));
    let requests = model.requests();
    let asked: Vec<_> = requests.iter().map(asks_for_summary).collect();
    assert_eq!(asked, [false, true, false]);
    assert_eq!(state.usage.model_calls, 3);
    // It keeps half the usual recent context, and the summary request sends only what that
    // covers.
    let kept: usize = (requests[2].history[1..].iter())
        .map(|entry| serde_json::to_vec(entry).unwrap().len())
        .sum();
    assert!(
        kept <= BYTES_PER_TOKEN * window().keep_recent_tokens as usize / 2,
        "{kept}"
    );
    assert!(requests[1].history.len() < requests[0].history.len());

    // A second rejection of the same turn fails the run.
    let model = Script::new(move |request, _| match asks_for_summary(request) {
        true => Ok(ModelResponse::final_output(json!(SUMMARY))),
        false => Err(overflow()),
    });
    let agent = builder(model.clone())
        .context_window(window())
        .build()
        .unwrap();
    let mut state = agent.new_run("c", json!("And now?"), &prior, None).unwrap();
    drive(&agent, &mut state).await;
    let Phase::Settled(Settlement::Failed(reason)) = &state.phase else {
        panic!("{:?}", state.phase)
    };
    assert_eq!(
        reason,
        "context window exceeded: request (9000 tokens) exceeds the context size"
    );
    assert_eq!(model.requests().len(), 3);

    // Without a context window there is nothing to do about it.
    let model = Script::new(move |_, _| Err(overflow()));
    let agent = builder(model.clone()).build().unwrap();
    let mut state = agent.new_run("c", json!("And now?"), &prior, None).unwrap();
    drive(&agent, &mut state).await;
    assert!(
        matches!(&state.phase, Phase::Settled(Settlement::Failed(reason))
        if reason.starts_with("context window exceeded"))
    );
    assert_eq!(model.requests().len(), 1);
}

#[tokio::test]
async fn output_is_capped_at_the_room_the_prompt_leaves() {
    let model = Script::new(reader);
    let agent = builder(model.clone())
        .context_window(window())
        .build()
        .unwrap();
    let prior = earlier_runs(4);
    let mut state = agent.new_run("c", json!("And now?"), &prior, None).unwrap();
    drive(&agent, &mut state).await;
    let requests = model.requests();
    let sent = |request: &ModelRequest| serde_json::to_vec(&request.history).unwrap().len() as u64;
    for request in &requests {
        // The estimate counts the context and what comes with every request.
        assert!(request.remaining_tokens < WINDOW_TOKENS - sent(request) / 3);
        assert!(request.remaining_tokens > WINDOW_TOKENS - sent(request) / 3 - 300);
    }
    // The run's own allowance still applies when it is the smaller.
    let model = Script::new(reader);
    let agent = builder(model.clone())
        .context_window(window())
        .limits(Limits::default().with_max_tokens(100))
        .build()
        .unwrap();
    let mut state = agent.new_run("c", json!("And now?"), &prior, None).unwrap();
    drive(&agent, &mut state).await;
    assert_eq!(model.requests()[0].remaining_tokens, 100);
}

#[test]
fn a_context_window_is_checked_and_bound() {
    let build = |window: ContextWindow| builder(Script::new(reader)).context_window(window).build();
    assert_eq!(
        build(ContextWindow::new(4_096)).unwrap().binding()["context_window"],
        json!({"tokens": 4096, "reserve_tokens": 1024, "keep_recent_tokens": 614, "summary_tokens": 204})
    );
    let custom = window().with_summary_instructions("List the people named.");
    assert_eq!(
        build(custom).unwrap().binding()["context_window"]["summary_instructions"],
        "List the people named."
    );
    // An agent without one is bound as it was before.
    let plain = builder(Script::new(reader)).build().unwrap();
    assert!(plain.binding().get("context_window").is_none());
    let window = || ContextWindow::new(4_096);
    for invalid in [
        ContextWindow::new(1_000),
        window().with_reserve_tokens(300),
        window().with_keep_recent_tokens(3_000),
        window().with_keep_recent_tokens(0),
        window().with_summary_tokens(0),
        window().with_summary_instructions(" "),
    ] {
        assert!(
            matches!(build(invalid.clone()), Err(Error::Config(_))),
            "{invalid:?}"
        );
    }
    // The defaults fit every window from the smallest up, and no size overflows them.
    for tokens in [
        1_024,
        2_048,
        8_192,
        32_768,
        131_072,
        262_144,
        1_000_000,
        u64::MAX,
    ] {
        build(ContextWindow::new(tokens)).unwrap();
    }
    let large = ContextWindow::new(262_144);
    assert_eq!(
        (
            large.reserve_tokens,
            large.keep_recent_tokens,
            large.summary_tokens
        ),
        (32_768, 20_000, 4_096)
    );
}

/// Stops the run's host the first time it is asked for a summary, and never answers that
/// request; otherwise the script.
struct StopsOnce {
    script: Arc<Script>,
    shutdown: CancellationToken,
}
#[async_trait]
impl Model for StopsOnce {
    async fn stream(
        &self,
        request: ModelRequest,
        cancel: CancellationToken,
    ) -> Result<ModelStream> {
        if asks_for_summary(&request) && !self.shutdown.is_cancelled() {
            self.shutdown.cancel();
            std::future::pending::<()>().await;
        }
        self.script.stream(request, cancel).await
    }
}

#[tokio::test]
async fn an_interrupted_compaction_is_made_again() {
    let script = Script::new(reader);
    let shutdown = CancellationToken::new();
    let model = Arc::new(StopsOnce {
        script: script.clone(),
        shutdown: shutdown.clone(),
    });
    let agent = builder(model).context_window(window()).build().unwrap();
    let prior = earlier_runs(8);
    let mut state = agent.new_run("c", json!("And now?"), &prior, None).unwrap();
    let control = RunControl::default().with_shutdown(shutdown);
    let interrupted = agent.drive(&mut state, &MemoryCheckpoint, control).await;
    assert!(matches!(interrupted, Err(Error::Interrupted)));
    // The attempt is charged and nothing was recorded for it.
    assert_eq!(state.usage.model_calls, 1);
    assert_eq!(compactions(&state.history), 0);
    assert!(!state.is_terminal());

    drive(&agent, &mut state).await;
    assert!(matches!(
        state.phase,
        Phase::Settled(Settlement::Completed(_))
    ));
    assert_eq!(compactions(&state.history), 1);
    assert_eq!(summary_of(&state.history[1]), SUMMARY);
    // The summary request, a read and the answer reached the script; the first attempt did not.
    let asked: Vec<_> = script.requests().iter().map(asks_for_summary).collect();
    assert_eq!(asked, [true, false, false]);
    assert_eq!(state.usage.model_calls, 4);
}

#[tokio::test]
async fn a_dense_prompt_makes_what_follows_count_for_more() {
    // What the engine adds to the provider's own count for a tool result recorded since, when
    // the provider counted the prompt at `bytes_per_token`.
    async fn added_tokens(bytes_per_token: usize) -> (u64, u64) {
        let model = Script::new(move |request, n| {
            let sent = serde_json::to_vec(&request.history).unwrap().len();
            let usage = TokenUsage::new((sent / bytes_per_token) as u64, 10);
            Ok(match request.history.last() {
                Some(HistoryEntry::Exchange { .. }) => answer(),
                _ => read(n),
            }
            .with_usage(usage))
        });
        let agent = builder(model.clone())
            .context_window(ContextWindow::new(32_768))
            .limits(Limits::default().with_max_tokens(1_000_000))
            .build()
            .unwrap();
        let prior = earlier_runs(4);
        let mut state = agent.new_run("c", json!("And now?"), &prior, None).unwrap();
        drive(&agent, &mut state).await;
        let requests = model.requests();
        let HistoryEntry::Exchange { response, results } = &state.history[1] else {
            unreachable!()
        };
        let reported = response.usage.input + response.usage.output;
        let estimate = 32_768 - requests[1].remaining_tokens;
        let recorded = serde_json::to_vec(results).unwrap().len() as u64;
        (estimate - reported, recorded)
    }
    // JSON at two bytes a token: the result recorded since is counted at about that density.
    // (A little under, because the engine also counts the instructions and tools as sent.)
    let (added, recorded) = added_tokens(2).await;
    assert!(
        added > recorded * 45 / 100 && added <= recorded / 2,
        "{added} of {recorded}"
    );
    // Prose at four or five: no sparser than three, since what follows is a tool result.
    for sparse in [4, 5] {
        let (added, recorded) = added_tokens(sparse).await;
        assert_eq!(added, recorded.div_ceil(3), "{sparse} bytes a token");
    }
}

#[tokio::test]
async fn a_compaction_that_would_free_little_waits_for_the_provider_to_object() {
    // One earlier run, then a run whose single read is most of the window, and as much as one
    // turn may record: over the threshold, but all a compaction could cover is the earlier run,
    // an eighth of the context.
    let big_read = |n: usize| {
        ModelResponse::tools(vec![ToolCall {
            id: format!("read-{n}"),
            name: "read".into(),
            arguments: json!(10_700),
        }])
    };
    let prior = earlier_runs(1);
    let model = Script::new(move |request, n| {
        Ok(match request.history.last() {
            _ if asks_for_summary(request) => ModelResponse::final_output(json!(SUMMARY)),
            Some(HistoryEntry::Exchange { .. }) => answer(),
            _ => big_read(n),
        })
    });
    let agent = builder(model.clone())
        .context_window(window())
        .build()
        .unwrap();
    let mut state = agent
        .new_run("c", json!("Read the long one"), &prior, None)
        .unwrap();
    drive(&agent, &mut state).await;
    assert!(matches!(
        state.phase,
        Phase::Settled(Settlement::Completed(_))
    ));
    // The turn is made as it is: a summary would cost a model call and a cold prompt to free
    // an eighth of it.
    let requests = model.requests();
    assert_eq!(requests.iter().filter(|r| asks_for_summary(r)).count(), 0);
    let sent = serde_json::to_vec(&requests[1].history).unwrap().len();
    assert!(sent / BYTES_PER_TOKEN > window().tokens as usize - window().reserve_tokens as usize);

    // If the provider rejects that turn, the little there is to cover is covered.
    let model = Script::new(move |request, n| match request.history.last() {
        _ if asks_for_summary(request) => Ok(ModelResponse::final_output(json!(SUMMARY))),
        Some(HistoryEntry::Exchange { .. }) if n == 2 => Err(Error::context_overflow("too large")),
        Some(HistoryEntry::Exchange { .. }) => Ok(answer()),
        _ => Ok(big_read(n)),
    });
    let agent = builder(model.clone())
        .context_window(window())
        .build()
        .unwrap();
    let mut state = agent
        .new_run("c", json!("Read the long one"), &prior, None)
        .unwrap();
    drive(&agent, &mut state).await;
    assert!(matches!(
        state.phase,
        Phase::Settled(Settlement::Completed(_))
    ));
    let asked: Vec<_> = model.requests().iter().map(asks_for_summary).collect();
    assert_eq!(asked, [false, false, true, false]);
}

#[tokio::test]
async fn results_that_one_turn_cannot_hold_are_refused_in_call_order() {
    // Three reads in one turn. The window has room for about 11,000 bytes of results beside the
    // instructions, the tool, the question and a summary of anything earlier.
    let reads = || {
        let call = |id: &str, bytes: usize| ToolCall {
            id: id.into(),
            name: "read".into(),
            arguments: json!(bytes),
        };
        ModelResponse::tools(vec![
            call("first", 6_000),
            call("second", 6_000),
            call("third", 2_000),
        ])
    };
    let results = |window: Option<ContextWindow>| async move {
        let model = Script::new(move |request, _| {
            Ok(match request.history.last() {
                Some(HistoryEntry::Exchange { .. }) => answer(),
                _ => reads(),
            })
        });
        let mut agent = builder(model.clone());
        if let Some(window) = window {
            agent = agent.context_window(window);
        }
        let agent = agent.build().unwrap();
        let mut state = agent.new_run("c", json!("Read three"), &[], None).unwrap();
        drive(&agent, &mut state).await;
        assert!(matches!(
            state.phase,
            Phase::Settled(Settlement::Completed(_))
        ));
        // The model was sent exactly what was recorded.
        let requests = model.requests();
        assert_eq!(requests[1].history.last(), Some(&state.history[1]));
        match &state.history[1] {
            HistoryEntry::Exchange { results, .. } => results.clone(),
            other => panic!("not an exchange: {other:?}"),
        }
    };

    let bounded = results(Some(window())).await;
    let kept: Vec<_> = bounded.iter().map(|result| !result.is_error).collect();
    // The second does not fit beside the first; the third still does.
    assert_eq!(kept, [true, false, true]);
    assert_eq!(bounded[0].value.as_str().unwrap().len(), 6_000);
    assert_eq!(bounded[2].value.as_str().unwrap().len(), 2_000);
    let refusal = bounded[1].value.as_str().unwrap();
    assert!(
        refusal.contains("output of 6002 bytes was not kept"),
        "{refusal}"
    );
    assert!(refusal.contains("make fewer calls at a time"), "{refusal}");
    // What was kept fits the window below its reserve, at the engine's three bytes a token.
    let recorded: usize = (bounded.iter())
        .map(|result| serde_json::to_vec(&result.value).unwrap().len())
        .sum();
    assert!(recorded / BYTES_PER_TOKEN < (window().tokens - window().reserve_tokens) as usize);

    // Without a context window nothing is known about room, and every result is recorded.
    let unbounded = results(None).await;
    assert!(unbounded.iter().all(|result| !result.is_error));
}

#[tokio::test]
async fn a_provider_failure_during_a_summary_fails_the_turn_and_is_asked_again() {
    // An outage or a rate limit says nothing about whether the model can summarize, so it is not
    // answered with the engine's lossy record: the run fails as on any model call, and the next
    // run of the conversation asks for the summary again.
    let fail = Arc::new(Mutex::new(true));
    let failing = fail.clone();
    let model = Script::new(move |request, n| match request.history.last() {
        _ if asks_for_summary(request) && *failing.lock().unwrap() => {
            Err(Error::provider("rate limited"))
        }
        _ if asks_for_summary(request) => Ok(ModelResponse::final_output(json!(SUMMARY))),
        Some(HistoryEntry::Exchange { .. }) => Ok(answer()),
        _ => Ok(read(n)),
    });
    let agent = builder(model.clone())
        .context_window(window())
        .build()
        .unwrap();
    let prior = earlier_runs(8);
    let mut state = agent.new_run("c", json!("And now?"), &prior, None).unwrap();
    drive(&agent, &mut state).await;
    assert!(
        matches!(&state.phase, Phase::Settled(Settlement::Failed(reason)) if reason.contains("rate limited")),
        "{:?}",
        state.phase
    );
    assert_eq!(compactions(&state.history), 0, "no lossy record was made");

    *fail.lock().unwrap() = false;
    let mut state = agent.new_run("c", json!("And now?"), &prior, None).unwrap();
    drive(&agent, &mut state).await;
    assert!(matches!(
        state.phase,
        Phase::Settled(Settlement::Completed(_))
    ));
    assert_eq!(summary_of(&state.history[1]), SUMMARY);
}
