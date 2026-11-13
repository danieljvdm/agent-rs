//! Context windows: when a conversation is compacted, what a compaction keeps, and the summary
//! that stands in for the rest. The engine makes the summary's model call; everything here is a
//! pure function of the run's context.
use crate::{
    Error, HistoryEntry, Result,
    context::{compacted, encoded_len, latest_exchange, pins},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt::Write;

/// Estimated bytes of encoded context per token. Providers report the real count after each
/// turn; this covers what was recorded since.
const BYTES_PER_TOKEN: u64 = 4;
/// Bytes of one earlier message in a summary the engine builds itself.
const FALLBACK_MESSAGE_BYTES: usize = 400;

/// A model's context window, set with
/// [`AgentBuilder::context_window`](crate::AgentBuilder::context_window), and how the engine
/// keeps a conversation inside it.
///
/// Before each model turn the engine estimates the turn's prompt in tokens. When the estimate
/// reaches [`tokens`](Self::tokens) less [`reserve_tokens`](Self::reserve_tokens), it first asks
/// the model to summarize the conversation, records the summary as a
/// [`HistoryEntry::Compaction`], and from then on sends that summary with the newest
/// [`keep_recent_tokens`](Self::keep_recent_tokens) of entries in place of everything older.
/// Stored history is never rewritten. `docs/context-compaction.md` describes the whole contract.
///
/// ```
/// # use agent_rs::ContextWindow;
/// let window = ContextWindow::new(32_768);
/// assert_eq!(window.reserve_tokens, 4_915);
/// assert_eq!(window.keep_recent_tokens, 9_830);
/// assert_eq!(window.summary_tokens, 1_638);
/// ```
///
/// It is part of the agent's [`binding`](crate::Agent::binding), so a stored run resumes only
/// under the window it was accepted with.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub struct ContextWindow {
    /// Tokens one request to the model may hold, prompt and answer together. At least 1,024.
    pub tokens: u64,
    /// Room kept free for the model's answer and for error in the estimate: the engine compacts
    /// once a prompt is estimated to leave less than this. At least twice
    /// [`summary_tokens`](Self::summary_tokens), so a summary always has room to be written.
    /// Reasoning a model reports apart from its answer counts against it too. Defaults to 15%
    /// of the window, at least 4,096 (or a quarter of a smaller window) and at most 16,384.
    pub reserve_tokens: u64,
    /// How much of the newest context a compaction keeps as recorded. The newest entry and the
    /// run's latest exchange are kept whatever their size. Defaults to 30% of the window, at
    /// most 20,000.
    pub keep_recent_tokens: u64,
    /// The size the model is asked to keep its summary within. An answer over twice this is
    /// not used. Defaults to 5% of the window, at most 4,096.
    pub summary_tokens: u64,
    /// Replaces the built-in instruction that asks the model for its summary, for conversations
    /// the built-in sections do not suit. Sent as a user message after the conversation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary_instructions: Option<String>,
}
impl ContextWindow {
    /// A window of `tokens` with the default thresholds for its size.
    pub fn new(tokens: u64) -> Self {
        let percent = |share: u64| tokens / 100 * share + tokens % 100 * share / 100;
        Self {
            tokens,
            reserve_tokens: percent(15).clamp((tokens / 4).min(4_096), 16_384),
            keep_recent_tokens: percent(30).min(20_000),
            summary_tokens: (tokens / 20).min(4_096),
            summary_instructions: None,
        }
    }
    /// Sets [`reserve_tokens`](Self::reserve_tokens).
    pub fn with_reserve_tokens(mut self, tokens: u64) -> Self {
        self.reserve_tokens = tokens;
        self
    }
    /// Sets [`keep_recent_tokens`](Self::keep_recent_tokens).
    pub fn with_keep_recent_tokens(mut self, tokens: u64) -> Self {
        self.keep_recent_tokens = tokens;
        self
    }
    /// Sets [`summary_tokens`](Self::summary_tokens).
    pub fn with_summary_tokens(mut self, tokens: u64) -> Self {
        self.summary_tokens = tokens;
        self
    }
    /// Sets [`summary_instructions`](Self::summary_instructions).
    pub fn with_summary_instructions(mut self, instructions: impl Into<String>) -> Self {
        self.summary_instructions = Some(instructions.into());
        self
    }

    /// What `AgentBuilder::build` checks: a compaction must leave the context under the
    /// threshold that caused it, and a summary must have room to be written.
    pub(crate) fn validate(&self) -> Result<()> {
        let fits = self.tokens >= 1_024
            && self.summary_tokens >= 1
            && self.keep_recent_tokens >= 1
            && self.summary_tokens.saturating_mul(2) <= self.reserve_tokens
            && self
                .reserve_tokens
                .saturating_add(self.keep_recent_tokens)
                .saturating_add(self.summary_tokens)
                < self.tokens
            && (self.summary_instructions.as_deref()).is_none_or(|text| !text.trim().is_empty());
        if fits {
            Ok(())
        } else {
            Err(Error::config(
                "context window needs at least 1,024 tokens, a reserve of twice the summary, and room for the reserve, the recent context and the summary together",
            ))
        }
    }
    /// The estimate at which a turn is compacted first.
    pub(crate) fn compact_at(&self) -> u64 {
        self.tokens.saturating_sub(self.reserve_tokens)
    }
    /// Bytes of recent context a compaction keeps: half as much once the provider has rejected
    /// a request, since the estimate that allowed it was too low.
    pub(crate) fn keep_bytes(&self, forced: bool) -> usize {
        bytes(self.keep_recent_tokens) / if forced { 2 } else { 1 }
    }
    /// Bytes of context a summary call may send beside `overhead` bytes of instructions, tools
    /// and the summary instruction, leaving the window room for the largest usable summary.
    pub(crate) fn summary_context_bytes(&self, overhead: usize, forced: bool) -> usize {
        let answer = self.summary_tokens.saturating_mul(2);
        let room = bytes(self.tokens.saturating_sub(answer)).saturating_sub(overhead);
        (room / if forced { 2 } else { 1 }).max(2)
    }
    /// The longest summary that is used, in bytes.
    pub(crate) fn summary_max_bytes(&self) -> usize {
        bytes(self.summary_tokens.saturating_mul(2))
    }
    /// Tokens left in the window beside a prompt of `prompt_tokens`, and never zero: a provider
    /// rejects a request it cannot answer, which the engine can act on.
    pub(crate) fn room(&self, prompt_tokens: u64) -> u64 {
        self.tokens.saturating_sub(prompt_tokens).max(1)
    }
    /// The user message that asks the model for a summary.
    pub(crate) fn instruction(&self) -> String {
        match &self.summary_instructions {
            Some(instructions) => instructions.clone(),
            // About three quarters of a word per token.
            None => {
                SUMMARY_INSTRUCTION.replace("{words}", &(self.summary_tokens / 4 * 3).to_string())
            }
        }
    }
}

const SUMMARY_INSTRUCTION: &str = "Stop here and write a checkpoint summary of the conversation above. Another model will continue the conversation from your summary and the most recent messages, without the rest.

Use exactly these sections, and write \"(none)\" in a section that does not apply:

## Goal
What the user is trying to accomplish.

## Requirements
Constraints, preferences and instructions the user gave.

## Progress
What has been done and decided, and what was learned from tool results.

## Open items
Unanswered questions, pending work and the next steps.

## Key details
Exact names, identifiers, numbers, quotations and references needed to continue.

Rules:
- If the conversation begins with an earlier summary, carry forward everything in it that still matters. Where newer messages contradict it, the newer messages are right.
- Keep identifiers and values exactly as they appear, and never invent one.
- Write at most about {words} words.
- Do not answer the user, do not call a tool, and write nothing outside the sections.";

/// Bytes of encoded context estimated to make `tokens`.
fn bytes(tokens: u64) -> usize {
    usize::try_from(tokens.saturating_mul(BYTES_PER_TOKEN)).unwrap_or(usize::MAX)
}
/// Tokens estimated for `bytes` of encoded context.
pub(crate) fn tokens(bytes: usize) -> u64 {
    (bytes as u64).div_ceil(BYTES_PER_TOKEN)
}

/// Estimated prompt tokens of the run's next model turn.
///
/// The run's latest exchange since its latest compaction says what its prompt and answer really
/// came to, if the provider reported usage; only what was recorded after it is estimated from
/// bytes. Otherwise everything is: `overhead` bytes of instructions, tools, output schema and
/// derived-context cap, and the context the turn would send.
pub(crate) fn estimate(
    prior: &[HistoryEntry],
    history: &[HistoryEntry],
    overhead: usize,
    pinned: &impl Fn(&HistoryEntry) -> bool,
) -> u64 {
    let mut since = 0;
    for entry in history.iter().rev() {
        match entry {
            HistoryEntry::Compaction { .. } => break,
            HistoryEntry::Exchange { response, results } if response.usage.input > 0 => {
                let reported = response.usage.input.saturating_add(response.usage.output);
                return reported.saturating_add(tokens(since + encoded_len(results)));
            }
            entry => since += encoded_len(entry),
        }
    }
    let compacted = compacted(prior, history);
    let floor = compacted.map_or(0, |compacted| compacted.floor);
    let latest_exchange = latest_exchange(history);
    let mut sent = overhead;
    for (index, entry) in prior.iter().chain(history).enumerate() {
        let own = index.checked_sub(prior.len());
        let send = match entry {
            HistoryEntry::Compaction { .. } => compacted.is_some_and(|c| c.at == index),
            _ => {
                index >= floor || own.is_some_and(|own| pins(history, own, latest_exchange, pinned))
            }
        };
        if send {
            sent += encoded_len(entry) + 1;
        }
    }
    tokens(sent)
}

/// Where a compaction of `prior ++ history` would cut: the index of the first entry it keeps,
/// or `None` when it would cover nothing that is not already covered or pinned.
///
/// Entries are kept newest first while they fit `keep_bytes`. The newest entry and everything
/// from the run's latest exchange on are kept whatever their size: the model cannot continue
/// without the results it just asked for.
pub(crate) fn cut(
    prior: &[HistoryEntry],
    history: &[HistoryEntry],
    keep_bytes: usize,
    pinned: &impl Fn(&HistoryEntry) -> bool,
) -> Option<usize> {
    let total = prior.len() + history.len();
    let entry = |index: usize| match index.checked_sub(prior.len()) {
        Some(own) => &history[own],
        None => &prior[index],
    };
    let covered = compacted(prior, history).map_or(0, |compacted| compacted.floor);
    let latest_exchange = latest_exchange(history);
    let required = latest_exchange.map_or(total.saturating_sub(1), |own| prior.len() + own);
    let (mut kept, mut floor) = (0_usize, total);
    for index in (covered..total).rev() {
        let entry = entry(index);
        if matches!(entry, HistoryEntry::Compaction { .. }) {
            continue;
        }
        let size = encoded_len(entry) + 1;
        if index < required && kept + size > keep_bytes {
            break;
        }
        kept += size;
        floor = index;
    }
    (covered..floor)
        .any(|index| {
            let own = index.checked_sub(prior.len());
            !matches!(entry(index), HistoryEntry::Compaction { .. })
                && !own.is_some_and(|own| pins(history, own, latest_exchange, pinned))
        })
        .then_some(floor)
}

/// The model's answer to a summary request as a usable summary: non-empty text within
/// `max_bytes`.
pub(crate) fn usable_summary(output: Option<&Value>, max_bytes: usize) -> Option<String> {
    let text = output?.as_str()?.trim();
    (!text.is_empty() && text.len() <= max_bytes).then(|| text.to_owned())
}

/// The summary the engine records when the model's cannot be used: the earlier summary, then
/// the covered messages newest first while they fit, each clipped, listed oldest first. Tool
/// results are left out; the calls that fetched them are named.
pub(crate) fn fallback_summary(
    prior: &[HistoryEntry],
    history: &[HistoryEntry],
    floor: usize,
    max_bytes: usize,
) -> String {
    let all = || prior.iter().chain(history);
    let compacted = compacted(prior, history);
    let mut summary = String::from(
        "(The model's summary of the earlier conversation could not be used. This record was assembled from that conversation instead.)\n",
    );
    if let Some(HistoryEntry::Compaction {
        summary: earlier, ..
    }) = compacted.and_then(|compacted| all().nth(compacted.at))
    {
        let _ = write!(
            summary,
            "\nEarlier summary:\n{}\n",
            clip(earlier, max_bytes / 2)
        );
    }
    let covered = compacted.map_or(0, |compacted| compacted.floor);
    let mut lines = Vec::new();
    let mut room = max_bytes.saturating_sub(summary.len());
    let covered: Vec<_> = all().take(floor).skip(covered).collect();
    for entry in covered.into_iter().rev() {
        let line = match entry {
            HistoryEntry::Input(Value::String(text)) => {
                format!("- User: {}", clip(text, FALLBACK_MESSAGE_BYTES))
            }
            HistoryEntry::Input(value) => format!(
                "- User: {}",
                clip(&value.to_string(), FALLBACK_MESSAGE_BYTES)
            ),
            HistoryEntry::Steering { instruction, .. } => {
                format!("- User: {}", clip(instruction, FALLBACK_MESSAGE_BYTES))
            }
            HistoryEntry::Exchange { response, .. } => match &response.output {
                Some(Value::String(text)) => {
                    format!("- Assistant: {}", clip(text, FALLBACK_MESSAGE_BYTES))
                }
                Some(value) => format!(
                    "- Assistant: {}",
                    clip(&value.to_string(), FALLBACK_MESSAGE_BYTES)
                ),
                None => {
                    let names: Vec<_> = response
                        .tools
                        .iter()
                        .map(|call| call.name.as_str())
                        .collect();
                    format!(
                        "- Assistant called: {}",
                        clip(&names.join(", "), FALLBACK_MESSAGE_BYTES)
                    )
                }
            },
            HistoryEntry::Compaction { .. } => continue,
        };
        if line.len() + 1 > room {
            break;
        }
        room -= line.len() + 1;
        lines.push(line);
    }
    if !lines.is_empty() {
        summary.push_str("\nEarlier messages, oldest first:\n");
        for line in lines.iter().rev() {
            summary.push_str(line);
            summary.push('\n');
        }
    }
    summary.truncate(summary.trim_end().len());
    summary
}

/// `text` cut to at most `max` bytes at a character boundary, marked when cut.
fn clip(text: &str, max: usize) -> std::borrow::Cow<'_, str> {
    if text.len() <= max {
        return text.into();
    }
    let mut end = max;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &text[..end]).into()
}
