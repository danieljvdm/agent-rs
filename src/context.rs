//! Context assembly: which history reaches the model on each turn, and what an optional
//! [`ContextPolicy`] adds beside it.
use crate::{Error, HistoryEntry, Result, RunId, RunState};
use async_trait::async_trait;
use serde::Serialize;
use serde_json::Value;
use std::{io, sync::Arc};
use tokio_util::sync::CancellationToken;

/// What [`ContextPolicy::prepare`] receives for one model turn.
#[non_exhaustive]
pub struct ContextRequest<'a> {
    /// Identifies the run to [`ContextPolicy::cleanup`].
    pub run_id: RunId,
    /// The run's conversation, for policies that keep per-conversation state.
    pub conversation_id: &'a str,
    /// The run's current input.
    pub input: &'a Value,
    /// The derived-context cap given to `AgentBuilder::context_policy`.
    pub max_bytes: usize,
}

/// Optional policy for what reaches the model on each turn, set with
/// [`AgentBuilder::context_policy`](crate::AgentBuilder::context_policy). Without one the engine
/// sends the default context described below and makes no calls through this trait.
///
/// # Contract
///
/// **History.** A turn sends whole entries of the run's prior context followed by its own
/// history, in their original order; an exchange always keeps its tool calls with their results.
/// - Pinned, always sent: the run's current input, its latest exchange, each
///   [`HistoryEntry::Steering`] instruction of the run's own history, and each entry of the run's
///   own history for which [`pinned`](Self::pinned) returns true. Prior context is never pinned
///   and `pinned` is not consulted for it. Steering is pinned with or without a policy, so a
///   policy never needs to pin it.
/// - Everything else may be dropped. Unpinned entries are taken newest first while they fit; the
///   first that does not fit ends the selection, so an older entry never displaces a newer one.
/// - If the pinned entries alone exceed `max_context_bytes`, the turn fails with
///   [`Error::Context`] and the run settles as failed. Pinned entries are never dropped.
///
/// **Derived context.** [`prepare`](Self::prepare) may return lower-trust reference data, such as
/// summaries or retrieved items, highest priority first. It reaches the model in
/// `ModelRequest::derived_context`, is never appended to history and is never checkpointed.
/// - Items are kept in order while their JSON array fits the policy's cap; an item that does not
///   fit is skipped and later items may still be kept.
/// - If the pinned history does not fit beside the kept items, the lowest-priority items are
///   dropped until it does. Derived context never evicts pinned history or fails a turn.
/// - Kept items shrink the budget left for unpinned history.
///
/// **The limit.** `max_context_bytes` bounds the encoded JSON array of history plus the encoded
/// array of derived items, which costs nothing when empty. Instructions, tool specs, the output
/// schema and provider framing are outside it.
///
/// **Lifecycle.** `prepare` runs once per model turn, after the turn's dispatch checkpoint and
/// inside the model operation's deadline and active-time reservation. Cancellation and the
/// deadline drop its future, and `cancel` fires when the operation ends, for work handed to other
/// tasks. An error fails the turn. [`cleanup`](Self::cleanup) then runs after every model
/// operation, beside `Model::cleanup` and within `cleanup_timeout_ms`; no work may outlive it.
///
/// **Binding.** [`binding`](Self::binding) and the cap are part of `Agent::binding`, so a
/// persisted run is never resumed under a different policy. It is read once, when the agent is
/// built. Describe everything that changes what the policy pins or prepares, stably and without
/// credentials.
#[async_trait]
pub trait ContextPolicy: Send + Sync {
    /// Stable, credential-free configuration included in the durable agent binding.
    fn binding(&self) -> Value;
    /// Whether an entry of the run's own history must survive truncation. Called several times
    /// per entry on every turn: keep it cheap and a pure function of the entry.
    fn pinned(&self, _entry: &HistoryEntry) -> bool {
        false
    }
    /// Derived context for this turn, highest priority first.
    async fn prepare(
        &self,
        _request: ContextRequest<'_>,
        _cancel: CancellationToken,
    ) -> Result<Vec<Value>> {
        Ok(Vec::new())
    }
    /// Release run-owned work after each model operation, however it ended.
    async fn cleanup(&self, _run_id: RunId) -> Result<()> {
        Ok(())
    }
}

/// Counts serialized bytes without buffering them.
struct Counter(usize);
impl io::Write for Counter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 += bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
/// Length of the compact JSON encoding, measured without allocating it.
///
/// For the crate's JSON types, which cannot fail to encode: the counter never fails to write,
/// and a `Value` has only string keys and finite numbers.
pub(crate) fn encoded_len<T: Serialize + ?Sized>(value: &T) -> usize {
    let mut counter = Counter(0);
    serde_json::to_writer(&mut counter, value).expect("JSON values always encode");
    counter.0
}

/// The encoded size of a JSON array as entries are added to it.
struct Window {
    max: usize,
    bytes: usize,
    entries: usize,
}
impl Window {
    fn new(max: usize) -> Result<Self> {
        if max < 2 {
            return Err(Error::context("context limit too small"));
        }
        Ok(Self {
            max,
            bytes: 2,
            entries: 0,
        })
    }
    /// Adds the entry if the array still fits; separators count once per added entry.
    fn add(&mut self, entry: &HistoryEntry) -> bool {
        let next = self
            .bytes
            .saturating_add(encoded_len(entry))
            .saturating_add(usize::from(self.entries > 0));
        if next > self.max {
            return false;
        }
        self.bytes = next;
        self.entries += 1;
        true
    }
}

/// Start of the longest suffix whose encoded array fits. The newest entry must fit.
pub(crate) fn suffix_start(history: &[HistoryEntry], max_bytes: usize) -> Result<usize> {
    let mut window = Window::new(max_bytes)?;
    let mut start = history.len();
    for (index, entry) in history.iter().enumerate().rev() {
        if !window.add(entry) {
            if start == history.len() {
                return Err(Error::context("newest history group exceeds context limit"));
            }
            break;
        }
        start = index;
    }
    Ok(start)
}

/// One turn's history, selected in a single pass and cloned once, as [`ContextPolicy`] describes.
///
/// Pinned: the run's current input (`history[0]`), its latest exchange, its steering
/// instructions, and each entry of the run's own history for which `pinned` holds. Every other entry of `prior ++ history` is
/// optional and is added newest first until the next one does not fit, so optional entries form
/// one contiguous suffix. The result keeps chronological order. `derived` is trimmed to its cap
/// and to the room the pinned entries leave, and the rest of that room bounds optional entries.
fn select_history(
    prior: &[HistoryEntry],
    history: &[HistoryEntry],
    max_bytes: usize,
    derived: &mut Vec<Value>,
    max_derived_bytes: usize,
    pinned: impl Fn(&HistoryEntry) -> bool,
) -> Result<Vec<HistoryEntry>> {
    let latest_exchange = history
        .iter()
        .rposition(|entry| matches!(entry, HistoryEntry::Exchange { .. }));
    let is_pinned = |index: usize| {
        index == 0
            || Some(index) == latest_exchange
            || matches!(history[index], HistoryEntry::Steering { .. })
            || pinned(&history[index])
    };
    let mut window = Window::new(max_bytes)?;
    for (index, entry) in history.iter().enumerate() {
        if is_pinned(index) && !window.add(entry) {
            return Err(Error::context(
                "current input and pinned history exceed context limit",
            ));
        }
    }
    window.max -= fit_derived(derived, max_derived_bytes, max_bytes - window.bytes);
    // Run history at or after `cut` is kept whole; before it, only pinned entries survive.
    let mut cut = 0;
    let mut prior_start = prior.len();
    'fill: {
        for index in (0..history.len()).rev() {
            if !is_pinned(index) && !window.add(&history[index]) {
                cut = index + 1;
                break 'fill;
            }
        }
        for (index, entry) in prior.iter().enumerate().rev() {
            if !window.add(entry) {
                break 'fill;
            }
            prior_start = index;
        }
    }
    let mut selected = Vec::with_capacity(window.entries);
    selected.extend_from_slice(&prior[prior_start..]);
    selected.extend(
        history
            .iter()
            .enumerate()
            .filter(|&(index, _)| index >= cut || is_pinned(index))
            .map(|(_, entry)| entry.clone()),
    );
    Ok(selected)
}

/// Keeps the items whose array fits `cap`, in order, then drops the lowest-priority items until the
/// array fits `room`. Returns the array's size, zero when no item is kept.
fn fit_derived(items: &mut Vec<Value>, cap: usize, room: usize) -> usize {
    let mut bytes = 2_usize;
    let mut index = 0;
    while index < items.len() {
        let next = bytes
            .saturating_add(encoded_len(&items[index]))
            .saturating_add(usize::from(index > 0));
        if next <= cap {
            bytes = next;
            index += 1;
        } else {
            items.remove(index);
        }
    }
    while bytes > room
        && let Some(item) = items.pop()
    {
        bytes -= encoded_len(&item) + usize::from(!items.is_empty());
    }
    if items.is_empty() { 0 } else { bytes }
}

/// History and derived context for one model turn. Without a policy this is the default context:
/// a bounded suffix of complete exchanges that always holds the current input.
pub(crate) async fn assemble(
    state: &RunState,
    policy: Option<&(Arc<dyn ContextPolicy>, usize)>,
    cancel: &CancellationToken,
) -> Result<(Vec<HistoryEntry>, Vec<Value>)> {
    let (prior, history, max_bytes) = (
        &state.prior_context,
        &state.history,
        state.limits.max_context_bytes,
    );
    let Some((policy, max_derived_bytes)) = policy else {
        let history = select_history(prior, history, max_bytes, &mut Vec::new(), 0, |_| false)?;
        return Ok((history, Vec::new()));
    };
    let mut derived = policy
        .prepare(
            ContextRequest {
                run_id: state.id,
                conversation_id: &state.conversation_id,
                input: &state.input,
                max_bytes: *max_derived_bytes,
            },
            cancel.clone(),
        )
        .await?;
    let history = select_history(
        prior,
        history,
        max_bytes,
        &mut derived,
        *max_derived_bytes,
        |entry| policy.pinned(entry),
    )?;
    Ok((history, derived))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ModelResponse;
    use serde_json::json;

    /// The two-pass selection this module replaced, kept verbatim as an oracle.
    fn previous(
        prior: &[HistoryEntry],
        history: &[HistoryEntry],
        max: usize,
    ) -> Result<Vec<HistoryEntry>> {
        fn len(entry: &HistoryEntry) -> usize {
            serde_json::to_vec(entry).unwrap().len()
        }
        fn suffix(history: &[HistoryEntry], max_bytes: usize) -> Result<Vec<HistoryEntry>> {
            let mut bytes = 2_usize;
            let mut start = history.len();
            for (index, entry) in history.iter().enumerate().rev() {
                let size = len(entry).saturating_add(usize::from(start < history.len()));
                if bytes.saturating_add(size) > max_bytes {
                    if start == history.len() {
                        return Err(Error::context("newest"));
                    }
                    break;
                }
                bytes += size;
                start = index;
            }
            if bytes > max_bytes {
                return Err(Error::context("too small"));
            }
            Ok(history[start..].to_vec())
        }
        let all: Vec<_> = prior.iter().chain(history).cloned().collect();
        let mut selected = suffix(&all, max)?;
        if all.len() - selected.len() > prior.len() {
            let input = history[0].clone();
            selected = suffix(&all, max.saturating_sub(len(&input) + 1))?;
            selected.insert(0, input);
        }
        Ok(selected)
    }

    #[test]
    fn default_selection_matches_the_previous_two_pass_selection() {
        let sizes = [1, 7, 30, 3, 50, 12, 0, 21];
        let mut next = 0;
        let mut entry = |exchange: bool| {
            next += 1;
            let text = "x".repeat(sizes[next % sizes.len()]);
            if exchange {
                HistoryEntry::Exchange {
                    response: ModelResponse::final_output(json!(text)),
                    results: vec![],
                }
            } else {
                HistoryEntry::Input(json!(text))
            }
        };
        let mut cases = 0;
        for prior_len in 0..5 {
            for exchanges in 0..4 {
                let prior: Vec<_> = (0..prior_len).map(|n| entry(n % 2 == 1)).collect();
                let history: Vec<_> = (0..=exchanges).map(|n| entry(n > 0)).collect();
                let total = encoded_len(&prior) + encoded_len(&history);
                for max in 0..total + 4 {
                    let expected = previous(&prior, &history, max).ok();
                    let actual =
                        select_history(&prior, &history, max, &mut Vec::new(), 0, |_| false).ok();
                    assert_eq!(
                        actual, expected,
                        "prior {prior_len}, exchanges {exchanges}, max {max}"
                    );
                    cases += 1;
                }
            }
        }
        assert!(cases > 1_000);
    }
}
