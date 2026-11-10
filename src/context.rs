//! Context assembly: which history reaches the model on each turn.
use crate::{Error, HistoryEntry, Result, RunState};
use serde::Serialize;
use std::io;

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
pub(crate) fn encoded_len<T: Serialize + ?Sized>(value: &T) -> Result<usize> {
    let mut counter = Counter(0);
    serde_json::to_writer(&mut counter, value).map_err(|e| Error::Invalid(e.to_string()))?;
    Ok(counter.0)
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
            return Err(Error::Invalid("context limit too small".into()));
        }
        Ok(Self {
            max,
            bytes: 2,
            entries: 0,
        })
    }
    /// Adds the entry if the array still fits; separators count once per added entry.
    fn add(&mut self, entry: &HistoryEntry) -> Result<bool> {
        let next = self
            .bytes
            .saturating_add(encoded_len(entry)?)
            .saturating_add(usize::from(self.entries > 0));
        if next > self.max {
            return Ok(false);
        }
        self.bytes = next;
        self.entries += 1;
        Ok(true)
    }
}

/// Select a bounded suffix while retaining each complete model/tool exchange intact.
pub fn select_context(history: &[HistoryEntry], max_bytes: usize) -> Result<Vec<HistoryEntry>> {
    Ok(history[suffix_start(history, max_bytes)?..].to_vec())
}
/// Start of the longest suffix whose encoded array fits. The newest entry must fit.
pub(crate) fn suffix_start(history: &[HistoryEntry], max_bytes: usize) -> Result<usize> {
    let mut window = Window::new(max_bytes)?;
    let mut start = history.len();
    for (index, entry) in history.iter().enumerate().rev() {
        if !window.add(entry)? {
            if start == history.len() {
                return Err(Error::Invalid(
                    "newest history group exceeds context limit".into(),
                ));
            }
            break;
        }
        start = index;
    }
    Ok(start)
}

/// One turn's history, selected in a single pass and cloned once.
///
/// Pinned: the run's current input (`history[0]`), its latest exchange, and each entry of the
/// run's own history for which `pinned` holds. Every other entry of `prior ++ history` is
/// optional and is added newest first until the next one does not fit, so optional entries form
/// one contiguous suffix. The result keeps chronological order.
pub(crate) fn select_history(
    prior: &[HistoryEntry],
    history: &[HistoryEntry],
    max_bytes: usize,
    pinned: impl Fn(&HistoryEntry) -> bool,
) -> Result<Vec<HistoryEntry>> {
    let latest_exchange = history
        .iter()
        .rposition(|entry| matches!(entry, HistoryEntry::Exchange { .. }));
    let is_pinned =
        |index: usize| index == 0 || Some(index) == latest_exchange || pinned(&history[index]);
    let mut window = Window::new(max_bytes)?;
    for (index, entry) in history.iter().enumerate() {
        if is_pinned(index) && !window.add(entry)? {
            return Err(Error::Invalid(
                "current input and pinned history exceed context limit".into(),
            ));
        }
    }
    // Run history at or after `cut` is kept whole; before it, only pinned entries survive.
    let mut cut = 0;
    let mut prior_start = prior.len();
    'fill: {
        for index in (0..history.len()).rev() {
            if !is_pinned(index) && !window.add(&history[index])? {
                cut = index + 1;
                break 'fill;
            }
        }
        for (index, entry) in prior.iter().enumerate().rev() {
            if !window.add(entry)? {
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

/// The default context: a bounded suffix of complete exchanges that always holds the current input.
pub(crate) fn model_context(state: &RunState) -> Result<Vec<HistoryEntry>> {
    select_history(
        &state.prior_context,
        &state.history,
        state.limits.max_context_bytes,
        |_| false,
    )
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
                        return Err(Error::Invalid("newest".into()));
                    }
                    break;
                }
                bytes += size;
                start = index;
            }
            if bytes > max_bytes {
                return Err(Error::Invalid("too small".into()));
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
                let total = encoded_len(&prior).unwrap() + encoded_len(&history).unwrap();
                for max in 0..total + 4 {
                    let expected = previous(&prior, &history, max).ok();
                    let actual = select_history(&prior, &history, max, |_| false).ok();
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
