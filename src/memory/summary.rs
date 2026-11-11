use super::{
    DERIVED_CONTEXT_VERSION, DerivedContext, DerivedKind, MAX_DERIVED_ITEMS, MAX_SOURCE_BYTES,
    MemorySource, SourceTrust,
};
use crate::{Error, Result};
use std::collections::HashSet;

/// Builds one summary from source snapshots, for [`index_summary`](super::index_summary).
pub trait Summarizer: Send + Sync {
    /// The algorithm and settings, stable across processes. [`MemoryPolicy`](super::MemoryPolicy)
    /// serves only summaries whose producer matches the one it was given.
    fn producer(&self) -> String;
    /// A summary of the approved `sources`, all from one scope.
    fn summarize(&self, sources: &[MemorySource]) -> Result<DerivedContext>;
}

/// A deterministic lead-sentence extractive summary. It takes complete, distinct sentences
/// round-robin across approved sources until its sentence or byte cap is reached, and cites only
/// the sources it took sentences from. It never infers facts, resolves contradictions, or cuts a
/// sentence: one that does not fit is skipped.
pub struct ExtractiveSummarizer {
    max_sentences: usize,
    max_bytes: usize,
}

impl ExtractiveSummarizer {
    /// At most `max_sentences` (1..=128) sentences in at most `max_bytes` (1..=1 MiB) of content.
    pub fn new(max_sentences: usize, max_bytes: usize) -> Result<Self> {
        if max_sentences == 0
            || max_sentences > MAX_DERIVED_ITEMS
            || max_bytes == 0
            || max_bytes > MAX_SOURCE_BYTES
        {
            return Err(Error::config(
                "summary requires 1..=128 sentences and 1..=1048576 content bytes",
            ));
        }
        Ok(Self {
            max_sentences,
            max_bytes,
        })
    }
}

/// Splits at newlines and at `.`, `!` or `?` followed by whitespace or the end.
fn sentences(text: &str) -> Vec<&str> {
    let mut result = Vec::new();
    let mut start = 0;
    for (offset, c) in text.char_indices() {
        let end = offset + c.len_utf8();
        if c == '\n'
            || (matches!(c, '.' | '!' | '?')
                && text[end..].chars().next().is_none_or(char::is_whitespace))
        {
            let sentence = text[start..end].trim();
            if !sentence.is_empty() {
                result.push(sentence);
            }
            start = end;
        }
    }
    if !text[start..].trim().is_empty() {
        result.push(text[start..].trim());
    }
    result
}

impl Summarizer for ExtractiveSummarizer {
    fn producer(&self) -> String {
        format!(
            "lead-sentences-v1/{}/{}",
            self.max_sentences, self.max_bytes
        )
    }

    fn summarize(&self, sources: &[MemorySource]) -> Result<DerivedContext> {
        if sources.is_empty() || sources.len() > MAX_DERIVED_ITEMS {
            return Err(Error::input("summary requires 1..=128 source snapshots"));
        }
        let scope = &sources[0].reference.scope;
        if sources
            .iter()
            .any(|source| source.reference.scope != *scope)
        {
            return Err(Error::input("summary cannot cross source scopes"));
        }
        let mut inputs: Vec<_> = sources
            .iter()
            .filter(|source| source.trust == SourceTrust::Approved)
            .map(|source| (source, sentences(&source.text).into_iter()))
            .collect();
        let mut content = String::new();
        let mut provenance = Vec::new();
        let mut seen = HashSet::new();
        let mut selected = 0;
        loop {
            let mut any = false;
            for (source, sentences) in &mut inputs {
                let Some(sentence) = sentences.next() else {
                    continue;
                };
                any = true;
                let bytes = content
                    .len()
                    .saturating_add(sentence.len())
                    .saturating_add(usize::from(!content.is_empty()));
                if selected == self.max_sentences
                    || bytes > self.max_bytes
                    || !seen.insert(sentence)
                {
                    continue;
                }
                if !content.is_empty() {
                    content.push('\n');
                }
                content.push_str(sentence);
                if !provenance.contains(&source.reference) {
                    provenance.push(source.reference.clone());
                }
                selected += 1;
            }
            if !any || selected == self.max_sentences {
                break;
            }
        }
        if content.is_empty() {
            return Err(Error::input(
                "no approved complete sentence fits the summary",
            ));
        }
        Ok(DerivedContext {
            version: DERIVED_CONTEXT_VERSION,
            kind: DerivedKind::Summary,
            producer: self.producer(),
            content,
            sources: provenance,
        })
    }
}
