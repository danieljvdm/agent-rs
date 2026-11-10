#![doc = include_str!("../../docs/memory.md")]
mod retrieval;
mod store;
mod summary;

pub use retrieval::{Embedder, Embedding, Retriever, SemanticRetriever};
pub use store::{InMemorySourceStore, SourceStore};
pub use summary::{ExtractiveSummarizer, Summarizer};

use crate::{ContextPolicy, ContextRequest, Error, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{borrow::Cow, sync::Arc};
use tokio_util::sync::CancellationToken;

/// The format version of [`DerivedContext`]. Items of another version are never sent.
pub const DERIVED_CONTEXT_VERSION: u32 = 1;
/// Largest source snapshot or derived content, in bytes.
pub(crate) const MAX_SOURCE_BYTES: usize = 1_048_576;
/// Largest store read, in records.
pub(crate) const MAX_RECORDS: usize = 4096;
/// Most derived items [`MemoryPolicy`] returns for one turn, and most sources one item cites.
pub(crate) const MAX_DERIVED_ITEMS: usize = 128;
/// Longest scope, ID, revision or producer, in bytes.
const MAX_KEY_BYTES: usize = 1024;

/// One source snapshot in an application-owned namespace. A revision is never reused after an
/// edit or deletion; the built-in stores assign opaque revisions.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceRef {
    /// The namespace: the conversation ID the source belongs to.
    pub scope: String,
    /// The source's ID within its scope.
    pub id: String,
    /// The snapshot's revision.
    pub revision: String,
}

/// What a [`DerivedContext`] item is.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub enum DerivedKind {
    /// A summary of one or more sources.
    Summary,
    /// A source retrieved for the current input.
    Retrieved,
}

/// One item of derived context: untrusted reference data, even when its sources were approved
/// for indexing. [`MemoryPolicy`] sends each item to the model as this type's JSON encoding,
/// in `ModelRequest::derived_context`; providers must keep it apart from instructions and
/// authoritative history.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DerivedContext {
    /// [`DERIVED_CONTEXT_VERSION`] for items this crate produces.
    pub version: u32,
    /// Summary or retrieved source.
    pub kind: DerivedKind,
    /// Algorithm or model identity and version; changing it invalidates cached items.
    pub producer: String,
    /// The text the model reads.
    pub content: String,
    /// The source snapshots the content came from, all in one scope.
    pub sources: Vec<SourceRef>,
}

impl DerivedContext {
    /// Whether this item is of the current version, bounded, and drawn only from `scope`.
    pub(crate) fn valid_for(&self, scope: &str) -> bool {
        self.version == DERIVED_CONTEXT_VERSION
            && !self.producer.is_empty()
            && self.producer.len() <= MAX_KEY_BYTES
            && !self.content.is_empty()
            && !self.sources.is_empty()
            && self.sources.len() <= MAX_DERIVED_ITEMS
            && self.sources.iter().all(|source| {
                source.scope == scope
                    && !source.id.is_empty()
                    && source.id.len() <= MAX_KEY_BYTES
                    && !source.revision.is_empty()
                    && source.revision.len() <= MAX_KEY_BYTES
            })
    }
}

/// Whether a source may be indexed. Approval is application policy, never authority to
/// instruct a model.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum SourceTrust {
    /// Not approved; never summarized or retrieved.
    #[default]
    Untrusted,
    /// Approved for indexing.
    Approved,
}

/// A source snapshot as a [`SourceStore`] holds it.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemorySource {
    /// Scope, ID and revision.
    pub reference: SourceRef,
    /// The source text.
    pub text: String,
    /// Whether it may be indexed.
    pub trust: SourceTrust,
}

/// A derived item a [`SourceStore`] holds, with the embedding a retrieved item is ranked by.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MemoryRecord {
    /// The record's ID within its scope.
    pub id: String,
    /// The item, whose sources fix the record's scope.
    pub context: DerivedContext,
    /// Required for [`DerivedKind::Retrieved`] records; absent for summaries.
    pub embedding: Option<Embedding>,
}

impl MemoryRecord {
    /// The scope of the record's first source; [`validate`](Self::validate) checks all agree.
    pub(crate) fn scope(&self) -> &str {
        self.context
            .sources
            .first()
            .map_or("", |source| source.scope.as_str())
    }

    pub(crate) fn validate(&self) -> Result<()> {
        validate_key(self.scope(), &self.id)?;
        if !self.context.valid_for(self.scope()) || self.context.content.len() > MAX_SOURCE_BYTES {
            return Err(Error::input("invalid derived memory record"));
        }
        // At most 128 sources, so a pairwise check is cheap and allocates nothing.
        let sources = &self.context.sources;
        if (1..sources.len()).any(|i| sources[..i].iter().any(|s| s.id == sources[i].id)) {
            return Err(Error::input("duplicate memory source"));
        }
        match (&self.context.kind, &self.embedding) {
            (DerivedKind::Summary, None) => Ok(()),
            (DerivedKind::Retrieved, Some(embedding)) => embedding.validate(),
            _ => Err(Error::input(
                "retrieved records require an embedding; summaries do not",
            )),
        }
    }
}

pub(crate) fn validate_key(scope: &str, id: &str) -> Result<()> {
    if scope.is_empty() || scope.len() > MAX_KEY_BYTES || id.is_empty() || id.len() > MAX_KEY_BYTES
    {
        return Err(Error::input(
            "memory scope and ID must contain 1..=1024 bytes",
        ));
    }
    Ok(())
}

pub(crate) fn validate_source(scope: &str, id: &str, text: &str) -> Result<()> {
    validate_key(scope, id)?;
    if text.trim().is_empty() || text.len() > MAX_SOURCE_BYTES {
        return Err(Error::input("memory source must contain 1..=1048576 bytes"));
    }
    Ok(())
}

pub(crate) fn validate_limit(limit: usize) -> Result<()> {
    if limit == 0 || limit > MAX_RECORDS {
        return Err(Error::input("memory read limit must be 1..=4096"));
    }
    Ok(())
}

/// A [`ContextPolicy`] that serves current summaries and, optionally, sources retrieved for the
/// run's input, from a [`SourceStore`].
///
/// Each turn it reads the conversation's records once: that read is the freshness point, so a
/// source edited after it affects the next turn, not a request already sent. It then checks
/// every item itself, since the engine does not look inside derived context: an item is sent
/// only if it is of [`DERIVED_CONTEXT_VERSION`], cites only sources in the run's conversation,
/// and fits the derived-context cap on its own. At most 128 items are returned, retrieved items
/// first, then summaries; the engine keeps those that fit the cap, in that order.
///
/// A store failure is returned as the store's error: [`Error::Storage`] leaves the run
/// resumable, as `drive` does for a failed checkpoint.
pub struct MemoryPolicy {
    store: Arc<dyn SourceStore>,
    summary_producer: Option<String>,
    max_records: usize,
    retriever: Option<Arc<dyn Retriever>>,
}

impl MemoryPolicy {
    /// Reads at most `max_records` (1..=4096) records per turn from `store`, in record-ID order.
    /// Serves nothing until [`with_summaries`](Self::with_summaries) or
    /// [`with_retriever`](Self::with_retriever) enables a kind of item.
    pub fn new(store: Arc<dyn SourceStore>, max_records: usize) -> Result<Self> {
        if max_records == 0 || max_records > MAX_RECORDS {
            return Err(Error::config("memory read limit must be 1..=4096"));
        }
        Ok(Self {
            store,
            summary_producer: None,
            max_records,
            retriever: None,
        })
    }

    /// Serves summaries made by `summary_producer`, a [`Summarizer::producer`]; summaries by any
    /// other producer are ignored until they are rebuilt.
    pub fn with_summaries(mut self, summary_producer: impl Into<String>) -> Result<Self> {
        let summary_producer = summary_producer.into();
        if summary_producer.is_empty() || summary_producer.len() > MAX_KEY_BYTES {
            return Err(Error::config(
                "a bounded summary producer/version is required",
            ));
        }
        self.summary_producer = Some(summary_producer);
        Ok(self)
    }

    /// Serves the records `retriever` ranks for the run's input, before any summaries.
    pub fn with_retriever(mut self, retriever: Arc<dyn Retriever>) -> Self {
        self.retriever = Some(retriever);
        self
    }
}

#[async_trait]
impl ContextPolicy for MemoryPolicy {
    fn binding(&self) -> Value {
        json!({"memory_policy":1, "summary_producer":self.summary_producer,
            "max_records":self.max_records, "retriever":self.retriever.as_ref().map(|r|r.binding())})
    }

    async fn prepare(
        &self,
        request: ContextRequest<'_>,
        cancel: CancellationToken,
    ) -> Result<Vec<Value>> {
        let scope = request.conversation_id;
        let mut records = self.store.load(scope, self.max_records).await?;
        records.truncate(self.max_records);
        records.retain(|record| record.scope() == scope && record.validate().is_ok());
        let mut items = match &self.retriever {
            // Retrieval ranks the snapshot, so an empty one needs no query embedding.
            Some(retriever) if !records.is_empty() => {
                let query = match request.input {
                    Value::String(text) => Cow::Borrowed(text.as_str()),
                    other => Cow::Owned(other.to_string()),
                };
                retriever.retrieve(&query, &records, cancel).await?
            }
            _ => Vec::new(),
        };
        if let Some(producer) = &self.summary_producer {
            items.extend(records.into_iter().filter_map(|record| {
                (record.context.kind == DerivedKind::Summary
                    && record.context.producer == *producer)
                    .then_some(record.context)
            }));
        }
        items
            .into_iter()
            .filter(|item| item.valid_for(scope) && item.content.len() <= request.max_bytes)
            .take(MAX_DERIVED_ITEMS)
            .map(|item| serde_json::to_value(item).map_err(Error::context))
            .collect()
    }
}

/// Summarizes `sources` and publishes the summary as record `id`, only if every source still
/// has the same revision and approval. A concurrent edit or deletion fails with
/// [`Error::Stale`]; reread the sources and rebuild.
pub async fn index_summary(
    store: &dyn SourceStore,
    id: &str,
    sources: &[MemorySource],
    summarizer: &dyn Summarizer,
) -> Result<()> {
    let context = summarizer.summarize(sources)?;
    store
        .publish(MemoryRecord {
            id: id.into(),
            context,
            embedding: None,
        })
        .await
}

/// Embeds an approved `source` and publishes it as retrievable record `id`, with the same
/// freshness check as [`index_summary`]. Indexing is application work, outside any run's
/// budgets.
pub async fn index_source(
    store: &dyn SourceStore,
    id: &str,
    source: &MemorySource,
    embedder: &dyn Embedder,
) -> Result<()> {
    if source.trust != SourceTrust::Approved {
        return Err(Error::input("source is not approved for indexing"));
    }
    let embedding = Embedding {
        model: embedder.identity().into(),
        values: embedder.embed(&source.text)?,
    };
    embedding.validate()?;
    let context = DerivedContext {
        version: DERIVED_CONTEXT_VERSION,
        kind: DerivedKind::Retrieved,
        producer: retrieval::producer(embedder.identity()),
        content: source.text.clone(),
        sources: vec![source.reference.clone()],
    };
    store
        .publish(MemoryRecord {
            id: id.into(),
            context,
            embedding: Some(embedding),
        })
        .await
}
