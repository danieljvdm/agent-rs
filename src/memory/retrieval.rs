use super::{DerivedContext, DerivedKind, MAX_DERIVED_ITEMS, MAX_RECORDS, MemoryRecord};
use crate::{Error, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Longest embedding model identity, in bytes.
const MAX_MODEL_BYTES: usize = 900;
/// Most embedding dimensions.
const MAX_DIMENSIONS: usize = 4096;
/// Records scored between cooperative yields, so a deadline or cancellation can interrupt.
const YIELD_EVERY: usize = 32;

/// A vector and the model that produced it.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Embedding {
    /// Model identity, including its revision, dimensions and preprocessing.
    pub model: String,
    /// 1..=4096 finite values, not all zero.
    pub values: Vec<f32>,
}

impl Embedding {
    pub(crate) fn validate(&self) -> Result<()> {
        validate(&self.model, &self.values)
    }
}

fn validate(model: &str, values: &[f32]) -> Result<()> {
    if model.is_empty()
        || model.len() > MAX_MODEL_BYTES
        || values.is_empty()
        || values.len() > MAX_DIMENSIONS
        || values.iter().any(|value| !value.is_finite())
        || values.iter().all(|value| *value == 0.0)
    {
        return Err(Error::input(
            "embedding requires a versioned model and 1..=4096 finite, nonzero dimensions",
        ));
    }
    Ok(())
}

/// Bounded, in-process embedding supplied by the application. Index and query with the same
/// immutable model and preprocessing. It runs inside a model turn's deadline, so it must not
/// make network or model calls hidden from the run's token accounting.
pub trait Embedder: Send + Sync {
    /// Model identity, including its revision, dimensions and preprocessing.
    fn identity(&self) -> &str;
    /// The embedding of `text`.
    fn embed(&self, text: &str) -> Result<Vec<f32>>;
}

/// Ranks a store snapshot for a query, for [`MemoryPolicy::with_retriever`](super::MemoryPolicy::with_retriever).
#[async_trait]
pub trait Retriever: Send + Sync {
    /// Stable, credential-free configuration, included in the policy's binding.
    fn binding(&self) -> Value;
    /// The items to send for `query`, best first, drawn from `records`.
    async fn retrieve(
        &self,
        query: &str,
        records: &[MemoryRecord],
        cancel: CancellationToken,
    ) -> Result<Vec<DerivedContext>>;
}

const PRODUCER_PREFIX: &str = "cosine-v1/";
/// The producer recorded on items retrieved with embeddings from `model`.
pub(crate) fn producer(model: &str) -> String {
    format!("{PRODUCER_PREFIX}{model}")
}
fn produced_by(producer: &str, model: &str) -> bool {
    producer.strip_prefix(PRODUCER_PREFIX) == Some(model)
}

/// Exact cosine similarity over a bounded store snapshot; no vector service required. Only
/// records embedded by the same model with the same dimensions compete; those below the minimum
/// similarity are omitted, and equal scores rank by record ID.
pub struct SemanticRetriever {
    embedder: Arc<dyn Embedder>,
    max_results: usize,
    min_similarity: f64,
}

impl SemanticRetriever {
    /// At most `max_results` (1..=128) records at least `min_similarity` (-1..=1) to the query.
    pub fn new(
        embedder: Arc<dyn Embedder>,
        max_results: usize,
        min_similarity: f64,
    ) -> Result<Self> {
        if max_results == 0
            || max_results > MAX_DERIVED_ITEMS
            || !(-1.0..=1.0).contains(&min_similarity)
        {
            return Err(Error::config(
                "retrieval requires 1..=128 results and similarity in [-1,1]",
            ));
        }
        if embedder.identity().is_empty() || embedder.identity().len() > MAX_MODEL_BYTES {
            return Err(Error::config("embedding model must be versioned"));
        }
        Ok(Self {
            embedder,
            max_results,
            min_similarity,
        })
    }
}

fn norm(values: &[f32]) -> f64 {
    values
        .iter()
        .map(|&v| f64::from(v).powi(2))
        .sum::<f64>()
        .sqrt()
}

#[async_trait]
impl Retriever for SemanticRetriever {
    fn binding(&self) -> Value {
        json!({"retriever": "cosine-v1", "model": self.embedder.identity(),
            "max_results":self.max_results, "min_similarity":self.min_similarity})
    }

    /// Fails with [`Error::Interrupted`] once `cancel` fires.
    async fn retrieve(
        &self,
        query: &str,
        records: &[MemoryRecord],
        cancel: CancellationToken,
    ) -> Result<Vec<DerivedContext>> {
        if cancel.is_cancelled() {
            return Err(Error::Interrupted);
        }
        let model = self.embedder.identity();
        let query = self.embedder.embed(query)?;
        validate(model, &query)?;
        let query_norm = norm(&query);
        let mut ranked = Vec::new();
        for (index, record) in records.iter().take(MAX_RECORDS).enumerate() {
            if index % YIELD_EVERY == YIELD_EVERY - 1 {
                tokio::task::yield_now().await;
                if cancel.is_cancelled() {
                    return Err(Error::Interrupted);
                }
            }
            let Some(embedding) = &record.embedding else {
                continue;
            };
            if record.context.kind != DerivedKind::Retrieved
                || !produced_by(&record.context.producer, model)
                || embedding.model != model
                || embedding.values.len() != query.len()
                || embedding.validate().is_err()
            {
                continue;
            }
            let dot = embedding
                .values
                .iter()
                .zip(&query)
                .map(|(&a, &b)| f64::from(a) * f64::from(b))
                .sum::<f64>();
            let similarity = (dot / (norm(&embedding.values) * query_norm)).clamp(-1.0, 1.0);
            if similarity >= self.min_similarity {
                ranked.push((similarity, record));
            }
        }
        ranked.sort_by(|(a, ar), (b, br)| b.total_cmp(a).then_with(|| ar.id.cmp(&br.id)));
        Ok(ranked
            .into_iter()
            .take(self.max_results)
            .map(|(_, record)| record.context.clone())
            .collect())
    }
}
