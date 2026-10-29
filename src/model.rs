use crate::*;
use async_trait::async_trait;
use futures::{Stream, stream};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering},
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ModelConfig {
    pub model: String,
    pub temperature: Option<f32>,
}
impl Default for ModelConfig {
    fn default() -> Self {
        Self {
            model: "scripted".into(),
            temperature: None,
        }
    }
}
#[derive(Clone, Debug)]
pub struct ModelRequest {
    pub run_id: RunId,
    pub instructions: String,
    pub config: ModelConfig,
    pub history: Vec<HistoryEntry>,
    pub tools: Vec<ToolSpec>,
    pub output_schema: Value,
    pub turn: u32,
    pub max_response_bytes: usize,
    /// Remaining cumulative token allowance; providers should cap generated output accordingly.
    pub remaining_tokens: u64,
}
#[derive(Clone, Debug)]
pub enum ModelEvent {
    TextDelta(String),
    Response(ModelResponse),
}
pub type ModelStream = Pin<Box<dyn Stream<Item = Result<ModelEvent>> + Send>>;
/// Implementations must bound transport buffers and release run-owned work in cleanup.
#[async_trait]
pub trait Model: Send + Sync {
    async fn stream(&self, request: ModelRequest, cancel: CancellationToken)
    -> Result<ModelStream>;
    async fn cleanup(&self, _run_id: RunId) -> Result<()> {
        Ok(())
    }
}
/// A repeatable script selected by the run's cumulative one-based model call number.
/// Shares immutable responses across concurrent conversations.
pub struct ScriptedModel {
    turns: Vec<Vec<ModelEvent>>,
    calls: AtomicUsize,
}
impl ScriptedModel {
    pub fn new(responses: Vec<ModelResponse>) -> Self {
        Self::with_events(
            responses
                .into_iter()
                .map(|r| vec![ModelEvent::Response(r)])
                .collect(),
        )
    }
    pub fn with_events(turns: Vec<Vec<ModelEvent>>) -> Self {
        Self {
            turns,
            calls: AtomicUsize::new(0),
        }
    }
    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}
#[async_trait]
impl Model for ScriptedModel {
    async fn stream(&self, request: ModelRequest, _: CancellationToken) -> Result<ModelStream> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let events = self
            .turns
            .get(request.turn.saturating_sub(1) as usize)
            .ok_or_else(|| Error::Provider("script exhausted".into()))?
            .clone();
        Ok(Box::pin(stream::iter(events.into_iter().map(Ok))))
    }
}
