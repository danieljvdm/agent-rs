use crate::{Error, HistoryEntry, Limits, ModelResponse, Result, RunId, ToolSpec};
use async_trait::async_trait;
use futures::{Stream, stream};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio_util::sync::CancellationToken;

/// Provider settings for every model turn of an agent, set with
/// [`AgentBuilder::config`](crate::AgentBuilder::config) and passed to the provider in
/// [`ModelRequest::config`]:
///
/// ```
/// # use yielded_agent::ModelConfig;
/// let config = ModelConfig::new("gpt-4.1-mini").with_temperature(0.0);
/// ```
///
/// It is part of the agent's [`binding`](crate::Agent::binding), so a stored run resumes only
/// under the configuration it was accepted with.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub struct ModelConfig {
    /// The provider's name for the model. Defaults to `"scripted"`, a placeholder for
    /// [`ScriptedModel`], which ignores the configuration.
    pub model: String,
    /// Sampling temperature, or `None` for the provider's default. Defaults to `None`.
    pub temperature: Option<f32>,
}
impl Default for ModelConfig {
    fn default() -> Self {
        Self::new("scripted")
    }
}
impl ModelConfig {
    /// Settings for `model`, with the provider's default temperature.
    pub fn new(model: impl Into<String>) -> Self {
        Self {
            model: model.into(),
            temperature: None,
        }
    }
    /// Sets [`temperature`](Self::temperature).
    pub fn with_temperature(mut self, temperature: f32) -> Self {
        self.temperature = Some(temperature);
        self
    }
}
/// One model turn. Instructions, model configuration, tools and the output schema are the
/// agent's own, shared rather than copied on every turn.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ModelRequest {
    pub run_id: RunId,
    pub instructions: Arc<str>,
    pub config: Arc<ModelConfig>,
    pub history: Vec<HistoryEntry>,
    /// Lower-trust reference data from the agent's `ContextPolicy`, kept separate from history
    /// and instructions. Providers must not present it as either.
    pub derived_context: Vec<Value>,
    pub tools: Arc<[ToolSpec]>,
    pub output_schema: Arc<Value>,
    pub turn: u32,
    pub max_response_bytes: usize,
    /// Remaining cumulative token allowance; providers should cap generated output accordingly.
    pub remaining_tokens: u64,
}
impl ModelRequest {
    /// A first turn of `run_id` over `history`, for calling a model outside the engine, such as
    /// in a provider's own tests. Set further fields on the result. It has no instructions,
    /// tools or derived context, the default [`ModelConfig`], an output schema that accepts any
    /// JSON (`true`), and the byte and token allowances of [`Limits::default`]. The engine builds
    /// the request for every turn it runs.
    pub fn new(run_id: RunId, history: Vec<HistoryEntry>) -> Self {
        let limits = Limits::default();
        Self {
            run_id,
            instructions: Arc::from(""),
            config: Arc::default(),
            history,
            derived_context: Vec::new(),
            tools: Arc::new([]),
            output_schema: Arc::new(Value::Bool(true)),
            turn: 1,
            max_response_bytes: limits.max_response_bytes,
            remaining_tokens: limits.max_tokens,
        }
    }
}
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ModelEvent {
    TextDelta(String),
    Response(ModelResponse),
}
pub type ModelStream = Pin<Box<dyn Stream<Item = Result<ModelEvent>> + Send>>;
/// Implementations must bound transport buffers and release run-owned work in cleanup.
///
/// Report failures with [`Error::provider`], which keeps the underlying error as its cause. The
/// engine reports any other kind of error from `stream`, its events or `cleanup` as
/// [`Error::Provider`] with the original as its cause, and it fails the run.
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
            .ok_or_else(|| Error::provider("script exhausted"))?
            .clone();
        Ok(Box::pin(stream::iter(events.into_iter().map(Ok))))
    }
}
