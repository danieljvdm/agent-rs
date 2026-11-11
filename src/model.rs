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
/// # use agent_rs::ModelConfig;
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
    /// The run the turn belongs to; [`Model::cleanup`] receives the same ID.
    pub run_id: RunId,
    /// The agent's system instructions.
    pub instructions: Arc<str>,
    /// The agent's provider settings.
    pub config: Arc<ModelConfig>,
    /// The turn's context, oldest first: whole entries of the run's prior context and its own
    /// history, selected to fit [`Limits::max_context_bytes`]. It always contains the run's input
    /// and its latest exchange.
    pub history: Vec<HistoryEntry>,
    /// Lower-trust reference data from the agent's `ContextPolicy`, kept separate from history
    /// and instructions. Providers must not present it as either.
    pub derived_context: Vec<Value>,
    /// The tools the model may call, in the order the agent declared them.
    pub tools: Arc<[ToolSpec]>,
    /// JSON Schema of the agent's output type. A final output that does not fit it fails the run.
    pub output_schema: Arc<Value>,
    /// The run's one-based model call number, counting every attempt across restarts.
    pub turn: u32,
    /// [`Limits::max_response_bytes`]: streamed text and the encoded response must each fit it.
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
/// One item of a model turn's stream.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ModelEvent {
    /// Text streamed as it arrives, passed on as [`ProgressKind::TextDelta`](crate::ProgressKind)
    /// and never retained.
    TextDelta(String),
    /// Cumulative tokens this attempt has consumed so far, as the provider reports them.
    ///
    /// Snapshots must not decrease, and [`ModelResponse::usage`] repeats the final totals rather
    /// than adding to them. The engine charges the highest snapshot even when the turn is then
    /// rejected, interrupted, timed out or its cleanup fails, so a provider that reports usage
    /// before its output is complete should emit it. Tokens a provider never reports cannot be
    /// charged.
    Usage(crate::TokenUsage),
    /// The turn's complete response, which must be the stream's last event.
    Response(ModelResponse),
}
/// The events of one model turn, returned by [`Model::stream`].
pub type ModelStream = Pin<Box<dyn Stream<Item = Result<ModelEvent>> + Send>>;
/// A model provider.
///
/// Implementations must bound transport buffers and release run-owned work in cleanup. One call
/// of [`stream`](Self::stream) is one attempt that the engine reserves and charges: do not retry
/// or fall back to another provider inside it.
///
/// Report failures with [`Error::provider`], which keeps the underlying error as its cause. The
/// engine reports any other kind of error from `stream`, its events or `cleanup` as
/// [`Error::Provider`] with the original as its cause, and it fails the run.
#[async_trait]
pub trait Model: Send + Sync {
    /// A stable, non-secret identity of the adapter and the route it calls, such as its API and
    /// endpoint, made part of the agent's [`binding`](crate::Agent::binding) when the agent is
    /// built. A stored run then resumes only through the same route, while rotating a credential
    /// keeps it compatible. It must not change for the life of the model; build a new agent to
    /// change the route. A wrapper must return its inner model's binding or one that includes it.
    ///
    /// The default, `null`, contributes nothing: the application must then change the agent's ID
    /// whenever the model's implementation or route changes.
    fn binding(&self) -> Value {
        Value::Null
    }
    /// Starts one turn. The stream may yield text deltas and usage snapshots, then must yield
    /// exactly one [`ModelEvent::Response`] and end; anything after the response, a stream that
    /// ends without one, a decreasing usage snapshot, or one that exceeds [`ModelRequest::max_response_bytes`] fails the run. The whole
    /// turn runs within [`Limits::operation_timeout_ms`], and `cancel` fires when it ends,
    /// however it ended, for work handed to other tasks.
    async fn stream(&self, request: ModelRequest, cancel: CancellationToken)
    -> Result<ModelStream>;
    /// Releases what the run's turn left behind, after every turn, however it ended, within
    /// [`Limits::cleanup_timeout_ms`]. An error fails the run.
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
    /// A script whose turn `n` streams only the `n`th response. A turn past the end fails with
    /// [`Error::Provider`] "script exhausted".
    pub fn new(responses: Vec<ModelResponse>) -> Self {
        Self::with_events(
            responses
                .into_iter()
                .map(|r| vec![ModelEvent::Response(r)])
                .collect(),
        )
    }
    /// A script whose turn `n` streams the `n`th list of events, for example text deltas
    /// before a response.
    pub fn with_events(turns: Vec<Vec<ModelEvent>>) -> Self {
        Self {
            turns,
            calls: AtomicUsize::new(0),
        }
    }
    /// How many turns have been started, across every run that used this model.
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
