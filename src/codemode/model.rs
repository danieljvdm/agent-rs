//! [`ScriptModel`]: a [`Model`] whose turns run a script instead of calling a provider.
use super::declarations::identifier;
use super::replay::{self, Outcome};
use super::runtime::{ScriptCall, ScriptFunction, ScriptLimits, ScriptRuntime};
use crate::{
    ContextPolicy, Error, HistoryEntry, Model, ModelEvent, ModelRequest, ModelResponse,
    ModelStream, Result, RunId, Tool,
};
use async_trait::async_trait;
use futures::stream;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

/// A [`Model`] that runs the script in its run's input, `{code}`, in a [`ScriptRuntime`].
///
/// Each turn starts the script afresh and replays it against the rounds its run's history
/// recorded, answering each call with the recorded result. When the history runs out, the
/// script's next calls are the turn's tool calls, `c0`, `c1`, … numbered on from the recorded
/// ones; a script that returned is the turn's final output `{result, logs}`; and a script that
/// threw is the final output `{error, logs, calls}`. A script whose calls differ from the
/// recorded ones fails the run.
///
/// Before a call becomes a tool call, its tool must be one of the model's and its arguments must
/// pass [`Tool::validate`]; a call that fails either is answered to the script as an error and is
/// never dispatched. It reports no token usage. A turn that ends before its script does, such as
/// at its deadline, interrupts the interpreter, and [`cleanup`](Model::cleanup) waits until it
/// has stopped.
///
/// [`CodeMode`](super::CodeMode) builds the agent this model belongs to; use it directly only to
/// build such an agent by hand.
pub struct ScriptModel {
    inner: Arc<Inner>,
}
struct Inner {
    /// For each run with a turn in flight, a receiver that closes when its interpreter's thread
    /// is done, so `cleanup` can wait for it.
    running: Mutex<HashMap<RunId, oneshot::Receiver<()>>>,
    runtime: Arc<dyn ScriptRuntime>,
    tools: HashMap<String, Arc<dyn Tool>>,
    functions: Vec<ScriptFunction>,
    limits: ScriptLimits,
    binding: Value,
}
impl ScriptModel {
    /// A model that runs scripts in `runtime`, calling `tools`, within `limits`. Fails with
    /// [`Error::Config`] if two tools share a name, or their names map to the same identifier.
    pub fn new(
        runtime: Arc<dyn ScriptRuntime>,
        tools: impl IntoIterator<Item = Arc<dyn Tool>>,
        limits: ScriptLimits,
    ) -> Result<Self> {
        let mut by_name = HashMap::new();
        let mut identifiers = HashMap::new();
        let mut functions = Vec::new();
        for tool in tools {
            let name = tool.spec().name.clone();
            let function = identifier(&name);
            if let Some(other) = identifiers.insert(function.clone(), name.clone()) {
                return Err(Error::config(format!(
                    "code mode tools {other} and {name} both map to the function {function}"
                )));
            }
            functions.push(ScriptFunction {
                identifier: function,
                tool: name.clone(),
            });
            by_name.insert(name, tool);
        }
        let binding = json!({
            "contract": "codemode-v1",
            "runtime": runtime.binding(),
            "limits": limits,
        });
        Ok(Self {
            inner: Arc::new(Inner {
                running: Mutex::default(),
                runtime,
                tools: by_name,
                functions,
                limits,
                binding,
            }),
        })
    }
    /// The functions a script can call, in the order the tools were given.
    pub fn functions(&self) -> &[ScriptFunction] {
        &self.inner.functions
    }
}
impl Inner {
    /// Refuses a call the engine would reject: an unknown tool or invalid arguments.
    fn check(&self, call: &ScriptCall) -> std::result::Result<(), String> {
        let tool =
            (self.tools.get(&call.tool)).ok_or_else(|| format!("unknown tool {}", call.tool))?;
        tool.validate(&call.arguments)
            .map_err(|error| format!("invalid arguments for {}: {error}", call.tool))
    }
    fn turn(&self, history: &[HistoryEntry], cancel: CancellationToken) -> Result<Outcome> {
        let journal = replay::journal(history)?;
        let mut session =
            (self.runtime).start(journal.code, &self.functions, &self.limits, cancel)?;
        replay::replay(&mut *session, &journal, &|call| self.check(call))
    }
}
#[async_trait]
impl Model for ScriptModel {
    fn binding(&self) -> Value {
        self.inner.binding.clone()
    }
    async fn stream(
        &self,
        request: ModelRequest,
        cancel: CancellationToken,
    ) -> Result<ModelStream> {
        let inner = self.inner.clone();
        let history = request.history;
        let max_response_bytes = request.max_response_bytes;
        let (done, finished) = oneshot::channel::<()>();
        (self.inner.running.lock())
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(request.run_id, finished);
        // A session is not `Send`: it is made, driven and dropped on one blocking thread. If the
        // turn ends first, `cancel` interrupts it and `cleanup` waits for the thread.
        let outcome = tokio::task::spawn_blocking(move || {
            let _done = done;
            inner.turn(&history, cancel)
        })
        .await
        .map_err(|error| Error::provider(format!("script turn panicked: {error}")))??;
        let response = match outcome {
            Outcome::Calls(calls) => ModelResponse::tools(calls),
            ended => fit(ended, max_response_bytes),
        };
        Ok(Box::pin(stream::iter([Ok(ModelEvent::Response(response))])))
    }
    async fn cleanup(&self, run_id: RunId) -> Result<()> {
        let finished = (self.inner.running.lock())
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&run_id);
        if let Some(finished) = finished {
            // Closed, whether sent or dropped, once the thread is done.
            let _ = finished.await;
        }
        Ok(())
    }
}

/// The final response of a script that ended, within `max_bytes`: an output too large for it,
/// which would fail the run, becomes an error naming its size and keeping the start of the
/// script's own error, so the parent's model still learns why it failed.
fn fit(ended: Outcome, max_bytes: usize) -> ModelResponse {
    let error = match &ended {
        Outcome::Threw { error, .. } => Some(error.clone()),
        _ => None,
    };
    let response = ModelResponse::final_output(ended.output().expect("the script ended"));
    let size = serde_json::to_vec(&response).map_or(usize::MAX, |bytes| bytes.len());
    if size <= max_bytes {
        return response;
    }
    let mut message =
        format!("the script's output is {size} bytes, over the response limit of {max_bytes}");
    if let Some(error) = error {
        let mut end = error.len().min(max_bytes / 4);
        while !error.is_char_boundary(end) {
            end -= 1;
        }
        message = format!("{message}; it threw: {}", &error[..end]);
    }
    ModelResponse::final_output(json!({"error": message, "logs": [], "calls": []}))
}

/// A [`ContextPolicy`] that pins every entry, so a code mode run's history is never truncated: a
/// replay needs every round. A history too large for `max_context_bytes` fails the run with
/// [`Error::Context`] instead.
pub struct PinAll;
#[async_trait]
impl ContextPolicy for PinAll {
    fn binding(&self) -> Value {
        json!("pin-all-v1")
    }
    fn pinned(&self, _entry: &HistoryEntry) -> bool {
        true
    }
}
