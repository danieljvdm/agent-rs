use crate::*;
use async_trait::async_trait;
use schemars::{JsonSchema, schema_for};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::{future::Future, marker::PhantomData, sync::Arc};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct ToolContext {
    pub run_id: RunId,
    pub call_id: String,
    pub idempotency_key: String,
    pub cancel: CancellationToken,
}
#[derive(Clone, Debug)]
pub enum ToolOutput<T> {
    Complete(T),
    /// A complete, reported tool failure. Unlike an execution error, its outcome is known.
    /// The engine records the payload with `ToolResult::is_error`, without retrying it.
    Failed(T),
    Wait(WaitRequest),
}
/// One dispatch boundary the model can call.
///
/// # Outcomes
///
/// `execute` reports what the engine knows about the call's effect:
/// - `Ok(ToolOutput::Complete(value))`: it finished and succeeded.
/// - `Ok(ToolOutput::Failed(value))`: it finished and failed, and the tool knows what did or did
///   not happen. The value is recorded as an error result for the model. It is never retried.
/// - `Ok(ToolOutput::Wait(request))`: it needs application input or a timer before it completes.
///
/// Every `Ok` payload must fit `Limits::max_tool_output_bytes` once encoded; a larger one is
/// handled like an `Err`.
/// - `Err(error)`: the outcome is not known. A `RetryPolicy::ReadOnly` tool has no effect to lose,
///   so the error is recorded for the model. Any other tool becomes `OperationStatus::Unknown`,
///   because its effect may or may not have happened; that stops further dispatch until the
///   application reconciles it. Timeouts and failed cleanups are treated the same way.
///
/// Return `Failed` rather than `Err` whenever the failure is known, for example a rejected
/// request that changed nothing, so the run can continue without reconciliation.
///
/// # Concurrency
///
/// All tool calls of one run execute concurrently on the task that drives the run, up to
/// `Limits::max_parallel_tools`. A tool that blocks the thread or runs CPU-bound work inside
/// `execute` stalls every sibling call, and no timeout can interrupt it until it yields. Move
/// such work to `tokio::task::spawn_blocking` and await the handle.
#[async_trait]
pub trait Tool: Send + Sync {
    fn spec(&self) -> &ToolSpec;
    fn validate(&self, arguments: &Value) -> Result<()>;
    async fn execute(&self, arguments: Value, context: ToolContext) -> Result<ToolOutput<Value>>;
    /// Called after execution, including interruption. Do not detach child work; join it here.
    async fn cleanup(&self, _context: ToolContext) -> Result<()> {
        Ok(())
    }
}
struct TypedTool<A, O, F> {
    spec: ToolSpec,
    handler: F,
    marker: PhantomData<fn(A) -> O>,
}
#[async_trait]
impl<A, O, F, Fut> Tool for TypedTool<A, O, F>
where
    A: DeserializeOwned + Send + 'static,
    O: Serialize + Send + 'static,
    F: Fn(A, ToolContext) -> Fut + Send + Sync,
    Fut: Future<Output = Result<ToolOutput<O>>> + Send,
{
    fn spec(&self) -> &ToolSpec {
        &self.spec
    }
    fn validate(&self, arguments: &Value) -> Result<()> {
        serde_json::from_value::<A>(arguments.clone())
            .map(|_| ())
            .map_err(|e| Error::Invalid(e.to_string()))
    }
    async fn execute(&self, arguments: Value, context: ToolContext) -> Result<ToolOutput<Value>> {
        let input = serde_json::from_value(arguments).map_err(|e| Error::Invalid(e.to_string()))?;
        match (self.handler)(input, context).await? {
            ToolOutput::Complete(output) => Ok(ToolOutput::Complete(
                serde_json::to_value(output).map_err(|e| Error::Tool(e.to_string()))?,
            )),
            ToolOutput::Failed(output) => Ok(ToolOutput::Failed(
                serde_json::to_value(output).map_err(|e| Error::Tool(e.to_string()))?,
            )),
            ToolOutput::Wait(wait) => Ok(ToolOutput::Wait(wait)),
        }
    }
}
/// Register one heterogeneous dispatch boundary with typed arguments and results.
pub fn typed_tool<A, O, F, Fut>(mut spec: ToolSpec, handler: F) -> Arc<dyn Tool>
where
    A: DeserializeOwned + JsonSchema + Send + 'static,
    O: Serialize + Send + 'static,
    F: Fn(A, ToolContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<ToolOutput<O>>> + Send + 'static,
{
    spec.input_schema = serde_json::to_value(schema_for!(A)).expect("schema serialization");
    Arc::new(TypedTool::<A, O, F> {
        spec,
        handler,
        marker: PhantomData,
    })
}
#[async_trait]
pub trait Authorizer: Send + Sync {
    async fn authorize(&self, run: &RunState, call: &ToolCall) -> Result<bool>;
}
pub struct AllowAll;
#[async_trait]
impl Authorizer for AllowAll {
    async fn authorize(&self, _: &RunState, _: &ToolCall) -> Result<bool> {
        Ok(true)
    }
}
