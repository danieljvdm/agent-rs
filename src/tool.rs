use crate::{Error, Result, RunId, RunState, ToolCall, ToolSpec, WaitRequest};
use async_trait::async_trait;
use schemars::{JsonSchema, schema_for};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::{future::Future, marker::PhantomData, sync::Arc};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
#[non_exhaustive]
pub struct ToolContext {
    pub run_id: RunId,
    pub call_id: String,
    pub idempotency_key: String,
    pub cancel: CancellationToken,
}
impl ToolContext {
    /// A context for calling a tool outside the engine, such as in the tool's own tests, with a
    /// fresh cancellation token. The engine builds the context for every call it dispatches.
    pub fn new(
        run_id: RunId,
        call_id: impl Into<String>,
        idempotency_key: impl Into<String>,
    ) -> Self {
        Self {
            run_id,
            call_id: call_id.into(),
            idempotency_key: idempotency_key.into(),
            cancel: CancellationToken::new(),
        }
    }
}
#[derive(Clone, Debug)]
#[non_exhaustive]
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
/// # Errors
///
/// Build an execution or cleanup error with [`Error::tool`], which keeps the underlying error
/// as its cause: `client.send(request).await.map_err(Error::tool)?`. `validate` rejects the
/// model's arguments with [`Error::model_output`], which fails the run.
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
        // `&Value` is a deserializer, so checking the arguments never copies them.
        A::deserialize(arguments)
            .map(|_| ())
            .map_err(Error::model_output)
    }
    async fn execute(&self, arguments: Value, context: ToolContext) -> Result<ToolOutput<Value>> {
        let input = serde_json::from_value(arguments).map_err(Error::model_output)?;
        match (self.handler)(input, context).await? {
            ToolOutput::Complete(output) => Ok(ToolOutput::Complete(
                serde_json::to_value(output).map_err(Error::tool)?,
            )),
            ToolOutput::Failed(output) => Ok(ToolOutput::Failed(
                serde_json::to_value(output).map_err(Error::tool)?,
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
/// Decides whether a tool call may run, before it is dispatched. Denial, an error and a timeout
/// each fail the run with [`Error::Authorization`]; an error is kept as its cause, whatever its
/// kind, so an authorizer's own storage failure is never taken for the run's.
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
