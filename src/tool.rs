use crate::{Error, Result, RunId, RunState, ToolCall, ToolSpec, WaitRequest};
use async_trait::async_trait;
use schemars::{JsonSchema, schema_for};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::{future::Future, marker::PhantomData, sync::Arc};
use tokio_util::sync::CancellationToken;

/// What a tool knows about the call it executes, passed to [`Tool::execute`] and
/// [`Tool::cleanup`].
#[derive(Clone)]
#[non_exhaustive]
pub struct ToolContext {
    /// The run making the call.
    pub run_id: RunId,
    /// The [`ToolCall::id`] of the call.
    pub call_id: String,
    /// Stable for this call across retries and restarts, and distinct from every other call's.
    /// Pass it to external systems to deduplicate effects; see [`RetryPolicy`](crate::RetryPolicy).
    pub idempotency_key: String,
    /// Cancelled when the call's execution ends, however it ended, before `cleanup` runs, so
    /// work the tool handed to other tasks can stop. On a timeout or interruption the engine
    /// also drops the `execute` future.
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
/// What a tool's execution reports when its outcome is known; see [`Tool`] for what an error
/// means instead. `T` is the output: a JSON value for [`Tool::execute`], or the handler's own
/// type for [`typed_tool`].
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ToolOutput<T> {
    /// The call finished and succeeded; the payload is its result.
    Complete(T),
    /// A complete, reported tool failure. Unlike an execution error, its outcome is known.
    /// The engine records the payload with `ToolResult::is_error`, without retrying it.
    Failed(T),
    /// The call cannot complete until the application replies or a timer passes.
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
    /// The tool's spec. It must not change while the agent exists: the agent's binding and every
    /// recorded operation keep a copy of it.
    fn spec(&self) -> &ToolSpec;
    /// JSON Schema of a successful result, or `None`, the default, to leave results undescribed.
    /// Code mode declares the tool's return type from it. It is not part of the spec, so it is
    /// neither stored with a run nor part of the agent's binding, and the engine does not check
    /// a result against it: a reported failure, or an application's reply to a
    /// [`WaitRequest::Input`], may have another shape. A tool that wraps another should return
    /// the inner tool's.
    fn output_schema(&self) -> Option<&Value> {
        None
    }
    /// Checks the model's arguments before any call of the batch starts. An error fails the run;
    /// report it with [`Error::model_output`].
    fn validate(&self, arguments: &Value) -> Result<()>;
    /// Runs the call within [`Limits::operation_timeout_ms`](crate::Limits). See the trait's
    /// documentation for what each outcome means.
    async fn execute(&self, arguments: Value, context: ToolContext) -> Result<ToolOutput<Value>>;
    /// Called after execution, including interruption. Do not detach child work; join it here.
    /// It runs within [`Limits::cleanup_timeout_ms`](crate::Limits); an error or a timeout
    /// leaves the call's outcome unknown, as an execution error does.
    async fn cleanup(&self, _context: ToolContext) -> Result<()> {
        Ok(())
    }
}
struct TypedTool<A, O, F> {
    spec: ToolSpec,
    output_schema: Value,
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
    fn output_schema(&self) -> Option<&Value> {
        Some(&self.output_schema)
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
///
/// The tool's input schema is derived from `A`, replacing `spec.input_schema`, and its
/// [`output_schema`](Tool::output_schema) is derived from `O`. Arguments that do not deserialize
/// as `A` are rejected by `validate`, which fails the run; an output that cannot be encoded as
/// JSON is an execution error.
pub fn typed_tool<A, O, F, Fut>(mut spec: ToolSpec, handler: F) -> Arc<dyn Tool>
where
    A: DeserializeOwned + JsonSchema + Send + 'static,
    O: Serialize + JsonSchema + Send + 'static,
    F: Fn(A, ToolContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<ToolOutput<O>>> + Send + 'static,
{
    spec.input_schema = serde_json::to_value(schema_for!(A)).expect("schema serialization");
    Arc::new(TypedTool::<A, O, F> {
        spec,
        output_schema: serde_json::to_value(schema_for!(O)).expect("schema serialization"),
        handler,
        marker: PhantomData,
    })
}
/// Decides whether a tool call may run, before it is dispatched. Denial, an error and a timeout
/// each fail the run with [`Error::Authorization`]; an error is kept as its cause, whatever its
/// kind, so an authorizer's own storage failure is never taken for the run's.
#[async_trait]
pub trait Authorizer: Send + Sync {
    /// Whether `call` may run now, as part of `run`. Asked before every execution, retries
    /// included, after any approval, within [`Limits::operation_timeout_ms`](crate::Limits).
    async fn authorize(&self, run: &RunState, call: &ToolCall) -> Result<bool>;
}
/// Allows every call; the default [`Authorizer`].
pub struct AllowAll;
#[async_trait]
impl Authorizer for AllowAll {
    async fn authorize(&self, _: &RunState, _: &ToolCall) -> Result<bool> {
        Ok(true)
    }
}
