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
    Wait(WaitRequest),
}
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
