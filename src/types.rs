use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use uuid::Uuid;
pub const STATE_VERSION: u32 = 1;
pub type RunId = Uuid;
pub type Result<T> = std::result::Result<T, Error>;
#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid input: {0}")]
    Invalid(String),
    #[error("provider: {0}")]
    Provider(String),
    #[error("tool: {0}")]
    Tool(String),
    #[error("storage: {0}")]
    Storage(String),
    #[error("conflicting idempotency key")]
    IdempotencyConflict,
    #[error("conversation already has an active run")]
    ConversationBusy,
    #[error("database already has an owner")]
    DatabaseBusy,
    #[error("stale execution generation or checkpoint revision")]
    Stale,
    #[error("incompatible state or agent binding: {0}")]
    Incompatible(String),
    #[error("run not found")]
    NotFound,
    #[error("operation is not waiting for this resolution")]
    NotWaiting,
    #[error("execution interrupted")]
    Interrupted,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Limits {
    pub max_model_calls: u32,
    pub max_tool_calls: u32,
    pub max_tokens: u64,
    pub max_parallel_tools: usize,
    pub max_response_bytes: usize,
    pub max_tool_output_bytes: usize,
    pub max_context_bytes: usize,
    pub operation_timeout_ms: u64,
    pub cleanup_timeout_ms: u64,
    pub active_budget_ms: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_model_calls: 32,
            max_tool_calls: 128,
            max_tokens: 100_000,
            max_parallel_tools: 4,
            max_response_bytes: 1_048_576,
            max_tool_output_bytes: 262_144,
            max_context_bytes: 2_097_152,
            operation_timeout_ms: 30_000,
            cleanup_timeout_ms: 1_000,
            active_budget_ms: 600_000,
        }
    }
}
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Usage {
    pub model_calls: u32,
    pub tool_calls: u32,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub active_ms: u64,
    pub events: u64,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TokenUsage {
    pub input: u64,
    pub output: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub arguments: Value,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ModelResponse {
    pub output: Option<Value>,
    pub tools: Vec<ToolCall>,
    pub usage: TokenUsage,
}
impl ModelResponse {
    pub fn final_output(value: Value) -> Self {
        Self {
            output: Some(value),
            tools: vec![],
            usage: TokenUsage::default(),
        }
    }
    pub fn tools(tools: Vec<ToolCall>) -> Self {
        Self {
            output: None,
            tools,
            usage: TokenUsage::default(),
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ToolResult {
    pub call_id: String,
    pub value: Value,
    pub is_error: bool,
}
/// Exchanges are indivisible during context selection; calls never lose their results.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum HistoryEntry {
    Input(Value),
    Exchange {
        response: ModelResponse,
        results: Vec<ToolResult>,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum RetryPolicy {
    Never,
    ReadOnly,
    Idempotent { contract: String },
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Execution {
    Parallel,
    Sequential,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    pub version: String,
    pub retry: RetryPolicy,
    pub execution: Execution,
    pub requires_approval: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum WaitRequest {
    Input { prompt: String },
    Until { unix_ms: u64 },
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum OperationStatus {
    Pending,
    Approval,
    Dispatched,
    Waiting(WaitRequest),
    Complete(ToolResult),
    Unknown { reason: String },
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Operation {
    pub call: ToolCall,
    pub spec: ToolSpec,
    pub idempotency_key: String,
    pub status: OperationStatus,
    pub approved: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum StopReason {
    ModelCalls,
    ToolCalls,
    Tokens,
    ActiveBudget,
    Expired,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum Settlement {
    Completed(Value),
    Stopped(StopReason),
    Failed(String),
    Cancelled,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum Phase {
    Ready,
    ModelInFlight,
    Tools,
    Settled(Settlement),
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunState {
    pub version: u32,
    pub id: RunId,
    pub conversation_id: String,
    pub agent_id: String,
    pub binding: Value,
    pub input: Value,
    pub limits: Limits,
    pub expires_at_ms: Option<u64>,
    pub generation: u64,
    pub revision: u64,
    pub cancel_requested: bool,
    pub phase: Phase,
    pub usage: Usage,
    /// Bounded context at acceptance, never the conversation's entire lifetime history.
    pub prior_context: Vec<HistoryEntry>,
    /// Evidence for this bounded run, appended atomically by the checkpoint store.
    pub history: Vec<HistoryEntry>,
    pub pending_response: Option<ModelResponse>,
    pub operations: Vec<Operation>,
}
impl RunState {
    pub fn is_terminal(&self) -> bool {
        matches!(self.phase, Phase::Settled(_))
    }
    pub fn is_runnable(&self, now_ms: u64) -> bool {
        if self.is_terminal() {
            return false;
        }
        if self.cancel_requested || self.expires_at_ms.is_some_and(|t| t <= now_ms) {
            return true;
        }
        match self.phase {
            Phase::Ready | Phase::ModelInFlight => true,
            Phase::Tools => self.operations.iter().all(|o| match &o.status {
                OperationStatus::Approval
                | OperationStatus::Unknown { .. }
                | OperationStatus::Waiting(WaitRequest::Input { .. }) => false,
                OperationStatus::Waiting(WaitRequest::Until { unix_ms }) => *unix_ms <= now_ms,
                _ => true,
            }),
            Phase::Settled(_) => false,
        }
    }
}
#[derive(Clone, Debug)]
pub enum Resolution {
    Approve(bool),
    Input(Value),
    Reconcile(ToolResult),
}
#[derive(Clone, Debug)]
pub enum ProgressEvent {
    ModelStarted { run_id: RunId },
    TextDelta(String),
    ToolsStarted { count: usize },
    Committed { run_id: RunId, revision: u64 },
    Settled { run_id: RunId },
}
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}
