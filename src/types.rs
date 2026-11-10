use crate::{Error, Result, context::encoded_len};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;
pub(crate) const STATE_VERSION: u32 = 1;
pub type RunId = Uuid;
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
    /// The accepting agent's [`binding`](crate::Agent::binding), shared with it in memory.
    pub binding: Arc<Value>,
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
        self.wake_at().is_some_and(|time| time <= now_ms)
    }
    /// The earliest UTC Unix millisecond at which a host should drive this run, or `None`
    /// while it is settled or blocked on approval, input, or reconciliation (and has no
    /// expiry). Every store persists this at each write; it is the only eligibility rule.
    pub fn wake_at(&self) -> Option<u64> {
        if self.is_terminal() {
            return None;
        }
        let ready = if self.cancel_requested {
            Some(0)
        } else {
            match self.phase {
                Phase::Ready | Phase::ModelInFlight => Some(0),
                Phase::Tools => {
                    let mut latest = 0;
                    let mut blocked = false;
                    for op in &self.operations {
                        match &op.status {
                            OperationStatus::Approval
                            | OperationStatus::Unknown { .. }
                            | OperationStatus::Waiting(WaitRequest::Input { .. }) => blocked = true,
                            OperationStatus::Waiting(WaitRequest::Until { unix_ms }) => {
                                latest = latest.max(*unix_ms)
                            }
                            _ => (),
                        }
                    }
                    if blocked { None } else { Some(latest) }
                }
                Phase::Settled(_) => None,
            }
        };
        match (ready, self.expires_at_ms) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }
    /// Whether an unsettled run holds dispatched work without a recorded outcome. Startup
    /// discovery drives such runs once even when a sibling wait keeps `wake_at` empty.
    pub fn needs_recovery(&self) -> bool {
        !self.is_terminal()
            && self
                .operations
                .iter()
                .any(|op| matches!(op.status, OperationStatus::Dispatched))
    }
    /// Answers the operation `call_id` is blocked on, in memory: an approval, an input wait, or
    /// the reconciliation of an unknown outcome. The caller commits the change, or calls
    /// [`Agent::resume`](crate::Agent::resume), which does; a durable run is resolved through
    /// its store or host instead. Reconciliation is allowed after cancellation, so uncertainty
    /// stays visible and resolvable.
    ///
    /// Fails with [`Error::NotWaiting`] unless `resolution` matches what that operation waits
    /// for, and with [`Error::Input`] for an input reply or reconciled value larger than
    /// `max_tool_output_bytes` or a reconciliation for another call.
    pub fn resolve(&mut self, call_id: &str, resolution: Resolution) -> Result<()> {
        let settled = self.is_terminal();
        let max_bytes = self.limits.max_tool_output_bytes;
        let op = self
            .operations
            .iter_mut()
            .find(|op| op.call.id == call_id)
            .ok_or(Error::NotWaiting)?;
        let next = match (&op.status, resolution) {
            (OperationStatus::Approval, Resolution::Approve(true)) if !settled => {
                op.approved = true;
                OperationStatus::Pending
            }
            (OperationStatus::Approval, Resolution::Approve(false)) if !settled => {
                OperationStatus::Complete(ToolResult {
                    call_id: call_id.into(),
                    value: json!({"error":"approval denied"}),
                    is_error: true,
                })
            }
            (OperationStatus::Waiting(WaitRequest::Input { .. }), Resolution::Input(value))
                if !settled =>
            {
                if encoded_len(&value) > max_bytes {
                    return Err(Error::input("input reply exceeds byte limit"));
                }
                OperationStatus::Complete(ToolResult {
                    call_id: call_id.into(),
                    value,
                    is_error: false,
                })
            }
            (OperationStatus::Unknown { .. }, Resolution::Reconcile(result)) => {
                if result.call_id != call_id || encoded_len(&result.value) > max_bytes {
                    return Err(Error::input(
                        "reconciliation result identity or size is invalid",
                    ));
                }
                OperationStatus::Complete(result)
            }
            _ => return Err(Error::NotWaiting),
        };
        op.status = next;
        Ok(())
    }
}
#[derive(Clone, Debug)]
pub enum Resolution {
    Approve(bool),
    Input(Value),
    Reconcile(ToolResult),
}
/// A transient, best-effort progress notification attributed to the run that produced it.
///
/// Every event names its run, so one channel can carry events from many concurrent runs, as a
/// `Host` does. Events are never persisted and a full channel drops them; durable status and
/// history remain authoritative. Fields may be added (for example, linkage to a parent run),
/// so match with `..`.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ProgressEvent {
    pub run_id: RunId,
    pub kind: ProgressKind,
}
/// What happened. Kinds may be added, and fields may be added to struct-like kinds.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ProgressKind {
    /// A model call was reserved; its intent is committed next.
    ModelStarted,
    /// Streamed model text, delivered as received and never retained.
    TextDelta(String),
    /// `count` tool operations were admitted together; their dispatch intent is committed next.
    #[non_exhaustive]
    ToolsStarted { count: usize },
    /// A checkpoint commit succeeded at this revision.
    #[non_exhaustive]
    Committed { revision: u64 },
    /// The run's settlement was committed; it is terminal.
    Settled,
}
pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}
