use crate::{Error, Result, context::encoded_len};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    fmt,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;
pub(crate) const STATE_VERSION: u32 = 1;
pub type RunId = Uuid;
/// Bounds on one run, set with [`AgentBuilder::limits`](crate::AgentBuilder::limits). Start from
/// [`Limits::default`] and change what differs:
///
/// ```
/// # use std::time::Duration;
/// # use yielded_agent::Limits;
/// let limits = Limits::default()
///     .with_max_model_calls(8)
///     .with_operation_timeout(Duration::from_secs(5));
/// assert_eq!(limits.operation_timeout_ms, 5_000);
/// ```
///
/// Limits are part of the agent's [`binding`](crate::Agent::binding) and are recorded with every
/// run it accepts, so a stored run resumes only under the limits it was accepted with. Times are
/// held in whole milliseconds because that is how they are recorded.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub struct Limits {
    /// Model calls per run, counting every attempt. The run stops with
    /// [`StopReason::ModelCalls`] instead of starting another. Default 32.
    pub max_model_calls: u32,
    /// Tool executions per run, counting every dispatch, including retries. A response with more
    /// calls fails the run; a batch whose calls would exceed what remains stops it with
    /// [`StopReason::ToolCalls`] before any of them starts. Default 128.
    pub max_tool_calls: u32,
    /// Input plus output tokens the model reports across the run. Reaching it stops the run with
    /// [`StopReason::Tokens`]; providers see what remains in
    /// [`ModelRequest::remaining_tokens`](crate::ModelRequest::remaining_tokens).
    /// Default 100,000.
    pub max_tokens: u64,
    /// Tool calls of one run that execute at once. At least 1. Default 4.
    pub max_parallel_tools: usize,
    /// Bytes of streamed text and of the encoded response in one model turn; a turn that exceeds
    /// it fails the run. Also bounds the number of stream events. At least 1. Default 1 MiB.
    pub max_response_bytes: usize,
    /// Encoded bytes of one tool output or wait request, an input reply or a reconciled result.
    /// A larger tool output is handled like a tool error. Also bounds a failed run's recorded
    /// message. At least 1. Default 256 KiB.
    pub max_tool_output_bytes: usize,
    /// Encoded bytes of the history and derived context sent on one model turn; see
    /// [`ContextPolicy`](crate::ContextPolicy). The run's input must fit on its own. At least 2.
    /// Default 2 MiB.
    pub max_context_bytes: usize,
    /// Deadline in milliseconds for each model call, tool call and authorization. Positive.
    /// Default 30,000.
    pub operation_timeout_ms: u64,
    /// Deadline in milliseconds for each model or tool cleanup. Positive. Default 1,000.
    pub cleanup_timeout_ms: u64,
    /// Active milliseconds a run may spend in model and tool operations, including their cleanup
    /// allowance; waiting for approval, input or a timer does not count. Exhausting it stops the
    /// run with [`StopReason::ActiveBudget`]. Default 600,000.
    pub active_budget_ms: u64,
}
impl Limits {
    /// Sets [`max_model_calls`](Self::max_model_calls).
    pub fn with_max_model_calls(mut self, calls: u32) -> Self {
        self.max_model_calls = calls;
        self
    }
    /// Sets [`max_tool_calls`](Self::max_tool_calls).
    pub fn with_max_tool_calls(mut self, calls: u32) -> Self {
        self.max_tool_calls = calls;
        self
    }
    /// Sets [`max_tokens`](Self::max_tokens).
    pub fn with_max_tokens(mut self, tokens: u64) -> Self {
        self.max_tokens = tokens;
        self
    }
    /// Sets [`max_parallel_tools`](Self::max_parallel_tools).
    pub fn with_max_parallel_tools(mut self, tools: usize) -> Self {
        self.max_parallel_tools = tools;
        self
    }
    /// Sets [`max_response_bytes`](Self::max_response_bytes).
    pub fn with_max_response_bytes(mut self, bytes: usize) -> Self {
        self.max_response_bytes = bytes;
        self
    }
    /// Sets [`max_tool_output_bytes`](Self::max_tool_output_bytes).
    pub fn with_max_tool_output_bytes(mut self, bytes: usize) -> Self {
        self.max_tool_output_bytes = bytes;
        self
    }
    /// Sets [`max_context_bytes`](Self::max_context_bytes).
    pub fn with_max_context_bytes(mut self, bytes: usize) -> Self {
        self.max_context_bytes = bytes;
        self
    }
    /// Sets [`operation_timeout_ms`](Self::operation_timeout_ms), rounded up to a millisecond.
    pub fn with_operation_timeout(mut self, timeout: Duration) -> Self {
        self.operation_timeout_ms = whole_ms(timeout);
        self
    }
    /// Sets [`cleanup_timeout_ms`](Self::cleanup_timeout_ms), rounded up to a millisecond.
    pub fn with_cleanup_timeout(mut self, timeout: Duration) -> Self {
        self.cleanup_timeout_ms = whole_ms(timeout);
        self
    }
    /// Sets [`active_budget_ms`](Self::active_budget_ms), rounded up to a millisecond.
    pub fn with_active_budget(mut self, budget: Duration) -> Self {
        self.active_budget_ms = whole_ms(budget);
        self
    }
}
/// Whole milliseconds, rounded up so that a positive duration never becomes zero.
fn whole_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos().div_ceil(1_000_000)).unwrap_or(u64::MAX)
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
#[non_exhaustive]
pub struct Usage {
    pub model_calls: u32,
    pub tool_calls: u32,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub active_ms: u64,
}
/// Tokens one model response consumed, as the provider reports them. The engine adds them to
/// the run's [`Usage`] and checks them against [`Limits::max_tokens`].
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub struct TokenUsage {
    /// Prompt tokens.
    pub input: u64,
    /// Generated tokens.
    pub output: u64,
}
impl TokenUsage {
    /// `input` prompt tokens and `output` generated tokens.
    pub fn new(input: u64, output: u64) -> Self {
        Self { input, output }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub arguments: Value,
}
/// One complete model turn: either a final output or the tool calls to make next, never both.
/// A [`Model`](crate::Model) delivers it as its stream's last
/// [`ModelEvent::Response`](crate::ModelEvent::Response); it is recorded in history.
///
/// ```
/// # use serde_json::json;
/// # use yielded_agent::{ModelResponse, TokenUsage};
/// let response = ModelResponse::final_output(json!("done")).with_usage(TokenUsage::new(12, 3));
/// ```
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub struct ModelResponse {
    /// The run's output, checked against the agent's output type, or `None` for a tool turn.
    pub output: Option<Value>,
    /// Calls to make before the next turn, in the order the model gave them, or empty for a
    /// final output.
    pub tools: Vec<ToolCall>,
    /// What the turn consumed. Defaults to zero.
    pub usage: TokenUsage,
}
impl ModelResponse {
    /// A final response that settles the run with `value`.
    pub fn final_output(value: Value) -> Self {
        Self {
            output: Some(value),
            tools: vec![],
            usage: TokenUsage::default(),
        }
    }
    /// A tool turn that makes `tools` before the model is called again.
    pub fn tools(tools: Vec<ToolCall>) -> Self {
        Self {
            output: None,
            tools,
            usage: TokenUsage::default(),
        }
    }
    /// Sets [`usage`](Self::usage).
    pub fn with_usage(mut self, usage: TokenUsage) -> Self {
        self.usage = usage;
        self
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
#[non_exhaustive]
pub enum HistoryEntry {
    Input(Value),
    Exchange {
        response: ModelResponse,
        results: Vec<ToolResult>,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
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
/// What the model is told about a tool, and how the engine may run it. Start from
/// [`ToolSpec::new`] and set what differs from its defaults:
///
/// ```
/// # use yielded_agent::{RetryPolicy, ToolSpec};
/// let spec = ToolSpec::new("create_reminder", "1")
///     .with_description("Create a reminder at a time.")
///     .with_retry(RetryPolicy::Idempotent { contract: "reminder-key-v1".into() })
///     .with_requires_approval(true);
/// assert!(spec.requires_approval);
/// ```
///
/// Every field is part of the agent's [`binding`](crate::Agent::binding), and a recorded operation
/// keeps the spec it was called under: a stored run resumes only if each tool it called still
/// has an equal spec. Change [`version`](Self::version) whenever the tool's behaviour changes.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub struct ToolSpec {
    /// The name the model calls the tool by, unique within an agent and 1..=256 bytes.
    pub name: String,
    /// What the tool does, for the model. Defaults to empty.
    pub description: String,
    /// JSON Schema of the arguments, for the model. Defaults to `{}`, which accepts any
    /// arguments. [`typed_tool`](crate::typed_tool) replaces it with the schema of its
    /// argument type.
    pub input_schema: Value,
    /// The version of the tool's contract. Must not be empty.
    pub version: String,
    /// Whether an execution whose outcome is unknown may run again. Defaults to
    /// [`RetryPolicy::Never`].
    pub retry: RetryPolicy,
    /// Whether the call may run beside the batch's other calls. Defaults to
    /// [`Execution::Parallel`].
    pub execution: Execution,
    /// Whether each call waits in [`OperationStatus::Approval`] until it is resolved with
    /// [`Resolution::Approve`]. Defaults to `false`.
    pub requires_approval: bool,
}
impl ToolSpec {
    /// A tool called `name` at contract `version`, with the defaults documented on each field.
    pub fn new(name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: String::new(),
            input_schema: json!({}),
            version: version.into(),
            retry: RetryPolicy::Never,
            execution: Execution::Parallel,
            requires_approval: false,
        }
    }
    /// Sets [`description`](Self::description).
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }
    /// Sets [`input_schema`](Self::input_schema).
    pub fn with_input_schema(mut self, schema: Value) -> Self {
        self.input_schema = schema;
        self
    }
    /// Sets [`retry`](Self::retry).
    pub fn with_retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }
    /// Sets [`execution`](Self::execution).
    pub fn with_execution(mut self, execution: Execution) -> Self {
        self.execution = execution;
        self
    }
    /// Sets [`requires_approval`](Self::requires_approval).
    pub fn with_requires_approval(mut self, requires_approval: bool) -> Self {
        self.requires_approval = requires_approval;
        self
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum WaitRequest {
    Input { prompt: String },
    Until { unix_ms: u64 },
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum OperationStatus {
    Pending,
    Approval,
    Dispatched,
    Waiting(WaitRequest),
    Complete(ToolResult),
    Unknown { reason: String },
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub struct Operation {
    pub call: ToolCall,
    pub spec: ToolSpec,
    pub idempotency_key: String,
    pub status: OperationStatus,
}
impl Operation {
    /// Assembles a recorded operation. The engine records operations itself; this is for
    /// stores and tests that construct run state directly, for example to simulate a crash
    /// after dispatch.
    pub fn new(
        call: ToolCall,
        spec: ToolSpec,
        idempotency_key: impl Into<String>,
        status: OperationStatus,
    ) -> Self {
        Self {
            call,
            spec,
            idempotency_key: idempotency_key.into(),
            status,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub enum StopReason {
    ModelCalls,
    ToolCalls,
    Tokens,
    ActiveBudget,
    Expired,
}
impl fmt::Display for StopReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ModelCalls => "model call limit reached",
            Self::ToolCalls => "tool call limit reached",
            Self::Tokens => "token limit reached",
            Self::ActiveBudget => "active time budget exhausted",
            Self::Expired => "run expired",
        })
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum Settlement {
    Completed(Value),
    Stopped(StopReason),
    Failed(String),
    Cancelled,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
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
#[non_exhaustive]
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
