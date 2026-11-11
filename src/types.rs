use crate::{Error, Result, context::encoded_len};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    fmt,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;
/// The [`RunState::version`] this build writes and accepts.
pub(crate) const STATE_VERSION: u32 = 1;
/// Identifies a run: a random UUID assigned when the run is accepted.
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
/// What a run has consumed so far, counted against its [`Limits`].
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub struct Usage {
    /// Model calls started, counting every attempt.
    pub model_calls: u32,
    /// Tool executions started, counting every retry.
    pub tool_calls: u32,
    /// Prompt tokens the model reported, summed over its responses.
    pub input_tokens: u64,
    /// Generated tokens the model reported, summed over its responses.
    pub output_tokens: u64,
    /// Active milliseconds charged against [`Limits::active_budget_ms`]. While an operation runs
    /// this includes the time reserved for it and its cleanup; what it does not use is returned
    /// when it finishes.
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
/// One call the model asked for. Plain data, built by [`Model`](crate::Model) adapters.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ToolCall {
    /// The model's ID for the call, 1..=256 bytes and unique within the run. Results and
    /// resolutions name the call by it.
    pub id: String,
    /// The name of the tool to call, as in its [`ToolSpec::name`].
    pub name: String,
    /// The call's JSON arguments. [`Tool::validate`](crate::Tool::validate) checks them before
    /// any call of the batch starts.
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
/// The recorded outcome of one tool call, returned to the model on the next turn. Plain data; an
/// application builds one to reconcile an unknown outcome with [`Resolution::Reconcile`].
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ToolResult {
    /// The [`ToolCall::id`] of the call.
    pub call_id: String,
    /// The tool's output, or a description of its failure when `is_error` is set.
    pub value: Value,
    /// Whether the call failed with a known outcome: [`ToolOutput::Failed`](crate::ToolOutput),
    /// an error from a [`RetryPolicy::ReadOnly`] tool, or a denied approval.
    pub is_error: bool,
}
/// One entry of a run's or a conversation's history.
///
/// Exchanges are indivisible during context selection; calls never lose their results.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum HistoryEntry {
    /// A run's input.
    Input(Value),
    /// An instruction a [`SteeringStore`](crate::SteeringStore) accepted for a run that was already
    /// active, sent to the model as user input. The run's model dispatch commit appends it, so it
    /// follows the exchanges completed before that turn. Every steering entry of a run's own
    /// history is pinned in its context.
    Steering {
        /// The instruction's [`SteeringReceipt::id`](crate::SteeringReceipt::id).
        id: crate::SteeringId,
        /// The instruction as accepted.
        instruction: String,
    },
    /// A model turn and, for a tool turn, the results of its calls.
    Exchange {
        /// The model's response.
        response: ModelResponse,
        /// One result per call of `response.tools`, in call order; empty for a final output.
        results: Vec<ToolResult>,
    },
}
/// What may happen when a call's outcome is not known: after an error, a timeout, an interrupted
/// execution or a crash after dispatch.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub enum RetryPolicy {
    /// The call may have had an effect that must not repeat. It becomes
    /// [`OperationStatus::Unknown`], which stops the run's further dispatch until the
    /// application reconciles it with [`Resolution::Reconcile`].
    Never,
    /// The tool has no effect. An error is recorded for the model as an error result, and a call
    /// interrupted by shutdown or a crash runs again.
    ReadOnly,
    /// The tool deduplicates repeated calls by [`ToolContext::idempotency_key`](crate::ToolContext),
    /// so a call interrupted by shutdown or a crash runs again with the same key. An error still
    /// leaves the outcome [`Unknown`](OperationStatus::Unknown).
    Idempotent {
        /// How the tool deduplicates, for example the name and version of its key scheme. Must
        /// not be empty.
        contract: String,
    },
}
/// How a call is scheduled beside the other calls of its batch.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Execution {
    /// Runs beside the batch's other parallel calls, up to [`Limits::max_parallel_tools`].
    Parallel,
    /// Starts once every earlier call of the batch has finished, and runs alone.
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
/// What a tool's call waits for before it completes, returned as
/// [`ToolOutput::Wait`](crate::ToolOutput::Wait). A waiting call stops the batch's further
/// dispatch, and the run holds no execution slot while it waits.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum WaitRequest {
    /// A reply from the application, given with [`Resolution::Input`], which becomes the call's
    /// result.
    Input {
        /// What to ask, for the application to present.
        prompt: String,
    },
    /// A timer: the call completes with a `null` result once this time has passed, and a host
    /// drives the run again then.
    Until {
        /// UTC Unix time in milliseconds.
        unix_ms: u64,
    },
}
/// Where one tool call is, recorded with every checkpoint.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum OperationStatus {
    /// Not started.
    Pending,
    /// Waiting for [`Resolution::Approve`], because its tool requires approval. No call of the
    /// batch starts while any call waits for approval.
    Approval,
    /// Its intent is committed and it may be running. Found after a crash, the call of a
    /// [`RetryPolicy::Never`] tool becomes `Unknown`; any other call runs again.
    Dispatched,
    /// The tool returned this wait. A reply or its timer completes the call.
    Waiting(WaitRequest),
    /// Finished, with the result the model will see.
    Complete(ToolResult),
    /// It may or may not have had its effect. No further call of the run starts until the
    /// application reconciles it with [`Resolution::Reconcile`].
    Unknown {
        /// Why the outcome is unknown, for the application.
        reason: String,
    },
}
/// One tool call of a run's current batch, with what the engine knows about it.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub struct Operation {
    /// The call the model made.
    pub call: ToolCall,
    /// The tool's spec when it was called. A run resumes only if the agent's tool of that name
    /// still has an equal spec.
    pub spec: ToolSpec,
    /// Stable for this call across retries and restarts, and distinct from every other call's.
    /// Tools pass it on to deduplicate external effects.
    pub idempotency_key: String,
    /// Where the call is.
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
/// Which limit stopped a run, in [`Settlement::Stopped`] and [`Error::Stopped`].
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub enum StopReason {
    /// Another model call would exceed [`Limits::max_model_calls`].
    ModelCalls,
    /// The batch's calls would exceed [`Limits::max_tool_calls`].
    ToolCalls,
    /// The tokens used reached [`Limits::max_tokens`].
    Tokens,
    /// [`Limits::active_budget_ms`] cannot cover another operation and its cleanup.
    ActiveBudget,
    /// The run's [`expires_at_ms`](RunState::expires_at_ms) passed.
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
/// How a run ended. Once settled, a run's phase never changes again.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum Settlement {
    /// The model's final output, which fits the agent's output type.
    Completed(Value),
    /// A limit stopped the run.
    Stopped(StopReason),
    /// The run's own work failed (see [`Error::fails_run`]): the error's message, truncated to
    /// [`Limits::max_tool_output_bytes`].
    Failed(String),
    /// Cancellation was requested through [`RunControl::cancel`](crate::RunControl), a host or a
    /// store.
    Cancelled,
}
/// Where a run is in its state machine.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum Phase {
    /// The model is called next.
    Ready,
    /// A model call's intent is committed. Found after a crash, the call is made again.
    ModelInFlight,
    /// The current batch of tool calls is running or waiting; see [`RunState::operations`].
    Tools,
    /// The run ended.
    Settled(Settlement),
}
/// One run: what it was accepted with, how far it got, and the evidence it recorded. It is the
/// run's checkpoint. [`Agent::new_run`](crate::Agent::new_run) creates it,
/// [`Agent::drive`](crate::Agent::drive) advances it and commits it at every step, and a
/// [`DurableStore`](crate::DurableStore) persists it.
///
/// Unlike most of the crate's structs this one is not `#[non_exhaustive]`: a store assembles it
/// from the parts it persisted, so its fields are public and it can be built as a literal. A new
/// field is a deliberate breaking change for store implementations, because each must persist
/// it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunState {
    /// The version of this state's format, set at acceptance. Stores persist it and return it
    /// unchanged; [`Agent::drive`](crate::Agent::drive) refuses a version it does not support
    /// with [`Error::Incompatible`].
    pub version: u32,
    /// The run's ID, a random UUID assigned at acceptance.
    pub id: RunId,
    /// The conversation the run belongs to, 1..=1024 bytes.
    pub conversation_id: String,
    /// The ID of the agent that accepted it. A host drives it with the agent registered under
    /// this ID.
    pub agent_id: String,
    /// The accepting agent's [`binding`](crate::Agent::binding), shared with it in memory.
    pub binding: Arc<Value>,
    /// The run's input, as accepted.
    pub input: Value,
    /// The accepting agent's limits, which bound the run for its whole life.
    pub limits: Limits,
    /// UTC Unix milliseconds after which the run stops with [`StopReason::Expired`], or `None`.
    pub expires_at_ms: Option<u64>,
    /// Advanced by each claim of the run. A commit from an earlier generation is
    /// [`Error::Stale`].
    pub generation: u64,
    /// Advanced by each committed change. A commit must name the stored revision, or it is
    /// [`Error::Stale`].
    pub revision: u64,
    /// Cancellation was requested; the run settles as [`Settlement::Cancelled`] when it is next
    /// driven.
    pub cancel_requested: bool,
    /// Where the run is.
    pub phase: Phase,
    /// What the run has consumed.
    pub usage: Usage,
    /// Bounded context at acceptance, never the conversation's entire lifetime history.
    pub prior_context: Vec<HistoryEntry>,
    /// Evidence for this bounded run, appended atomically by the checkpoint store. Its first
    /// entry is the run's input.
    pub history: Vec<HistoryEntry>,
    /// The model response of the current tool turn, kept until its exchange is added to
    /// `history`. Also keeps a complete response that was then rejected, as evidence.
    pub pending_response: Option<ModelResponse>,
    /// The current batch's calls, in call order. Cleared when the batch's exchange is added to
    /// `history`; kept when the run settles, so uncertain outcomes stay visible.
    pub operations: Vec<Operation>,
}
impl RunState {
    /// Whether the run has settled.
    pub fn is_terminal(&self) -> bool {
        matches!(self.phase, Phase::Settled(_))
    }
    /// Whether a host should drive the run at `now_ms`, UTC Unix milliseconds: its
    /// [`wake_at`](Self::wake_at) is at or before then.
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
/// An application's answer to a call that waits, given to [`RunState::resolve`],
/// [`Host::resolve`](crate::Host::resolve) or
/// [`DurableStore::resolve`](crate::DurableStore::resolve).
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Resolution {
    /// Answers [`OperationStatus::Approval`]. `true` lets the call run; `false` records the
    /// error result `{"error":"approval denied"}` for the model without running it.
    Approve(bool),
    /// Answers [`WaitRequest::Input`]: the value becomes the call's result. At most
    /// [`Limits::max_tool_output_bytes`] once encoded.
    Input(Value),
    /// Answers [`OperationStatus::Unknown`] with what actually happened. The result must name the
    /// call and fit [`Limits::max_tool_output_bytes`]. Allowed after the run settled.
    Reconcile(ToolResult),
}
/// A transient, best-effort progress notification attributed to the run that produced it.
///
/// Every event names its run, so one channel can carry events from many concurrent runs, as a
/// `Host` does, and a child's events also name its parent. Events are never persisted and a full
/// channel drops them; durable status and history remain authoritative. Fields may be added, so
/// match with `..`.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ProgressEvent {
    /// The run that produced the event.
    pub run_id: RunId,
    /// For an event of a child run, the parent run that owns the child; `None` for a run's own.
    pub parent_run_id: Option<RunId>,
    /// What happened.
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
    ToolsStarted {
        /// How many operations were admitted.
        count: usize,
    },
    /// A checkpoint commit succeeded at this revision.
    #[non_exhaustive]
    Committed {
        /// The run's revision after the commit.
        revision: u64,
    },
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
