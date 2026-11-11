//! What a delegating run records about the children its [`Subagent`](crate::Subagent)s admit,
//! and the state changes that keep a parent's budget and results consistent with them.
use crate::context::encoded_len;
use crate::{
    Error, Limits, OperationStatus, Phase, Resolution, Result, RunId, RunState, Settlement,
    StopReason, ToolResult, Usage,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

/// The run and delegation call that own a child, recorded in the child's
/// [`RunState::parent`]. Plain data, left open, because a store assembles it.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParentRun {
    /// The parent's [`RunState::id`].
    pub run_id: RunId,
    /// The [`ToolCall::id`](crate::ToolCall::id) of the delegation that admitted the child.
    pub call_id: String,
}

/// One child admitted by a delegation, as its parent retains it in [`RunState::children`].
/// Plain data, left open, because a store assembles it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AttachedChild {
    /// The delegation call that admitted the child; its result is the child's outcome.
    pub call_id: String,
    /// The child's own run, with its own ID, history and operations. Its `revision` advances
    /// with every change to it, which is how a store finds the children a commit changed.
    pub state: Box<RunState>,
}
impl AttachedChild {
    /// The settled child's result, as the parent's model received it, with the output decoded
    /// as `T`. Fails with [`Error::NotWaiting`] while the child is unsettled and with
    /// [`Error::ModelOutput`] if a completed output does not decode as `T`.
    pub fn result<T: DeserializeOwned>(&self) -> Result<ChildResult<T>> {
        let outcome = match &self.state.phase {
            Phase::Settled(Settlement::Completed(value)) => {
                ChildOutcome::Completed(T::deserialize(value).map_err(Error::model_output)?)
            }
            Phase::Settled(Settlement::Stopped(reason)) => ChildOutcome::Stopped(reason.clone()),
            Phase::Settled(Settlement::Failed(message)) => ChildOutcome::Failed(message.clone()),
            Phase::Settled(Settlement::Cancelled) => ChildOutcome::Cancelled,
            Phase::Ready | Phase::ModelInFlight | Phase::Tools => return Err(Error::NotWaiting),
        };
        Ok(ChildResult {
            parent_run_id: self
                .state
                .parent
                .as_ref()
                .map_or(RunId::nil(), |p| p.run_id),
            child_run_id: self.state.id,
            outcome,
            usage: self.state.usage.clone(),
        })
    }
}

/// How a child ended. Only `Completed` is a successful result for the parent's model; the others
/// are error results it can handle.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum ChildOutcome<T> {
    /// The child's final output, which fits its agent's output type.
    Completed(T),
    /// A limit of the child's allocation stopped it.
    Stopped(StopReason),
    /// The child's own work failed, with its bounded message.
    Failed(String),
    /// The child was cancelled, by its parent's cancellation or settlement.
    Cancelled,
}

/// The result of one delegation: the parent's model receives it, encoded, as the call's tool
/// result. `T` is the child agent's output type.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub struct ChildResult<T> {
    /// The delegating run.
    pub parent_run_id: RunId,
    /// The child's run, retained in the parent's [`RunState::children`].
    pub child_run_id: RunId,
    /// How the child ended.
    pub outcome: ChildOutcome<T>,
    /// What the child used, already included in its parent's usage.
    pub usage: Usage,
}

impl RunState {
    /// The child with this ID, admitted by one of this run's delegations.
    pub fn child(&self, id: RunId) -> Option<&AttachedChild> {
        self.children.iter().find(|child| child.state.id == id)
    }
    /// Answers what the child `child_id` waits on, in memory, as [`resolve`](Self::resolve)
    /// does for this run's own calls. The caller commits this run or resumes it; a durable run is
    /// resolved with [`Host::resolve_child`](crate::Host::resolve_child) or
    /// [`RunUpdate::ResolveChild`](crate::RunUpdate::ResolveChild). Fails with
    /// [`Error::NotFound`] for an unknown child, otherwise as `resolve` does.
    pub fn resolve_child(
        &mut self,
        child_id: RunId,
        call_id: &str,
        resolution: Resolution,
    ) -> Result<()> {
        let child = self
            .children
            .iter_mut()
            .find(|child| child.state.id == child_id)
            .ok_or(Error::NotFound)?;
        child.state.resolve(call_id, resolution)?;
        child.state.revision = child.state.revision.saturating_add(1);
        Ok(())
    }
    /// Whether the run or one of its children holds an operation whose outcome is not known:
    /// dispatched without a recorded outcome, or [`Unknown`](OperationStatus::Unknown) until
    /// reconciled. Stays true after settlement until each is reconciled.
    pub fn has_uncertainty(&self) -> bool {
        self.operations.iter().any(|op| {
            matches!(
                op.status,
                OperationStatus::Unknown { .. } | OperationStatus::Dispatched
            )
        }) || self
            .children
            .iter()
            .any(|child| child.state.has_uncertainty())
    }
}

/// What a child's limits reserve from its parent until it settles. Tokens are one pool, reserved
/// as output and replaced by the child's actual input and output.
fn allocation(limits: &Limits) -> Usage {
    Usage {
        model_calls: limits.max_model_calls,
        tool_calls: limits.max_tool_calls,
        input_tokens: 0,
        output_tokens: limits.max_tokens,
        active_ms: limits.active_budget_ms,
    }
}
fn add(total: &mut Usage, usage: &Usage) {
    total.model_calls = total.model_calls.saturating_add(usage.model_calls);
    total.tool_calls = total.tool_calls.saturating_add(usage.tool_calls);
    total.input_tokens = total.input_tokens.saturating_add(usage.input_tokens);
    total.output_tokens = total.output_tokens.saturating_add(usage.output_tokens);
    total.active_ms = total.active_ms.saturating_add(usage.active_ms);
}
fn subtract(total: &mut Usage, usage: &Usage) {
    total.model_calls = total.model_calls.saturating_sub(usage.model_calls);
    total.tool_calls = total.tool_calls.saturating_sub(usage.tool_calls);
    total.input_tokens = total.input_tokens.saturating_sub(usage.input_tokens);
    total.output_tokens = total.output_tokens.saturating_sub(usage.output_tokens);
    total.active_ms = total.active_ms.saturating_sub(usage.active_ms);
}

/// The first parent limit a child's allocation does not fit in what remains, if any.
pub(crate) fn exceeds(parent: &RunState, limits: &Limits) -> Option<StopReason> {
    let (budget, used) = (&parent.limits, &parent.usage);
    let tokens = used.input_tokens.saturating_add(used.output_tokens);
    if limits.max_model_calls > budget.max_model_calls.saturating_sub(used.model_calls) {
        Some(StopReason::ModelCalls)
    } else if limits.max_tool_calls > budget.max_tool_calls.saturating_sub(used.tool_calls) {
        Some(StopReason::ToolCalls)
    } else if limits.max_tokens > budget.max_tokens.saturating_sub(tokens) {
        Some(StopReason::Tokens)
    } else if limits.active_budget_ms > budget.active_budget_ms.saturating_sub(used.active_ms) {
        Some(StopReason::ActiveBudget)
    } else {
        None
    }
}
/// Reserves a newly admitted child's allocation in its parent's usage.
pub(crate) fn reserve(parent: &mut RunState, limits: &Limits) {
    let reserved = allocation(limits);
    add(&mut parent.usage, &reserved);
    add(&mut parent.child_usage, &reserved);
}
/// Returns a child's allocation to its parent, for an admission that was never committed.
pub(crate) fn unreserve(parent: &mut RunState, limits: &Limits) {
    let reserved = allocation(limits);
    subtract(&mut parent.usage, &reserved);
    subtract(&mut parent.child_usage, &reserved);
}

/// What [`settle`] changed in the parent, so a failed commit can restore it.
pub(crate) struct Settled {
    op: usize,
    status: OperationStatus,
    usage: Usage,
    child_usage: Usage,
}
impl Settled {
    pub(crate) fn undo(self, parent: &mut RunState) {
        parent.operations[self.op].status = self.status;
        parent.usage = self.usage;
        parent.child_usage = self.child_usage;
    }
}
/// Once the child at `index` has settled: replaces its allocation with what it used, exactly
/// once, and records its result as its delegation's outcome. Both commit with the child's
/// settlement.
pub(crate) fn settle(parent: &mut RunState, index: usize) -> Option<Settled> {
    let child = &parent.children[index];
    let id = child.state.id;
    let op = parent
        .operations
        .iter()
        .position(|op| matches!(op.status, OperationStatus::Child { run_id } if run_id == id))?;
    let max_bytes = parent.limits.max_tool_output_bytes;
    let (value, is_error) = match child.result::<Value>() {
        Ok(result) => {
            let failed = !matches!(result.outcome, ChildOutcome::Completed(_));
            match serde_json::to_value(&result) {
                Ok(value) if encoded_len(&value) <= max_bytes => (value, failed),
                _ => (
                    error_value("child result exceeds the tool output limit", max_bytes),
                    true,
                ),
            }
        }
        Err(error) => (error_value(&error.to_string(), max_bytes), true),
    };
    let used = child.state.usage.clone();
    let reserved = allocation(&child.state.limits);
    let call_id = child.call_id.clone();
    let undo = Settled {
        op,
        status: std::mem::replace(
            &mut parent.operations[op].status,
            OperationStatus::Complete(ToolResult {
                call_id,
                value,
                is_error,
            }),
        ),
        usage: parent.usage.clone(),
        child_usage: parent.child_usage.clone(),
    };
    for usage in [&mut parent.usage, &mut parent.child_usage] {
        subtract(usage, &reserved);
        add(usage, &used);
    }
    Some(undo)
}
/// Settles every unsettled child as cancelled, with its dispatched work left
/// [`Unknown`](OperationStatus::Unknown), before its parent settles. Their work is not in flight:
/// a parent settles only between tool windows.
pub(crate) fn cancel_unsettled(parent: &mut RunState) {
    for index in 0..parent.children.len() {
        let child = &mut parent.children[index].state;
        if child.is_terminal() {
            continue;
        }
        for op in &mut child.operations {
            if matches!(op.status, OperationStatus::Dispatched) {
                op.status = OperationStatus::Unknown {
                    reason: "parent settled after dispatch".into(),
                };
            }
        }
        child.phase = Phase::Settled(Settlement::Cancelled);
        child.revision = child.revision.saturating_add(1);
        settle(parent, index);
    }
}
/// The error result for a delegation that admitted no child or whose result cannot be recorded.
pub(crate) fn error_value(message: &str, max_bytes: usize) -> Value {
    let mut end = message.len().min(max_bytes.saturating_sub(2) / 6);
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    Value::String(message[..end].to_owned())
}
