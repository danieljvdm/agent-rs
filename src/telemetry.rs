//! Content-free tracing helpers. Never format errors, state, or payloads here.
use crate::{Error, Operation, OperationStatus, Phase, Result, RunId, RunState, Settlement};
use futures::future::Either;
use std::{future::Future, pin::Pin, time::Duration};
use tracing::Instrument;
use tracing::Span;

pub(crate) fn milliseconds(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

pub(crate) fn error_type(error: &Error) -> &'static str {
    match error {
        Error::Config(_) => "config",
        Error::Input(_) => "input",
        Error::ModelOutput(_) => "model_output",
        Error::Provider(_) => "provider",
        Error::Tool(_) => "tool",
        Error::Authorization(_) => "authorization",
        Error::Context(_) => "context",
        Error::ContextOverflow(_) => "context_overflow",
        Error::Storage(_) => "storage",
        Error::IdempotencyConflict => "idempotency_conflict",
        Error::ConversationBusy => "conversation_busy",
        Error::StoreBusy => "store_busy",
        Error::Stale => "stale",
        Error::Incompatible(_) => "incompatible",
        Error::NotFound => "not_found",
        Error::NotWaiting => "not_waiting",
        Error::RunNotActive => "run_not_active",
        Error::Interrupted => "interrupted",
        Error::Waiting => "waiting",
        Error::Stopped(_) => "stopped",
        Error::Failed(_) => "failed",
        Error::Cancelled => "cancelled",
    }
}

pub(crate) fn record_drive(span: &Span, state: &RunState, result: &Result<()>) {
    let outcome = match result {
        Err(error) => {
            span.record("error.type", error_type(error));
            error_type(error)
        }
        Ok(()) => match &state.phase {
            Phase::Settled(Settlement::Completed(_)) => "completed",
            Phase::Settled(Settlement::Cancelled) => "cancelled",
            Phase::Settled(Settlement::Stopped(_)) => "stopped",
            Phase::Settled(Settlement::Failed(_)) => "failed",
            _ => "waiting",
        },
    };
    span.record("outcome", outcome);
}

/// Identifies a call without trusting any model-supplied identifier.
#[derive(Clone, Copy)]
pub(crate) struct Call {
    pub(crate) run_id: RunId,
    pub(crate) turn: u32,
    pub(crate) index: Option<usize>,
}
impl Call {
    pub(crate) fn model(state: &RunState) -> Self {
        Self {
            run_id: state.id,
            turn: state
                .usage
                .model_calls
                .saturating_sub(state.child_usage.model_calls),
            index: None,
        }
    }
    pub(crate) fn tool(state: &RunState, index: usize) -> Self {
        Self {
            index: Some(index),
            ..Self::model(state)
        }
    }
    pub(crate) fn timed_out(self, operation: &'static str, tool: Option<&str>) {
        tracing::event!(name: "operation_timed_out", target: module_path!(), tracing::Level::WARN, {
            operation, run_id = %self.run_id, turn = self.turn,
            gen_ai.tool.name = tool, tool_call_index = self.index
        });
    }
    pub(crate) fn recovered(self, tool: Option<&str>) {
        tracing::event!(name: "work_recovered", target: module_path!(), tracing::Level::WARN, {
            operation = if tool.is_some() { "execute_tool" } else { "chat" },
            run_id = %self.run_id, turn = self.turn,
            gen_ai.tool.name = tool, tool_call_index = self.index
        });
    }
}

pub(crate) async fn cleanup(
    span: Span,
    call: Call,
    tool: Option<&str>,
    timeout_ms: u64,
    future: impl Future<Output = Result<()>>,
) -> std::result::Result<Result<()>, tokio::time::error::Elapsed> {
    let result = tokio::time::timeout(Duration::from_millis(timeout_ms), future)
        .instrument(span.clone())
        .await;
    record_cleanup(&span, call, tool, timeout_ms, &result);
    result
}

pub(crate) fn record_cleanup(
    span: &Span,
    call: Call,
    tool: Option<&str>,
    timeout_ms: u64,
    result: &std::result::Result<Result<()>, tokio::time::error::Elapsed>,
) {
    let operation = if tool.is_some() {
        "execute_tool"
    } else {
        "chat"
    };
    span.in_scope(|| match result {
        Ok(Ok(())) => {
            span.record("outcome", "done");
        }
        Ok(Err(error)) => {
            span.record("outcome", "failed");
            span.record("error.type", error_type(error));
            tracing::event!(name: "cleanup_failed", target: module_path!(), tracing::Level::WARN, {
                operation, run_id = %call.run_id, turn = call.turn,
                gen_ai.tool.name = tool, tool_call_index = call.index, "error.type" = error_type(error)
            });
        }
        Err(_) => {
            span.record("outcome", "timed_out");
            span.record("error.type", "timeout");
            tracing::event!(name: "cleanup_timed_out", target: module_path!(), tracing::Level::WARN, {
                operation, run_id = %call.run_id, turn = call.turn,
                gen_ai.tool.name = tool, tool_call_index = call.index, timeout_ms
            });
        }
    });
}

pub(crate) fn record_operation(span: &Span, op: &Operation, call: Call, bytes: Option<usize>) {
    if matches!(op.status, OperationStatus::Unknown { .. }) {
        unknown(span, op, call);
    }
    if span.is_disabled() {
        return;
    }
    let status = match &op.status {
        OperationStatus::Pending => "pending",
        OperationStatus::Approval => "approval",
        OperationStatus::Dispatched => "dispatched",
        OperationStatus::Waiting(_) => "waiting",
        OperationStatus::Complete(_) => "complete",
        OperationStatus::Unknown { .. } => "unknown",
        OperationStatus::Child { .. } => "child",
    };
    span.record("operation.status", status);
    if let Some(bytes) = bytes {
        span.record("output_bytes", bytes);
    }
}

pub(crate) fn unknown(span: &Span, op: &Operation, call: Call) {
    span.in_scope(|| {
        tracing::event!(name: "operation_unknown", target: module_path!(), tracing::Level::WARN, {
            run_id = %call.run_id, turn = call.turn,
            gen_ai.tool.name = %op.spec.name, tool_call_index = call.index
        })
    });
}

pub(crate) fn tool_span(op: &Operation, call: Call) -> Span {
    // Callsite interest is shared across dispatchers. Another thread's subscriber can
    // make the macro return a non-disabled span even when this thread has no subscriber.
    if !has_subscriber() {
        return Span::none();
    }
    tracing::info_span!("execute_tool",
        otel.name = %format_args!("execute_tool {}", op.spec.name), otel.kind = "internal",
        gen_ai.operation.name = "execute_tool", gen_ai.tool.name = %op.spec.name,
        run_id = %call.run_id, turn = call.turn, tool_call_index = call.index,
        operation.status = tracing::field::Empty, output_bytes = tracing::field::Empty
    )
}

/// Selects a compact untraced future, or a boxed instrumented future. Keeping this choice
/// outside the future preserves the no-subscriber allocation layout of concurrent work.
pub(crate) fn instrument<'a, A, F, G>(
    span: Span,
    args: A,
    plain: impl FnOnce(A) -> F,
    traced: impl FnOnce(A) -> G,
) -> Either<F, Pin<Box<dyn Future<Output = F::Output> + Send + 'a>>>
where
    F: Future + Send + 'a,
    G: Future<Output = F::Output> + Send + 'a,
{
    if has_subscriber() {
        Either::Right(Box::pin(traced(args).instrument(span)))
    } else {
        Either::Left(plain(args))
    }
}

// The public tracing API cannot expose macro callsite metadata for an exact probe.
// With a subscriber installed, prefer a possibly unused traced path over losing diagnostics.
fn has_subscriber() -> bool {
    tracing::dispatcher::get_default(|subscriber| {
        !subscriber.is::<tracing::subscriber::NoSubscriber>()
    })
}
fn current_subscriber() -> Option<tracing::Dispatch> {
    tracing::dispatcher::get_default(|subscriber| {
        (!subscriber.is::<tracing::subscriber::NoSubscriber>()).then(|| subscriber.clone())
    })
}

/// Background work keeps its subscriber, but never the span of its initiating call.
pub(crate) fn spawn<F>(future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    use tracing::instrument::WithSubscriber;
    match current_subscriber() {
        Some(subscriber) => tokio::spawn(future.with_subscriber(subscriber)),
        None => tokio::spawn(future),
    }
}

pub(crate) fn spawn_in<F>(tasks: &mut tokio::task::JoinSet<F::Output>, future: F)
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    use tracing::instrument::WithSubscriber;
    match current_subscriber() {
        Some(subscriber) => {
            tasks.spawn(future.with_subscriber(subscriber));
        }
        None => {
            tasks.spawn(future);
        }
    }
}
