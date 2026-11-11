//! Content-free tracing helpers. Never format errors, state, or payloads here.
use crate::{Error, Operation, OperationStatus, Phase, Result, RunState, Settlement};
use std::{future::Future, time::Duration};
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

pub(crate) async fn cleanup(
    operation: &'static str,
    timeout_ms: u64,
    future: impl Future<Output = Result<()>>,
) -> std::result::Result<Result<()>, tokio::time::error::Elapsed> {
    let result = tokio::time::timeout(Duration::from_millis(timeout_ms), future).await;
    let span = Span::current();
    match &result {
        Ok(Ok(())) => {
            span.record("outcome", "done");
        }
        Ok(Err(error)) => {
            span.record("outcome", "failed");
            span.record("error.type", error_type(error));
            tracing::event!(name: "cleanup_failed", target: module_path!(), tracing::Level::WARN, { operation, "error.type" = error_type(error) });
        }
        Err(_) => {
            span.record("outcome", "timed_out");
            span.record("error.type", "timeout");
            tracing::warn!(name: "cleanup_timed_out", operation, timeout_ms);
        }
    }
    result
}

pub(crate) fn record_operation(span: &Span, op: &Operation) {
    if matches!(op.status, OperationStatus::Unknown { .. }) {
        unknown(span, op);
    }
    if span.is_disabled() {
        return;
    }
    let (status, bytes) = match &op.status {
        OperationStatus::Pending => ("pending", None),
        OperationStatus::Approval => ("approval", None),
        OperationStatus::Dispatched => ("dispatched", None),
        OperationStatus::Waiting(wait) => ("waiting", Some(crate::context::encoded_len(wait))),
        OperationStatus::Complete(result) => {
            ("complete", Some(crate::context::encoded_len(&result.value)))
        }
        OperationStatus::Unknown { .. } => ("unknown", None),
        OperationStatus::Child { .. } => ("child", None),
    };
    span.record("operation.status", status);
    if let Some(bytes) = bytes {
        span.record("output_bytes", bytes);
    }
}

pub(crate) fn unknown(span: &Span, op: &Operation) {
    tracing::event!(name: "operation_unknown", target: module_path!(), parent: span.clone().or_current(), tracing::Level::WARN, { gen_ai.tool.name = %op.call.name, gen_ai.tool.call.id = %op.call.id });
}

pub(crate) fn tool_span(op: &Operation) -> Span {
    tracing::info_span!("execute_tool",
        otel.name = %format_args!("execute_tool {}", op.call.name), otel.kind = "internal",
        gen_ai.operation.name = "execute_tool", gen_ai.tool.name = %op.call.name,
        gen_ai.tool.call.id = %op.call.id, operation.status = tracing::field::Empty,
        output_bytes = tracing::field::Empty
    )
}

/// Preserve both the current span and task-local subscriber without enlarging an untraced
/// task's allocation. Long-lived services capture their startup context; queued jobs capture
/// the caller's context separately.
pub(crate) fn spawn<F>(future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    use tracing::{Instrument, instrument::WithSubscriber};
    if is_disabled() {
        tokio::spawn(future)
    } else {
        tokio::spawn(future.in_current_span().with_current_subscriber())
    }
}
pub(crate) fn is_disabled() -> bool {
    tracing::dispatcher::get_default(|dispatch| dispatch.is::<tracing::subscriber::NoSubscriber>())
}
