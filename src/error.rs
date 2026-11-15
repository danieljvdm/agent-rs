//! The crate's one error type, [`Error`], and what [`Agent::drive`](crate::Agent::drive) does
//! with each kind of it.
use crate::StopReason;
use std::{any::Any, borrow::Cow, error::Error as StdError, fmt};

/// The crate's result type. Its error type defaults to [`Error`], so a glob import of the crate
/// leaves a two-parameter `Result<T, E>` meaning the standard one.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A boxed error from anywhere, kept as the cause of an [`Error`].
type Cause = Box<dyn StdError + Send + Sync>;

/// What went wrong, by what a caller would do about it.
///
/// - **Rejected before anything ran:** [`Config`](Self::Config) and [`Input`](Self::Input).
///   Retrying the same call fails the same way.
/// - **A run's own work went wrong:** [`ModelOutput`](Self::ModelOutput),
///   [`Provider`](Self::Provider), [`Tool`](Self::Tool), [`Authorization`](Self::Authorization)
///   and [`Context`](Self::Context). Raised while driving a run, these settle it as
///   [`Settlement::Failed`](crate::Settlement::Failed) with the error's message; see
///   [`fails_run`](Self::fails_run). They reach callers directly from implementations called
///   outside a run, and from [`Model`](crate::Model) adapters or tools that compose others.
/// - **The run could not be advanced:** [`Storage`](Self::Storage), [`Stale`](Self::Stale),
///   [`Incompatible`](Self::Incompatible) and [`Interrupted`](Self::Interrupted).
///   [`Agent::drive`](crate::Agent::drive) returns these and the run stays resumable.
/// - **A store refused a request:** [`IdempotencyConflict`](Self::IdempotencyConflict),
///   [`ConversationBusy`](Self::ConversationBusy), [`StoreBusy`](Self::StoreBusy),
///   [`NotFound`](Self::NotFound), [`NotWaiting`](Self::NotWaiting) and
///   [`RunNotActive`](Self::RunNotActive).
/// - **An in-memory run ended without an output:** [`Waiting`](Self::Waiting),
///   [`Stopped`](Self::Stopped), [`Failed`](Self::Failed) and [`Cancelled`](Self::Cancelled),
///   from [`Agent::run`](crate::Agent::run) and [`Agent::resume`](crate::Agent::resume).
///
/// An error that wraps another displays the cause's message after its own, so `to_string()` is
/// complete on its own, and returns the cause from [`source`](StdError::source) for
/// downcasting. Implementors of [`Tool`](crate::Tool), [`Model`](crate::Model),
/// [`ContextPolicy`](crate::ContextPolicy), [`Checkpoint`](crate::Checkpoint) and
/// [`DurableStore`](crate::DurableStore) build errors with the constructors below, such as
/// [`Error::tool`] and [`Error::storage`]. Each takes a message (a `&'static str` or `String`)
/// or any error, which it keeps as the cause:
///
/// ```
/// # use agent_rs::Error;
/// let read = std::fs::read("checkpoint.json").map_err(Error::storage);
/// let exhausted = Error::provider("script exhausted");
/// ```
///
/// Variants may be added, so match with a wildcard arm.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// An agent, host, store or provider was configured incorrectly: rejected by
    /// [`AgentBuilder::build`](crate::AgentBuilder::build), `Host::start`, a store's `open` or
    /// a provider's constructor. Fixing it takes a code or deployment change.
    Config(ErrorDetail),
    /// A request was rejected before it ran: run input that fails its type or does not fit the
    /// context limit, an identifier, key or expiry out of bounds, a resolution reply that is too
    /// large or names another call, or an unregistered agent ID.
    Input(ErrorDetail),
    /// The model's response broke the agent's contract: neither or both of a final output and
    /// tool calls, an unknown tool, arguments the tool rejects, invalid or repeated call IDs,
    /// too many calls, or a final output that fails the agent's output type.
    ModelOutput(ErrorDetail),
    /// The model provider failed: transport, protocol, a malformed or oversized stream, a
    /// timeout, or a failed cleanup. The engine wraps any error a [`Model`](crate::Model)
    /// returns as this, keeping it as the cause.
    Provider(ErrorDetail),
    /// A tool's execution or cleanup failed or timed out. The engine records this as the
    /// call's outcome, never as the run's error: see [`Tool`](crate::Tool).
    Tool(ErrorDetail),
    /// A tool call was not authorized: the [`Authorizer`](crate::Authorizer) denied it,
    /// failed (its error is the cause), or timed out. Only the engine raises this.
    Authorization(ErrorDetail),
    /// A model turn's context cannot fit `max_context_bytes`: the current input and pinned
    /// history alone exceed it.
    Context(ErrorDetail),
    /// The provider rejected a model request because it does not fit the model's context
    /// window. A [`Model`](crate::Model) adapter returns this in place of
    /// [`Provider`](Self::Provider) when it can tell. An agent with a
    /// [`ContextWindow`](crate::ContextWindow) answers the first one on a turn by compacting and
    /// making the turn again; otherwise, and for a second one, it fails the run.
    ContextOverflow(ErrorDetail),
    /// Durable state could not be read or written. A checkpoint's error is always reported as
    /// this, whatever its kind, keeping the original as the cause.
    Storage(ErrorDetail),
    /// A run was already accepted for this conversation and key with different input, binding
    /// or expiry.
    IdempotencyConflict,
    /// The conversation already has an unsettled run, or one with an unresolved outcome.
    ConversationBusy,
    /// Another lease holds the store: `DurableStore::lease` while another host is resident.
    StoreBusy,
    /// The run moved on: another execution claimed it, it settled, or the caller's generation
    /// or revision is behind the store's.
    Stale,
    /// The stored state or schema is from another version, or the run's agent binding differs
    /// from the agent asked to drive it.
    Incompatible(ErrorDetail),
    /// No such run.
    NotFound,
    /// The resolution does not match an outstanding approval, input wait or unknown outcome.
    NotWaiting,
    /// A [`SteeringStore`](crate::SteeringStore) refused a new instruction because the run
    /// settled, has a cancellation request or expired. Start another run instead.
    RunNotActive,
    /// Execution was interrupted by shutdown before the run settled. It resumes from its last
    /// checkpoint.
    Interrupted,
    /// The in-memory run is waiting for an approval, an input reply, the reconciliation of an
    /// unknown outcome, or a timer. Resolve its operations through `Conversation::last_run` and
    /// call [`Agent::resume`](crate::Agent::resume).
    Waiting,
    /// A limit stopped the in-memory run before it completed.
    Stopped(StopReason),
    /// The in-memory run failed; this is its bounded [`Settlement::Failed`](crate::Settlement::Failed)
    /// message.
    Failed(ErrorDetail),
    /// The in-memory run was cancelled.
    Cancelled,
}

impl Error {
    /// The code or configuration that set up an agent, host, store or provider is wrong.
    pub fn config<E: Into<Cause> + 'static>(cause: E) -> Self {
        Self::Config(ErrorDetail::new(cause))
    }
    /// The caller's request was rejected before it ran.
    pub fn input<E: Into<Cause> + 'static>(cause: E) -> Self {
        Self::Input(ErrorDetail::new(cause))
    }
    /// The model's response broke the agent's contract, for example tool arguments that
    /// [`Tool::validate`](crate::Tool::validate) rejects.
    pub fn model_output<E: Into<Cause> + 'static>(cause: E) -> Self {
        Self::ModelOutput(ErrorDetail::new(cause))
    }
    /// The model provider failed.
    pub fn provider<E: Into<Cause> + 'static>(cause: E) -> Self {
        Self::Provider(ErrorDetail::new(cause))
    }
    /// A tool's execution or cleanup failed without a known outcome.
    pub fn tool<E: Into<Cause> + 'static>(cause: E) -> Self {
        Self::Tool(ErrorDetail::new(cause))
    }
    /// A model turn's context cannot fit.
    pub fn context<E: Into<Cause> + 'static>(cause: E) -> Self {
        Self::Context(ErrorDetail::new(cause))
    }
    /// The provider rejected a model request as larger than the model's context window.
    pub fn context_overflow<E: Into<Cause> + 'static>(cause: E) -> Self {
        Self::ContextOverflow(ErrorDetail::new(cause))
    }
    /// Durable state could not be read or written.
    pub fn storage<E: Into<Cause> + 'static>(cause: E) -> Self {
        Self::Storage(ErrorDetail::new(cause))
    }
    /// The stored state, schema or binding is incompatible.
    pub fn incompatible<E: Into<Cause> + 'static>(cause: E) -> Self {
        Self::Incompatible(ErrorDetail::new(cause))
    }
    pub(crate) fn authorization(detail: ErrorDetail) -> Self {
        Self::Authorization(detail)
    }

    /// Whether this error, raised while [`Agent::drive`](crate::Agent::drive) advances a run,
    /// ends the run.
    ///
    /// `true`: the run's own work went wrong, in the model, a tool contract, authorization, its
    /// context or its input, and driving it again would fail the same way. The run settles as
    /// [`Settlement::Failed`](crate::Settlement::Failed) with this error's message, bounded to
    /// `Limits::max_tool_output_bytes`, and `drive` returns `Ok`.
    ///
    /// `false`: the run could not be advanced, but nothing is wrong with it. Storage failed
    /// ([`Storage`](Self::Storage)), another execution owns it ([`Stale`](Self::Stale)), this
    /// agent cannot drive it ([`Incompatible`](Self::Incompatible)), or shutdown interrupted it
    /// ([`Interrupted`](Self::Interrupted); a cancelled run settles as cancelled instead).
    /// `drive` returns the error and the run stays as of its last checkpoint, to be driven
    /// again once the cause is fixed.
    ///
    /// Errors from a [`Checkpoint`](crate::Checkpoint) never end the run: the engine reports one
    /// whose kind would as [`Storage`](Self::Storage), with the original as its cause.
    pub fn fails_run(&self) -> bool {
        // No wildcard: every new variant must decide.
        match self {
            Self::Storage(_) | Self::Stale | Self::Incompatible(_) | Self::Interrupted => false,
            Self::Config(_)
            | Self::Input(_)
            | Self::ModelOutput(_)
            | Self::Provider(_)
            | Self::Tool(_)
            | Self::Authorization(_)
            | Self::Context(_)
            | Self::ContextOverflow(_)
            | Self::IdempotencyConflict
            | Self::ConversationBusy
            | Self::StoreBusy
            | Self::NotFound
            | Self::NotWaiting
            | Self::RunNotActive
            | Self::Waiting
            | Self::Stopped(_)
            | Self::Failed(_)
            | Self::Cancelled => true,
        }
    }

    /// Whether a stored run that [`Agent::drive`](crate::Agent::drive) (or claiming it) left
    /// with this error must wait for an explicit retry before it is driven again.
    ///
    /// `true` for every error but two: a [`Host`](crate::Host) records it with
    /// [`DurableStore::record_error`](crate::DurableStore::record_error), which stops automatic
    /// discovery of the run until [`DurableStore::retry`](crate::DurableStore::retry) after the
    /// cause, such as unavailable storage or an unregistered agent, is repaired. Driving it again
    /// first would fail the same way.
    ///
    /// `false` for [`Stale`](Self::Stale), because another execution owns the run, and for
    /// [`Interrupted`](Self::Interrupted), because shutdown stopped it and the next start resumes
    /// it from its last checkpoint. Neither says anything is wrong with the run.
    pub fn needs_retry(&self) -> bool {
        !matches!(self, Self::Stale | Self::Interrupted)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(detail) => write!(f, "invalid configuration: {detail}"),
            Self::Input(detail) => write!(f, "invalid input: {detail}"),
            Self::ModelOutput(detail) => write!(f, "invalid model output: {detail}"),
            Self::Provider(detail) => write!(f, "provider: {detail}"),
            Self::Tool(detail) => write!(f, "tool: {detail}"),
            // The engine's messages name authorization themselves.
            Self::Authorization(detail) => write!(f, "{detail}"),
            Self::Context(detail) => write!(f, "context: {detail}"),
            Self::ContextOverflow(detail) => write!(f, "context window exceeded: {detail}"),
            Self::Storage(detail) => write!(f, "storage: {detail}"),
            Self::IdempotencyConflict => f.write_str("conflicting idempotency key"),
            Self::ConversationBusy => f.write_str("conversation already has an active run"),
            Self::StoreBusy => f.write_str("store is leased by another host"),
            Self::Stale => f.write_str("stale execution generation or checkpoint revision"),
            Self::Incompatible(detail) => {
                write!(f, "incompatible state or agent binding: {detail}")
            }
            Self::NotFound => f.write_str("run not found"),
            Self::NotWaiting => f.write_str("operation is not waiting for this resolution"),
            Self::RunNotActive => f.write_str("run no longer accepts steering"),
            Self::Interrupted => f.write_str("execution interrupted"),
            Self::Waiting => {
                f.write_str("run is waiting; resolve Conversation::last_run and call Agent::resume")
            }
            Self::Stopped(reason) => write!(f, "run stopped: {reason}"),
            Self::Failed(detail) => write!(f, "run failed: {detail}"),
            Self::Cancelled => f.write_str("run cancelled"),
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Config(detail)
            | Self::Input(detail)
            | Self::ModelOutput(detail)
            | Self::Provider(detail)
            | Self::Tool(detail)
            | Self::Authorization(detail)
            | Self::Context(detail)
            | Self::ContextOverflow(detail)
            | Self::Storage(detail)
            | Self::Incompatible(detail)
            | Self::Failed(detail) => detail.0.cause.as_deref().map(|cause| cause as _),
            Self::IdempotencyConflict
            | Self::ConversationBusy
            | Self::StoreBusy
            | Self::Stale
            | Self::NotFound
            | Self::NotWaiting
            | Self::RunNotActive
            | Self::Interrupted
            | Self::Waiting
            | Self::Stopped(_)
            | Self::Cancelled => None,
        }
    }
}

/// What went wrong: a message, the underlying error, or a message and the error that caused it.
/// Displays as the message, the cause, or `message: cause`. Boxed, so [`Error`] stays small.
pub struct ErrorDetail(Box<Detail>);
struct Detail {
    message: Option<Cow<'static, str>>,
    cause: Option<Cause>,
}
impl ErrorDetail {
    /// A `&'static str` or `String` is a message, without a cause. Anything else is the cause.
    fn new<E: Into<Cause> + 'static>(cause: E) -> Self {
        let mut cause = Some(cause);
        let slot: &mut dyn Any = &mut cause;
        let message = if let Some(text) = slot.downcast_mut::<Option<&'static str>>() {
            text.take().map(Cow::Borrowed)
        } else if let Some(text) = slot.downcast_mut::<Option<String>>() {
            text.take().map(Cow::Owned)
        } else {
            None
        };
        Self(Box::new(Detail {
            message,
            cause: cause.map(Into::into),
        }))
    }
    pub(crate) fn message(message: impl Into<Cow<'static, str>>) -> Self {
        Self(Box::new(Detail {
            message: Some(message.into()),
            cause: None,
        }))
    }
    pub(crate) fn caused_by(
        message: impl Into<Cow<'static, str>>,
        cause: impl Into<Cause>,
    ) -> Self {
        Self(Box::new(Detail {
            message: Some(message.into()),
            cause: Some(cause.into()),
        }))
    }
}
impl fmt::Display for ErrorDetail {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (&self.0.message, &self.0.cause) {
            (Some(message), Some(cause)) => write!(f, "{message}: {cause}"),
            (Some(message), None) => f.write_str(message),
            (None, Some(cause)) => write!(f, "{cause}"),
            (None, None) => Ok(()),
        }
    }
}
impl fmt::Debug for ErrorDetail {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("ErrorDetail");
        if let Some(message) = &self.0.message {
            debug.field("message", message);
        }
        if let Some(cause) = &self.0.cause {
            debug.field("cause", cause);
        }
        debug.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strings_are_messages_and_errors_are_causes() {
        let message = Error::tool("timed out");
        assert_eq!(message.to_string(), "tool: timed out");
        assert!(message.source().is_none());
        let owned = Error::storage(format!("{} exhausted", "revision"));
        assert_eq!(owned.to_string(), "storage: revision exhausted");
        assert!(owned.source().is_none());

        let io = std::io::Error::other("disk full");
        let wrapped = Error::storage(io);
        assert_eq!(wrapped.to_string(), "storage: disk full");
        let cause = wrapped.source().unwrap();
        assert!(cause.downcast_ref::<std::io::Error>().is_some());

        let detail = ErrorDetail::caused_by("authorization failed", Error::tool("offline"));
        let error = Error::authorization(detail);
        assert_eq!(error.to_string(), "authorization failed: tool: offline");
        assert!(matches!(
            error.source().unwrap().downcast_ref::<Error>(),
            Some(Error::Tool(_))
        ));
    }
}
