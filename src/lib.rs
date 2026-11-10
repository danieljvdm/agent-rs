//! A resumable agent engine. SQLite and HTTP dependencies are optional features.
//!
//! Everything except the optional adapters is exported from the crate root, so
//! `use yielded_agent::*;` brings in the whole engine, the durable host and the storage contract.
//! The adapters live in their own modules: [`sqlite`] behind the `sqlite` feature and [`openai`]
//! behind the `openai` feature.
mod context;
mod engine;
mod error;
mod host;
mod model;
#[cfg(feature = "openai")]
pub mod openai;
#[cfg(feature = "sqlite")]
pub mod sqlite;
mod store;
mod tool;
mod types;

pub use context::{ContextPolicy, ContextRequest};
pub use engine::{Agent, AgentBuilder, Checkpoint, Conversation, MemoryCheckpoint, RunControl};
pub use error::{Error, ErrorDetail, Result};
pub use host::{Host, HostOptions};
pub use model::{Model, ModelConfig, ModelEvent, ModelRequest, ModelStream, ScriptedModel};
pub use store::{
    CheckpointPosition, DurableStore, HostSlot, MemoryStore, RunUpdate, StoreLease, UpdateEffect,
};
pub use tool::{AllowAll, Authorizer, Tool, ToolContext, ToolOutput, typed_tool};
pub use types::{
    Execution, HistoryEntry, Limits, ModelResponse, Operation, OperationStatus, Phase,
    ProgressEvent, ProgressKind, Resolution, RetryPolicy, RunId, RunState, Settlement, StopReason,
    TokenUsage, ToolCall, ToolResult, ToolSpec, Usage, WaitRequest,
};

/// Helpers the crate uses internally stay unreachable from outside it.
///
/// ```compile_fail,E0432
/// use yielded_agent::now_ms;
/// ```
///
/// ```compile_fail,E0432
/// use yielded_agent::STATE_VERSION;
/// ```
///
/// ```compile_fail,E0432
/// use yielded_agent::select_context;
/// ```
///
/// ```compile_fail,E0432
/// // Answer a wait with `RunState::resolve`.
/// use yielded_agent::resolve;
/// ```
///
/// ```compile_fail,E0603
/// // Modules other than the adapters are private; their items are exported at the root.
/// use yielded_agent::types::RunState;
/// ```
#[cfg(doctest)]
struct PrivateHelpers;
