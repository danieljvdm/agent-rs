// The README is the crate documentation, so its examples compile as doctests. They use the
// default features; a build without them gets a summary in its place.
#![cfg_attr(
    all(feature = "sqlite", feature = "openai"),
    doc = include_str!("../README.md")
)]
#![cfg_attr(
    not(all(feature = "sqlite", feature = "openai")),
    doc = "A resumable agent engine. Build with the `sqlite` and `openai` features for the full crate documentation."
)]
// docs.rs labels each item with the feature that enables it.
#![cfg_attr(docsrs, feature(doc_cfg))]
#![forbid(unsafe_code)]
#![warn(missing_docs)]
mod automation;
mod children;
#[cfg(feature = "codemode")]
pub mod codemode;
mod context;
mod engine;
mod error;
#[cfg(feature = "google-calendar")]
pub mod google_calendar;
mod host;
#[cfg(feature = "mcp")]
pub mod mcp;
#[cfg(feature = "memory")]
pub mod memory;
mod model;
#[cfg(feature = "openai")]
pub mod openai;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "sqlite")]
pub mod sqlite;
mod steering;
mod store;
mod telemetry;
mod tool;
mod types;

pub use automation::{
    Automation, AutomationChange, AutomationOptions, AutomationState, AutomationStore,
    FiringReport, IntervalAutomation, Occurrence, OccurrenceOutcome,
};
pub use children::{AttachedChild, ChildOutcome, ChildResult, ParentRun};
pub use context::{ContextPolicy, ContextRequest};
pub use engine::{
    Agent, AgentBuilder, Checkpoint, Conversation, MemoryCheckpoint, RunControl, Subagent,
};
pub use error::{Error, ErrorDetail, Result};
pub use host::{Host, HostOptions};
pub use model::{Model, ModelConfig, ModelEvent, ModelRequest, ModelStream, ScriptedModel};
pub use steering::{SteeringId, SteeringInput, SteeringReceipt, SteeringStatus, SteeringStore};
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
/// use agent_rs::now_ms;
/// ```
///
/// ```compile_fail,E0432
/// use agent_rs::STATE_VERSION;
/// ```
///
/// ```compile_fail,E0432
/// use agent_rs::select_context;
/// ```
///
/// ```compile_fail,E0432
/// // Answer a wait with `RunState::resolve`.
/// use agent_rs::resolve;
/// ```
///
/// ```compile_fail,E0603
/// // Modules other than the optional features' are private; their items are exported at the
/// // root.
/// use agent_rs::types::RunState;
/// ```
#[cfg(doctest)]
struct PrivateHelpers;

#[cfg(test)]
#[path = "tracing_test.rs"]
mod tracing_capture;
