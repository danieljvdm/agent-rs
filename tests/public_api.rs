//! The crate's intended public surface, named by path. Removing, renaming or moving any of these
//! items fails to compile here, so a change to the surface is visible in review. Helpers that
//! must stay private are guarded by `compile_fail` doctests in `src/lib.rs`.
#![allow(unused_imports)]

// Defining and running agents.
use agent_rs::{
    Agent, AgentBuilder, AllowAll, Authorizer, Checkpoint, ContextPolicy, ContextRequest,
    Conversation, MemoryCheckpoint, Model, ModelConfig, ModelEvent, ModelRequest, ModelStream,
    RunControl, ScriptedModel, Tool, ToolContext, ToolOutput, typed_tool,
};
// Attached subagents and the children they record.
use agent_rs::{AttachedChild, ChildOutcome, ChildResult, ParentRun, Subagent};
// Errors.
use agent_rs::{Error, ErrorDetail, Result};
// Run state and the data it records.
use agent_rs::{
    Execution, HistoryEntry, Limits, ModelResponse, Operation, OperationStatus, Phase,
    ProgressEvent, ProgressKind, Resolution, RetryPolicy, RunId, RunState, Settlement, StopReason,
    TokenUsage, ToolCall, ToolResult, ToolSpec, Usage, WaitRequest,
};
// The durable host and its storage contract.
use agent_rs::{
    CheckpointPosition, DurableStore, Host, HostOptions, HostSlot, MemoryStore, RunUpdate,
    StoreLease, UpdateEffect,
};
// Steering of active runs, for stores that support it.
use agent_rs::{SteeringId, SteeringInput, SteeringReceipt, SteeringStatus, SteeringStore};
// Interval automations, an extension of the storage contract.
use agent_rs::{
    Automation, AutomationChange, AutomationOptions, AutomationState, AutomationStore,
    FiringReport, IntervalAutomation, Occurrence, OccurrenceOutcome,
};
// Optional adapters, each behind its feature.
#[cfg(feature = "google-calendar")]
use agent_rs::google_calendar::{
    CalendarDay, CalendarScope, GoogleCalendar, Meeting, ReminderReceipt,
};
#[cfg(feature = "mcp")]
use agent_rs::mcp::{McpClient, McpOptions, McpTool, ServerIdentity, ToolDefinition, ToolPolicy};
#[cfg(feature = "openai")]
use agent_rs::openai::{OpenAiModel, OpenAiResponsesModel};
// Hidden and unsupported: probes for `tests/cost_budget.rs`.
#[cfg(feature = "openai")]
use agent_rs::openai::cost_probe::{Api, decode, request_body};
#[cfg(feature = "postgres")]
use agent_rs::postgres::{
    Config, NoTls, PostgresOptions, PostgresStore, StoreMetrics as PostgresStoreMetrics,
};
#[cfg(feature = "sqlite")]
use agent_rs::sqlite::{SqliteStore, StoreMetrics};
// Optional memory, behind its feature.
#[cfg(feature = "memory")]
use agent_rs::memory::{
    DERIVED_CONTEXT_VERSION, DerivedContext, DerivedKind, Embedder, Embedding,
    ExtractiveSummarizer, InMemorySourceStore, MemoryPolicy, MemoryRecord, MemorySource, Retriever,
    SemanticRetriever, SourceRef, SourceStore, SourceTrust, Summarizer, index_source,
    index_summary,
};

/// The root can be glob-imported beside the standard library: its `Result` defaults its error
/// type, so a two-parameter `Result` still means the standard one.
mod glob {
    use agent_rs::*;

    #[test]
    fn glob_import_keeps_two_parameter_results() {
        fn parse(text: &str) -> Result<u32, std::num::ParseIntError> {
            text.parse()
        }
        fn crate_result() -> Result<()> {
            Err(Error::NotFound)
        }
        assert_eq!(parse("7"), Ok(7));
        assert!(matches!(crate_result(), Err(Error::NotFound)));
    }
}
