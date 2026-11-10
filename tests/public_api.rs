//! The crate's intended public surface, named by path. Removing, renaming or moving any of these
//! items fails to compile here, so a change to the surface is visible in review. Helpers that
//! must stay private are guarded by `compile_fail` doctests in `src/lib.rs`.
#![allow(unused_imports)]

// Defining and running agents.
use yielded_agent::{
    Agent, AgentBuilder, AllowAll, Authorizer, Checkpoint, ContextPolicy, ContextRequest,
    Conversation, MemoryCheckpoint, Model, ModelConfig, ModelEvent, ModelRequest, ModelStream,
    RunControl, ScriptedModel, Tool, ToolContext, ToolOutput, typed_tool,
};
// Errors.
use yielded_agent::{Error, ErrorDetail, Result};
// Run state and the data it records.
use yielded_agent::{
    Execution, HistoryEntry, Limits, ModelResponse, Operation, OperationStatus, Phase,
    ProgressEvent, ProgressKind, Resolution, RetryPolicy, RunId, RunState, Settlement, StopReason,
    TokenUsage, ToolCall, ToolResult, ToolSpec, Usage, WaitRequest,
};
// The durable host and its storage contract.
use yielded_agent::host::{Host, HostOptions};
use yielded_agent::{
    CheckpointPosition, DurableStore, HostSlot, MemoryStore, RunUpdate, StoreLease, UpdateEffect,
};
// Optional adapters, each behind its feature.
#[cfg(feature = "openai")]
use yielded_agent::openai::OpenAiModel;
#[cfg(feature = "sqlite")]
use yielded_agent::sqlite::{SqliteStore, StoreMetrics};
