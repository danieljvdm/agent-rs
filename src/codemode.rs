//! Code mode: one tool whose input is JavaScript that calls the agent's other tools as functions.
//!
//! [`CodeMode`] builds a [`Subagent`] the parent's model calls like any tool, by default named
//! `code`, with input `{code: string}`. The code is the body of an async function: it awaits
//! tool calls, runs them in parallel with `Promise.all`, catches their failures, and returns one
//! value. The parent's model receives `{result, logs}`, or an error result
//! `{error, logs, calls}` if the code threw.
//!
//! The script runs as an attached child run whose [`Model`](crate::Model) is a
//! [`ScriptModel`]. Each round of calls the script makes is a tool batch of that child, so every
//! call gets the child's authorization, approval, retry policy, budgets and crash recovery from
//! the subagent machinery, and its result is durable before the script goes on. Each model turn
//! replays the script from the top against the rounds recorded so far, so a [`ScriptRuntime`]
//! must be deterministic; [`QuickJs`] removes the built-ins that are not. [`QuickJs`] runs
//! in the host's process; [`Subprocess`] runs the same interpreter in a process of its own.
//!
//! Each round is a model call of the child, so a script of ten sequential calls spends eleven of
//! its parent's model calls. Replaying grows with the square of the rounds.
//!
//! ```
//! # use std::sync::Arc;
//! # use serde_json::{Value, json};
//! # use agent_rs::{*, codemode::CodeMode};
//! # fn main() -> Result<()> {
//! let search = typed_tool::<Value, Value, _, _>(
//!     ToolSpec::new("search", "1").with_retry(RetryPolicy::ReadOnly),
//!     |query, _| async move { Ok(ToolOutput::Complete(json!({"hits": [query]}))) },
//! );
//! let code = CodeMode::new("code").tools([search]).build()?;
//! let planner = Agent::builder("planner", Arc::new(ScriptedModel::new(vec![])))
//!     .subagents(2, [code])
//!     .build()?;
//! # Ok(())
//! # }
//! ```
mod declarations;
mod model;
mod process;
mod quickjs;
mod replay;
mod runtime;

pub use declarations::{declaration, description, identifier, input_type};
pub use model::{PinAll, ScriptModel};
pub use process::{Subprocess, serve};
pub use quickjs::QuickJs;
pub use runtime::{
    Answer, Logs, ScriptCall, ScriptFunction, ScriptLimits, ScriptRuntime, ScriptSession, Step,
};

use crate::{
    Agent, AllowAll, Authorizer, ChildOutcome, ChildResult, Limits, Result, Subagent, Tool,
    ToolSpec,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};

/// The input of a code mode tool.
#[derive(Serialize, Deserialize, JsonSchema)]
struct CodeInput {
    /// The body of an async JavaScript function that calls the declared functions and returns
    /// the result.
    code: String,
}

/// The limits a code mode child runs within unless [`CodeMode::limits`] sets others: 16 model
/// calls, which is 15 rounds of calls, 64 tool calls, one token, since a script reports none,
/// and 60 seconds of active time. The rest are [`Limits::default`].
pub fn default_limits() -> Limits {
    Limits::default()
        .with_max_model_calls(16)
        .with_max_tool_calls(64)
        .with_max_tokens(1)
        .with_active_budget(Duration::from_secs(60))
}

/// Builds a code mode tool: a [`Subagent`] that runs the parent's model's JavaScript against an
/// explicit list of tools. See the [module documentation](self).
#[must_use = "a code mode builder does nothing until `build` is called"]
pub struct CodeMode {
    name: String,
    tools: Vec<Arc<dyn Tool>>,
    authorizer: Arc<dyn Authorizer>,
    limits: Limits,
    script_limits: ScriptLimits,
    runtime: Arc<dyn ScriptRuntime>,
}
impl CodeMode {
    /// A code mode tool named `name`, with no tools yet, the [`default_limits`],
    /// [`ScriptLimits::default`], [`AllowAll`] and the [`QuickJs`] runtime.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            tools: Vec::new(),
            authorizer: Arc::new(AllowAll),
            limits: default_limits(),
            script_limits: ScriptLimits::default(),
            runtime: Arc::new(QuickJs),
        }
    }
    /// Adds tools the script may call, after any added before. Only these are declared to the
    /// model and callable; a tool of the parent is not, unless it is added here too.
    pub fn tools(mut self, tools: impl IntoIterator<Item = Arc<dyn Tool>>) -> Self {
        self.tools.extend(tools);
        self
    }
    /// Decides whether each of the script's calls may run, as
    /// [`AgentBuilder::authorizer`](crate::AgentBuilder::authorizer) does. Defaults to
    /// [`AllowAll`]. The authorizer sees the child's run.
    pub fn authorizer(mut self, authorizer: Arc<dyn Authorizer>) -> Self {
        self.authorizer = authorizer;
        self
    }
    /// The child run's limits, an allocation from its parent; see [`Subagent`]. Every round of
    /// calls spends a model call. Defaults to [`default_limits`].
    pub fn limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }
    /// Bounds on the script itself. Defaults to [`ScriptLimits::default`].
    pub fn script_limits(mut self, limits: ScriptLimits) -> Self {
        self.script_limits = limits;
        self
    }
    /// The interpreter scripts run in. Defaults to [`QuickJs`].
    pub fn runtime(mut self, runtime: Arc<dyn ScriptRuntime>) -> Self {
        self.runtime = runtime;
        self
    }
    /// Builds the child agent and the subagent that delegates to it. Its description is the
    /// [`description`] of the tools. Fails with [`Error::Config`](crate::Error::Config) as
    /// [`AgentBuilder::build`](crate::AgentBuilder::build) does, or if two tools map to the same
    /// function name.
    pub fn build(self) -> Result<Subagent> {
        let Self {
            name,
            tools,
            authorizer,
            limits,
            script_limits,
            runtime,
        } = self;
        let specs: Vec<ToolSpec> = tools.iter().map(|tool| tool.spec().clone()).collect();
        let model = ScriptModel::new(runtime, tools.iter().cloned(), script_limits)?;
        let agent = Agent::builder("codemode", Arc::new(model))
            .tools(tools)
            .limits(limits)
            .authorizer(authorizer)
            .context_policy(Arc::new(PinAll), 0)
            .input::<CodeInput>()
            .output::<Value>()
            .build()?;
        Ok(Subagent::new(name, description(&specs), agent).with_result(result))
    }
}

/// The delegation's tool result: the script's own output, an error if it threw, or an error
/// naming how the child ended otherwise.
fn result(result: ChildResult<Value>) -> (Value, bool) {
    match result.outcome {
        ChildOutcome::Completed(output) => {
            let threw = output.get("error").is_some();
            (output, threw)
        }
        ChildOutcome::Stopped(reason) => (json!({"error": format!("stopped: {reason}")}), true),
        ChildOutcome::Failed(message) => (json!({"error": format!("failed: {message}")}), true),
        ChildOutcome::Cancelled => (json!({"error": "cancelled"}), true),
    }
}
