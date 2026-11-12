use crate::context::{assemble, encoded_len, suffix_start};
use crate::steering::Boundary;
use crate::types::{STATE_VERSION, now_ms};
use crate::{
    AllowAll, Authorizer, ContextPolicy, Error, ErrorDetail, Execution, HistoryEntry, Limits,
    Model, ModelConfig, ModelEvent, ModelRequest, ModelResponse, Operation, OperationStatus, Phase,
    ProgressEvent, ProgressKind, Result, RetryPolicy, RunId, RunState, Settlement, StopReason,
    Tool, ToolCall, ToolContext, ToolOutput, ToolResult, ToolSpec, Usage, WaitRequest,
};
use async_trait::async_trait;
use futures::{StreamExt, stream::FuturesUnordered};
use schemars::{JsonSchema, schema_for};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{
    any::{Any, TypeId},
    collections::{HashMap, HashSet},
    future::Future,
    marker::PhantomData,
    sync::Arc,
    time::Duration,
};
use tokio::{sync::mpsc, time::Instant};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span, field::Empty};

mod subagent;
use crate::telemetry;
use futures::future::Either;
pub use subagent::Subagent;
use subagent::{ChildWindow, Subagents};

type Validator = fn(&Value) -> serde_json::Result<()>;
/// Encodes a run's input. Input that already is a `Value` is moved, not copied.
fn into_json<T: Serialize + 'static>(input: T) -> Result<Value> {
    let mut input = Some(input);
    if let Some(value) = (&mut input as &mut dyn Any).downcast_mut::<Option<Value>>() {
        return Ok(value.take().unwrap_or_default());
    }
    serde_json::to_value(input).map_err(Error::input)
}
/// Checks that `value` deserializes as `T`. `&Value` is a deserializer, so this never copies it,
/// and any JSON is a `Value`, so an untyped agent checks nothing.
fn validate<T: DeserializeOwned + 'static>(value: &Value) -> serde_json::Result<()> {
    if TypeId::of::<T>() == TypeId::of::<Value>() {
        return Ok(());
    }
    T::deserialize(value).map(|_| ())
}

/// An immutable agent definition, built with [`Agent::builder`]. Product policy and clients are
/// supplied by its caller. The definition is shared: cloning an agent increments a reference
/// count.
///
/// `I` and `O` are the input and output types named with [`AgentBuilder::input`] and
/// [`AgentBuilder::output`]. The in-memory [`run`](Self::run) takes an `I` and returns an `O`,
/// so a mismatch does not compile:
///
/// ```
/// # use std::sync::Arc;
/// # use serde_json::json;
/// # use agent_rs::*;
/// # async fn example() -> Result<()> {
/// let model = ScriptedModel::new(vec![ModelResponse::final_output(json!("hi"))]);
/// let agent: Agent<String, String> = Agent::builder("greeter", Arc::new(model))
///     .input::<String>()
///     .output::<String>()
///     .build()?;
/// let reply: String = agent.run(&mut Conversation::default(), "hello").await?;
/// # Ok(()) }
/// ```
///
/// ```compile_fail
/// # use std::sync::Arc;
/// # use agent_rs::*;
/// # async fn example(agent: Agent<String, String>) -> Result<()> {
/// // The input must convert into `String`.
/// let reply: String = agent.run(&mut Conversation::default(), 42_u64).await?;
/// # Ok(()) }
/// ```
///
/// ```compile_fail
/// # use std::sync::Arc;
/// # use agent_rs::*;
/// # async fn example(agent: Agent<String, String>) -> Result<()> {
/// // The output is a `String`.
/// let reply: u64 = agent.run(&mut Conversation::default(), "hello").await?;
/// # Ok(()) }
/// ```
///
/// The plain `Agent`, with both types `Value`, is the erased form: a [`Host`](crate::Host)
/// serves agents of different types side by side, and a [`DurableStore`](crate::DurableStore) accepts JSON input for
/// any of them. [`erased`](Self::erased) converts a typed agent, sharing its definition.
pub struct Agent<I = Value, O = Value> {
    definition: Arc<Definition>,
    types: PhantomData<fn(I) -> O>,
}
impl<I, O> Clone for Agent<I, O> {
    fn clone(&self) -> Self {
        Self {
            definition: self.definition.clone(),
            types: PhantomData,
        }
    }
}
/// Everything an agent is, built and checked once and then shared by every run.
struct Definition {
    id: String,
    // Shared with every `ModelRequest` instead of copied into each one.
    instructions: Arc<str>,
    model: Arc<dyn Model>,
    config: Arc<ModelConfig>,
    /// Tools by name. `tool_specs` keeps their declaration order for the model, followed by the
    /// delegation specs of any subagents.
    tools: HashMap<String, Arc<dyn Tool>>,
    tool_specs: Arc<[ToolSpec]>,
    /// Child agents the model calls like tools, if any were configured.
    subagents: Option<Subagents>,
    limits: Limits,
    input_schema: Value,
    output_schema: Arc<Value>,
    validate_input: Validator,
    validate_output: Validator,
    authorizer: Arc<dyn Authorizer>,
    context_policy: Option<(Arc<dyn ContextPolicy>, usize)>,
    /// Computed once from the fields above by [`Definition::compose_binding`], shared by every
    /// run this agent accepts, and compared with each run `drive` resumes.
    binding: Arc<Value>,
}
impl Agent {
    /// Starts defining an agent with the two parts every agent needs. Everything else has a
    /// default and its own [`AgentBuilder`] method; [`AgentBuilder::build`] checks the whole
    /// definition once.
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use agent_rs::*;
    /// # fn main() -> Result<()> {
    /// let agent = Agent::builder("support", Arc::new(ScriptedModel::new(vec![])))
    ///     .instructions("Answer briefly.")
    ///     .limits(Limits::default().with_max_model_calls(8))
    ///     .build()?;
    /// assert_eq!(agent.id(), "support");
    /// # Ok(())
    /// # }
    /// ```
    pub fn builder(id: impl Into<String>, model: Arc<dyn Model>) -> AgentBuilder {
        AgentBuilder {
            id: id.into(),
            instructions: String::new(),
            model,
            config: ModelConfig::default(),
            tools: Vec::new(),
            limits: Limits::default(),
            authorizer: Arc::new(AllowAll),
            context_policy: None,
            subagents: None,
            types: PhantomData,
        }
    }
}
impl<I, O> Agent<I, O> {
    /// The ID given to [`Agent::builder`], recorded with every run the agent accepts.
    pub fn id(&self) -> &str {
        &self.definition.id
    }
    /// The limits every run of this agent is accepted with.
    pub fn limits(&self) -> &Limits {
        &self.definition.limits
    }
    /// The durable identity of this definition. Every run it accepts records it, and a stored
    /// run resumes only with an agent whose binding is equal.
    pub fn binding(&self) -> &Value {
        &self.definition.binding
    }
    /// Accepts `input`, retaining the bounded suffix of `conversation` that fits the context limit.
    ///
    /// The run is in memory only; drive it with [`drive`](Self::drive). Fails with
    /// [`Error::Input`] for input that does not deserialize as the agent's input type or does not
    /// fit `max_context_bytes`, or a conversation ID outside 1..=1024 bytes, and with
    /// [`Error::Context`] when the conversation's newest entry cannot fit the limit.
    pub fn new_run(
        &self,
        conversation_id: impl Into<String>,
        input: Value,
        conversation: &[HistoryEntry],
        expires_at_ms: Option<u64>,
    ) -> Result<RunState> {
        let definition = &*self.definition;
        (definition.validate_input)(&input).map_err(Error::input)?;
        let conversation_id = conversation_id.into();
        if conversation_id.is_empty() || conversation_id.len() > 1024 {
            return Err(Error::input("conversation ID must be 1..1024 bytes"));
        }
        let max_context_bytes = definition.limits.max_context_bytes;
        let input_entry = HistoryEntry::Input(input.clone());
        if encoded_len(&input_entry) + 2 > max_context_bytes {
            return Err(Error::input("input exceeds context limit"));
        }
        let prior_context = conversation[suffix_start(conversation, max_context_bytes)?..].to_vec();
        Ok(RunState {
            version: STATE_VERSION,
            id: RunId::new_v4(),
            conversation_id,
            agent_id: definition.id.clone(),
            binding: definition.binding.clone(),
            input,
            limits: definition.limits.clone(),
            expires_at_ms,
            generation: 0,
            revision: 0,
            cancel_requested: false,
            phase: Phase::Ready,
            usage: Usage::default(),
            prior_context,
            history: vec![input_entry],
            pending_response: None,
            operations: vec![],
            parent: None,
            children: vec![],
            child_usage: Usage::default(),
        })
    }
    /// The same agent with its types erased, to serve beside agents of other types or to
    /// accept JSON input. Shares the definition, so its binding is the same.
    pub fn erased(&self) -> Agent {
        Agent {
            definition: self.definition.clone(),
            types: PhantomData,
        }
    }
    /// Drives a run this agent accepted, or one stored with an equal binding, until it settles
    /// or waits. The same state machine serves memory and durable execution. No child work is
    /// detached.
    ///
    /// A failure of the run's own work settles it as [`Settlement::Failed`] and returns `Ok`;
    /// only errors for which [`Error::fails_run`] is false are returned, leaving the run as of
    /// its last checkpoint.
    pub async fn drive(
        &self,
        state: &mut RunState,
        checkpoint: &dyn Checkpoint,
        control: RunControl,
    ) -> Result<()> {
        self.definition.drive(state, checkpoint, control).await
    }
}
impl<I: Serialize + 'static, O: DeserializeOwned> Agent<I, O> {
    /// Convenience API for runs that finish without application input. Use `drive` for
    /// resumable waits.
    ///
    /// Returns the run's output once it completes. A run that ends any other way is an error
    /// naming how, and stays in `conversation.last_run`:
    /// - [`Error::Waiting`]: a tool waits for approval, input, reconciliation or a timer.
    ///   Resolve it through `last_run` and call [`resume`](Self::resume).
    /// - [`Error::Stopped`]: a limit stopped the run, with its [`StopReason`].
    /// - [`Error::Failed`]: the run failed, with its [`Settlement::Failed`] message.
    /// - [`Error::Cancelled`]: the run was cancelled.
    ///
    /// Other errors are the request's or the engine's own, as for [`drive`](Self::drive).
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use serde_json::json;
    /// # use agent_rs::*;
    /// # async fn example(agent: Agent<String, String>) -> Result<()> {
    /// let mut conversation = Conversation::default();
    /// match agent.run(&mut conversation, "hello").await {
    ///     Ok(reply) => println!("{reply}"),
    ///     Err(Error::Stopped(StopReason::Tokens)) => println!("out of tokens"),
    ///     Err(Error::Waiting) => {
    ///         let run = conversation.last_run.as_mut().expect("retained");
    ///         let call_id = run.operations[0].call.id.clone();
    ///         run.resolve(&call_id, Resolution::Approve(true))?;
    ///         let reply = agent.resume(&mut conversation, RunControl::default()).await?;
    ///         println!("{reply}");
    ///     }
    ///     Err(error) => return Err(error),
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn run(&self, conversation: &mut Conversation, input: impl Into<I>) -> Result<O> {
        conversation.check_owns_last_run()?;
        if conversation
            .last_run
            .as_ref()
            .is_some_and(|run| !run.is_terminal() || run.has_uncertainty())
        {
            return Err(Error::ConversationBusy);
        }
        let input = into_json(input.into())?;
        let state = self.new_run(conversation.id(), input, &conversation.history, None)?;
        conversation.last_run = Some(state);
        self.resume_from(conversation, RunControl::default(), 0)
            .await
    }
    /// Resume a retained in-memory run after resolving its wait through `last_run`. Ends as
    /// [`run`](Self::run) does.
    pub async fn resume(&self, conversation: &mut Conversation, control: RunControl) -> Result<O> {
        let old_len = conversation
            .last_run
            .as_ref()
            .ok_or(Error::NotFound)?
            .history
            .len();
        self.resume_from(conversation, control, old_len).await
    }
    async fn resume_from(
        &self,
        conversation: &mut Conversation,
        control: RunControl,
        old_len: usize,
    ) -> Result<O> {
        conversation.check_owns_last_run()?;
        let state = conversation.last_run.as_mut().ok_or(Error::NotFound)?;
        let result = self.drive(state, &MemoryCheckpoint, control).await;
        conversation
            .history
            .extend(state.history[old_len..].iter().cloned());
        result?;
        match &state.phase {
            Phase::Settled(Settlement::Completed(value)) => {
                O::deserialize(value).map_err(Error::model_output)
            }
            Phase::Settled(Settlement::Stopped(reason)) => Err(Error::Stopped(reason.clone())),
            Phase::Settled(Settlement::Failed(reason)) => {
                Err(Error::Failed(ErrorDetail::message(reason.clone())))
            }
            Phase::Settled(Settlement::Cancelled) => Err(Error::Cancelled),
            Phase::Ready | Phase::ModelInFlight | Phase::Tools => Err(Error::Waiting),
        }
    }
}
impl Definition {
    /// The one place the durable binding is composed, once, by [`AgentBuilder::build`].
    /// Everything a stored run must agree with to resume contributes here and only here: a new
    /// part, such as a model adapter's identity or attached subagents, is one more optional key.
    /// A part that is absent adds no key, so adding one leaves the binding of every existing
    /// definition byte-identical.
    fn compose_binding(&self) -> Value {
        let mut binding = json!({"id":self.id,"instructions":&*self.instructions,"model":&*self.config,"tools":&*self.tool_specs,
            "input_schema":self.input_schema,"output_schema":&*self.output_schema,"limits":self.limits});
        if let Some((policy, max_bytes)) = &self.context_policy {
            binding["context_policy"] = json!({"binding":policy.binding(),"max_bytes":max_bytes});
        }
        let adapter = self.model.binding();
        if !adapter.is_null() {
            binding["model_adapter"] = adapter;
        }
        if let Some(subagents) = &self.subagents {
            binding["subagents"] = subagents.binding();
        }
        binding
    }
    /// What the model's call of `name` runs: the one place an operation is classified.
    fn work(&self, name: &str) -> Option<Work<'_>> {
        match self.tools.get(name) {
            Some(tool) => Some(Work::Tool(tool)),
            None => self.subagents.as_ref()?.get(name).map(Work::Delegate),
        }
    }
    /// What an operation recorded runs, provided this definition has it under the same contract.
    fn recorded(&self, op: &Operation) -> Result<Work<'_>> {
        self.work(&op.call.name)
            .filter(|work| work.spec() == &op.spec)
            .ok_or_else(|| Error::incompatible(format!("tool contract differs: {}", op.call.name)))
    }
}
/// What one operation runs: a tool, or a delegation to a child agent.
#[derive(Clone, Copy)]
enum Work<'a> {
    Tool(&'a Arc<dyn Tool>),
    Delegate(&'a Subagent),
}
impl Work<'_> {
    fn spec(&self) -> &ToolSpec {
        match self {
            Self::Tool(tool) => tool.spec(),
            Self::Delegate(subagent) => subagent.spec(),
        }
    }
    fn validate(&self, arguments: &Value) -> Result<()> {
        match self {
            Self::Tool(tool) => tool.validate(arguments),
            Self::Delegate(subagent) => subagent.validate(arguments),
        }
    }
}

/// Defines an [`Agent`], from [`Agent::builder`]. Options may be set in any order and nothing
/// is checked until [`build`](Self::build).
///
/// `I` and `O` are the agent's input and output types, `Value` until
/// [`input`](Self::input) or [`output`](Self::output) names them. They fix the schemas the
/// agent publishes and validates against.
#[must_use = "an agent builder does nothing until `build` is called"]
pub struct AgentBuilder<I = Value, O = Value> {
    id: String,
    instructions: String,
    model: Arc<dyn Model>,
    config: ModelConfig,
    tools: Vec<Arc<dyn Tool>>,
    limits: Limits,
    authorizer: Arc<dyn Authorizer>,
    context_policy: Option<(Arc<dyn ContextPolicy>, usize)>,
    subagents: Option<(usize, Vec<Subagent>)>,
    types: PhantomData<fn(I) -> O>,
}
impl<I, O> AgentBuilder<I, O> {
    /// System instructions for every model turn. Defaults to none.
    pub fn instructions(mut self, instructions: impl Into<String>) -> Self {
        self.instructions = instructions.into();
        self
    }
    /// Provider configuration for every model turn. Defaults to [`ModelConfig::default`].
    pub fn config(mut self, config: ModelConfig) -> Self {
        self.config = config;
        self
    }
    /// Adds one tool. Tools are offered to the model in the order they are added.
    pub fn tool(mut self, tool: Arc<dyn Tool>) -> Self {
        self.tools.push(tool);
        self
    }
    /// Adds several tools, after any already added.
    pub fn tools(mut self, tools: impl IntoIterator<Item = Arc<dyn Tool>>) -> Self {
        self.tools.extend(tools);
        self
    }
    /// Bounds for every run. Defaults to [`Limits::default`].
    pub fn limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }
    /// Decides whether each tool call may run. Defaults to [`AllowAll`].
    pub fn authorizer(mut self, authorizer: Arc<dyn Authorizer>) -> Self {
        self.authorizer = authorizer;
        self
    }
    /// Opt into a [`ContextPolicy`]. `max_derived_bytes` caps the encoded array of its derived
    /// context within `max_context_bytes`; a policy that only pins entries may pass zero.
    pub fn context_policy(
        mut self,
        policy: Arc<dyn ContextPolicy>,
        max_derived_bytes: usize,
    ) -> Self {
        self.context_policy = Some((policy, max_derived_bytes));
        self
    }
    /// Child agents the model can delegate to, each called like a tool; see [`Subagent`]. At
    /// most `max_parallel` children run at once, 1..=1024, beside up to `max_parallel_tools`
    /// tools. Replaces any subagents set before.
    pub fn subagents(
        mut self,
        max_parallel: usize,
        subagents: impl IntoIterator<Item = Subagent>,
    ) -> Self {
        self.subagents = Some((max_parallel, subagents.into_iter().collect()));
        self
    }
    /// Names the input type. The agent publishes its JSON schema and rejects input that does not
    /// deserialize as `T`.
    pub fn input<T>(self) -> AgentBuilder<T, O> {
        self.retype()
    }
    /// Names the output type. The model is given its JSON schema, and a final output that does
    /// not deserialize as `T` fails the run.
    pub fn output<T>(self) -> AgentBuilder<I, T> {
        self.retype()
    }
    fn retype<I2, O2>(self) -> AgentBuilder<I2, O2> {
        AgentBuilder {
            id: self.id,
            instructions: self.instructions,
            model: self.model,
            config: self.config,
            tools: self.tools,
            limits: self.limits,
            authorizer: self.authorizer,
            context_policy: self.context_policy,
            subagents: self.subagents,
            types: PhantomData,
        }
    }
}
impl<I, O> AgentBuilder<I, O>
where
    I: DeserializeOwned + JsonSchema + 'static,
    O: DeserializeOwned + JsonSchema + 'static,
{
    /// Checks the definition and composes its binding. Fails with [`Error::Config`] for an ID
    /// outside 1..=256 bytes, a zero parallelism, byte limit or timeout, a duplicate, unbounded
    /// or unversioned tool name, an idempotent tool without a retry contract, a derived
    /// context cap above `max_context_bytes`, or subagents whose parallelism is outside
    /// 1..=1024, whose names are unbounded or shared with a tool or another subagent, or whose
    /// agents have subagents of their own.
    pub fn build(self) -> Result<Agent<I, O>> {
        let Self {
            id,
            instructions,
            model,
            config,
            tools,
            limits,
            authorizer,
            context_policy,
            subagents,
            types: _,
        } = self;
        if id.is_empty() || id.len() > 256 {
            return Err(Error::config("agent ID must be 1..256 bytes"));
        }
        if limits.max_parallel_tools == 0
            || limits.max_response_bytes == 0
            || limits.max_tool_output_bytes == 0
            || limits.max_context_bytes < 2
            || limits.operation_timeout_ms == 0
            || limits.cleanup_timeout_ms == 0
        {
            return Err(Error::config(
                "parallelism, byte limits, and timeouts must be positive",
            ));
        }
        let mut by_name = HashMap::with_capacity(tools.len());
        for tool in &tools {
            let spec = tool.spec();
            if spec.name.is_empty()
                || spec.name.len() > 256
                || by_name.insert(spec.name.clone(), tool.clone()).is_some()
                || spec.version.is_empty()
            {
                return Err(Error::config(
                    "tool names must be unique, bounded, and versioned",
                ));
            }
            if matches!(&spec.retry, RetryPolicy::Idempotent { contract } if contract.is_empty()) {
                return Err(Error::config("idempotent tools require a retry contract"));
            }
        }
        if context_policy
            .as_ref()
            .is_some_and(|(_, max_derived_bytes)| *max_derived_bytes > limits.max_context_bytes)
        {
            return Err(Error::config(
                "derived context cap must fit the context limit",
            ));
        }
        let subagents = subagents
            .map(|(max_parallel, subagents)| Subagents::new(max_parallel, subagents, &by_name))
            .transpose()?
            .flatten();
        let mut definition = Definition {
            id,
            instructions: instructions.into(),
            model,
            config: Arc::new(config),
            tool_specs: match &subagents {
                None => tools.iter().map(|tool| tool.spec().clone()).collect(),
                Some(subagents) => (tools.iter().map(|tool| tool.spec().clone()))
                    .chain(subagents.specs())
                    .collect(),
            },
            tools: by_name,
            subagents,
            limits,
            input_schema: serde_json::to_value(schema_for!(I)).expect("schema serialization"),
            output_schema: Arc::new(
                serde_json::to_value(schema_for!(O)).expect("schema serialization"),
            ),
            validate_input: validate::<I>,
            validate_output: validate::<O>,
            authorizer,
            context_policy,
            binding: Arc::default(),
        };
        definition.binding = Arc::new(definition.compose_binding());
        Ok(Agent {
            definition: Arc::new(definition),
            types: PhantomData,
        })
    }
}

/// A commit must atomically retain evidence, operations, and the new checkpoint.
/// On success it advances `revision`; on failure callers must reload authoritative state.
///
/// A commit that is fenced fails with [`Error::Stale`], one whose stored state cannot hold this
/// run with [`Error::Incompatible`], and any other failure with [`Error::Storage`]. The engine
/// reports any other kind of error as `Storage`, with the original as its cause, so a failed
/// commit never fails the run: see [`Error::fails_run`].
///
/// # What a commit may change
///
/// A successful commit advances `state.revision` and changes nothing else in the caller's state,
/// with two exceptions. Only a [`SteeringStore`](crate::SteeringStore) makes them, only while the
/// run has queued instructions, and each is part of the same atomic, fenced write:
///
/// - **Dispatch.** A commit of a state in [`Phase::ModelInFlight`], which is the commit of a model
///   call's intent, appends every queued instruction to `state.history` as a
///   [`HistoryEntry::Steering`], in acceptance order, and persists them as applied. The engine
///   assembles the turn's context after this commit, so the model request includes them.
/// - **Completion.** A commit of [`Settlement::Completed`] persists [`Phase::Ready`] instead and
///   sets `state.phase` to `Ready`: the completed exchange stays in history, and the run goes on
///   to another model turn, which applies the instructions. Other settlements are final, and
///   instructions still queued then are never applied.
///
/// Steering targets a root run, never a child of an [attached subagent](crate::Subagent). A
/// child's progress, admission and settlement reach its root's checkpoint as commits of the root,
/// in [`Phase::Tools`]: the engine applies each to the root's copy of the child before the root
/// commits, so they are the caller's changes, not the commit's, and a commit never changes
/// `state.children`. None of these commits is a dispatch, so queued instructions wait for the
/// root's own next model turn; a root completes only once every child has settled, so a reopened
/// completion never leaves a child unsettled; and no child's history ever receives an
/// instruction. Child work adds no exception to the list above.
///
/// The engine checks every commit against this list and reports any other change as
/// [`Error::Storage`].
#[async_trait]
pub trait Checkpoint: Send + Sync {
    /// Persists `state` as the run's new checkpoint and advances `state.revision`.
    async fn commit(&self, state: &mut RunState) -> Result<()>;
}
/// A checkpoint that persists nothing and only advances the revision, for runs that live in
/// memory, as [`Agent::run`] does.
#[derive(Default)]
pub struct MemoryCheckpoint;
#[async_trait]
impl Checkpoint for MemoryCheckpoint {
    async fn commit(&self, state: &mut RunState) -> Result<()> {
        state.revision += 1;
        Ok(())
    }
}
/// How the caller of [`Agent::drive`] can stop a run and observe it. The default never stops
/// and sends no events:
///
/// ```
/// # use tokio_util::sync::CancellationToken;
/// # use agent_rs::RunControl;
/// let cancel = CancellationToken::new();
/// let control = RunControl::default().with_cancel(cancel.clone());
/// ```
#[derive(Clone, Default)]
#[non_exhaustive]
pub struct RunControl {
    /// Cancelling it settles the run as [`Settlement::Cancelled`], interrupting the work in
    /// flight; an operation dispatched without a recorded outcome is left
    /// [`Unknown`](OperationStatus::Unknown).
    pub cancel: CancellationToken,
    /// Cancelling it stops driving the run without settling it: `drive` returns
    /// [`Error::Interrupted`] and the run resumes from its last checkpoint.
    pub shutdown: CancellationToken,
    /// Best-effort transient events, each naming its run, so one channel may serve many runs.
    /// A full channel drops events and never blocks execution.
    pub events: Option<mpsc::Sender<ProgressEvent>>,
}
impl RunControl {
    /// Sets [`cancel`](Self::cancel).
    pub fn with_cancel(mut self, cancel: CancellationToken) -> Self {
        self.cancel = cancel;
        self
    }
    /// Sets [`shutdown`](Self::shutdown).
    pub fn with_shutdown(mut self, shutdown: CancellationToken) -> Self {
        self.shutdown = shutdown;
        self
    }
    /// Sends progress events to `events`; see [`events`](Self::events).
    pub fn with_events(mut self, events: mpsc::Sender<ProgressEvent>) -> Self {
        self.events = Some(events);
        self
    }
}
/// An in-memory conversation for [`Agent::run`] and [`Agent::resume`]: its ID, its history and
/// its latest run. Start an empty one with [`Conversation::default`].
///
/// Every run of the conversation records its [`id`](Self::id) as `RunState::conversation_id`,
/// and a [`ContextPolicy`] receives it to scope per-conversation state. A default conversation
/// has a fresh, unique ID; name one with [`Conversation::new`] when that state outlives the
/// process.
#[derive(Default, Debug)]
#[non_exhaustive]
pub struct Conversation {
    /// Every entry of every run, in order. A run starts from the suffix that fits its context
    /// limit; `run` and `resume` append the run's new entries when they return.
    pub history: Vec<HistoryEntry>,
    /// The latest run, retained even when `run` or `resume` returns an error, so its waits and
    /// uncertain outcomes stay visible and resolvable with [`RunState::resolve`]. `run` and
    /// `resume` fail with [`Error::Input`] if it belongs to another conversation ID.
    pub last_run: Option<RunState>,
    id: ConversationId,
}
impl Conversation {
    /// An empty conversation named `id`, which must be 1..=1024 bytes for a run to start.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            history: Vec::new(),
            last_run: None,
            id: ConversationId::Named(id.into()),
        }
    }
    /// A conversation with a fresh, unique ID that continues from `history`, with no run in
    /// progress.
    pub fn with_history(history: Vec<HistoryEntry>) -> Self {
        Self {
            history,
            last_run: None,
            id: ConversationId::default(),
        }
    }
    /// The conversation ID every run of it records. It never changes.
    pub fn id(&self) -> &str {
        self.id.as_str()
    }
    /// Refuses a retained run accepted under another ID, so per-conversation state stays scoped.
    fn check_owns_last_run(&self) -> Result<()> {
        match &self.last_run {
            Some(run) if run.conversation_id != self.id() => Err(Error::input(
                "the retained run belongs to another conversation ID",
            )),
            _ => Ok(()),
        }
    }
}
/// A conversation's ID. A generated one is kept inline, so a default conversation allocates
/// nothing until a run copies its ID.
enum ConversationId {
    Named(String),
    /// A hyphenated, lowercase v4 UUID.
    Generated([u8; 36]),
}
impl Default for ConversationId {
    fn default() -> Self {
        let mut id = [0; 36];
        RunId::new_v4().hyphenated().encode_lower(&mut id);
        Self::Generated(id)
    }
}
impl ConversationId {
    fn as_str(&self) -> &str {
        match self {
            Self::Named(id) => id,
            Self::Generated(id) => std::str::from_utf8(id).expect("a hyphenated UUID is ASCII"),
        }
    }
}
impl std::fmt::Debug for ConversationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_str().fmt(f)
    }
}
/// The state machine runs on the shared definition of the agent being driven.
impl Definition {
    async fn drive(
        &self,
        state: &mut RunState,
        checkpoint: &dyn Checkpoint,
        control: RunControl,
    ) -> Result<()> {
        let span = tracing::info_span!("invoke_agent",
            otel.name = %format_args!("invoke_agent {}", self.id), otel.kind = "internal",
            gen_ai.operation.name = "invoke_agent", gen_ai.agent.id = %self.id, run_id = %state.id,
            parent_run_id = state.parent.as_ref().map(|p| tracing::field::display(p.run_id)),
            gen_ai.conversation.id = state.parent.is_none().then_some(state.conversation_id.as_str()),
            outcome = Empty, error.type = Empty
        );
        let result = async {
            // A run accepted by this definition shares its binding; a stored one compares by value.
            if state.version != STATE_VERSION
                || state.agent_id != self.id
                || !(Arc::ptr_eq(&state.binding, &self.binding) || state.binding == self.binding)
                || state.limits != self.limits
            {
                return Err(Error::incompatible(
                    "persisted state or immutable agent definition differs",
                ));
            }
            if state.is_terminal() {
                return Ok(());
            }
            let mut recovered = self.recover(state)?;
            if !state.children.is_empty() {
                recovered |= subagent::recover_children(self, state)?;
            }
            if recovered {
                commit(checkpoint, state, &control).await?;
            }
            loop {
                if state.cancel_requested || control.cancel.is_cancelled() {
                    state.cancel_requested = true;
                    uncertain_dispatched(state, "cancelled after dispatch");
                    return settle(self, checkpoint, state, &control, Settlement::Cancelled).await;
                }
                if control.shutdown.is_cancelled() {
                    return Err(Error::Interrupted);
                }
                if state.expires_at_ms.is_some_and(|t| t <= now_ms()) {
                    return settle(
                        self,
                        checkpoint,
                        state,
                        &control,
                        Settlement::Stopped(StopReason::Expired),
                    )
                    .await;
                }
                let batch = matches!(state.phase, Phase::Tools);
                let step = match state.phase {
                    Phase::Ready => {
                        if let Some(reason) = exhausted(state, true, 0) {
                            return settle(
                                self,
                                checkpoint,
                                state,
                                &control,
                                Settlement::Stopped(reason),
                            )
                            .await;
                        }
                        self.model_turn(state, checkpoint, &control).await
                    }
                    Phase::Tools => self.tool_batch(state, checkpoint, &control).await,
                    Phase::Settled(_) => return Ok(()),
                    Phase::ModelInFlight => {
                        return Err(Error::input("unexpected in-flight model state"));
                    }
                };
                match step {
                    Ok(()) => {}
                    // Cancellation interrupted the work; the next iteration settles the run.
                    Err(Error::Interrupted) if control.cancel.is_cancelled() => continue,
                    Err(error) if error.fails_run() => {
                        let reason = bounded_error(&error, state.limits.max_tool_output_bytes);
                        return settle(
                            self,
                            checkpoint,
                            state,
                            &control,
                            Settlement::Failed(reason),
                        )
                        .await;
                    }
                    Err(error) => return Err(error),
                }
                // A batch still in `Tools` waits on approval, input, reconciliation or a timer.
                if batch && matches!(state.phase, Phase::Tools) {
                    return Ok(());
                }
            }
        }
        .instrument(span.clone())
        .await;
        telemetry::record_drive(&span, state, &result);
        result
    }

    /// Recovers work an earlier execution left in flight: a model call is made again, and a
    /// dispatched call that must not repeat becomes `Unknown`. Returns whether anything changed.
    fn recover(&self, state: &mut RunState) -> Result<bool> {
        let mut recovered = false;
        if matches!(state.phase, Phase::ModelInFlight) {
            telemetry::Call::model(state).recovered(None);
            state.phase = Phase::Ready;
            recovered = true;
        }
        let call = telemetry::Call::model(state);
        for (index, op) in state.operations.iter_mut().enumerate() {
            if matches!(op.status, OperationStatus::Dispatched) {
                self.recorded(op)?;
                if op.spec.retry == RetryPolicy::Never {
                    op.status = OperationStatus::Unknown {
                        reason: "execution ended after dispatch without a recorded outcome".into(),
                    };
                    let call = telemetry::Call {
                        index: Some(index),
                        ..call
                    };
                    call.recovered(Some(&op.spec.name));
                    telemetry::unknown(&Span::none(), op, call);
                    recovered = true;
                }
                // Keep retryable uncertain work Dispatched until it really retries. This
                // preserves the uncertainty if cancellation arrives during authorization.
            }
        }
        Ok(recovered)
    }
    async fn model_turn(
        &self,
        state: &mut RunState,
        checkpoint: &dyn Checkpoint,
        control: &RunControl,
    ) -> Result<()> {
        let span = tracing::info_span!("chat",
            otel.name = %format_args!("chat {}", self.config.model), otel.kind = "client",
            gen_ai.operation.name = "chat", gen_ai.request.model = %self.config.model,
            turn = state.usage.model_calls.saturating_sub(state.child_usage.model_calls) + 1,
            gen_ai.usage.input_tokens = Empty, gen_ai.usage.output_tokens = Empty,
            response_bytes = Empty, time_to_first_delta_ms = Empty, outcome = Empty, error.type = Empty
        );
        // Reported usage survives rejection, interruption, timeout, and cleanup failure.
        let mut usage = crate::TokenUsage::default();
        let mut bytes = 0_usize;
        let mut response_bytes = 0_usize;
        let mut first_delta = None;
        let mut outcome_name = "rejected";
        let result = async {
            let (reservation, work_ms) = reserve(state);
            state.usage.model_calls += 1;
            state.phase = Phase::ModelInFlight;
            emit(state, control, ProgressKind::ModelStarted);
            commit(checkpoint, state, control).await?;
            let started = Instant::now();
            let operation_cancel = CancellationToken::new();
            let outcome = {
                let future = async {
                    // Assemble context after the dispatch checkpoint and inside the operation's
                    // deadline, so the checkpoint can contribute history and preparation is bounded.
                    let (history, derived_context) =
                        assemble(state, self.context_policy.as_ref(), &operation_cancel).await?;
                    let request = ModelRequest {
                        run_id: state.id,
                        instructions: self.instructions.clone(),
                        config: self.config.clone(),
                        history,
                        derived_context,
                        tools: self.tool_specs.clone(),
                        output_schema: self.output_schema.clone(),
                        // Children's model calls are in the run's usage but are not its turns.
                        turn: (state.usage.model_calls)
                            .saturating_sub(state.child_usage.model_calls),
                        max_response_bytes: state.limits.max_response_bytes,
                        remaining_tokens: state.limits.max_tokens.saturating_sub(
                            state
                                .usage
                                .input_tokens
                                .saturating_add(state.usage.output_tokens),
                        ),
                    };
                    let mut stream = self
                        .model
                        .stream(request, operation_cancel.clone())
                        .await
                        .map_err(provider_error)?;
                    let mut response = None;
                    let mut events = 0_usize;
                    while let Some(event) = stream.next().await {
                        events += 1;
                        if events > state.limits.max_response_bytes.clamp(1, 65_536) {
                            return Err(Error::provider("model event count exceeds limit"));
                        }
                        if response.is_some() {
                            return Err(Error::provider(
                                "model emitted data after its complete response",
                            ));
                        }
                        match event.map_err(provider_error)? {
                            ModelEvent::Usage(reported) => record_usage(&mut usage, &reported)?,
                            ModelEvent::TextDelta(text) => {
                                first_delta.get_or_insert_with(|| started.elapsed());
                                bytes = bytes.saturating_add(text.len());
                                if bytes > state.limits.max_response_bytes {
                                    return Err(Error::provider(
                                        "stream exceeds response byte limit",
                                    ));
                                }
                                emit(state, control, ProgressKind::TextDelta(text));
                            }
                            ModelEvent::ReasoningDelta(text) => {
                                first_delta.get_or_insert_with(|| started.elapsed());
                                bytes = bytes.saturating_add(text.len());
                                if bytes > state.limits.max_response_bytes {
                                    return Err(Error::provider(
                                        "stream exceeds response byte limit",
                                    ));
                                }
                                emit(state, control, ProgressKind::ReasoningDelta(text));
                            }
                            ModelEvent::Response(value) => {
                                record_usage(&mut usage, &value.usage)?;
                                response_bytes = encoded_len(&value);
                                if response_bytes > state.limits.max_response_bytes {
                                    return Err(Error::provider("response exceeds byte limit"));
                                }
                                response = Some(value);
                            }
                        }
                    }
                    response
                        .ok_or_else(|| Error::provider("stream ended without a complete response"))
                };
                run_work(future, work_deadline(state, started, work_ms), control).await
            };
            match &outcome {
                WorkOutcome::TimedOut => {
                    outcome_name = "timed_out";
                    telemetry::Call::model(state).timed_out("chat", None);
                }
                WorkOutcome::Interrupted => outcome_name = "interrupted",
                WorkOutcome::Done(Ok(_)) => outcome_name = "done",
                WorkOutcome::Done(Err(_)) => {}
            }
            operation_cancel.cancel();
            let cleanup = telemetry::cleanup(
                tracing::debug_span!("model_cleanup", outcome = Empty, error.type = Empty),
                telemetry::Call::model(state),
                None,
                state.limits.cleanup_timeout_ms,
                async {
                    let model = self.model.cleanup(state.id);
                    match &self.context_policy {
                        None => model.await,
                        Some((policy, _)) => {
                            let context = policy
                                .cleanup(state.id)
                                .instrument(tracing::debug_span!("context_cleanup"));
                            let (model, context) = tokio::join!(model, context);
                            model.and(context)
                        }
                    }
                },
            )
            .await;
            refund(state, reservation, started.elapsed());
            state.phase = Phase::Ready;
            // Reported tokens are charged even when the turn is then rejected, interrupted, timed out
            // or its cleanup fails. The response repeats the totals; it is not a second charge.
            state.usage.input_tokens = state.usage.input_tokens.saturating_add(usage.input);
            state.usage.output_tokens = state.usage.output_tokens.saturating_add(usage.output);
            let response_result = match outcome {
                WorkOutcome::Done(result) => result,
                WorkOutcome::Interrupted => {
                    commit(checkpoint, state, control).await?;
                    return Err(Error::Interrupted);
                }
                WorkOutcome::TimedOut => Err(Error::provider("model operation timed out")),
            };
            if let Ok(response) = &response_result {
                // Retain the complete response even when cleanup or validation rejects it.
                state.pending_response = Some(response.clone());
            }
            if is_expired(state) {
                return settle(
                    self,
                    checkpoint,
                    state,
                    control,
                    Settlement::Stopped(StopReason::Expired),
                )
                .await;
            }
            let response = response_result?;
            cleanup
                .map_err(|_| Error::provider("model cleanup timed out"))?
                .map_err(provider_error)?;
            self.validate_response(&response, state)?;
            if state
                .usage
                .input_tokens
                .saturating_add(state.usage.output_tokens)
                > state.limits.max_tokens
            {
                return settle(
                    self,
                    checkpoint,
                    state,
                    control,
                    Settlement::Stopped(StopReason::Tokens),
                )
                .await;
            }
            if let Some(output) = &response.output {
                (self.validate_output)(output).map_err(Error::model_output)?;
                state.history.push(HistoryEntry::Exchange {
                    response: response.clone(),
                    results: vec![],
                });
                state.pending_response = None;
                return settle(
                    self,
                    checkpoint,
                    state,
                    control,
                    Settlement::Completed(output.clone()),
                )
                .await;
            }
            state.operations = response
                .tools
                .iter()
                .map(|call| {
                    let spec = self
                        .work(&call.name)
                        .expect("validated call")
                        .spec()
                        .clone();
                    let status = if spec.requires_approval {
                        OperationStatus::Approval
                    } else {
                        OperationStatus::Pending
                    };
                    Operation {
                        call: call.clone(),
                        spec,
                        idempotency_key: format!(
                            "{}:{}:{}",
                            state.id, state.usage.model_calls, call.id
                        ),
                        status,
                    }
                })
                .collect();
            state.phase = Phase::Tools;
            commit(checkpoint, state, control).await
        }
        .instrument(span.clone())
        .await;
        span.record("gen_ai.usage.input_tokens", usage.input);
        span.record("gen_ai.usage.output_tokens", usage.output);
        span.record("response_bytes", response_bytes.max(bytes));
        if let Some(duration) = first_delta {
            span.record("time_to_first_delta_ms", telemetry::milliseconds(duration));
        }
        if result.is_err() && outcome_name == "done" {
            outcome_name = "rejected";
        }
        span.record("outcome", outcome_name);
        if outcome_name == "timed_out" {
            span.record("error.type", "timeout");
        } else if let Err(error) = &result {
            span.record("error.type", telemetry::error_type(error));
        }
        result
    }

    fn validate_response(&self, response: &ModelResponse, state: &RunState) -> Result<()> {
        if response.output.is_some() == !response.tools.is_empty() {
            return Err(Error::model_output(
                "model response must contain either final output or at least one tool call",
            ));
        }
        if response.tools.len() > state.limits.max_tool_calls as usize {
            return Err(Error::model_output("tool batch exceeds call limit"));
        }
        let mut ids: HashSet<&str> = state
            .history
            .iter()
            .filter_map(|entry| match entry {
                HistoryEntry::Exchange { response, .. } => {
                    Some(response.tools.iter().map(|call| call.id.as_str()))
                }
                _ => None,
            })
            .flatten()
            .collect();
        for call in &response.tools {
            if call.id.is_empty() || call.id.len() > 256 || !ids.insert(call.id.as_str()) {
                return Err(Error::model_output(
                    "tool call IDs must be unique and 1..256 bytes",
                ));
            }
            let Some(work) = self.work(&call.name) else {
                return Err(Error::model_output(format!("unknown tool {}", call.name)));
            };
            work.validate(&call.arguments)?;
        }
        Ok(())
    }
    async fn tool_batch(
        &self,
        state: &mut RunState,
        checkpoint: &dyn Checkpoint,
        control: &RunControl,
    ) -> Result<()> {
        complete_due_waits(state, checkpoint, control).await?;
        // Approval, user input and uncertainty stop the batch at a semantic boundary.
        if state
            .operations
            .iter()
            .any(|op| matches!(op.status, OperationStatus::Approval) || stops_dispatch(&op.status))
        {
            return Ok(());
        }
        loop {
            let startable = Startable::of(state);
            if startable.count == 0 {
                break;
            }
            if control.cancel.is_cancelled() || control.shutdown.is_cancelled() {
                return Err(Error::Interrupted);
            }
            if is_expired(state) {
                return settle(
                    self,
                    checkpoint,
                    state,
                    control,
                    Settlement::Stopped(StopReason::Expired),
                )
                .await;
            }
            // Every remaining call must fit the call budget before any of them starts. Resuming
            // children spends nothing more: each holds its own allocation.
            if startable.charged > 0
                && let Some(reason) = exhausted(state, false, startable.charged)
            {
                return settle(
                    self,
                    checkpoint,
                    state,
                    control,
                    Settlement::Stopped(reason),
                )
                .await;
            }
            self.tool_window(state, checkpoint, control, &startable)
                .await?;
            if is_expired(state) {
                return settle(
                    self,
                    checkpoint,
                    state,
                    control,
                    Settlement::Stopped(StopReason::Expired),
                )
                .await;
            }
            if state.operations.iter().any(|op| stops_dispatch(&op.status))
                || !state.children.is_empty() && Startable::of(state).resume_only
            {
                return Ok(());
            }
        }
        finish_tool_batch(state, checkpoint, control).await
    }
    /// Runs runnable operations in declaration order through a sliding window of at most
    /// `max_parallel_tools` slots: the next operation starts as soon as a slot frees. Returns once
    /// nothing more can start and every started operation has finished and been recorded.
    ///
    /// Each commit records the outcomes finished since the previous one together with the
    /// `Dispatched` intent of the operations about to take their slots, so no operation runs
    /// before its intent is durable and no unstarted operation is committed as dispatched.
    ///
    /// A delegation takes a slot of its own, up to the subagents' `max_parallel`: its intent is
    /// the child's admission, and its execution drives the child, whose commits this window makes
    /// as commits of the run (see `subagent`). A window that only resumes children reserves
    /// nothing: they charge their own work.
    #[tracing::instrument(level = "debug", skip_all)]
    async fn tool_window(
        &self,
        state: &mut RunState,
        checkpoint: &dyn Checkpoint,
        control: &RunControl,
        startable: &Startable,
    ) -> Result<()> {
        let mut reservation = 0;
        if startable.charged > 0 {
            reservation = reserve(state).0;
            // Reserve before authorization too, so retries cannot replenish its active-work budget.
            commit(checkpoint, state, control).await?;
        }
        let mut window = Window::new(reservation);
        window.resume_only = startable.resume_only;
        let mut in_flight = FuturesUnordered::new();
        loop {
            let mut admitted = if window.open {
                self.admit(state, control, &mut window, &mut in_flight)
                    .await
            } else {
                Vec::new()
            };
            let closing = in_flight.is_empty() && admitted.is_empty();
            if closing {
                // Nothing runs any more and nothing has an effect before the next commit, so the
                // refund rides on the commit of the last outcome instead of a commit of its own.
                window.refund(state);
            }
            if window.storage.is_none()
                && (window.unsaved || intends(&admitted) || closing && window.interrupted)
            {
                if !admitted.is_empty() {
                    let count = admitted.len();
                    emit(state, control, ProgressKind::ToolsStarted { count });
                }
                match commit(checkpoint, state, control).await {
                    Ok(()) => window.unsaved = false,
                    Err(error) => {
                        // Work whose intent is not durable never starts, and nothing new does.
                        window.revert(state, &mut admitted);
                        window.open = false;
                        window.storage = Some(error);
                    }
                }
            }
            if closing {
                break;
            }
            for admission in admitted {
                let op = &state.operations[admission.index];
                let call = telemetry::Call::tool(state, admission.index);
                if matches!(admission.previous, OperationStatus::Dispatched) {
                    call.recovered(Some(&op.spec.name));
                }
                let tool = match admission.start {
                    Start::Tool(tool) => tool,
                    Start::Child(child) => {
                        let future =
                            self.start_child(state, &mut window, control, admission.index, child);
                        in_flight.push(Either::Right(future));
                        window.running += 1;
                        continue;
                    }
                };
                let context = ToolContext {
                    run_id: state.id,
                    call_id: op.call.id.clone(),
                    idempotency_key: op.idempotency_key.clone(),
                    cancel: CancellationToken::new(),
                };
                let span = telemetry::tool_span(op, call);
                if !span.is_disabled() {
                    window.spans.push((admission.index, span.clone()));
                }
                let work = ToolExecution {
                    index: admission.index,
                    tool,
                    deadline: admission.deadline,
                    arguments: op.call.arguments.clone(),
                    context,
                    control,
                    cleanup_ms: state.limits.cleanup_timeout_ms,
                };
                in_flight.push(telemetry::instrument(
                    span,
                    (work, call),
                    |(work, _)| execute(work, Untraced),
                    |(work, call)| {
                        execute(
                            work,
                            Traced {
                                call,
                                cleanup: Span::none(),
                            },
                        )
                    },
                ));
                window.running += 1;
            }
            // A storage failure never drops other live children. Drain and clean all of them first.
            match window.next(&mut in_flight).await {
                Some(Event::Finished(finished)) => window.record(state, finished),
                Some(Event::Commit(request)) => {
                    subagent::commit_child(self, state, checkpoint, control, &mut window, request)
                        .await
                }
                None => {}
            }
        }
        window.result()
    }
    /// Fills free slots in declaration order. A `Sequential` operation waits for every earlier
    /// one to finish and holds the window alone. Admitted operations are authorized, counted,
    /// covered by the window's reservation and marked `Dispatched` in memory; the caller commits
    /// that intent before starting them. If dispatch stops, this round's admissions are undone.
    async fn admit<F: Future<Output = Finished>>(
        &self,
        state: &mut RunState,
        control: &RunControl,
        window: &mut Window,
        in_flight: &mut FuturesUnordered<F>,
    ) -> Vec<Admitted> {
        let mut admitted = Vec::new();
        let slots =
            state.limits.max_parallel_tools + self.subagents.as_ref().map_or(0, |s| s.max_parallel);
        while window.open && !window.sequential && window.running + admitted.len() < slots {
            let Some(index) =
                (window.next..state.operations.len()).find(|&i| window.startable(state, i))
            else {
                break;
            };
            let sequential = state.operations[index].spec.execution == Execution::Sequential;
            if sequential && window.running + admitted.len() > 0 {
                break;
            }
            match self
                .admit_one(state, control, window, in_flight, index, &admitted)
                .await
            {
                Admission::Start(admission) => {
                    window.next = index + 1;
                    window.sequential = sequential;
                    admitted.push(admission);
                }
                // Every slot of its kind is taken; it starts when one frees.
                Admission::Wait => break,
                // A delegation was answered without a child; move on.
                Admission::Skip => window.next = index + 1,
                Admission::Full => {
                    window.open = false;
                }
                Admission::Stop => {
                    window.open = false;
                    window.revert(state, &mut admitted);
                }
            }
        }
        admitted
    }
    async fn admit_one<F: Future<Output = Finished>>(
        &self,
        state: &mut RunState,
        control: &RunControl,
        window: &mut Window,
        in_flight: &mut FuturesUnordered<F>,
        index: usize,
        admitted: &[Admitted],
    ) -> Admission {
        if control.cancel.is_cancelled() || control.shutdown.is_cancelled() {
            window.interrupted = true;
            return Admission::Stop;
        }
        if is_expired(state) {
            return Admission::Stop;
        }
        let work = match self.recorded(&state.operations[index]) {
            Ok(work) => work,
            Err(error) => {
                window.failure = Some(error);
                return Admission::Stop;
            }
        };
        let delegation = match work {
            Work::Tool(_) if self.subagents.is_some() && !window.tool_slot(state, admitted) => {
                return Admission::Wait;
            }
            Work::Tool(_) => None,
            // A child that is resumed, waits for a slot or cannot be allocated is decided here.
            Work::Delegate(subagent) => {
                match subagent::prepare(self, state, window, index, subagent, admitted) {
                    Ok(()) => Some(subagent),
                    Err(admission) => return admission,
                }
            }
        };
        let Some(deadline) = window.cover(state) else {
            return Admission::Full;
        };
        let mut finished = Vec::new();
        let authorization = authorize_beside(
            &*self.authorizer,
            state,
            &state.operations[index].call,
            deadline,
            control,
            in_flight,
            &mut finished,
        )
        .await;
        for finished in finished {
            window.record(state, finished);
        }
        let name = &state.operations[index].call.name;
        if matches!(authorization, WorkOutcome::TimedOut) {
            telemetry::Call::tool(state, index).timed_out("authorize_tool", Some(name));
        }
        match authorization {
            WorkOutcome::Done(Ok(true)) => {}
            WorkOutcome::Done(Ok(false)) => {
                let denied = ErrorDetail::message(format!("authorization denied for {name}"));
                window.failure = Some(Error::authorization(denied));
                return Admission::Stop;
            }
            // Whatever the authorizer's error, it fails the run rather than its storage.
            WorkOutcome::Done(Err(error)) => {
                let failed = ErrorDetail::caused_by("authorization failed", error);
                window.failure = Some(Error::authorization(failed));
                return Admission::Stop;
            }
            // Expiry settles the run once the window drains.
            WorkOutcome::TimedOut if is_expired(state) => return Admission::Stop,
            WorkOutcome::TimedOut => {
                window.failure = Some(Error::authorization(ErrorDetail::message(
                    "authorization timed out",
                )));
                return Admission::Stop;
            }
            WorkOutcome::Interrupted => {
                window.interrupted = true;
                return Admission::Stop;
            }
        }
        // An outcome recorded during authorization may have stopped dispatch.
        if !window.open {
            return Admission::Stop;
        }
        if let Some(subagent) = delegation {
            return subagent::admit(state, window, index, subagent, deadline);
        }
        let Work::Tool(tool) = work else {
            unreachable!("a delegation was admitted above")
        };
        // Measure the operation's own timeout from now, after its authorization.
        let Some(deadline) = window.cover(state) else {
            return Admission::Full;
        };
        let op = &mut state.operations[index];
        let previous = std::mem::replace(&mut op.status, OperationStatus::Dispatched);
        state.usage.tool_calls += 1;
        Admission::Start(Admitted {
            index,
            previous,
            sequential: op.spec.execution == Execution::Sequential,
            start: Start::Tool(tool.clone()),
            deadline,
        })
    }
}
/// Completes timer waits whose deadline has passed.
async fn complete_due_waits(
    state: &mut RunState,
    checkpoint: &dyn Checkpoint,
    control: &RunControl,
) -> Result<()> {
    let now = now_ms();
    let mut changed = false;
    for op in &mut state.operations {
        if matches!(op.status, OperationStatus::Waiting(WaitRequest::Until { unix_ms }) if unix_ms <= now)
        {
            op.status = OperationStatus::Complete(ToolResult {
                call_id: op.call.id.clone(),
                value: Value::Null,
                is_error: false,
            });
            changed = true;
        }
    }
    if changed {
        commit(checkpoint, state, control).await?;
    }
    Ok(())
}
/// Once every operation is complete, appends the exchange to history and returns to the model.
async fn finish_tool_batch(
    state: &mut RunState,
    checkpoint: &dyn Checkpoint,
    control: &RunControl,
) -> Result<()> {
    if !state
        .operations
        .iter()
        .all(|op| matches!(op.status, OperationStatus::Complete(_)))
    {
        return Ok(());
    }
    let results = state
        .operations
        .iter()
        .map(|op| match &op.status {
            OperationStatus::Complete(result) => result.clone(),
            _ => unreachable!(),
        })
        .collect();
    let response = state
        .pending_response
        .take()
        .ok_or_else(|| Error::input("tool batch has no model response"))?;
    state
        .history
        .push(HistoryEntry::Exchange { response, results });
    state.operations.clear();
    state.phase = Phase::Ready;
    commit(checkpoint, state, control).await
}
/// Not yet started in this process: never dispatched, or dispatched before a crash and safe to retry.
fn is_runnable(op: &Operation) -> bool {
    matches!(
        op.status,
        OperationStatus::Pending | OperationStatus::Dispatched
    )
}
/// Outcomes after which no further operation of the batch may start until they are resolved.
fn stops_dispatch(status: &OperationStatus) -> bool {
    matches!(
        status,
        OperationStatus::Waiting(_) | OperationStatus::Unknown { .. }
    )
}
/// Whether a round of admissions records new intent to commit before anything starts. Resuming
/// an admitted child changes nothing that is not already durable.
fn intends(admitted: &[Admitted]) -> bool {
    admitted
        .iter()
        .any(|admission| !matches!(admission.previous, OperationStatus::Child { .. }))
}
/// What the current batch can start, found in one pass over its operations.
struct Startable {
    /// Operations that can start now.
    count: usize,
    /// Of those, the ones that start a tool call or a delegation and count against
    /// `max_tool_calls`; resuming an admitted child does not.
    charged: u32,
    /// A child is blocked on approval, input, reconciliation or a timer. Like a waiting call it
    /// stops new dispatch, but children that can progress still resume.
    resume_only: bool,
}
impl Startable {
    fn of(state: &RunState) -> Self {
        let mut charged = 0_usize;
        let mut resumable = 0;
        let mut blocked = false;
        let mut now = None;
        for op in &state.operations {
            match op.status {
                OperationStatus::Pending | OperationStatus::Dispatched => charged += 1,
                OperationStatus::Child { run_id } => {
                    if subagent::resumable(state, run_id, *now.get_or_insert_with(now_ms)) {
                        resumable += 1;
                    } else {
                        blocked = true;
                    }
                }
                _ => {}
            }
        }
        if blocked {
            Self {
                count: resumable,
                charged: 0,
                resume_only: true,
            }
        } else {
            Self {
                count: charged + resumable,
                charged: u32::try_from(charged).unwrap_or(u32::MAX),
                resume_only: false,
            }
        }
    }
}

/// Whole milliseconds, rounded up, as active time is charged.
fn ceil_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos().div_ceil(1_000_000)).unwrap_or(u64::MAX)
}

/// One sliding window of tool operations. Its reservation covers the wall-clock time from
/// `started` while any operation may be in flight, so overlapping work is charged once.
struct Window {
    started: Instant,
    /// Active milliseconds reserved from `started`, already included in `usage.active_ms`.
    reserved_ms: u64,
    /// Declaration-order position of the next operation to consider.
    next: usize,
    running: usize,
    /// A `Sequential` operation is admitted or running, so nothing else may start.
    sequential: bool,
    /// Further operations may still be admitted.
    open: bool,
    /// A child is blocked, so only children that can progress may start; see [`Startable`].
    resume_only: bool,
    /// Outcomes have been recorded since the last commit.
    unsaved: bool,
    interrupted: bool,
    /// An authorization failure that fails the run, or a contract difference that `drive`
    /// returns, once the window drains.
    failure: Option<Error>,
    storage: Option<Error>,
    /// Children running in this window, from the first one started.
    children: Option<ChildWindow>,
    spans: Vec<(usize, Span)>,
}
impl Window {
    fn new(reserved_ms: u64) -> Self {
        Self {
            started: Instant::now(),
            reserved_ms,
            next: 0,
            running: 0,
            sequential: false,
            open: true,
            resume_only: false,
            unsaved: false,
            interrupted: false,
            failure: None,
            storage: None,
            children: None,
            spans: Vec::new(),
        }
    }
    /// Whether the operation at `index` can start in this window.
    fn startable(&self, state: &RunState, index: usize) -> bool {
        let op = &state.operations[index];
        match op.status {
            OperationStatus::Child { run_id } => subagent::resumable(state, run_id, now_ms()),
            _ => !self.resume_only && is_runnable(op),
        }
    }
    /// Extends the reservation so that an operation starting now can use its whole timeout and
    /// cleanup allowance, as far as the active budget allows. Returns that operation's deadline,
    /// or `None` when the budget leaves it no time. The extension becomes durable with the commit
    /// that records the operation's intent, before the operation starts.
    fn cover(&mut self, state: &mut RunState) -> Option<Instant> {
        let timeout_ms = state.limits.operation_timeout_ms;
        let cleanup_ms = state.limits.cleanup_timeout_ms;
        let now = Instant::now();
        let wanted = ceil_ms(now.duration_since(self.started))
            .saturating_add(timeout_ms)
            .saturating_add(cleanup_ms);
        if wanted > self.reserved_ms {
            let available = state
                .limits
                .active_budget_ms
                .saturating_sub(state.usage.active_ms);
            let extra = (wanted - self.reserved_ms).min(available);
            self.reserved_ms += extra;
            state.usage.active_ms += extra;
        }
        let work_end =
            self.started + Duration::from_millis(self.reserved_ms.saturating_sub(cleanup_ms));
        let deadline = work_deadline(state, now, timeout_ms).min(work_end);
        (deadline > now).then_some(deadline)
    }
    fn take_span(&mut self, index: usize) -> Span {
        self.spans
            .iter()
            .position(|(i, _)| *i == index)
            .map(|position| self.spans.swap_remove(position).1)
            .unwrap_or_else(Span::none)
    }
    fn record_span(
        &mut self,
        op: &Operation,
        index: usize,
        call: telemetry::Call,
        bytes: Option<usize>,
    ) {
        let span = self.take_span(index);
        telemetry::record_operation(&span, op, call, bytes);
    }
    fn record(&mut self, state: &mut RunState, finished: Finished) {
        let (index, outcome, cleanup) = match finished {
            Finished::Tool {
                index,
                outcome,
                cleanup,
            } => (index, outcome, cleanup),
            Finished::Child { index, result } => {
                self.record_span(
                    &state.operations[index],
                    index,
                    telemetry::Call::tool(state, index),
                    None,
                );
                return self.record_child(state, index, result);
            }
        };
        if matches!(outcome, WorkOutcome::Interrupted) {
            self.interrupted = true;
        }
        let call = telemetry::Call::tool(state, index);
        let span = self.take_span(index);
        let op = &mut state.operations[index];
        let (status, bytes) = outcome_status(
            op,
            outcome,
            cleanup,
            state.limits.max_tool_output_bytes,
            !span.is_disabled(),
        );
        op.status = status;
        telemetry::record_operation(&span, op, call, bytes);
        if self.interrupted || stops_dispatch(&op.status) {
            self.open = false;
        }
        if op.spec.execution == Execution::Sequential {
            self.sequential = false;
        }
        self.running -= 1;
        self.unsaved = true;
    }
    /// Undoes admissions whose intent was never committed. Those operations never started.
    fn revert(&mut self, state: &mut RunState, admitted: &mut Vec<Admitted>) {
        // In reverse, so each child admitted in this round is the last one when it is removed.
        for admission in admitted.drain(..).rev() {
            if let Start::Child(child) = admission.start {
                if matches!(admission.previous, OperationStatus::Child { .. }) {
                    continue;
                }
                subagent::unadmit(state, child);
            }
            state.operations[admission.index].status = admission.previous;
            state.usage.tool_calls -= 1;
            if admission.sequential {
                self.sequential = false;
            }
        }
    }
    /// Charges the wall-clock time the window was open and returns the rest of its reservation.
    /// Time in which only children ran is theirs: each child charges its own work.
    fn refund(&self, state: &mut RunState) {
        let idle = self.children.as_ref().map_or(Duration::ZERO, |c| c.idle);
        refund(
            state,
            self.reserved_ms,
            self.started.elapsed().saturating_sub(idle),
        );
    }
    fn result(self) -> Result<()> {
        if let Some(error) = self.storage {
            return Err(error);
        }
        if self.interrupted {
            return Err(Error::Interrupted);
        }
        self.failure.map_or(Ok(()), Err)
    }
    /// The next operation to finish or, while children run, the next child commit to make.
    async fn next<F: Future<Output = Finished>>(
        &mut self,
        in_flight: &mut FuturesUnordered<F>,
    ) -> Option<Event> {
        let Some(children) = &mut self.children else {
            return in_flight.next().await.map(Event::Finished);
        };
        // Children stop when the run cannot go on: cancelled if it fails, stopped otherwise.
        if self.failure.as_ref().is_some_and(Error::fails_run) {
            children.cancel.cancel();
        } else if self.failure.is_some() || self.storage.is_some() {
            children.shutdown.cancel();
        }
        children.next(in_flight, self.running).await
    }
}
/// What a window waits for next.
enum Event {
    Finished(Finished),
    Commit(subagent::ChildCommit),
}
/// An operation marked `Dispatched` in memory, waiting for its intent to be committed.
struct Admitted {
    index: usize,
    previous: OperationStatus,
    sequential: bool,
    start: Start,
    deadline: Instant,
}
/// What an admission starts once its intent is durable.
enum Start {
    Tool(Arc<dyn Tool>),
    /// The child at this index of `RunState::children`: just admitted, or resumed when the
    /// operation already was `Child`.
    Child(usize),
}
enum Admission {
    Start(Admitted),
    /// Every slot for this kind of operation is taken.
    Wait,
    /// The operation was answered without starting anything.
    Skip,
    /// The active budget leaves no time for another operation in this window.
    Full,
    /// Nothing more may start; this round's admissions are undone.
    Stop,
}
enum Finished {
    Tool {
        index: usize,
        outcome: WorkOutcome<ToolOutput<Value>>,
        cleanup: std::result::Result<Result<()>, tokio::time::error::Elapsed>,
    },
    /// A child's execution returned: it settled, it is blocked, or it was stopped.
    Child { index: usize, result: Result<()> },
}
struct ToolExecution<'a> {
    index: usize,
    tool: Arc<dyn Tool>,
    deadline: Instant,
    arguments: Value,
    context: ToolContext,
    control: &'a RunControl,
    cleanup_ms: u64,
}
trait ExecutionTrace: Send {
    type Cleanup<F: Future + Send>: Future<Output = F::Output> + Send;
    fn finished(&self, outcome: &WorkOutcome<ToolOutput<Value>>, spec: &ToolSpec);
    fn cleanup<F: Future + Send>(&mut self, future: F) -> Self::Cleanup<F>;
    fn cleaned(
        &self,
        result: &std::result::Result<Result<()>, tokio::time::error::Elapsed>,
        spec: &ToolSpec,
        timeout_ms: u64,
    );
}
struct Untraced;
impl ExecutionTrace for Untraced {
    type Cleanup<F: Future + Send> = F;
    fn finished(&self, _: &WorkOutcome<ToolOutput<Value>>, _: &ToolSpec) {}
    fn cleanup<F: Future + Send>(&mut self, future: F) -> F {
        future
    }
    fn cleaned(
        &self,
        _: &std::result::Result<Result<()>, tokio::time::error::Elapsed>,
        _: &ToolSpec,
        _: u64,
    ) {
    }
}
struct Traced {
    call: telemetry::Call,
    cleanup: Span,
}
impl ExecutionTrace for Traced {
    type Cleanup<F: Future + Send> = tracing::instrument::Instrumented<F>;
    fn finished(&self, outcome: &WorkOutcome<ToolOutput<Value>>, spec: &ToolSpec) {
        if matches!(outcome, WorkOutcome::TimedOut) {
            self.call.timed_out("execute_tool", Some(&spec.name));
        }
    }
    fn cleanup<F: Future + Send>(&mut self, future: F) -> Self::Cleanup<F> {
        self.cleanup = tracing::debug_span!("tool_cleanup", outcome = Empty, error.type = Empty);
        future.instrument(self.cleanup.clone())
    }
    fn cleaned(
        &self,
        result: &std::result::Result<Result<()>, tokio::time::error::Elapsed>,
        spec: &ToolSpec,
        timeout_ms: u64,
    ) {
        telemetry::record_cleanup(
            &self.cleanup,
            self.call,
            Some(&spec.name),
            timeout_ms,
            result,
        );
    }
}
/// Executes one admitted operation, then always runs its cleanup. Only the observer varies;
/// the zero-sized observer keeps the untraced future's allocation layout compact.
async fn execute(work: ToolExecution<'_>, mut trace: impl ExecutionTrace) -> Finished {
    let ToolExecution {
        index,
        tool,
        deadline,
        arguments,
        context,
        control,
        cleanup_ms,
    } = work;
    let outcome = run_work(tool.execute(arguments, context.clone()), deadline, control).await;
    trace.finished(&outcome, tool.spec());
    context.cancel.cancel();
    let cleanup = trace
        .cleanup(tokio::time::timeout(
            Duration::from_millis(cleanup_ms),
            tool.cleanup(context),
        ))
        .await;
    trace.cleaned(&cleanup, tool.spec(), cleanup_ms);
    Finished::Tool {
        index,
        outcome,
        cleanup,
    }
}
/// Authorizes one call while the operations already in flight keep running. Their outcomes are
/// buffered, because recording them needs the state the authorizer is reading.
async fn authorize_beside<F: Future<Output = Finished>>(
    authorizer: &dyn Authorizer,
    state: &RunState,
    call: &ToolCall,
    deadline: Instant,
    control: &RunControl,
    in_flight: &mut FuturesUnordered<F>,
    finished: &mut Vec<Finished>,
) -> WorkOutcome<bool> {
    let mut authorization = std::pin::pin!(run_work(
        authorizer.authorize(state, call),
        deadline,
        control
    ));
    loop {
        tokio::select! {
            outcome = &mut authorization => return outcome,
            Some(done) = in_flight.next(), if !in_flight.is_empty() => finished.push(done),
        }
    }
}
fn outcome_status(
    op: &Operation,
    outcome: WorkOutcome<ToolOutput<Value>>,
    cleanup: std::result::Result<Result<()>, tokio::time::error::Elapsed>,
    max_bytes: usize,
    record_bytes: bool,
) -> (OperationStatus, Option<usize>) {
    let interrupted = matches!(outcome, WorkOutcome::Interrupted);
    let mut value = match outcome {
        WorkOutcome::Done(result) => result,
        WorkOutcome::TimedOut => Err(Error::tool("tool operation timed out")),
        WorkOutcome::Interrupted => Err(Error::Interrupted),
    };
    if let Err(error) = cleanup
        .map_err(|_| Error::tool("tool cleanup timed out"))
        .and_then(|v| v)
    {
        value = Err(error);
    }
    let status = match value {
        Ok(output) => match output_status(op, output, max_bytes) {
            Ok((status, bytes)) => return (status, Some(bytes)),
            Err(message) => failure_status(op, &message, max_bytes, false),
        },
        Err(error) => failure_status(
            op,
            &error.to_string(),
            max_bytes,
            interrupted || matches!(error, Error::Interrupted),
        ),
    };
    let bytes = match &status {
        OperationStatus::Complete(result) if record_bytes => Some(encoded_len(&result.value)),
        _ => None,
    };
    (status, bytes)
}

/// Records a returned output, or explains why it cannot be recorded. Never returns early, so an
/// unencodable output cannot drop the outcomes of sibling operations still in flight.
fn output_status(
    op: &Operation,
    output: ToolOutput<Value>,
    max_bytes: usize,
) -> std::result::Result<(OperationStatus, usize), String> {
    let fits = |len: usize| {
        if len <= max_bytes {
            Ok(len)
        } else {
            Err("tool output exceeds byte limit".to_owned())
        }
    };
    let result = |value, is_error| {
        OperationStatus::Complete(ToolResult {
            call_id: op.call.id.clone(),
            value,
            is_error,
        })
    };
    Ok(match output {
        ToolOutput::Complete(value) => {
            let bytes = fits(encoded_len(&value))?;
            (result(value, false), bytes)
        }
        ToolOutput::Failed(value) => {
            let bytes = fits(encoded_len(&value))?;
            (result(value, true), bytes)
        }
        ToolOutput::Wait(wait) => {
            let bytes = fits(encoded_len(&wait))?;
            (OperationStatus::Waiting(wait), bytes)
        }
    })
}
fn failure_status(
    op: &Operation,
    message: &str,
    max_bytes: usize,
    interrupted: bool,
) -> OperationStatus {
    if interrupted && op.spec.retry != RetryPolicy::Never {
        OperationStatus::Dispatched
    } else if op.spec.retry != RetryPolicy::ReadOnly {
        OperationStatus::Unknown {
            reason: truncate(message, max_bytes),
        }
    } else {
        OperationStatus::Complete(ToolResult {
            call_id: op.call.id.clone(),
            value: Value::String(truncate(message, max_bytes.saturating_sub(2) / 6)),
            is_error: true,
        })
    }
}
fn uncertain_dispatched(state: &mut RunState, reason: &str) {
    let call = telemetry::Call::model(state);
    for (index, op) in state.operations.iter_mut().enumerate() {
        if matches!(op.status, OperationStatus::Dispatched) {
            op.status = OperationStatus::Unknown {
                reason: reason.into(),
            };
            telemetry::unknown(
                &Span::none(),
                op,
                telemetry::Call {
                    index: Some(index),
                    ..call
                },
            );
        }
    }
}
fn truncate(value: &str, max: usize) -> String {
    let mut end = value.len().min(max);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_owned()
}
/// Raises an attempt's usage to a cumulative snapshot. A decrease fails the turn after keeping
/// the higher of each count, so nothing already reported goes uncharged.
fn record_usage(usage: &mut crate::TokenUsage, reported: &crate::TokenUsage) -> Result<()> {
    let decreased = reported.input < usage.input || reported.output < usage.output;
    usage.input = usage.input.max(reported.input);
    usage.output = usage.output.max(reported.output);
    if decreased {
        return Err(Error::provider("model usage snapshot decreased"));
    }
    Ok(())
}
/// A model's error, as the provider failure it is to the run, keeping it as the cause. One that
/// already is a provider failure is kept as it is.
fn provider_error(error: Error) -> Error {
    match error {
        Error::Provider(_) => error,
        error => Error::provider(error),
    }
}
fn bounded_error(error: &Error, max: usize) -> String {
    truncate(&error.to_string(), max)
}
fn exhausted(state: &RunState, model: bool, tools: u32) -> Option<StopReason> {
    if state
        .usage
        .active_ms
        .saturating_add(state.limits.cleanup_timeout_ms)
        >= state.limits.active_budget_ms
    {
        return Some(StopReason::ActiveBudget);
    }
    if state
        .usage
        .input_tokens
        .saturating_add(state.usage.output_tokens)
        >= state.limits.max_tokens
    {
        return Some(StopReason::Tokens);
    }
    if model && state.usage.model_calls >= state.limits.max_model_calls {
        return Some(StopReason::ModelCalls);
    }
    if state.usage.tool_calls.saturating_add(tools) > state.limits.max_tool_calls {
        return Some(StopReason::ToolCalls);
    }
    None
}
fn is_expired(state: &RunState) -> bool {
    state
        .expires_at_ms
        .is_some_and(|deadline| deadline <= now_ms())
}
fn work_deadline(state: &RunState, started: Instant, work_ms: u64) -> Instant {
    let remaining_ms = state
        .expires_at_ms
        .map(|expires| expires.saturating_sub(now_ms()))
        .unwrap_or(u64::MAX);
    started + Duration::from_millis(work_ms.min(remaining_ms))
}
/// Reserves active time for one operation and its cleanup: `(reservation, work)` milliseconds.
/// Callers check [`exhausted`] first, which guarantees work: it stops a run once the budget
/// cannot cover `cleanup_timeout_ms`, and `build` requires a positive `operation_timeout_ms`.
fn reserve(state: &mut RunState) -> (u64, u64) {
    let remaining = state
        .limits
        .active_budget_ms
        .saturating_sub(state.usage.active_ms);
    let reservation = remaining.min(
        state
            .limits
            .operation_timeout_ms
            .saturating_add(state.limits.cleanup_timeout_ms),
    );
    let work = reservation.saturating_sub(state.limits.cleanup_timeout_ms);
    debug_assert!(work > 0, "exhausted() is checked before every reservation");
    state.usage.active_ms = state.usage.active_ms.saturating_add(reservation);
    (reservation, work)
}
fn refund(state: &mut RunState, reservation: u64, elapsed: Duration) {
    let charged = u64::try_from(elapsed.as_nanos().div_ceil(1_000_000))
        .unwrap_or(u64::MAX)
        .min(reservation);
    state.usage.active_ms = state.usage.active_ms.saturating_sub(reservation - charged);
}
/// Best-effort and non-blocking: a closed or full channel drops the event. Events are not
/// counted or retained in run state.
#[inline]
fn emit(state: &RunState, control: &RunControl, kind: ProgressKind) {
    if let Some(sender) = &control.events {
        let _ = sender.try_send(ProgressEvent {
            run_id: state.id,
            parent_run_id: state.parent.as_ref().map(|parent| parent.run_id),
            kind,
        });
    }
}
async fn commit(
    checkpoint: &dyn Checkpoint,
    state: &mut RunState,
    control: &RunControl,
) -> Result<()> {
    let span = tracing::debug_span!("commit", revision = Empty);
    async {
        let (phase, history_len) = (std::mem::discriminant(&state.phase), state.history.len());
        let children = children_len(state);
        let steering = Boundary::of(state);
        checkpoint
            .commit(state)
            .await
            .map_err(|error| match error {
                Error::Stale | Error::Storage(_) | Error::Incompatible(_) => error,
                // Whatever else a checkpoint reports, the commit failed and the run is unchanged.
                error => Error::storage(error),
            })?;
        span.record("revision", state.revision);
        // The one place the engine takes in what a commit changed, as `Checkpoint` lists it. Appended
        // steering needs nothing more: context is assembled after the dispatch commit. A reopened
        // settlement is handled by `settle`, which returns into the drive loop.
        let allowed = match steering {
            _ if state.history.len() < history_len || children_len(state) != children => false,
            Boundary::Apply => {
                std::mem::discriminant(&state.phase) == phase
                    && state.history[history_len..]
                        .iter()
                        .all(|entry| matches!(entry, HistoryEntry::Steering { .. }))
            }
            Boundary::Reopen if matches!(state.phase, Phase::Ready) => {
                state.history.len() == history_len
            }
            Boundary::Reopen | Boundary::None => {
                std::mem::discriminant(&state.phase) == phase && state.history.len() == history_len
            }
        };
        if !allowed {
            return Err(Error::storage(
                "checkpoint changed the run beyond what its contract allows",
            ));
        }
        emit(
            state,
            control,
            ProgressKind::Committed {
                revision: state.revision,
            },
        );
        Ok(())
    }
    .instrument(span.clone())
    .await
}
/// How many children a run holds and how much history they hold together: no commit changes
/// either, since a child's commits are applied to its root by the engine before the root commits.
fn children_len(state: &RunState) -> (usize, usize) {
    let history = (state.children.iter()).map(|child| child.state.history.len());
    (state.children.len(), history.sum())
}
#[tracing::instrument(level = "debug", skip_all)]
async fn settle(
    definition: &Definition,
    checkpoint: &dyn Checkpoint,
    state: &mut RunState,
    control: &RunControl,
    settlement: Settlement,
) -> Result<()> {
    uncertain_dispatched(state, "execution settled without a recorded tool outcome");
    // A run completes only after its tool batch, so every child has settled and a completion
    // that steering reopens has cancelled nothing.
    debug_assert!(
        !matches!(settlement, Settlement::Completed(_))
            || state.children.iter().all(|child| child.state.is_terminal())
    );
    // A settled run owns no unsettled children; their work stopped with the last window.
    crate::children::cancel_unsettled(state, |state, index| {
        subagent::result_mapper(definition, state, index)
    });
    state.phase = Phase::Settled(settlement);
    commit(checkpoint, state, control).await?;
    // A completion that queued steering reopened is not a settlement; the drive loop goes on.
    if state.is_terminal() {
        emit(state, control, ProgressKind::Settled);
    }
    Ok(())
}
enum WorkOutcome<T> {
    Done(Result<T>),
    TimedOut,
    Interrupted,
}
async fn run_work<T>(
    future: impl std::future::Future<Output = Result<T>>,
    deadline: Instant,
    control: &RunControl,
) -> WorkOutcome<T> {
    tokio::select! { biased;
        _ = control.cancel.cancelled() => WorkOutcome::Interrupted,
        _ = control.shutdown.cancelled() => WorkOutcome::Interrupted,
        _ = tokio::time::sleep_until(deadline) => WorkOutcome::TimedOut,
        result = future => WorkOutcome::Done(result),
    }
}

#[cfg(test)]
mod telemetry_tests {
    use super::*;
    #[test]
    fn recorded_tool_output_is_encoded_once() {
        for output in [
            ToolOutput::Complete(json!("output")),
            ToolOutput::Failed(json!("failure")),
            ToolOutput::Wait(WaitRequest::Input {
                prompt: "input".into(),
            }),
        ] {
            let expected = match &output {
                ToolOutput::Complete(value) | ToolOutput::Failed(value) => {
                    serde_json::to_vec(value).unwrap().len()
                }
                ToolOutput::Wait(wait) => serde_json::to_vec(wait).unwrap().len(),
            };
            let agent = Agent::builder("encoding", Arc::new(crate::ScriptedModel::new(vec![])))
                .build()
                .unwrap();
            let mut state = agent
                .new_run("conversation", json!("input"), &[], None)
                .unwrap();
            let spec = ToolSpec::new("tool", "1");
            state.operations.push(Operation {
                call: ToolCall {
                    id: "id".into(),
                    name: "tool".into(),
                    arguments: Value::Null,
                },
                spec,
                idempotency_key: "key".into(),
                status: OperationStatus::Dispatched,
            });
            let capture = crate::tracing_capture::Capture::default();
            tracing::subscriber::with_default(capture.clone(), || {
                let mut window = Window::new(0);
                window.running = 1;
                window.spans.push((
                    0,
                    telemetry::tool_span(&state.operations[0], telemetry::Call::tool(&state, 0)),
                ));
                let encodings = crate::context::count_encodings(|| {
                    window.record(
                        &mut state,
                        Finished::Tool {
                            index: 0,
                            outcome: WorkOutcome::Done(Ok(output)),
                            cleanup: Ok(Ok(())),
                        },
                    )
                });
                assert_eq!(encodings, 1, "recording output repeated its JSON encoding");
            });
            assert_eq!(
                capture.0.lock().unwrap().spans[0].fields["output_bytes"],
                expected.to_string()
            );
        }
    }
    #[test]
    fn failed_tool_outputs_are_only_encoded_for_an_enabled_span() {
        for traced in [false, true] {
            for fault in 0..3 {
                let agent = Agent::builder("encoding", Arc::new(crate::ScriptedModel::new(vec![])))
                    .build()
                    .unwrap();
                let mut state = agent
                    .new_run("conversation", json!("input"), &[], None)
                    .unwrap();
                state.operations.push(Operation {
                    call: ToolCall {
                        id: "id".into(),
                        name: "tool".into(),
                        arguments: Value::Null,
                    },
                    spec: ToolSpec::new("tool", "1").with_retry(RetryPolicy::ReadOnly),
                    idempotency_key: "key".into(),
                    status: OperationStatus::Dispatched,
                });
                let capture = crate::tracing_capture::Capture::default();
                let dispatch = if traced {
                    tracing::Dispatch::new(capture.clone())
                } else {
                    tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default())
                };
                tracing::dispatcher::with_default(&dispatch, || {
                    let mut window = Window::new(0);
                    window.running = 1;
                    if traced {
                        window.spans.push((
                            0,
                            telemetry::tool_span(
                                &state.operations[0],
                                telemetry::Call::tool(&state, 0),
                            ),
                        ));
                    }
                    let outcome = if fault == 0 {
                        WorkOutcome::TimedOut
                    } else if fault == 1 {
                        WorkOutcome::Done(Err(Error::tool("failure")))
                    } else {
                        WorkOutcome::Done(Ok(ToolOutput::Complete(json!("result"))))
                    };
                    let cleanup = if fault == 2 {
                        Ok(Err(Error::tool("cleanup")))
                    } else {
                        Ok(Ok(()))
                    };
                    let encodings = crate::context::count_encodings(|| {
                        window.record(
                            &mut state,
                            Finished::Tool {
                                index: 0,
                                outcome,
                                cleanup,
                            },
                        )
                    });
                    assert_eq!(
                        encodings,
                        usize::from(traced),
                        "failure encoding must depend on a recorded span"
                    );
                });
                if traced {
                    assert!(
                        capture.0.lock().unwrap().spans[0]
                            .fields
                            .contains_key("output_bytes")
                    );
                }
            }
        }
    }
}
