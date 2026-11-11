use crate::context::{assemble, encoded_len, suffix_start};
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
/// # use yielded_agent::*;
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
/// # use yielded_agent::*;
/// # async fn example(agent: Agent<String, String>) -> Result<()> {
/// // The input must convert into `String`.
/// let reply: String = agent.run(&mut Conversation::default(), 42_u64).await?;
/// # Ok(()) }
/// ```
///
/// ```compile_fail
/// # use std::sync::Arc;
/// # use yielded_agent::*;
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
    /// Tools by name. `tool_specs` keeps their declaration order for the model.
    tools: HashMap<String, Arc<dyn Tool>>,
    tool_specs: Arc<[ToolSpec]>,
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
    /// # use yielded_agent::*;
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
    /// # use yielded_agent::*;
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
        if conversation.last_run.as_ref().is_some_and(|run| {
            !run.is_terminal()
                || run.operations.iter().any(|operation| {
                    matches!(
                        operation.status,
                        OperationStatus::Unknown { .. } | OperationStatus::Dispatched
                    )
                })
        }) {
            return Err(Error::ConversationBusy);
        }
        let input = into_json(input.into())?;
        let state = self.new_run("memory", input, &conversation.history, None)?;
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
        binding
    }
    /// The tool an operation recorded, provided this definition has it under the same contract.
    fn recorded_tool(&self, op: &Operation) -> Result<&Arc<dyn Tool>> {
        self.tools
            .get(&op.call.name)
            .filter(|tool| tool.spec() == &op.spec)
            .ok_or_else(|| Error::incompatible(format!("tool contract differs: {}", op.call.name)))
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
    /// or unversioned tool name, an idempotent tool without a retry contract, or a derived
    /// context cap above `max_context_bytes`.
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
        let mut definition = Definition {
            id,
            instructions: instructions.into(),
            model,
            config: Arc::new(config),
            tool_specs: tools.iter().map(|tool| tool.spec().clone()).collect(),
            tools: by_name,
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
/// # use yielded_agent::RunControl;
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
/// An in-memory conversation for [`Agent::run`] and [`Agent::resume`]: its history and its latest
/// run. Start an empty one with [`Conversation::default`].
#[derive(Default, Debug)]
#[non_exhaustive]
pub struct Conversation {
    /// Every entry of every run, in order. A run starts from the suffix that fits its context
    /// limit; `run` and `resume` append the run's new entries when they return.
    pub history: Vec<HistoryEntry>,
    /// The latest run, retained even when `run` or `resume` returns an error, so its waits and
    /// uncertain outcomes stay visible and resolvable with [`RunState::resolve`].
    pub last_run: Option<RunState>,
}
impl Conversation {
    /// A conversation that continues from `history`, with no run in progress.
    pub fn with_history(history: Vec<HistoryEntry>) -> Self {
        Self {
            history,
            last_run: None,
        }
    }
}
/// The state machine runs on the shared definition of the agent being driven.
impl Definition {
    #[tracing::instrument(skip_all, fields(run_id = %state.id))]
    async fn drive(
        &self,
        state: &mut RunState,
        checkpoint: &dyn Checkpoint,
        control: RunControl,
    ) -> Result<()> {
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
        let mut recovered = false;
        if matches!(state.phase, Phase::ModelInFlight) {
            state.phase = Phase::Ready;
            recovered = true;
        }
        for op in &mut state.operations {
            if matches!(op.status, OperationStatus::Dispatched) {
                self.recorded_tool(op)?;
                if op.spec.retry == RetryPolicy::Never {
                    op.status = OperationStatus::Unknown {
                        reason: "execution ended after dispatch without a recorded outcome".into(),
                    };
                    recovered = true;
                }
                // Keep retryable uncertain work Dispatched until it really retries. This
                // preserves the uncertainty if cancellation arrives during authorization.
            }
        }
        if recovered {
            commit(checkpoint, state, &control).await?;
        }
        loop {
            if state.cancel_requested || control.cancel.is_cancelled() {
                state.cancel_requested = true;
                uncertain_dispatched(state, "cancelled after dispatch");
                return settle(checkpoint, state, &control, Settlement::Cancelled).await;
            }
            if control.shutdown.is_cancelled() {
                return Err(Error::Interrupted);
            }
            if state.expires_at_ms.is_some_and(|t| t <= now_ms()) {
                return settle(
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
                        return settle(checkpoint, state, &control, Settlement::Stopped(reason))
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
                    return settle(checkpoint, state, &control, Settlement::Failed(reason)).await;
                }
                Err(error) => return Err(error),
            }
            // A batch still in `Tools` waits on approval, input, reconciliation or a timer.
            if batch && matches!(state.phase, Phase::Tools) {
                return Ok(());
            }
        }
    }
    async fn model_turn(
        &self,
        state: &mut RunState,
        checkpoint: &dyn Checkpoint,
        control: &RunControl,
    ) -> Result<()> {
        let (reservation, work_ms) = reserve(state);
        state.usage.model_calls += 1;
        state.phase = Phase::ModelInFlight;
        emit(state, control, ProgressKind::ModelStarted);
        commit(checkpoint, state, control).await?;
        let started = Instant::now();
        let operation_cancel = CancellationToken::new();
        // The attempt's reported usage, outside the operation so it is charged however that ends.
        let mut usage = crate::TokenUsage::default();
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
                    turn: state.usage.model_calls,
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
                let mut bytes = 0_usize;
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
                            bytes = bytes.saturating_add(text.len());
                            if bytes > state.limits.max_response_bytes {
                                return Err(Error::provider("stream exceeds response byte limit"));
                            }
                            emit(state, control, ProgressKind::TextDelta(text));
                        }
                        ModelEvent::Response(value) => {
                            record_usage(&mut usage, &value.usage)?;
                            if encoded_len(&value) > state.limits.max_response_bytes {
                                return Err(Error::provider("response exceeds byte limit"));
                            }
                            response = Some(value);
                        }
                    }
                }
                response.ok_or_else(|| Error::provider("stream ended without a complete response"))
            };
            run_work(future, work_deadline(state, started, work_ms), control).await
        };
        operation_cancel.cancel();
        let cleanup = tokio::time::timeout(
            Duration::from_millis(state.limits.cleanup_timeout_ms),
            async {
                let model = self.model.cleanup(state.id);
                match &self.context_policy {
                    None => model.await,
                    Some((policy, _)) => {
                        let (model, context) = tokio::join!(model, policy.cleanup(state.id));
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
                let spec = self.tools[&call.name].spec().clone();
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
            let Some(tool) = self.tools.get(&call.name) else {
                return Err(Error::model_output(format!("unknown tool {}", call.name)));
            };
            tool.validate(&call.arguments)?;
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
            let runnable = state.operations.iter().filter(|op| is_runnable(op)).count();
            if runnable == 0 {
                break;
            }
            if control.cancel.is_cancelled() || control.shutdown.is_cancelled() {
                return Err(Error::Interrupted);
            }
            if is_expired(state) {
                return settle(
                    checkpoint,
                    state,
                    control,
                    Settlement::Stopped(StopReason::Expired),
                )
                .await;
            }
            // Every remaining call must fit the call budget before any of them starts.
            let runnable = u32::try_from(runnable).unwrap_or(u32::MAX);
            if let Some(reason) = exhausted(state, false, runnable) {
                return settle(checkpoint, state, control, Settlement::Stopped(reason)).await;
            }
            self.tool_window(state, checkpoint, control).await?;
            if is_expired(state) {
                return settle(
                    checkpoint,
                    state,
                    control,
                    Settlement::Stopped(StopReason::Expired),
                )
                .await;
            }
            if state.operations.iter().any(|op| stops_dispatch(&op.status)) {
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
    async fn tool_window(
        &self,
        state: &mut RunState,
        checkpoint: &dyn Checkpoint,
        control: &RunControl,
    ) -> Result<()> {
        let (reservation, _) = reserve(state);
        // Reserve before authorization too, so retries cannot replenish its active-work budget.
        commit(checkpoint, state, control).await?;
        let mut window = Window::new(reservation);
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
                && (window.unsaved || !admitted.is_empty() || closing && window.interrupted)
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
                let context = ToolContext {
                    run_id: state.id,
                    call_id: op.call.id.clone(),
                    idempotency_key: op.idempotency_key.clone(),
                    cancel: CancellationToken::new(),
                };
                in_flight.push(execute(
                    admission,
                    op.call.arguments.clone(),
                    context,
                    control,
                    state.limits.cleanup_timeout_ms,
                ));
                window.running += 1;
            }
            // A storage failure never drops other live children. Drain and clean all of them first.
            if let Some(finished) = in_flight.next().await {
                window.record(state, finished);
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
        while window.open
            && !window.sequential
            && window.running + admitted.len() < state.limits.max_parallel_tools
        {
            let Some(index) =
                (window.next..state.operations.len()).find(|&i| is_runnable(&state.operations[i]))
            else {
                break;
            };
            let sequential = state.operations[index].spec.execution == Execution::Sequential;
            if sequential && window.running + admitted.len() > 0 {
                break;
            }
            match self
                .admit_one(state, control, window, in_flight, index)
                .await
            {
                Admission::Start(admission) => {
                    window.next = index + 1;
                    window.sequential = sequential;
                    admitted.push(admission);
                }
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
    ) -> Admission {
        if control.cancel.is_cancelled() || control.shutdown.is_cancelled() {
            window.interrupted = true;
            return Admission::Stop;
        }
        if is_expired(state) {
            return Admission::Stop;
        }
        let tool = match self.recorded_tool(&state.operations[index]) {
            Ok(tool) => tool.clone(),
            Err(error) => {
                window.failure = Some(error);
                return Admission::Stop;
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
            tool,
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
    /// Outcomes have been recorded since the last commit.
    unsaved: bool,
    interrupted: bool,
    /// An authorization failure that fails the run, or a contract difference that `drive`
    /// returns, once the window drains.
    failure: Option<Error>,
    storage: Option<Error>,
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
            unsaved: false,
            interrupted: false,
            failure: None,
            storage: None,
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
    fn record(&mut self, state: &mut RunState, finished: Finished) {
        let Finished {
            index,
            outcome,
            cleanup,
        } = finished;
        if matches!(outcome, WorkOutcome::Interrupted) {
            self.interrupted = true;
        }
        let op = &mut state.operations[index];
        op.status = outcome_status(op, outcome, cleanup, state.limits.max_tool_output_bytes);
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
        for admission in admitted.drain(..) {
            state.operations[admission.index].status = admission.previous;
            state.usage.tool_calls -= 1;
            if admission.sequential {
                self.sequential = false;
            }
        }
    }
    /// Charges the wall-clock time the window was open and returns the rest of its reservation.
    fn refund(&self, state: &mut RunState) {
        refund(state, self.reserved_ms, self.started.elapsed());
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
}
/// An operation marked `Dispatched` in memory, waiting for its intent to be committed.
struct Admitted {
    index: usize,
    previous: OperationStatus,
    sequential: bool,
    tool: Arc<dyn Tool>,
    deadline: Instant,
}
enum Admission {
    Start(Admitted),
    /// The active budget leaves no time for another operation in this window.
    Full,
    /// Nothing more may start; this round's admissions are undone.
    Stop,
}
struct Finished {
    index: usize,
    outcome: WorkOutcome<ToolOutput<Value>>,
    cleanup: std::result::Result<Result<()>, tokio::time::error::Elapsed>,
}
/// Executes one admitted operation, then always runs its cleanup.
async fn execute(
    admission: Admitted,
    arguments: Value,
    context: ToolContext,
    control: &RunControl,
    cleanup_ms: u64,
) -> Finished {
    let tool = admission.tool;
    let outcome = run_work(
        tool.execute(arguments, context.clone()),
        admission.deadline,
        control,
    )
    .await;
    context.cancel.cancel();
    let cleanup =
        tokio::time::timeout(Duration::from_millis(cleanup_ms), tool.cleanup(context)).await;
    Finished {
        index: admission.index,
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
) -> OperationStatus {
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
    match value {
        Ok(output) => output_status(op, output, max_bytes)
            .unwrap_or_else(|message| failure_status(op, &message, max_bytes, false)),
        Err(error) => failure_status(
            op,
            &error.to_string(),
            max_bytes,
            interrupted || matches!(error, Error::Interrupted),
        ),
    }
}

/// Records a returned output, or explains why it cannot be recorded. Never returns early, so an
/// unencodable output cannot drop the outcomes of sibling operations still in flight.
fn output_status(
    op: &Operation,
    output: ToolOutput<Value>,
    max_bytes: usize,
) -> std::result::Result<OperationStatus, String> {
    let fits = |len: usize| {
        if len <= max_bytes {
            Ok(())
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
            fits(encoded_len(&value))?;
            result(value, false)
        }
        ToolOutput::Failed(value) => {
            fits(encoded_len(&value))?;
            result(value, true)
        }
        ToolOutput::Wait(wait) => {
            fits(encoded_len(&wait))?;
            OperationStatus::Waiting(wait)
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
    for op in &mut state.operations {
        if matches!(op.status, OperationStatus::Dispatched) {
            op.status = OperationStatus::Unknown {
                reason: reason.into(),
            };
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
            kind,
        });
    }
}
async fn commit(
    checkpoint: &dyn Checkpoint,
    state: &mut RunState,
    control: &RunControl,
) -> Result<()> {
    checkpoint
        .commit(state)
        .await
        .map_err(|error| match error {
            Error::Stale | Error::Storage(_) | Error::Incompatible(_) => error,
            // Whatever else a checkpoint reports, the commit failed and the run is unchanged.
            error => Error::storage(error),
        })?;
    emit(
        state,
        control,
        ProgressKind::Committed {
            revision: state.revision,
        },
    );
    Ok(())
}
async fn settle(
    checkpoint: &dyn Checkpoint,
    state: &mut RunState,
    control: &RunControl,
    settlement: Settlement,
) -> Result<()> {
    uncertain_dispatched(state, "execution settled without a recorded tool outcome");
    state.phase = Phase::Settled(settlement);
    commit(checkpoint, state, control).await?;
    emit(state, control, ProgressKind::Settled);
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
