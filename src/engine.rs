use crate::context::{assemble, encoded_len, suffix_start};
use crate::*;
use async_trait::async_trait;
use futures::{StreamExt, stream::FuturesUnordered};
use schemars::{JsonSchema, schema_for};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{any::TypeId, collections::HashSet, future::Future, sync::Arc, time::Duration};
use tokio::{sync::mpsc, time::Instant};
use tokio_util::sync::CancellationToken;

type Validator = fn(&Value) -> Result<()>;
/// Checks that `value` deserializes as `T`. `&Value` is a deserializer, so this never copies it,
/// and any JSON is a `Value`, so an untyped agent checks nothing.
fn validate<T: DeserializeOwned + 'static>(value: &Value) -> Result<()> {
    if TypeId::of::<T>() == TypeId::of::<Value>() {
        return Ok(());
    }
    T::deserialize(value)
        .map(|_| ())
        .map_err(|e| Error::Invalid(e.to_string()))
}

/// Immutable agent definition. Product policy and clients are supplied by its caller.
#[derive(Clone)]
pub struct Agent {
    id: String,
    // Shared with every `ModelRequest` instead of copied into each one.
    instructions: Arc<str>,
    model: Arc<dyn Model>,
    config: Arc<ModelConfig>,
    tools: Vec<Arc<dyn Tool>>,
    tool_specs: Arc<[ToolSpec]>,
    limits: Limits,
    input_schema: Value,
    output_schema: Arc<Value>,
    validate_input: Validator,
    validate_output: Validator,
    authorizer: Arc<dyn Authorizer>,
    context_policy: Option<(Arc<dyn ContextPolicy>, usize)>,
}
impl Agent {
    pub fn new<I, O>(
        id: impl Into<String>,
        instructions: impl Into<String>,
        model: Arc<dyn Model>,
        config: ModelConfig,
        tools: Vec<Arc<dyn Tool>>,
        limits: Limits,
    ) -> Result<Self>
    where
        I: DeserializeOwned + JsonSchema + 'static,
        O: DeserializeOwned + JsonSchema + 'static,
    {
        let id = id.into();
        if id.is_empty() || id.len() > 256 {
            return Err(Error::Invalid("agent ID must be 1..256 bytes".into()));
        }
        if limits.max_parallel_tools == 0
            || limits.max_response_bytes == 0
            || limits.max_tool_output_bytes == 0
            || limits.max_context_bytes < 2
            || limits.operation_timeout_ms == 0
            || limits.cleanup_timeout_ms == 0
        {
            return Err(Error::Invalid(
                "parallelism, byte limits, and timeouts must be positive".into(),
            ));
        }
        let mut names = HashSet::new();
        for tool in &tools {
            let spec = tool.spec();
            if spec.name.is_empty()
                || spec.name.len() > 256
                || !names.insert(spec.name.clone())
                || spec.version.is_empty()
            {
                return Err(Error::Invalid(
                    "tool names must be unique, bounded, and versioned".into(),
                ));
            }
            if matches!(&spec.retry, RetryPolicy::Idempotent { contract } if contract.is_empty()) {
                return Err(Error::Invalid(
                    "idempotent tools require a retry contract".into(),
                ));
            }
        }
        Ok(Self {
            id,
            instructions: instructions.into().into(),
            model,
            config: Arc::new(config),
            tool_specs: tools.iter().map(|tool| tool.spec().clone()).collect(),
            tools,
            limits,
            input_schema: serde_json::to_value(schema_for!(I)).expect("schema serialization"),
            output_schema: Arc::new(
                serde_json::to_value(schema_for!(O)).expect("schema serialization"),
            ),
            validate_input: validate::<I>,
            validate_output: validate::<O>,
            authorizer: Arc::new(AllowAll),
            context_policy: None,
        })
    }
    pub fn with_authorizer(mut self, authorizer: Arc<dyn Authorizer>) -> Self {
        self.authorizer = authorizer;
        self
    }
    /// Opt into a [`ContextPolicy`]. `max_derived_bytes` caps the encoded array of its derived
    /// context within `max_context_bytes`; a policy that only pins entries may pass zero.
    pub fn with_context_policy(
        mut self,
        policy: Arc<dyn ContextPolicy>,
        max_derived_bytes: usize,
    ) -> Result<Self> {
        if max_derived_bytes > self.limits.max_context_bytes {
            return Err(Error::Invalid(
                "derived context cap must fit the context limit".into(),
            ));
        }
        self.context_policy = Some((policy, max_derived_bytes));
        Ok(self)
    }
    pub fn id(&self) -> &str {
        &self.id
    }
    pub fn limits(&self) -> &Limits {
        &self.limits
    }
    pub fn binding(&self) -> Value {
        let mut binding = json!({"id":self.id,"instructions":&*self.instructions,"model":&*self.config,"tools":&*self.tool_specs,
            "input_schema":self.input_schema,"output_schema":&*self.output_schema,"limits":self.limits});
        if let Some((policy, max_bytes)) = &self.context_policy {
            binding["context_policy"] = json!({"binding":policy.binding(),"max_bytes":max_bytes});
        }
        binding
    }
    /// Accepts `input`, retaining the bounded suffix of `conversation` that fits the context limit.
    pub fn new_run(
        &self,
        conversation_id: impl Into<String>,
        input: Value,
        conversation: &[HistoryEntry],
        expires_at_ms: Option<u64>,
    ) -> Result<RunState> {
        (self.validate_input)(&input)?;
        let conversation_id = conversation_id.into();
        if conversation_id.is_empty() || conversation_id.len() > 1024 {
            return Err(Error::Invalid(
                "conversation ID must be 1..1024 bytes".into(),
            ));
        }
        let input_entry = HistoryEntry::Input(input.clone());
        if encoded_len(&input_entry)? + 2 > self.limits.max_context_bytes {
            return Err(Error::Invalid("input exceeds context limit".into()));
        }
        let prior_context =
            conversation[suffix_start(conversation, self.limits.max_context_bytes)?..].to_vec();
        Ok(RunState {
            version: STATE_VERSION,
            id: RunId::new_v4(),
            conversation_id,
            agent_id: self.id.clone(),
            binding: self.binding(),
            input,
            limits: self.limits.clone(),
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
    fn tool(&self, name: &str) -> Result<Arc<dyn Tool>> {
        self.tools
            .iter()
            .find(|t| t.spec().name == name)
            .cloned()
            .ok_or_else(|| Error::Invalid(format!("unknown tool {name}")))
    }
}

/// A commit must atomically retain evidence, operations, and the new checkpoint.
/// On success it advances `revision`; on failure callers must reload authoritative state.
#[async_trait]
pub trait Checkpoint: Send + Sync {
    async fn commit(&self, state: &mut RunState) -> Result<()>;
}
#[derive(Default)]
pub struct MemoryCheckpoint;
#[async_trait]
impl Checkpoint for MemoryCheckpoint {
    async fn commit(&self, state: &mut RunState) -> Result<()> {
        state.revision += 1;
        Ok(())
    }
}
#[derive(Clone, Default)]
pub struct RunControl {
    pub cancel: CancellationToken,
    pub shutdown: CancellationToken,
    /// Best-effort transient events, each naming its run, so one channel may serve many runs.
    /// A full channel drops events and never blocks execution.
    pub events: Option<mpsc::Sender<ProgressEvent>>,
}
#[derive(Default, Debug)]
pub struct Conversation {
    pub history: Vec<HistoryEntry>,
    /// Retains waits and uncertain outcomes even when the convenience API returns an error.
    pub last_run: Option<RunState>,
}
#[derive(Clone)]
pub struct Engine {
    agent: Agent,
}
impl Engine {
    pub fn new(agent: Agent) -> Self {
        Self { agent }
    }
    pub fn agent(&self) -> &Agent {
        &self.agent
    }
    /// Convenience API for runs that finish without application input. Use `drive` for resumable waits.
    pub async fn run<I: Serialize, O: DeserializeOwned>(
        &self,
        conversation: &mut Conversation,
        input: I,
    ) -> Result<O> {
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
        let input = serde_json::to_value(input).map_err(|e| Error::Invalid(e.to_string()))?;
        let state = self
            .agent
            .new_run("memory", input, &conversation.history, None)?;
        conversation.last_run = Some(state);
        self.resume_from(conversation, RunControl::default(), 0)
            .await
    }
    /// Resume a retained in-memory run after resolving its wait through `last_run`.
    pub async fn resume<O: DeserializeOwned>(
        &self,
        conversation: &mut Conversation,
        control: RunControl,
    ) -> Result<O> {
        let old_len = conversation
            .last_run
            .as_ref()
            .ok_or(Error::NotFound)?
            .history
            .len();
        self.resume_from(conversation, control, old_len).await
    }
    async fn resume_from<O: DeserializeOwned>(
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
                serde_json::from_value(value.clone()).map_err(|e| Error::Invalid(e.to_string()))
            }
            Phase::Settled(other) => Err(Error::Invalid(format!("run settled: {other:?}"))),
            _ => Err(Error::Invalid(
                "run is waiting; resolve Conversation::last_run and call Engine::resume".into(),
            )),
        }
    }
    /// Drives the same state machine for memory and durable execution. No child work is detached.
    #[tracing::instrument(skip_all, fields(run_id = %state.id))]
    pub async fn drive(
        &self,
        state: &mut RunState,
        checkpoint: &dyn Checkpoint,
        control: RunControl,
    ) -> Result<()> {
        if state.version != STATE_VERSION
            || state.agent_id != self.agent.id
            || state.binding != self.agent.binding()
            || state.limits != self.agent.limits
        {
            return Err(Error::Incompatible(
                "persisted state or immutable agent definition differs".into(),
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
                let tool = self.agent.tool(&op.call.name)?;
                if tool.spec() != &op.spec {
                    return Err(Error::Incompatible(format!(
                        "tool contract differs: {}",
                        op.call.name
                    )));
                }
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
            match state.phase {
                Phase::Ready => {
                    if let Some(reason) = exhausted(state, true, 0) {
                        return settle(checkpoint, state, &control, Settlement::Stopped(reason))
                            .await;
                    }
                    match self.model_turn(state, checkpoint, &control).await {
                        Ok(()) => {}
                        Err(Error::Interrupted) => {
                            if control.cancel.is_cancelled() {
                                continue;
                            }
                            return Err(Error::Interrupted);
                        }
                        Err(
                            error @ (Error::Storage(_) | Error::Stale | Error::Incompatible(_)),
                        ) => return Err(error),
                        Err(error) => {
                            return settle(
                                checkpoint,
                                state,
                                &control,
                                Settlement::Failed(bounded_error(
                                    &error,
                                    state.limits.max_tool_output_bytes,
                                )),
                            )
                            .await;
                        }
                    }
                }
                Phase::Tools => {
                    match self.tool_batch(state, checkpoint, &control).await {
                        Ok(()) => {}
                        Err(Error::Interrupted) => {
                            if control.cancel.is_cancelled() {
                                continue;
                            }
                            return Err(Error::Interrupted);
                        }
                        Err(
                            error @ (Error::Storage(_) | Error::Stale | Error::Incompatible(_)),
                        ) => return Err(error),
                        Err(error) => {
                            return settle(
                                checkpoint,
                                state,
                                &control,
                                Settlement::Failed(bounded_error(
                                    &error,
                                    state.limits.max_tool_output_bytes,
                                )),
                            )
                            .await;
                        }
                    }
                    if matches!(state.phase, Phase::Tools) {
                        return Ok(());
                    }
                }
                Phase::Settled(_) => return Ok(()),
                Phase::ModelInFlight => {
                    return Err(Error::Invalid("unexpected in-flight model state".into()));
                }
            }
        }
    }
    async fn model_turn(
        &self,
        state: &mut RunState,
        checkpoint: &dyn Checkpoint,
        control: &RunControl,
    ) -> Result<()> {
        let (reservation, work_ms) = reserve(state)?;
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
                    assemble(state, self.agent.context_policy.as_ref(), &operation_cancel).await?;
                let request = ModelRequest {
                    run_id: state.id,
                    instructions: self.agent.instructions.clone(),
                    config: self.agent.config.clone(),
                    history,
                    derived_context,
                    tools: self.agent.tool_specs.clone(),
                    output_schema: self.agent.output_schema.clone(),
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
                    .agent
                    .model
                    .stream(request, operation_cancel.clone())
                    .await
                    .map_err(|error| Error::Provider(error.to_string()))?;
                let mut response = None;
                let mut bytes = 0_usize;
                let mut events = 0_usize;
                while let Some(event) = stream.next().await {
                    events += 1;
                    if events > state.limits.max_response_bytes.clamp(1, 65_536) {
                        return Err(Error::Provider("model event count exceeds limit".into()));
                    }
                    if response.is_some() {
                        return Err(Error::Provider(
                            "model emitted data after its complete response".into(),
                        ));
                    }
                    match event.map_err(|error| Error::Provider(error.to_string()))? {
                        ModelEvent::TextDelta(text) => {
                            bytes = bytes.saturating_add(text.len());
                            if bytes > state.limits.max_response_bytes {
                                return Err(Error::Provider(
                                    "stream exceeds response byte limit".into(),
                                ));
                            }
                            emit(state, control, ProgressKind::TextDelta(text));
                        }
                        ModelEvent::Response(value) => {
                            if encoded_len(&value)? > state.limits.max_response_bytes {
                                return Err(Error::Provider("response exceeds byte limit".into()));
                            }
                            response = Some(value);
                        }
                    }
                }
                response.ok_or_else(|| {
                    Error::Provider("stream ended without a complete response".into())
                })
            };
            run_work(future, work_deadline(state, started, work_ms), control).await
        };
        operation_cancel.cancel();
        let cleanup = tokio::time::timeout(
            Duration::from_millis(state.limits.cleanup_timeout_ms),
            async {
                let model = self.agent.model.cleanup(state.id);
                match &self.agent.context_policy {
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
        let response_result = match outcome {
            WorkOutcome::Done(result) => result,
            WorkOutcome::Interrupted => {
                commit(checkpoint, state, control).await?;
                return Err(Error::Interrupted);
            }
            WorkOutcome::TimedOut => Err(Error::Provider("model operation timed out".into())),
        };
        if let Ok(response) = &response_result {
            state.usage.input_tokens = state
                .usage
                .input_tokens
                .saturating_add(response.usage.input);
            state.usage.output_tokens = state
                .usage
                .output_tokens
                .saturating_add(response.usage.output);
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
            .map_err(|_| Error::Provider("model cleanup timed out".into()))?
            .map_err(|error| Error::Provider(error.to_string()))?;
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
            (self.agent.validate_output)(output)?;
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
                let spec = self
                    .agent
                    .tool(&call.name)
                    .expect("validated tool")
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
                    approved: false,
                }
            })
            .collect();
        state.phase = Phase::Tools;
        commit(checkpoint, state, control).await
    }
    fn validate_response(&self, response: &ModelResponse, state: &RunState) -> Result<()> {
        if response.output.is_some() == !response.tools.is_empty() {
            return Err(Error::Invalid(
                "model response must contain either final output or at least one tool call".into(),
            ));
        }
        if response.tools.len() > state.limits.max_tool_calls as usize {
            return Err(Error::Invalid("tool batch exceeds call limit".into()));
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
                return Err(Error::Invalid(
                    "tool call IDs must be unique and 1..256 bytes".into(),
                ));
            }
            self.agent.tool(&call.name)?.validate(&call.arguments)?;
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
        let (reservation, _) = reserve(state)?;
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
        let op = &state.operations[index];
        let tool = match self.agent.tool(&op.call.name) {
            Ok(tool) if tool.spec() == &op.spec => tool,
            Ok(_) => {
                let error = Error::Incompatible(format!("tool contract differs: {}", op.call.name));
                window.failure = Some(error);
                return Admission::Stop;
            }
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
            &*self.agent.authorizer,
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
                window.failure = Some(Error::Invalid(format!("authorization denied for {name}")));
                return Admission::Stop;
            }
            WorkOutcome::Done(Err(error)) => {
                window.failure = Some(Error::Invalid(format!("authorization failed: {error}")));
                return Admission::Stop;
            }
            // Expiry settles the run once the window drains.
            WorkOutcome::TimedOut if is_expired(state) => return Admission::Stop,
            WorkOutcome::TimedOut => {
                window.failure = Some(Error::Invalid("authorization timed out".into()));
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
        .ok_or_else(|| Error::Invalid("tool batch has no model response".into()))?;
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
    /// An authorization or contract failure that fails the run once the window drains.
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
        WorkOutcome::TimedOut => Err(Error::Tool("tool operation timed out".into())),
        WorkOutcome::Interrupted => Err(Error::Interrupted),
    };
    if let Err(error) = cleanup
        .map_err(|_| Error::Tool("tool cleanup timed out".into()))
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
    let fits = |len: Result<usize>| match len {
        Ok(len) if len <= max_bytes => Ok(()),
        Ok(_) => Err("tool output exceeds byte limit".to_owned()),
        Err(error) => Err(format!("tool output cannot be encoded: {error}")),
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
fn reserve(state: &mut RunState) -> Result<(u64, u64)> {
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
    if work == 0 {
        return Err(Error::Invalid("active budget exhausted".into()));
    }
    state.usage.active_ms = state.usage.active_ms.saturating_add(reservation);
    Ok((reservation, work))
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
            _ => Error::Storage(error.to_string()),
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
/// Resolve only the exact outstanding operation. Resolution itself must be committed by the caller.
/// Reconciliation is allowed after cancellation so uncertainty stays visible and resolvable.
pub fn resolve(state: &mut RunState, call_id: &str, resolution: Resolution) -> Result<()> {
    let op = state
        .operations
        .iter_mut()
        .find(|op| op.call.id == call_id)
        .ok_or(Error::NotWaiting)?;
    let next = match (&op.status, resolution) {
        (OperationStatus::Approval, Resolution::Approve(true))
            if !matches!(state.phase, Phase::Settled(_)) =>
        {
            op.approved = true;
            OperationStatus::Pending
        }
        (OperationStatus::Approval, Resolution::Approve(false))
            if !matches!(state.phase, Phase::Settled(_)) =>
        {
            OperationStatus::Complete(ToolResult {
                call_id: call_id.into(),
                value: json!({"error":"approval denied"}),
                is_error: true,
            })
        }
        (OperationStatus::Waiting(WaitRequest::Input { .. }), Resolution::Input(value))
            if !matches!(state.phase, Phase::Settled(_)) =>
        {
            if encoded_len(&value)? > state.limits.max_tool_output_bytes {
                return Err(Error::Invalid("input reply exceeds byte limit".into()));
            }
            OperationStatus::Complete(ToolResult {
                call_id: call_id.into(),
                value,
                is_error: false,
            })
        }
        (OperationStatus::Unknown { .. }, Resolution::Reconcile(result)) => {
            if result.call_id != call_id
                || encoded_len(&result.value)? > state.limits.max_tool_output_bytes
            {
                return Err(Error::Invalid(
                    "reconciliation result identity or size is invalid".into(),
                ));
            }
            OperationStatus::Complete(result)
        }
        _ => return Err(Error::NotWaiting),
    };
    op.status = next;
    Ok(())
}
