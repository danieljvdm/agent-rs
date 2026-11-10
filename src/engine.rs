use crate::context::{assemble, encoded_len, suffix_start};
use crate::*;
use async_trait::async_trait;
use futures::{StreamExt, stream::FuturesUnordered};
use schemars::{JsonSchema, schema_for};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{collections::HashSet, sync::Arc, time::Duration};
use tokio::{sync::mpsc, time::Instant};
use tokio_util::sync::CancellationToken;

type Validator = Arc<dyn Fn(&Value) -> Result<()> + Send + Sync>;

/// Immutable agent definition. Product policy and clients are supplied by its caller.
#[derive(Clone)]
pub struct Agent {
    id: String,
    // Shared with every `ModelRequest` instead of copied into each one.
    instructions: Arc<str>,
    model: Arc<dyn Model>,
    config: ModelConfig,
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
            config,
            tool_specs: tools.iter().map(|tool| tool.spec().clone()).collect(),
            tools,
            limits,
            input_schema: serde_json::to_value(schema_for!(I)).expect("schema serialization"),
            output_schema: Arc::new(
                serde_json::to_value(schema_for!(O)).expect("schema serialization"),
            ),
            validate_input: Arc::new(|value| {
                serde_json::from_value::<I>(value.clone())
                    .map(|_| ())
                    .map_err(|e| Error::Invalid(e.to_string()))
            }),
            validate_output: Arc::new(|value| {
                serde_json::from_value::<O>(value.clone())
                    .map(|_| ())
                    .map_err(|e| Error::Invalid(e.to_string()))
            }),
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
        let mut binding = json!({"id":self.id,"instructions":&*self.instructions,"model":self.config,"tools":&*self.tool_specs,
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
    /// Best-effort transient events. A full channel drops events, never blocks execution.
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
        emit(
            state,
            control,
            ProgressEvent::ModelStarted { run_id: state.id },
        );
        commit(checkpoint, state, control).await?;
        let started = Instant::now();
        let operation_cancel = CancellationToken::new();
        let mut delta_events = 0_u64;
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
                            delta_events += 1;
                            if let Some(sender) = &control.events {
                                let _ = sender.try_send(ProgressEvent::TextDelta(text));
                            }
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
        state.usage.events = state.usage.events.saturating_add(delta_events);
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
        // Approval, user input and uncertainty stop the batch at a semantic boundary.
        if state.operations.iter().any(|op| {
            matches!(
                op.status,
                OperationStatus::Approval
                    | OperationStatus::Waiting(_)
                    | OperationStatus::Unknown { .. }
            )
        }) {
            return Ok(());
        }
        while let Some(first) = state.operations.iter().position(|op| {
            matches!(
                op.status,
                OperationStatus::Pending | OperationStatus::Dispatched
            )
        }) {
            if control.cancel.is_cancelled() || control.shutdown.is_cancelled() {
                return Err(Error::Interrupted);
            }
            if state.expires_at_ms.is_some_and(|t| t <= now_ms()) {
                return settle(
                    checkpoint,
                    state,
                    control,
                    Settlement::Stopped(StopReason::Expired),
                )
                .await;
            }
            let mut indices = vec![first];
            if state.operations[first].spec.execution == Execution::Parallel {
                for index in first + 1..state.operations.len() {
                    let op = &state.operations[index];
                    if op.spec.execution == Execution::Sequential {
                        break;
                    }
                    if matches!(
                        op.status,
                        OperationStatus::Pending | OperationStatus::Dispatched
                    ) {
                        indices.push(index);
                    }
                    if indices.len() >= state.limits.max_parallel_tools {
                        break;
                    }
                }
            }
            indices.truncate(state.limits.max_parallel_tools);
            if let Some(reason) = exhausted(state, false, indices.len() as u32) {
                return settle(checkpoint, state, control, Settlement::Stopped(reason)).await;
            }
            let (reservation, work_ms) = reserve(state)?;
            state.usage.tool_calls += indices.len() as u32;
            // Reserve before authorization too, so retries cannot replenish its active-work budget.
            commit(checkpoint, state, control).await?;
            let started = Instant::now();
            let deadline = work_deadline(state, started, work_ms);
            for &index in &indices {
                let op = state.operations[index].clone();
                if self.agent.tool(&op.call.name)?.spec() != &op.spec {
                    return Err(Error::Incompatible(format!(
                        "tool contract differs: {}",
                        op.call.name
                    )));
                }
                match run_work(
                    self.agent.authorizer.authorize(state, &op.call),
                    deadline,
                    control,
                )
                .await
                {
                    WorkOutcome::Done(Ok(true)) => {}
                    WorkOutcome::Done(Ok(false)) => {
                        refund(state, reservation, started.elapsed());
                        return Err(Error::Invalid(format!(
                            "authorization denied for {}",
                            op.call.name
                        )));
                    }
                    WorkOutcome::Done(Err(error)) => {
                        refund(state, reservation, started.elapsed());
                        return Err(Error::Invalid(format!("authorization failed: {error}")));
                    }
                    WorkOutcome::TimedOut => {
                        refund(state, reservation, started.elapsed());
                        if is_expired(state) {
                            return settle(
                                checkpoint,
                                state,
                                control,
                                Settlement::Stopped(StopReason::Expired),
                            )
                            .await;
                        }
                        return Err(Error::Invalid("authorization timed out".into()));
                    }
                    WorkOutcome::Interrupted => {
                        refund(state, reservation, started.elapsed());
                        commit(checkpoint, state, control).await?;
                        return Err(Error::Interrupted);
                    }
                }
            }
            for &index in &indices {
                state.operations[index].status = OperationStatus::Dispatched;
            }
            emit(
                state,
                control,
                ProgressEvent::ToolsStarted {
                    count: indices.len(),
                },
            );
            commit(checkpoint, state, control).await?;
            let mut futures = FuturesUnordered::new();
            for index in indices {
                let op = state.operations[index].clone();
                let tool = self.agent.tool(&op.call.name)?;
                let context = ToolContext {
                    run_id: state.id,
                    call_id: op.call.id.clone(),
                    idempotency_key: op.idempotency_key,
                    cancel: CancellationToken::new(),
                };
                let control = control.clone();
                let cleanup_ms = state.limits.cleanup_timeout_ms;
                futures.push(async move {
                    let outcome = run_work(
                        tool.execute(op.call.arguments, context.clone()),
                        deadline,
                        &control,
                    )
                    .await;
                    context.cancel.cancel();
                    let cleanup = tokio::time::timeout(
                        Duration::from_millis(cleanup_ms),
                        tool.cleanup(context),
                    )
                    .await;
                    (index, outcome, cleanup)
                });
            }
            let mut commit_error = None;
            let mut interrupted = false;
            while let Some((index, outcome, cleanup)) = futures.next().await {
                let op = &mut state.operations[index];
                let was_interrupted = matches!(&outcome, WorkOutcome::Interrupted);
                let mut value = match outcome {
                    WorkOutcome::Done(result) => result,
                    WorkOutcome::TimedOut => Err(Error::Tool("tool operation timed out".into())),
                    WorkOutcome::Interrupted => {
                        interrupted = true;
                        Err(Error::Interrupted)
                    }
                };
                if let Err(error) = cleanup
                    .map_err(|_| Error::Tool("tool cleanup timed out".into()))
                    .and_then(|v| v)
                {
                    value = Err(error);
                }
                op.status = match value {
                    Ok(ToolOutput::Complete(value))
                        if encoded_len(&value)? <= state.limits.max_tool_output_bytes =>
                    {
                        OperationStatus::Complete(ToolResult {
                            call_id: op.call.id.clone(),
                            value,
                            is_error: false,
                        })
                    }
                    Ok(ToolOutput::Wait(wait))
                        if encoded_len(&wait)? <= state.limits.max_tool_output_bytes =>
                    {
                        OperationStatus::Waiting(wait)
                    }
                    Ok(_) => failure_status(
                        op,
                        "tool output exceeds byte limit",
                        state.limits.max_tool_output_bytes,
                        false,
                    ),
                    Err(error) => failure_status(
                        op,
                        &error.to_string(),
                        state.limits.max_tool_output_bytes,
                        was_interrupted || matches!(error, Error::Interrupted),
                    ),
                };
                // A storage failure never drops other live children. Drain and clean all of them first.
                if commit_error.is_none()
                    && let Err(error) = commit(checkpoint, state, control).await
                {
                    commit_error = Some(error);
                }
            }
            if let Some(error) = commit_error {
                return Err(error);
            }
            refund(state, reservation, started.elapsed());
            commit(checkpoint, state, control).await?;
            if interrupted {
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
            if state.operations.iter().any(|op| {
                matches!(
                    op.status,
                    OperationStatus::Waiting(_) | OperationStatus::Unknown { .. }
                )
            }) {
                return Ok(());
            }
        }
        if state
            .operations
            .iter()
            .all(|op| matches!(op.status, OperationStatus::Complete(_)))
        {
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
            commit(checkpoint, state, control).await?;
        }
        Ok(())
    }
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
fn emit(state: &mut RunState, control: &RunControl, event: ProgressEvent) {
    state.usage.events = state.usage.events.saturating_add(1);
    if let Some(sender) = &control.events {
        let _ = sender.try_send(event);
    }
}
async fn commit(
    checkpoint: &dyn Checkpoint,
    state: &mut RunState,
    control: &RunControl,
) -> Result<()> {
    // Count committed progress before writing so the final counter is retained too.
    state.usage.events = state.usage.events.saturating_add(1);
    checkpoint
        .commit(state)
        .await
        .map_err(|error| match error {
            Error::Stale | Error::Storage(_) | Error::Incompatible(_) => error,
            _ => Error::Storage(error.to_string()),
        })?;
    if let Some(sender) = &control.events {
        let _ = sender.try_send(ProgressEvent::Committed {
            run_id: state.id,
            revision: state.revision,
        });
    }
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
    state.usage.events = state.usage.events.saturating_add(1);
    commit(checkpoint, state, control).await?;
    if let Some(sender) = &control.events {
        let _ = sender.try_send(ProgressEvent::Settled { run_id: state.id });
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
