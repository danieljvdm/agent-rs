//! Attached subagents: child agents a parent's model calls like tools.
//!
//! A delegation is an operation of the parent's tool batch and runs through the same sliding
//! window as a tool, with the same invariants:
//!
//! - **Intent before effect.** Admission marks the operation `Child`, adds the child's run to
//!   `RunState::children` and reserves its allocation in the parent's usage. The window commits
//!   that as the operation's intent before the child starts.
//! - **One writer.** The child runs on a working copy of its state, with a [`ChildCheckpoint`]
//!   that hands each of its commits to the window. The window applies it to the parent's copy of
//!   the child and commits the parent, so the child's progress and the parent's evidence become
//!   durable together under the parent's fence, and nothing else writes the parent meanwhile.
//! - **Settlement with the parent's result.** The commit that settles a child also records its
//!   result as the delegation's outcome and replaces its allocation with what it used.
//! - **Stop on wait.** A child that returns unsettled is blocked, and, like a waiting call, stops
//!   new dispatch. Children that can progress still resume, without being authorized or charged
//!   again.
//! - **Failed commits.** Once a commit of the parent fails, no child commit is accepted and every
//!   child is stopped; the window drains and reports the failure.
use super::{Admission, Admitted, Definition, Event, Finished, Start, Window, Work, commit};
use crate::children::{self, error_value};
use crate::context::encoded_len;
use crate::{
    Agent, AttachedChild, Checkpoint, Error, HistoryEntry, ModelResponse, Operation,
    OperationStatus, ParentRun, Phase, Result, RetryPolicy, RunControl, RunId, RunState, Tool,
    ToolResult, ToolSpec, Usage,
};
use async_trait::async_trait;
use futures::{StreamExt, stream::FuturesUnordered};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

/// A child agent that a parent's model calls like a tool, set with
/// [`AgentBuilder::subagents`](crate::AgentBuilder::subagents).
///
/// The model sees a tool named `name` whose arguments are the child agent's input type. Each
/// call admits one child run with those arguments as its input and no conversation, drives it
/// to settlement as part of the parent's run, and gives the model a [`ChildResult`] of the
/// child's output type as the call's result: an error result unless the child completed.
///
/// A child's [`Limits`](crate::Limits) are an allocation from its parent: admission reserves
/// all of them in the parent's usage, and settlement replaces them, once, with what the child
/// used. Give children smaller limits than their parent, which also spends one tool call per
/// delegation. A delegation whose allocation does not fit what remains is answered with an
/// error result, without a child.
///
/// ```
/// # use std::sync::Arc;
/// # use serde_json::json;
/// # use agent_rs::*;
/// # fn main() -> Result<()> {
/// let researcher = Agent::builder("researcher", Arc::new(ScriptedModel::new(vec![])))
///     .limits(Limits::default().with_max_model_calls(4).with_active_budget(std::time::Duration::from_secs(60)))
///     .input::<String>()
///     .output::<String>()
///     .build()?;
/// let planner = Agent::builder("planner", Arc::new(ScriptedModel::new(vec![])))
///     .subagents(2, [Subagent::new("research", "Research one topic.", researcher)])
///     .build()?;
/// assert!(planner.binding().get("subagents").is_some());
/// # Ok(())
/// # }
/// ```
///
/// [`ChildResult`]: crate::ChildResult
#[derive(Clone)]
pub struct Subagent {
    spec: ToolSpec,
    agent: Agent,
}
impl Subagent {
    /// Delegates calls of `name` to `agent`, described to the model by `description`. The
    /// delegation's argument schema is the agent's input schema.
    pub fn new<I, O>(
        name: impl Into<String>,
        description: impl Into<String>,
        agent: Agent<I, O>,
    ) -> Self {
        let agent = agent.erased();
        let spec = ToolSpec::new(name, "1")
            .with_description(description)
            .with_input_schema(agent.definition.input_schema.clone())
            .with_retry(RetryPolicy::Idempotent {
                contract: "attached-child-v1".into(),
            });
        Self { spec, agent }
    }
    /// Requires each delegation to wait in [`OperationStatus::Approval`] before its child is
    /// admitted, as [`ToolSpec::requires_approval`] does for a tool. Defaults to `false`.
    pub fn with_requires_approval(mut self, requires_approval: bool) -> Self {
        self.spec.requires_approval = requires_approval;
        self
    }
    /// The tool the parent's model is offered.
    pub fn spec(&self) -> &ToolSpec {
        &self.spec
    }
    /// The child agent.
    pub fn agent(&self) -> &Agent {
        &self.agent
    }
    fn definition(&self) -> &Definition {
        &self.agent.definition
    }
    /// The model's arguments must be a valid input for the child and fit its context.
    pub(super) fn validate(&self, arguments: &Value) -> Result<()> {
        let definition = self.definition();
        (definition.validate_input)(arguments).map_err(Error::model_output)?;
        if encoded_len(&HistoryEntry::Input(arguments.clone())) + 2
            > definition.limits.max_context_bytes
        {
            return Err(Error::model_output("child input exceeds its context limit"));
        }
        Ok(())
    }
}

/// An agent's subagents, checked once by `build`.
pub(super) struct Subagents {
    pub(super) max_parallel: usize,
    list: Vec<Subagent>,
    by_name: HashMap<String, usize>,
}
impl Subagents {
    /// `None` for an empty list, so an agent without subagents keeps its binding.
    pub(super) fn new(
        max_parallel: usize,
        list: Vec<Subagent>,
        tools: &HashMap<String, Arc<dyn Tool>>,
    ) -> Result<Option<Self>> {
        if !(1..=1024).contains(&max_parallel) {
            return Err(Error::config("subagent parallelism must be 1..=1024"));
        }
        let mut by_name = HashMap::with_capacity(list.len());
        for (index, subagent) in list.iter().enumerate() {
            let name = &subagent.spec.name;
            if name.is_empty()
                || name.len() > 256
                || tools.contains_key(name)
                || by_name.insert(name.clone(), index).is_some()
            {
                return Err(Error::config(
                    "tool and subagent names must be unique and bounded",
                ));
            }
            if subagent.definition().subagents.is_some() {
                return Err(Error::config(
                    "a subagent cannot have subagents: delegation is one level deep",
                ));
            }
        }
        Ok((!list.is_empty()).then_some(Self {
            max_parallel,
            list,
            by_name,
        }))
    }
    pub(super) fn get(&self, name: &str) -> Option<&Subagent> {
        self.by_name.get(name).map(|&index| &self.list[index])
    }
    pub(super) fn specs(&self) -> impl Iterator<Item = ToolSpec> + '_ {
        self.list.iter().map(|subagent| subagent.spec.clone())
    }
    /// The binding contribution: each child's binding, under the name it is delegated by. Their
    /// specs are among the binding's tools.
    pub(super) fn binding(&self) -> Value {
        let agents: Vec<Value> = (self.list.iter())
            .map(|s| json!({"name": s.spec.name, "agent": s.agent.binding()}))
            .collect();
        json!({"max_parallel": self.max_parallel, "agents": agents})
    }
}

/// Whether the admitted child `run_id` can progress at `now`.
pub(super) fn resumable(state: &RunState, run_id: RunId, now: u64) -> bool {
    state
        .child(run_id)
        .is_some_and(|child| child.state.is_runnable(now))
}

/// The delegation an unsettled child belongs to, in the parent's current batch.
fn subagent_of<'a>(
    definition: &'a Definition,
    state: &RunState,
    index: usize,
) -> Result<&'a Subagent> {
    let call_id = &state.children[index].call_id;
    let op = (state.operations.iter())
        .find(|op| &op.call.id == call_id)
        .ok_or_else(|| Error::incompatible("an unsettled child has no delegation"))?;
    match definition.recorded(op)? {
        Work::Delegate(subagent) => Ok(subagent),
        Work::Tool(_) => Err(Error::incompatible(format!(
            "{} is not a delegation",
            op.call.name
        ))),
    }
}
/// Recovers the work each unsettled child left in flight, as the parent's own is recovered when
/// it is driven again. Returns whether any child changed.
pub(super) fn recover_children(definition: &Definition, state: &mut RunState) -> Result<bool> {
    let mut recovered = false;
    for index in 0..state.children.len() {
        if state.children[index].state.is_terminal() {
            continue;
        }
        let subagent = subagent_of(definition, state, index)?;
        let child = &mut state.children[index].state;
        if subagent.definition().recover(child)? {
            child.revision = child.revision.saturating_add(1);
            recovered = true;
        }
    }
    Ok(recovered)
}

/// Decides what a delegation at `index` does before it is authorized: an admitted child resumes,
/// a delegation without a free child slot waits, and one whose allocation does not fit is
/// answered with an error result once no sibling can return budget. `Ok` admits it.
pub(super) fn prepare(
    definition: &Definition,
    state: &mut RunState,
    window: &mut Window,
    index: usize,
    subagent: &Subagent,
    admitted: &[Admitted],
) -> std::result::Result<(), Admission> {
    let max_parallel = definition.subagents.as_ref().map_or(0, |s| s.max_parallel);
    let starting = admitted
        .iter()
        .filter(|admission| matches!(admission.start, Start::Child(_)))
        .count();
    let running = window.children.as_ref().map_or(0, |c| c.running);
    if running + starting >= max_parallel {
        return Err(Admission::Wait);
    }
    let op = &state.operations[index];
    if let OperationStatus::Child { run_id } = op.status {
        let child = (state.children.iter())
            .position(|child| child.state.id == run_id)
            .expect("an admitted child is retained");
        return Err(Admission::Start(Admitted {
            index,
            previous: op.status.clone(),
            sequential: false,
            start: Start::Child(child),
            deadline: Instant::now(),
        }));
    }
    let Some(reason) = children::exceeds(state, subagent.agent.limits()) else {
        return Ok(());
    };
    if window.running + admitted.len() > 0 {
        // A sibling may still return budget; try again when it finishes.
        return Err(Admission::Wait);
    }
    let message = format!("child allocation does not fit the remaining budget: {reason}");
    state.operations[index].status = OperationStatus::Complete(ToolResult {
        call_id: op.call.id.clone(),
        value: error_value(&message, state.limits.max_tool_output_bytes),
        is_error: true,
    });
    window.unsaved = true;
    Err(Admission::Skip)
}
/// Admits an authorized delegation: its child run, the parent's dependency on it and its
/// allocation, in memory. The window commits them together before the child starts.
pub(super) fn admit(
    state: &mut RunState,
    window: &mut Window,
    index: usize,
    subagent: &Subagent,
    deadline: Instant,
) -> Admission {
    let op = &state.operations[index];
    let call_id = op.call.id.clone();
    let conversation = format!("child:{}:{}", state.id, call_id);
    let admitted = subagent.agent.new_run(
        conversation,
        op.call.arguments.clone(),
        &[],
        state.expires_at_ms,
    );
    let mut child = match admitted {
        Ok(child) => child,
        Err(error) => {
            window.failure = Some(error);
            return Admission::Stop;
        }
    };
    child.parent = Some(ParentRun {
        run_id: state.id,
        call_id: call_id.clone(),
    });
    children::reserve(state, &child.limits);
    let previous = std::mem::replace(
        &mut state.operations[index].status,
        OperationStatus::Child { run_id: child.id },
    );
    state.usage.tool_calls += 1;
    state.children.push(AttachedChild {
        call_id,
        state: Box::new(child),
    });
    Admission::Start(Admitted {
        index,
        previous,
        sequential: false,
        start: Start::Child(state.children.len() - 1),
        deadline,
    })
}
/// Undoes an admission whose intent was never committed: the child never started.
pub(super) fn unadmit(state: &mut RunState, child: usize) {
    let removed = state.children.remove(child);
    children::unreserve(state, &removed.state.limits);
}

/// The future that drives one child within a parent's window.
pub(super) type ChildFuture<'a> = Pin<Box<dyn Future<Output = Finished> + Send + 'a>>;

impl Definition {
    /// Starts the admitted child at `child` for the operation at `index`, on a working copy of
    /// its state. Its commits reach the parent through the window.
    pub(super) fn start_child<'a>(
        &'a self,
        state: &RunState,
        window: &mut Window,
        control: &RunControl,
        index: usize,
        child: usize,
    ) -> ChildFuture<'a> {
        let subagent = subagent_of(self, state, child).expect("admitted delegation");
        let children = window
            .children
            .get_or_insert_with(|| ChildWindow::new(control));
        children.running += 1;
        let mut working = (*state.children[child].state).clone();
        let checkpoint = ChildCheckpoint {
            child,
            sender: children.sender.clone(),
            synced: AtomicUsize::new(working.history.len()),
        };
        let control = RunControl {
            cancel: children.cancel.clone(),
            shutdown: children.shutdown.clone(),
            events: control.events.clone(),
        };
        let definition = subagent.definition();
        let span = crate::telemetry::tool_span(
            &state.operations[index],
            crate::telemetry::Call::tool(state, index),
        );
        if !span.is_disabled() {
            window.spans.push((index, span.clone()));
        }
        let future = async move {
            let result = definition.drive(&mut working, &checkpoint, control).await;
            Finished::Child { index, result }
        };
        match crate::telemetry::instrument(span, future, |future| future, |future| future) {
            futures::future::Either::Left(future) => Box::pin(future),
            futures::future::Either::Right(future) => future,
        }
    }
}

/// The children of one window: how their commits reach it, and how it stops them.
pub(super) struct ChildWindow {
    sender: mpsc::UnboundedSender<ChildCommit>,
    receiver: mpsc::UnboundedReceiver<ChildCommit>,
    /// Cancels children when the parent is cancelled or its window fails the run.
    pub(super) cancel: CancellationToken,
    /// Stops children, unsettled, at shutdown or when the parent's commit fails.
    pub(super) shutdown: CancellationToken,
    pub(super) running: usize,
    /// Time spent waiting while only children ran, which the parent's window does not charge.
    pub(super) idle: Duration,
}
impl ChildWindow {
    fn new(control: &RunControl) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        Self {
            sender,
            receiver,
            cancel: control.cancel.child_token(),
            shutdown: control.shutdown.child_token(),
            running: 0,
            idle: Duration::ZERO,
        }
    }
    pub(super) async fn next<F: Future<Output = Finished>>(
        &mut self,
        in_flight: &mut FuturesUnordered<F>,
        running: usize,
    ) -> Option<Event> {
        let idle = (running == self.running).then(Instant::now);
        let event = tokio::select! {
            biased;
            Some(finished) = in_flight.next() => Some(Event::Finished(finished)),
            Some(request) = self.receiver.recv() => Some(Event::Commit(request)),
            else => None,
        };
        if let Some(since) = idle {
            self.idle += since.elapsed();
        }
        event
    }
}

impl Window {
    /// Whether the window closes once the one child still running returns: nothing else runs
    /// and nothing more can start.
    fn closes_after_child(&self, state: &RunState) -> bool {
        self.running == 1
            && (!self.open
                || (self.next..state.operations.len()).all(|i| !self.startable(state, i)))
    }
    /// Whether a tool may take a slot, when children take slots of their own.
    pub(super) fn tool_slot(&self, state: &RunState, admitted: &[Admitted]) -> bool {
        let children = self.children.as_ref().map_or(0, |c| c.running)
            + admitted
                .iter()
                .filter(|admission| matches!(admission.start, Start::Child(_)))
                .count();
        self.running + admitted.len() - children < state.limits.max_parallel_tools
    }
    /// Records that the child of the operation at `index` returned. Its outcome, if it settled,
    /// was committed with its settlement.
    pub(super) fn record_child(&mut self, state: &RunState, index: usize, result: Result<()>) {
        self.running -= 1;
        if let Some(children) = &mut self.children {
            children.running -= 1;
        }
        // The child's own commits are durable, but the window's refund must ride on a commit
        // after its last one, as it rides on the commit of a tool's outcome.
        if self.reserved_ms > 0 {
            self.unsaved = true;
        }
        match result {
            // Blocked: nothing new starts, but children that can progress still resume.
            Ok(())
                if matches!(
                    state.operations[index].status,
                    OperationStatus::Child { .. }
                ) =>
            {
                self.resume_only = true;
            }
            Ok(()) => {}
            Err(Error::Interrupted) => {
                self.interrupted = true;
                self.open = false;
            }
            // The parent's state is no longer authoritative, or a child cannot be driven.
            Err(error) => {
                self.storage.get_or_insert(error);
                self.open = false;
            }
        }
    }
}

/// The part of a child's state its own commits change, besides appending history.
pub(super) struct ChildProgress {
    cancel_requested: bool,
    phase: Phase,
    usage: Usage,
    pending_response: Option<ModelResponse>,
    operations: Vec<Operation>,
}
impl ChildProgress {
    fn swap(&mut self, state: &mut RunState) {
        std::mem::swap(&mut self.cancel_requested, &mut state.cancel_requested);
        std::mem::swap(&mut self.phase, &mut state.phase);
        std::mem::swap(&mut self.usage, &mut state.usage);
        std::mem::swap(&mut self.pending_response, &mut state.pending_response);
        std::mem::swap(&mut self.operations, &mut state.operations);
    }
}
/// One commit of a child, handed to its parent's window.
pub(super) struct ChildCommit {
    child: usize,
    revision: u64,
    progress: ChildProgress,
    /// History the child appended since its previous commit.
    history: Vec<HistoryEntry>,
    reply: oneshot::Sender<Result<u64>>,
}
/// A child's checkpoint: each commit becomes a commit of its parent, made by the parent's window.
struct ChildCheckpoint {
    child: usize,
    sender: mpsc::UnboundedSender<ChildCommit>,
    /// The child's history length as of its last commit.
    synced: AtomicUsize,
}
#[async_trait]
impl Checkpoint for ChildCheckpoint {
    async fn commit(&self, state: &mut RunState) -> Result<()> {
        let synced = self.synced.load(Ordering::Relaxed);
        let history = (state.history.get(synced..))
            .ok_or_else(|| Error::input("history is append-only"))?
            .to_vec();
        let (reply, response) = oneshot::channel();
        let request = ChildCommit {
            child: self.child,
            revision: state.revision,
            progress: ChildProgress {
                cancel_requested: state.cancel_requested,
                phase: state.phase.clone(),
                usage: state.usage.clone(),
                pending_response: state.pending_response.clone(),
                operations: state.operations.clone(),
            },
            history,
            reply,
        };
        // A window that went away, or refused the commit, has stopped this child.
        self.sender.send(request).map_err(|_| Error::Stale)?;
        state.revision = response.await.map_err(|_| Error::Stale)??;
        self.synced.store(state.history.len(), Ordering::Relaxed);
        Ok(())
    }
}

/// Makes a child's commit a commit of its parent: applies it to the parent's copy of the child,
/// settles the delegation if the child settled, and commits the parent. On failure the parent's
/// copy is restored, so it never runs ahead of what is durable, and the window stops.
pub(super) async fn commit_child(
    state: &mut RunState,
    checkpoint: &dyn Checkpoint,
    control: &RunControl,
    window: &mut Window,
    request: ChildCommit,
) {
    let ChildCommit {
        child,
        revision,
        mut progress,
        history,
        reply,
    } = request;
    let stored = &mut state.children[child].state;
    if window.storage.is_some() || stored.is_terminal() || stored.revision != revision {
        let _ = reply.send(Err(Error::Stale));
        return;
    }
    let history_len = stored.history.len();
    progress.swap(stored);
    stored.history.extend(history);
    stored.revision += 1;
    let settled = if stored.is_terminal() {
        children::settle(state, child)
    } else {
        None
    };
    // When this settles the window's last work, its refund rides on this commit instead of a
    // commit of its own once the child returns.
    let reserved = window.reserved_ms;
    let active_ms = state.usage.active_ms;
    if settled.is_some() && window.closes_after_child(state) {
        window.refund(state);
        window.reserved_ms = 0;
    }
    match commit(checkpoint, state, control).await {
        Ok(()) => {
            if let Some(settled) = &settled
                && let Some((_, span)) = window.spans.iter().find(|(index, _)| *index == settled.op)
            {
                span.record("output_bytes", settled.output_bytes);
            }
            window.unsaved = false;
            let _ = reply.send(Ok(state.children[child].state.revision));
        }
        Err(error) => {
            window.reserved_ms = reserved;
            state.usage.active_ms = active_ms;
            if let Some(settled) = settled {
                settled.undo(state);
            }
            let stored = &mut state.children[child].state;
            progress.swap(stored);
            stored.history.truncate(history_len);
            stored.revision -= 1;
            window.open = false;
            window.storage = Some(error);
            let _ = reply.send(Err(Error::storage("the parent's checkpoint failed")));
        }
    }
}
