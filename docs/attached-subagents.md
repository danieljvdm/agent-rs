# Attached subagents

An agent can delegate to child agents that its model calls like tools. A delegation admits one child run, drives it to settlement inside the parent's run, and returns the child's typed result as the call's result. The child is *attached*: it has its own run ID, history and operations, but it commits only as part of its parent's checkpoint, spends an allocation of its parent's budget, and is never scheduled on its own. This works the same in memory and through a durable `Host` over any `DurableStore`.

```rust
use std::sync::Arc;
use std::time::Duration;
use yielded_agent::{Agent, Limits, Result, ScriptedModel, Subagent};

fn planner(model: Arc<ScriptedModel>, researcher_model: Arc<ScriptedModel>) -> Result<Agent<u32, String>> {
    let researcher = Agent::builder("researcher", researcher_model)
        .limits(Limits::default().with_max_model_calls(4).with_active_budget(Duration::from_secs(60)))
        .input::<String>()
        .output::<String>()
        .build()?;
    Agent::builder("planner", model)
        .input::<u32>()
        .output::<String>()
        .subagents(2, [Subagent::new("research", "Research one topic.", researcher)])
        .build()
}
```

The model is offered a tool named `research` whose argument schema is the child's input type. Its result is a `ChildResult<O>` of the child's output type, with `parent_run_id`, `child_run_id`, the child's `usage`, and an `outcome`: `Completed(O)`, `Stopped(StopReason)`, `Failed(String)` or `Cancelled`. Only `Completed` is a successful result; the others are error results the parent's model can handle. A child receives the delegation's arguments as its input and no conversation, and its output is checked against its own output type. `Subagent::with_requires_approval(true)` makes each delegation wait for approval, as a tool's `requires_approval` does. The parent's authorizer authorizes each delegation; the child's authorizer authorizes the child's own calls.

`build` checks that `max_parallel` is 1..=1024, that subagent names are 1..=256 bytes and unique among the agent's tools and subagents, and that no subagent has subagents of its own: delegation is one level deep. The child agents' bindings and `max_parallel` are part of the parent's binding; an agent without subagents keeps its binding byte for byte.

`RunState::children` retains each admitted child, in admission order, after it settles. `RunState::child(id)` finds one and `AttachedChild::result::<O>()` decodes its result. Each child's `parent` names the parent's run and the delegation call. A result larger than the parent's `max_tool_output_bytes` becomes an error result; the full result stays in the retained child.

## Scheduling

A delegation is an operation of the parent's tool batch and runs through the same sliding window as a tool. The window classifies each operation once, as a tool or a delegation, and a delegation takes one of `max_parallel` child slots beside the batch's `max_parallel_tools` tool slots. Declaration order, `Sequential` barriers (a sequential call waits for running children too), authorization before admission and stop-on-failure apply unchanged. What differs for a child:

- **Intent before effect.** A delegation's intent is its child's admission: the operation becomes `OperationStatus::Child`, the child's run joins `RunState::children`, and its allocation is reserved. The window commits that before the child starts, exactly as it commits a tool's `Dispatched` intent. An admission whose commit fails is undone, and the child never runs.
- **One writer.** The child runs on a working copy of its state with a checkpoint that hands each of its commits to the parent's window. The window applies the commit to the parent's copy of the child and commits the parent, so the child's progress becomes durable together with the parent's, under the parent's generation and revision fence. Nothing else writes the parent meanwhile.
- **Settlement.** The commit that settles a child also records its result as the delegation's outcome and replaces its allocation with what it used, once.
- **Waits.** A child that stops unsettled, waiting on approval, input, reconciliation or a timer, is *blocked*. Like a waiting call it stops new dispatch in the batch, but admitted children that can progress still resume, without being authorized or charged again. A tool's wait in the parent's batch stops children from resuming until it is resolved.
- **Failed commits.** Once a commit of the parent fails, the window accepts no more child commits, stops every child, starts nothing more and reports the failure. The parent's copy of the child is restored to what is durable.
- **Stopping.** Cancellation reaches children, which settle as cancelled through the window. If the window fails the run, for example when an authorization is denied, its children are cancelled. Shutdown stops them, unsettled and resumable.

`tests/subagents.rs` covers these, and `tests/tool_scheduling.rs` still covers the window's invariants for tools.

## Budgets

A child's `Limits` are an allocation from its parent. Admission reserves all of them (model calls, tool calls, tokens, as output tokens, and active time) in the parent's `usage` and `child_usage`, and settlement replaces the reservation with what the child actually used, exactly once. A delegation also costs the parent one tool call. Give children smaller limits than their parent: a delegation whose allocation does not fit what remains is answered with an error result and admits no child, after any running sibling has had the chance to return budget. The parent's model `turn` counts only the parent's own calls; its limits apply to the total. Unsettled children keep their reservations across restarts, so delegation never replenishes a budget.

A child's own usage includes everything its model reports, as a run's does: tokens reported by a turn that is then rejected or interrupted are charged to the child and committed with that turn, through its parent's window. While the child is unsettled its parent holds the reservation, not these charges; settlement replaces the reservation with the child's usage, so each reported token reaches the parent's `usage` and `child_usage` exactly once, and a child that reports more than it was allotted is charged all of it.

The parent's window charges active time while its tools or authorizations may be in flight. Time in which only children run is theirs: each child charges its own work, through its allocation. Resuming children reserves nothing from the parent. Every child inherits its parent's expiry.

## Steering, memory and adapters

[Steering](steering.md) targets a root run. A child is not a run of the store, so `steer` and `steering` with a child's ID fail with `Error::NotFound`. A child's commits are commits of its root in `Phase::Tools`, never a dispatch, so instructions queued for the root while children run or wait stay queued and are applied at the root's next model dispatch, after the delegations' exchange. A root completes only once every child has settled, so a completion that queued steering reopens never cancels or reruns a child. No child's history ever receives an instruction. The `Checkpoint` contract adds no exception for child work: the engine applies each child commit to the root's copy of the child before committing the root, and reports a commit that changes `state.children` as `Error::Storage`.

A child's conversation ID is `child:{root}:{call}`, derived from its root's run and its delegation call. A `ContextPolicy` on a child agent, such as [memory](memory.md)'s `MemoryPolicy`, therefore receives that ID and reads memory in that scope, never the parent conversation's. To give a child memory, index its sources under that ID, or give the child what it needs in the delegation's arguments.

A child's binding, inside its parent's, includes the child's own `model_adapter` and `context_policy` keys when it has them, so a run is never resumed with a child on another adapter or memory configuration.

`tests/steering.rs` runs the steering cases against every `SteeringStore`, and `tests/subagents.rs` pins the rest.

## Storage and recovery

Every store persists children with their root, through `Checkpoint::commit` and `RunUpdate`, and `tests/storage_conformance.rs` runs the same children contract against `MemoryStore` and `SqliteStore`. A commit adds each newly admitted child with its acceptance fixed, and writes the progress and new history of each child whose revision advanced; a child's revision advances with every change to it. A child is read through its root: its ID is not a run of the store, but `operations(child_id)` retains its operations and `history` its conversation, `child:{root}:{call}`.

SQLite writes each child's acceptance once, as a `runs` row, its history and operations as rows like a run's, and its progress as one row of a `children` table keyed to its root. A commit therefore writes only the child it changed and the root's small progress row, in the root's fenced transaction. `tests/cost_budget.rs` pins the cost: a parent that delegates once to a child that answers directly takes 10 transactions and about 15.5 KB of records; each tool round of the child adds 6 transactions and about 11.6 KB, the same for its first round and its eighth. Nesting children inside the parent's progress row would instead have rewritten every child's whole state, binding and history included, at every commit: about 14 KB for a child's first round, rising to 21 KB by its eighth, for each child.

A root's `wake_at` is the earliest time one of its children can progress, and `needs_recovery` includes children's dispatched work, so stores discover a parent from their stored columns, including abandoned child work behind a sibling's input wait. When a parent is driven again, its children's work in flight is recovered as its own is: an interrupted model call is made again, and a dispatched call that must not repeat becomes `Unknown`. A committed admission keeps the child's identity across restarts, and a committed child result is never recomputed. One state version covers runs with and without children.

## Waiting, cancellation and cleanup

A child's waits stay in the child's `operations`; the parent is in `Tools` and holds no host worker while every unsettled child is blocked. Resolve a child's wait with `Host::resolve_child(run, child, call, resolution)`, `RunUpdate::ResolveChild` through any store, or `RunState::resolve_child` in memory followed by `Agent::resume`. Resolving one child leaves its siblings as they are, and fences any older execution of the parent.

When a parent settles in any way with unsettled children, they settle as cancelled, with any dispatched effect left `Unknown`. `RunState::has_uncertainty()` includes children, and a child's unknown outcome can be reconciled after its parent settled. Host shutdown does not cancel: children stay resumable with their input, identity, allocation and waits.

Child progress events carry the child's `run_id` and its parent's `parent_run_id`, set in the engine's one `emit`, so consumers tell concurrent child streams apart from the parent's own events. Events stay transient and best effort.

## Example and benchmark

`cargo run --example delegated_workflow` runs a deterministic planner that delegates to two venue researchers. One finishes; the other asks for input. SQLite is closed and reopened, the child's question is answered through the host, the completed child is reused unchanged, and the planner picks the venue within budget. `tests/delegated_workflow.rs` grades it: the selection and sources, two distinct children, exactly two lookups, six model calls and five tool calls. This is a deterministic runtime check, not a measure of live-model delegation quality. `cargo bench --bench attached_subagents` times delegation to 1 and 4 children in memory and through a resident SQLite host; see [benchmarks](benchmarks.md).

## Scope

This supports direct attached children. Nested delegation, detached children, delivery across hosts, arbitrary workflow graphs and live-model evaluation of delegation are out of scope.
