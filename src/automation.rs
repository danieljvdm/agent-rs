//! Persistent interval automations: schedules that create new runs.
//!
//! An automation is a fixed elapsed-time interval, an input and a conversation, recorded with
//! the agent that runs it. Firing it creates an ordinary run: the [`Host`] discovers it through
//! [`DurableStore::due`] like any other. A store implements [`AutomationStore`] to keep
//! automations beside its runs; the scheduling rules and the firing protocol are defined once
//! here, and each store supplies only the primitive reads and writes, inside one atomic write.
//!
//! No task is spawned: the application calls [`Host::fire_automations`] from its own poll loop.
//! Calendar/cron schedules and external event subscriptions are not supported.
use crate::types::now_ms;
use crate::{Agent, DurableStore, Error, Host, Result, RunId};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, hash_map::Entry};

/// Every timestamp, interval and counter fits a signed 64-bit SQL integer.
const MAX_MS: u64 = i64::MAX as u64;

/// A fixed elapsed-time schedule in UTC Unix milliseconds, unaffected by time zones or
/// daylight-saving transitions. Each firing submits `input` to `conversation_id`, validated
/// against the agent's input type exactly as ordinary run admission is.
///
/// ```
/// # use serde_json::json;
/// # use agent_rs::IntervalAutomation;
/// let inbox = json!("Review my inbox");
/// let hourly = IntervalAutomation::new("personal-inbox", inbox, 1_000, 3_600_000);
/// assert_eq!(hourly.interval_ms, 3_600_000);
/// ```
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct IntervalAutomation {
    /// The conversation every firing submits to, 1..=1024 bytes.
    pub conversation_id: String,
    /// The run input of every firing.
    pub input: Value,
    /// The first scheduled time. A past time fires at the next poll.
    pub first_at_ms: u64,
    /// The time between scheduled firings; positive.
    pub interval_ms: u64,
}
impl IntervalAutomation {
    /// A schedule firing at `first_at_ms` and every `interval_ms` after it.
    pub fn new(
        conversation_id: impl Into<String>,
        input: Value,
        first_at_ms: u64,
        interval_ms: u64,
    ) -> Self {
        Self {
            conversation_id: conversation_id.into(),
            input,
            first_at_ms,
            interval_ms,
        }
    }
    /// Sets [`conversation_id`](Self::conversation_id).
    pub fn with_conversation_id(mut self, conversation_id: impl Into<String>) -> Self {
        self.conversation_id = conversation_id.into();
        self
    }
    /// Sets [`input`](Self::input).
    pub fn with_input(mut self, input: Value) -> Self {
        self.input = input;
        self
    }
    /// Sets [`first_at_ms`](Self::first_at_ms).
    pub fn with_first_at_ms(mut self, first_at_ms: u64) -> Self {
        self.first_at_ms = first_at_ms;
        self
    }
    /// Sets [`interval_ms`](Self::interval_ms).
    pub fn with_interval_ms(mut self, interval_ms: u64) -> Self {
        self.interval_ms = interval_ms;
        self
    }
    /// Rejects what no firing could admit, before anything is persisted.
    fn validate(&self, agent: &Agent) -> Result<()> {
        if self.interval_ms == 0 || self.first_at_ms > MAX_MS || self.interval_ms > MAX_MS {
            return Err(Error::input(
                "an interval must be positive, and its times must fit signed 64-bit milliseconds",
            ));
        }
        // The agent's own input type and context limit, as every firing will apply them.
        agent.new_run(&self.conversation_id, self.input.clone(), &[], None)?;
        Ok(())
    }
    /// The occurrence a firing at `now` selects for a cursor due at `next_at <= now`: the latest
    /// due time, the earlier intervals it covers, and the next cursor. `None` when the next
    /// cursor would leave the supported range.
    pub(crate) fn slot(&self, next_at: u64, now: u64) -> Option<Slot> {
        let elapsed = now - next_at;
        let scheduled_at = now - elapsed % self.interval_ms;
        let next_at = scheduled_at
            .checked_add(self.interval_ms)
            .filter(|at| *at <= MAX_MS)?;
        Some(Slot {
            scheduled_at,
            coalesced: elapsed / self.interval_ms,
            next_at,
        })
    }
}
/// See [`IntervalAutomation::slot`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Slot {
    pub scheduled_at: u64,
    pub coalesced: u64,
    pub next_at: u64,
}

/// Whether an automation fires.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum AutomationState {
    /// Fires when due.
    Enabled,
    /// Fires nothing until resumed; time spent paused coalesces into one firing.
    Paused,
    /// A tombstone: never fires again, and its ID cannot be reused.
    Deleted,
}

/// A durable snapshot of one automation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Automation {
    /// Store-wide, 1..=256 bytes.
    pub id: String,
    /// The agent every firing runs with, from [`Agent::id`]. Its binding is recorded too: a
    /// firing whose registered agent's binding differs pauses the automation instead.
    pub agent_id: String,
    /// Advanced by every management change and by an automatic pause, never by a firing.
    pub revision: u64,
    /// Advanced by every [`AutomationChange::Update`]. Each version is a separate occurrence
    /// namespace, even when the schedule is unchanged.
    pub schedule_version: u64,
    /// The current schedule.
    pub schedule: IntervalAutomation,
    /// Whether it fires.
    pub state: AutomationState,
    /// The cursor: the earliest time the next firing may select.
    pub next_at_ms: u64,
    /// The run of the latest admitted occurrence. Its status is authoritative in the store.
    pub last_run_id: Option<RunId>,
    /// Why the automation paused itself, until a resume or update clears it. Run errors are
    /// the run's own, through [`DurableStore::last_error`].
    pub last_error: Option<String>,
}

/// What became of one occurrence.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum OccurrenceOutcome {
    /// A new run was accepted.
    Admitted {
        /// The accepted run.
        run_id: RunId,
    },
    /// The conversation, or the automation's previous run, was still unsettled.
    SkippedOverlap,
}

/// One firing's durable evidence. Its identity is the automation, schedule version and
/// scheduled time; each identity fires at most once.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Occurrence {
    /// A store-wide cursor for paging, increasing in firing order; not the identity.
    pub sequence: u64,
    /// The automation that fired.
    pub automation_id: String,
    /// Its [`Automation::schedule_version`] at the firing.
    pub schedule_version: u64,
    /// The latest due time the firing selected.
    pub scheduled_at_ms: u64,
    /// The clock reading the firing was made at.
    pub observed_at_ms: u64,
    /// Earlier due intervals this occurrence covers. They create no rows and never fire.
    pub coalesced: u64,
    /// A run, or a skip.
    pub outcome: OccurrenceOutcome,
}

/// Bounds of one firing call. Use the same values for every caller sharing a store; they are
/// deployment limits, not persisted schedule policy.
///
/// ```
/// # use agent_rs::AutomationOptions;
/// let options = AutomationOptions::default().with_max_in_flight(8);
/// assert_eq!(options.max_per_fire, 64);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct AutomationOptions {
    /// Unsettled runs created by any automation in the store, waiting or not, including those
    /// of paused and deleted automations; 1..=1024. Runs submitted directly do not count, and
    /// neither do children: a delegating root counts once, so concurrent model work is at most
    /// `max_in_flight × (1 + max_parallel)` of its subagents. Default 64.
    pub max_in_flight: usize,
    /// Due automations examined, and occurrences written, by one call; 1..=1024. Default 64.
    pub max_per_fire: usize,
}
impl Default for AutomationOptions {
    fn default() -> Self {
        Self {
            max_in_flight: 64,
            max_per_fire: 64,
        }
    }
}
impl AutomationOptions {
    /// Sets [`max_in_flight`](Self::max_in_flight).
    pub fn with_max_in_flight(mut self, runs: usize) -> Self {
        self.max_in_flight = runs;
        self
    }
    /// Sets [`max_per_fire`](Self::max_per_fire).
    pub fn with_max_per_fire(mut self, automations: usize) -> Self {
        self.max_per_fire = automations;
        self
    }
}

/// What one firing call did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct FiringReport {
    /// Occurrences written, admitted or skipped, in firing order.
    pub occurrences: Vec<Occurrence>,
    /// Automations this call paused, with the reason in [`Automation::last_error`].
    pub paused: Vec<String>,
    /// A due automation found no capacity. Its cursor is kept, so a later call coalesces it
    /// again instead of queueing runs.
    pub at_capacity: bool,
}

/// A management change, fenced on the automation's [`revision`](Automation::revision).
#[derive(Clone)]
#[non_exhaustive]
pub enum AutomationChange {
    /// Replace the schedule and adopt `agent`'s ID and binding. Keeps the pause state and
    /// accepted runs, clears the error, starts a new schedule version and resets the cursor
    /// to the new `first_at_ms`.
    Update {
        /// The agent future firings run with.
        agent: Agent,
        /// The new schedule.
        schedule: IntervalAutomation,
    },
    /// Stop firing. Accepted runs continue.
    Pause,
    /// Fire again, clearing the error. Time spent paused coalesces into one firing. It does not
    /// adopt a changed binding: the next firing pauses again until an `Update` adopts it.
    Resume,
    /// Leave a tombstone. Accepted runs and occurrences remain; no later firing admits work.
    Delete,
}
impl AutomationChange {
    /// The one definition of management semantics: stores load the automation and its encoded
    /// binding, apply this within the same atomic write, and persist both when it returns true.
    pub(crate) fn apply(
        self,
        automation: &mut Automation,
        binding: &mut String,
        revision: u64,
    ) -> Result<bool> {
        if automation.revision != revision {
            return Err(Error::Stale);
        }
        if automation.state == AutomationState::Deleted {
            return match self {
                Self::Delete => Ok(false),
                _ => Err(Error::input("a deleted automation cannot be changed")),
            };
        }
        match self {
            Self::Update { agent, schedule } => {
                schedule.validate(&agent)?;
                automation.schedule_version = advance(automation.schedule_version)?;
                automation.next_at_ms = schedule.first_at_ms;
                automation.schedule = schedule;
                automation.agent_id = agent.id().to_owned();
                *binding = encode(agent.binding())?;
                automation.last_error = None;
            }
            Self::Pause | Self::Resume | Self::Delete => {
                let state = match self {
                    Self::Pause => AutomationState::Paused,
                    Self::Resume => AutomationState::Enabled,
                    _ => AutomationState::Deleted,
                };
                if state == automation.state {
                    return Ok(false);
                }
                automation.state = state;
                if state == AutomationState::Enabled {
                    automation.last_error = None;
                }
            }
        }
        automation.revision = advance(automation.revision)?;
        Ok(true)
    }
}

/// Automations kept beside a store's runs. Implemented by [`MemoryStore`](crate::MemoryStore)
/// and `SqliteStore`; a [`Host`] over such a store adds [`Host::fire_automations`].
///
/// Implementors must meet these obligations, beside those of [`DurableStore`]:
///
/// - **Atomic firing.** One firing's run acceptance (exactly as [`DurableStore::accept`]
///   performs it, idempotency key and one-unsettled-run rule included), its occurrence and its
///   cursor advance become durable together or not at all. A durable store commits a whole
///   `fire_due` call as one write; on error nothing it did remains.
/// - **Shared rules.** Firing follows the protocol in this module (coalescing, overlap,
///   capacity, binding checks), and management applies [`AutomationChange`]'s semantics.
/// - **Unique identities.** An occurrence identity fires at most once, even across concurrent
///   calls and restarts. Runs use the idempotency key `automation/{id}/{version}/{scheduled}`:
///   reserve the `automation/` prefix for automations.
/// - **Errors.** An unknown ID is [`Error::NotFound`]; a stale revision is [`Error::Stale`];
///   out-of-bounds arguments are [`Error::Input`].
#[async_trait]
pub trait AutomationStore: DurableStore {
    /// Create automation `id` firing `agent` on `schedule`, enabled, after validating the
    /// schedule against the agent. Repeating the same ID, schedule and agent returns the
    /// current record; anything else, including a deleted ID, is
    /// [`Error::IdempotencyConflict`].
    async fn create_automation(
        &self,
        agent: &Agent,
        id: &str,
        schedule: IntervalAutomation,
    ) -> Result<Automation>;
    /// The automation's current snapshot, tombstones included.
    async fn automation(&self, id: &str) -> Result<Automation>;
    /// Automations in ID order after `after`, tombstones included; `limit` is 1..=1024.
    async fn automations(&self, after: Option<&str>, limit: usize) -> Result<Vec<Automation>>;
    /// Apply a management change at exactly `revision` and return the result.
    async fn change_automation(
        &self,
        id: &str,
        revision: u64,
        change: AutomationChange,
    ) -> Result<Automation>;
    /// The automation's occurrences with a sequence after `after`, oldest first; `limit` is
    /// 1..=1024.
    async fn occurrences(&self, id: &str, after: u64, limit: usize) -> Result<Vec<Occurrence>>;
    /// Fire the automations due at `now_ms` whose recorded agent is in `agents`. Automations
    /// of other agents are left untouched and due.
    async fn fire_due(
        &self,
        agents: &[Agent],
        now_ms: u64,
        options: AutomationOptions,
    ) -> Result<FiringReport>;
}

impl<S: AutomationStore + ?Sized + 'static> Host<S> {
    /// Fire the store's due automations whose agents this host serves, at the current UTC
    /// time, and wake the host for the runs admitted. Call it from the application's own poll
    /// loop; repeating a call, or calling after a lost reply, is safe. One call examines at
    /// most [`max_per_fire`](AutomationOptions::max_per_fire) automations.
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use serde_json::json;
    /// # use agent_rs::*;
    /// # async fn example(agent: Agent) -> Result<()> {
    /// let store = Arc::new(MemoryStore::new());
    /// let hourly = IntervalAutomation::new("inbox", json!("Review my inbox"), 0, 3_600_000);
    /// store.create_automation(&agent, "inbox-review", hourly).await?;
    /// let mut host = Host::start(store.clone(), agent, HostOptions::default())?;
    /// let fired = host.fire_automations(AutomationOptions::default()).await?;
    /// assert_eq!(fired.occurrences.len(), 1);
    /// host.shutdown().await?;
    /// # Ok(()) }
    /// ```
    pub async fn fire_automations(&self, options: AutomationOptions) -> Result<FiringReport> {
        self.health()?;
        let agents: Vec<Agent> = self.agents().cloned().collect();
        let report = self.store().fire_due(&agents, now_ms(), options).await?;
        if report
            .occurrences
            .iter()
            .any(|occurrence| matches!(occurrence.outcome, OccurrenceOutcome::Admitted { .. }))
        {
            self.wake();
        }
        Ok(report)
    }
}

/// One enabled automation a firing selected, as a store reads it inside its atomic write.
pub(crate) struct Due {
    pub id: String,
    pub agent_id: String,
    pub revision: u64,
    pub schedule_version: u64,
    pub schedule: IntervalAutomation,
    /// The binding recorded with the automation, encoded.
    pub binding: String,
    pub next_at: u64,
    pub last_run: Option<RunId>,
}

/// The primitive reads and writes of one firing call, all inside one atomic write.
pub(crate) trait Firing {
    /// Unsettled runs any automation created.
    fn in_flight(&mut self) -> Result<usize>;
    /// Enabled automations of `agent_id` with a cursor at or before `now`, by cursor then ID.
    fn due(&mut self, agent_id: &str, now: u64, limit: usize) -> Result<Vec<Due>>;
    /// Whether `conversation` has an unsettled run, or `run` is unsettled.
    fn busy(&mut self, conversation: &str, run: Option<RunId>) -> Result<bool>;
    /// Accept as [`DurableStore::accept`] does; true when the run is new.
    fn accept(
        &mut self,
        agent: &Agent,
        conversation: &str,
        key: &str,
        input: Value,
    ) -> Result<(RunId, bool)>;
    /// Persist an occurrence and return its sequence.
    fn record(&mut self, occurrence: &Occurrence) -> Result<u64>;
    /// Move the cursor, and the latest run when one was admitted.
    fn advance(&mut self, id: &str, next_at: u64, last_run: Option<RunId>) -> Result<()>;
    /// Pause at `revision`, advancing it, with `error`.
    fn pause(&mut self, id: &str, revision: u64, error: String) -> Result<()>;
}

/// The one firing protocol. Missed intervals coalesce into their latest due time; an unsettled
/// conversation or previous run skips the occurrence and still advances the cursor; a full
/// capacity keeps the cursor; a binding change, an unadmittable input or a cursor leaving the
/// supported range pauses the automation. Any other error aborts the whole call.
pub(crate) fn fire(
    firing: &mut dyn Firing,
    agents: &[Agent],
    now: u64,
    options: AutomationOptions,
) -> Result<FiringReport> {
    if now > MAX_MS {
        return Err(Error::input(
            "the clock must fit signed 64-bit milliseconds",
        ));
    }
    if !(1..=1024).contains(&options.max_in_flight) || !(1..=1024).contains(&options.max_per_fire) {
        return Err(Error::input("automation limits must be in 1..=1024"));
    }
    let mut registered: HashMap<&str, &Agent> = HashMap::new();
    for agent in agents {
        registered.entry(agent.id()).or_insert(agent);
    }
    let mut due = Vec::new();
    for agent_id in registered.keys() {
        due.extend(firing.due(agent_id, now, options.max_per_fire)?);
    }
    due.sort_by(|a, b| (a.next_at, &a.id).cmp(&(b.next_at, &b.id)));
    due.truncate(options.max_per_fire);
    let mut report = FiringReport::default();
    if due.is_empty() {
        return Ok(report);
    }
    let mut in_flight = firing.in_flight()?;
    let mut bindings: HashMap<&str, String> = HashMap::new();
    for automation in &due {
        let agent = registered[automation.agent_id.as_str()];
        let binding = match bindings.entry(agent.id()) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(encode(agent.binding())?),
        };
        if *binding != automation.binding {
            let error = format!(
                "agent {} changed its binding; update the automation to adopt it",
                automation.agent_id
            );
            pause(firing, &mut report, automation, error)?;
            continue;
        }
        let schedule = &automation.schedule;
        let Some(slot) = schedule.slot(automation.next_at, now) else {
            let error = "the next interval leaves the supported time range; update the schedule";
            pause(firing, &mut report, automation, error.into())?;
            continue;
        };
        let outcome = if firing.busy(&schedule.conversation_id, automation.last_run)? {
            OccurrenceOutcome::SkippedOverlap
        } else if in_flight >= options.max_in_flight {
            report.at_capacity = true;
            continue;
        } else {
            let key = format!(
                "automation/{}/{}/{}",
                automation.id, automation.schedule_version, slot.scheduled_at
            );
            match firing.accept(
                agent,
                &schedule.conversation_id,
                &key,
                schedule.input.clone(),
            ) {
                Ok((run_id, new)) => {
                    in_flight += usize::from(new);
                    OccurrenceOutcome::Admitted { run_id }
                }
                Err(Error::ConversationBusy) => OccurrenceOutcome::SkippedOverlap,
                Err(error @ (Error::Input(_) | Error::Context(_) | Error::IdempotencyConflict)) => {
                    pause(firing, &mut report, automation, error.to_string())?;
                    continue;
                }
                // Not the automation's fault, so the whole call aborts. That includes
                // `RunNotActive`, which only steering returns and acceptance never does: a
                // firing never steers, and an admitted run is steered like any other.
                Err(error) => return Err(error),
            }
        };
        let mut occurrence = Occurrence {
            sequence: 0,
            automation_id: automation.id.clone(),
            schedule_version: automation.schedule_version,
            scheduled_at_ms: slot.scheduled_at,
            observed_at_ms: now,
            coalesced: slot.coalesced,
            outcome,
        };
        occurrence.sequence = firing.record(&occurrence)?;
        let last_run = match outcome {
            OccurrenceOutcome::Admitted { run_id } => Some(run_id),
            OccurrenceOutcome::SkippedOverlap => None,
        };
        firing.advance(&automation.id, slot.next_at, last_run)?;
        report.occurrences.push(occurrence);
    }
    Ok(report)
}
fn pause(
    firing: &mut dyn Firing,
    report: &mut FiringReport,
    automation: &Due,
    error: String,
) -> Result<()> {
    firing.pause(&automation.id, automation.revision, error)?;
    report.paused.push(automation.id.clone());
    Ok(())
}

/// Checks a new automation's ID and schedule, and returns its first snapshot and encoded binding.
pub(crate) fn new_automation(
    agent: &Agent,
    id: &str,
    schedule: IntervalAutomation,
) -> Result<(Automation, String)> {
    if id.is_empty() || id.len() > 256 {
        return Err(Error::input("an automation ID must contain 1..=256 bytes"));
    }
    schedule.validate(agent)?;
    Ok((
        Automation {
            id: id.to_owned(),
            agent_id: agent.id().to_owned(),
            revision: 1,
            schedule_version: 1,
            next_at_ms: schedule.first_at_ms,
            schedule,
            state: AutomationState::Enabled,
            last_run_id: None,
            last_error: None,
        },
        encode(agent.binding())?,
    ))
}
/// Whether an existing record is what `create_automation` would create again.
pub(crate) fn same_creation(
    existing: &Automation,
    existing_binding: &str,
    created: &Automation,
    binding: &str,
) -> bool {
    existing.state != AutomationState::Deleted
        && existing.agent_id == created.agent_id
        && existing.schedule == created.schedule
        && existing_binding == binding
}
/// Bounds of a page request.
pub(crate) fn check_page(limit: usize, after: u64) -> Result<()> {
    if !(1..=1024).contains(&limit) || after > MAX_MS {
        return Err(Error::input(
            "a page holds 1..=1024 items after an in-range cursor",
        ));
    }
    Ok(())
}
pub(crate) fn encode(value: &impl Serialize) -> Result<String> {
    serde_json::to_string(value).map_err(Error::storage)
}
fn advance(value: u64) -> Result<u64> {
    crate::store::advance(value, "automation revision")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn every(first: u64, interval: u64) -> IntervalAutomation {
        IntervalAutomation::new("c", Value::Null, first, interval)
    }

    #[test]
    fn missed_intervals_coalesce_into_the_latest_due_time() {
        let schedule = every(100, 100);
        let slot = |next, now| schedule.slot(next, now);
        // Exactly due: nothing coalesced.
        let on_time = Slot {
            scheduled_at: 100,
            coalesced: 0,
            next_at: 200,
        };
        assert_eq!(slot(100, 100), Some(on_time));
        // Between two due times: the earlier one, never the future one.
        assert_eq!(
            slot(100, 450),
            Some(Slot {
                scheduled_at: 400,
                coalesced: 3,
                next_at: 500,
            })
        );
        // Years of downtime cost constant arithmetic.
        let late = slot(100, 100 + 100 * 1_000_000_000 + 7).unwrap();
        assert_eq!(late.coalesced, 1_000_000_000);
        assert_eq!(late.scheduled_at, 100 + 100 * 1_000_000_000);
        // Intervals keep their phase relative to the cursor, not to the clock.
        assert_eq!(every(7, 10).slot(7, 31).unwrap().scheduled_at, 27);
    }

    #[test]
    fn a_cursor_leaving_the_supported_range_has_no_slot() {
        assert_eq!(every(MAX_MS, 1).slot(MAX_MS, MAX_MS), None);
        let last = every(MAX_MS - 10, 10)
            .slot(MAX_MS - 10, MAX_MS - 10)
            .unwrap();
        assert_eq!(last.next_at, MAX_MS);
    }
}
