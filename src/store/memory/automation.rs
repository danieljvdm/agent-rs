//! [`AutomationStore`] for [`MemoryStore`]: automations live beside the runs, under the same
//! lock, so a firing's acceptance, occurrence and cursor are one atomic step.
use super::{Inner, MemoryStore};
use crate::automation::{self, Due, Firing};
use crate::{
    Agent, Automation, AutomationChange, AutomationOptions, AutomationState, AutomationStore,
    Error, FiringReport, IntervalAutomation, Occurrence, Result, RunId,
};
use async_trait::async_trait;
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
use std::ops::Bound;

#[derive(Default)]
pub(super) struct Automations {
    by_id: BTreeMap<String, Stored>,
    /// Every occurrence in firing order; an occurrence's sequence is its index plus one.
    occurrences: Vec<Occurrence>,
    /// Runs automations created that may still be unsettled, pruned whenever counted.
    in_flight: HashSet<RunId>,
}
struct Stored {
    automation: Automation,
    binding: String,
    /// Indices of this automation's occurrences.
    occurrences: Vec<usize>,
}

struct MemoryFiring<'a>(&'a mut Inner);
impl Firing for MemoryFiring<'_> {
    fn in_flight(&mut self) -> Result<usize> {
        let inner = &mut *self.0;
        let runs = &inner.runs;
        inner
            .automations
            .in_flight
            .retain(|id| runs.get(id).is_some_and(|run| !run.state.is_terminal()));
        Ok(inner.automations.in_flight.len())
    }
    fn due(&mut self, agent_id: &str, now: u64, limit: usize) -> Result<Vec<Due>> {
        let mut due: Vec<_> = self
            .0
            .automations
            .by_id
            .values()
            .filter(|stored| {
                let automation = &stored.automation;
                automation.state == AutomationState::Enabled
                    && automation.agent_id == agent_id
                    && automation.next_at_ms <= now
            })
            .collect();
        due.sort_by(|a, b| {
            (a.automation.next_at_ms, &a.automation.id)
                .cmp(&(b.automation.next_at_ms, &b.automation.id))
        });
        Ok(due
            .into_iter()
            .take(limit)
            .map(|stored| {
                let automation = &stored.automation;
                Due {
                    id: automation.id.clone(),
                    agent_id: automation.agent_id.clone(),
                    revision: automation.revision,
                    schedule_version: automation.schedule_version,
                    schedule: automation.schedule.clone(),
                    binding: stored.binding.clone(),
                    next_at: automation.next_at_ms,
                    last_run: automation.last_run_id,
                }
            })
            .collect())
    }
    fn busy(&mut self, conversation: &str, run: Option<RunId>) -> Result<bool> {
        let inner = &*self.0;
        Ok(inner.unsettled.contains_key(conversation)
            || run.is_some_and(|id| {
                inner
                    .runs
                    .get(&id)
                    .is_some_and(|run| !run.state.is_terminal())
            }))
    }
    fn accept(
        &mut self,
        agent: &Agent,
        conversation: &str,
        key: &str,
        input: Value,
    ) -> Result<(RunId, bool)> {
        let (id, new) = self.0.accept(agent, conversation, key, input, None)?;
        self.0.automations.in_flight.insert(id);
        Ok((id, new))
    }
    fn record(&mut self, occurrence: &Occurrence) -> Result<u64> {
        let automations = &mut self.0.automations;
        let index = automations.occurrences.len();
        let mut occurrence = occurrence.clone();
        occurrence.sequence = index as u64 + 1;
        automations
            .by_id
            .get_mut(&occurrence.automation_id)
            .ok_or(Error::NotFound)?
            .occurrences
            .push(index);
        let sequence = occurrence.sequence;
        automations.occurrences.push(occurrence);
        Ok(sequence)
    }
    fn advance(&mut self, id: &str, next_at: u64, last_run: Option<RunId>) -> Result<()> {
        let automation = &mut stored(&mut self.0.automations, id)?.automation;
        automation.next_at_ms = next_at;
        if last_run.is_some() {
            automation.last_run_id = last_run;
        }
        Ok(())
    }
    fn pause(&mut self, id: &str, revision: u64, error: String) -> Result<()> {
        let automation = &mut stored(&mut self.0.automations, id)?.automation;
        if automation.revision != revision {
            return Err(Error::Stale);
        }
        automation.revision = crate::store::advance(revision, "automation revision")?;
        automation.state = AutomationState::Paused;
        automation.last_error = Some(error);
        Ok(())
    }
}
fn stored<'a>(automations: &'a mut Automations, id: &str) -> Result<&'a mut Stored> {
    automations.by_id.get_mut(id).ok_or(Error::NotFound)
}

#[async_trait]
impl AutomationStore for MemoryStore {
    async fn create_automation(
        &self,
        agent: &Agent,
        id: &str,
        schedule: IntervalAutomation,
    ) -> Result<Automation> {
        let (created, binding) = automation::new_automation(agent, id, schedule)?;
        let mut inner = self.lock()?;
        if let Some(existing) = inner.automations.by_id.get(id) {
            return if automation::same_creation(
                &existing.automation,
                &existing.binding,
                &created,
                &binding,
            ) {
                Ok(existing.automation.clone())
            } else {
                Err(Error::IdempotencyConflict)
            };
        }
        inner.automations.by_id.insert(
            id.to_owned(),
            Stored {
                automation: created.clone(),
                binding,
                occurrences: Vec::new(),
            },
        );
        Ok(created)
    }
    async fn automation(&self, id: &str) -> Result<Automation> {
        let inner = self.lock()?;
        let stored = inner.automations.by_id.get(id).ok_or(Error::NotFound)?;
        Ok(stored.automation.clone())
    }
    async fn automations(&self, after: Option<&str>, limit: usize) -> Result<Vec<Automation>> {
        automation::check_page(limit, 0)?;
        let inner = self.lock()?;
        let start = after.map_or(Bound::Unbounded, Bound::Excluded);
        Ok(inner
            .automations
            .by_id
            .range::<str, _>((start, Bound::Unbounded))
            .take(limit)
            .map(|(_, stored)| stored.automation.clone())
            .collect())
    }
    async fn change_automation(
        &self,
        id: &str,
        revision: u64,
        change: AutomationChange,
    ) -> Result<Automation> {
        let mut inner = self.lock()?;
        let stored = stored(&mut inner.automations, id)?;
        let (mut automation, mut binding) = (stored.automation.clone(), stored.binding.clone());
        if change.apply(&mut automation, &mut binding, revision)? {
            stored.automation = automation;
            stored.binding = binding;
        }
        Ok(stored.automation.clone())
    }
    async fn occurrences(&self, id: &str, after: u64, limit: usize) -> Result<Vec<Occurrence>> {
        automation::check_page(limit, after)?;
        let inner = self.lock()?;
        let automations = &inner.automations;
        let stored = automations.by_id.get(id).ok_or(Error::NotFound)?;
        // Indices are in sequence order, and a sequence is its index plus one.
        let first = stored
            .occurrences
            .partition_point(|index| (*index as u64) < after);
        Ok(stored.occurrences[first..]
            .iter()
            .take(limit)
            .map(|index| automations.occurrences[*index].clone())
            .collect())
    }
    async fn fire_due(
        &self,
        agents: &[Agent],
        now_ms: u64,
        options: AutomationOptions,
    ) -> Result<FiringReport> {
        let mut inner = self.lock()?;
        automation::fire(&mut MemoryFiring(&mut inner), agents, now_ms, options)
    }
}
