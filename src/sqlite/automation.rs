//! [`AutomationStore`] for [`SqliteStore`]. Tables are split by mutability, as runs are:
//!
//! - `automations` holds the managed definition (agent, binding, schedule, state, revision),
//!   rewritten only by management changes and automatic pauses.
//! - `automation_cursors` holds one small row per automation that every firing rewrites: the
//!   next due time and the latest run, plus the agent and enabled flag the due index needs.
//! - `automation_occurrences` holds each occurrence once, unique by identity and by run.
//!
//! A firing call is one transaction: each run is accepted with [`accept_in`], exactly as
//! [`DurableStore::accept`](crate::DurableStore::accept) accepts, beside its occurrence and
//! cursor. A store with no automations writes nothing here.
use super::{Database, SqliteStore, accept_in, decode, encode, run_id};
use crate::automation::{self, Due, Firing};
use crate::{
    Agent, Automation, AutomationChange, AutomationOptions, AutomationState, AutomationStore,
    Error, FiringReport, IntervalAutomation, Occurrence, OccurrenceOutcome, Result, RunId,
};
use async_trait::async_trait;
use rusqlite::{Connection, OptionalExtension, Row, Transaction, params};
use serde_json::Value;

/// Additive and idempotent, so the schema version is unchanged: a database created without
/// automations gains these tables when opened, and one created with them stays readable by
/// builds that ignore them.
pub(super) fn create_tables(connection: &Connection) -> Result<()> {
    connection
        .execute_batch(
            "BEGIN IMMEDIATE;
            CREATE TABLE IF NOT EXISTS automations (
                id TEXT PRIMARY KEY, agent_id TEXT NOT NULL, binding TEXT NOT NULL,
                revision INTEGER NOT NULL, schedule_version INTEGER NOT NULL, state TEXT NOT NULL,
                schedule TEXT NOT NULL, last_error TEXT);
            CREATE TABLE IF NOT EXISTS automation_cursors (
                automation_id TEXT PRIMARY KEY REFERENCES automations(id),
                agent_id TEXT NOT NULL, enabled INTEGER NOT NULL, next_at INTEGER NOT NULL,
                last_run TEXT REFERENCES runs(id));
            CREATE INDEX IF NOT EXISTS automation_cursors_due
                ON automation_cursors(agent_id, next_at, automation_id) WHERE enabled=1;
            CREATE TABLE IF NOT EXISTS automation_occurrences (
                sequence INTEGER PRIMARY KEY, automation_id TEXT NOT NULL REFERENCES automations(id),
                schedule_version INTEGER NOT NULL, scheduled_at INTEGER NOT NULL,
                observed_at INTEGER NOT NULL, coalesced INTEGER NOT NULL,
                run_id TEXT UNIQUE REFERENCES runs(id),
                UNIQUE(automation_id, schedule_version, scheduled_at));
            CREATE INDEX IF NOT EXISTS automation_occurrences_page
                ON automation_occurrences(automation_id, sequence);
            COMMIT;",
        )
        .map_err(Error::storage)
}

const SELECT: &str = "SELECT a.id,a.agent_id,a.revision,a.schedule_version,a.state,a.schedule,
    a.last_error,c.next_at,c.last_run,a.binding
    FROM automations a JOIN automation_cursors c ON c.automation_id=a.id";

fn state_name(state: AutomationState) -> &'static str {
    match state {
        AutomationState::Enabled => "enabled",
        AutomationState::Paused => "paused",
        AutomationState::Deleted => "deleted",
    }
}
fn state_of(name: &str) -> Result<AutomationState> {
    match name {
        "enabled" => Ok(AutomationState::Enabled),
        "paused" => Ok(AutomationState::Paused),
        "deleted" => Ok(AutomationState::Deleted),
        other => Err(Error::storage(format!("unknown automation state {other}"))),
    }
}
fn unsigned(value: i64) -> Result<u64> {
    u64::try_from(value).map_err(Error::storage)
}
/// The columns of `SELECT`, undecoded.
type Columns = (
    String,
    String,
    i64,
    i64,
    String,
    String,
    Option<String>,
    i64,
    Option<String>,
    String,
);
fn columns(row: &Row<'_>) -> rusqlite::Result<Columns> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
        row.get(6)?,
        row.get(7)?,
        row.get(8)?,
        row.get(9)?,
    ))
}
/// A snapshot and its encoded binding.
fn assemble(columns: Columns) -> Result<(Automation, String)> {
    let (id, agent_id, revision, version, state, schedule, last_error, next_at, last_run, binding) =
        columns;
    let schedule: IntervalAutomation = decode(&schedule)?;
    if schedule.interval_ms == 0 {
        return Err(Error::storage("persisted interval is zero"));
    }
    Ok((
        Automation {
            id,
            agent_id,
            revision: unsigned(revision)?,
            schedule_version: unsigned(version)?,
            schedule,
            state: state_of(&state)?,
            next_at_ms: unsigned(next_at)?,
            last_run_id: last_run.as_deref().map(run_id).transpose()?,
            last_error,
        },
        binding,
    ))
}
fn lookup(connection: &Connection, id: &str) -> Result<(Automation, String)> {
    let columns = connection
        .prepare_cached(&format!("{SELECT} WHERE a.id=?1"))
        .map_err(Error::storage)?
        .query_row([id], columns)
        .optional()
        .map_err(Error::storage)?
        .ok_or(Error::NotFound)?;
    assemble(columns)
}
/// Rewrite both rows after a management change.
fn store_managed(tx: &Transaction<'_>, automation: &Automation, binding: &str) -> Result<u64> {
    tx.prepare_cached(
        "UPDATE automations SET agent_id=?2,binding=?3,revision=?4,schedule_version=?5,state=?6,
            schedule=?7,last_error=?8 WHERE id=?1",
    )
    .map_err(Error::storage)?
    .execute(params![
        automation.id,
        automation.agent_id,
        binding,
        automation.revision,
        automation.schedule_version,
        state_name(automation.state),
        encode(&automation.schedule)?,
        automation.last_error,
    ])
    .map_err(Error::storage)?;
    tx.prepare_cached(
        "UPDATE automation_cursors SET agent_id=?2,enabled=?3,next_at=?4 WHERE automation_id=?1",
    )
    .map_err(Error::storage)?
    .execute(params![
        automation.id,
        automation.agent_id,
        automation.state == AutomationState::Enabled,
        automation.next_at_ms,
    ])
    .map_err(Error::storage)?;
    Ok(2)
}

/// One firing call's reads and writes inside its transaction, counting rows written.
struct SqliteFiring<'a> {
    tx: &'a Transaction<'a>,
    writes: u64,
}
impl Firing for SqliteFiring<'_> {
    fn in_flight(&mut self) -> Result<usize> {
        let count: i64 = self
            .tx
            .prepare_cached(
                "SELECT COUNT(*) FROM checkpoints c WHERE c.settled=0
                AND EXISTS(SELECT 1 FROM automation_occurrences o WHERE o.run_id=c.run_id)",
            )
            .map_err(Error::storage)?
            .query_row([], |row| row.get(0))
            .map_err(Error::storage)?;
        usize::try_from(count).map_err(Error::storage)
    }
    fn due(&mut self, agent_id: &str, now: u64, limit: usize) -> Result<Vec<Due>> {
        let mut stmt = self
            .tx
            .prepare_cached(&format!(
                "{SELECT} WHERE c.agent_id=?1 AND c.enabled=1 AND c.next_at<=?2
                ORDER BY c.next_at,c.automation_id LIMIT ?3"
            ))
            .map_err(Error::storage)?;
        let rows = stmt
            .query_map(params![agent_id, now, limit as i64], columns)
            .map_err(Error::storage)?;
        rows.map(|row| {
            let (automation, binding) = assemble(row.map_err(Error::storage)?)?;
            Ok(Due {
                id: automation.id,
                agent_id: automation.agent_id,
                revision: automation.revision,
                schedule_version: automation.schedule_version,
                schedule: automation.schedule,
                binding,
                next_at: automation.next_at_ms,
                last_run: automation.last_run_id,
            })
        })
        .collect()
    }
    fn busy(&mut self, conversation: &str, run: Option<RunId>) -> Result<bool> {
        self.tx
            .prepare_cached(
                "SELECT EXISTS(SELECT 1 FROM checkpoints WHERE conversation=?1 AND settled=0)
                OR EXISTS(SELECT 1 FROM checkpoints WHERE run_id=?2 AND settled=0)",
            )
            .map_err(Error::storage)?
            .query_row(params![conversation, run.map(|id| id.to_string())], |row| {
                row.get(0)
            })
            .map_err(Error::storage)
    }
    fn accept(
        &mut self,
        agent: &Agent,
        conversation: &str,
        key: &str,
        input: Value,
    ) -> Result<(RunId, bool)> {
        let (id, writes) = accept_in(self.tx, agent, conversation, key, input, None)?;
        self.writes += writes;
        Ok((id, writes > 0))
    }
    fn record(&mut self, occurrence: &Occurrence) -> Result<u64> {
        let run = match occurrence.outcome {
            OccurrenceOutcome::Admitted { run_id } => Some(run_id.to_string()),
            OccurrenceOutcome::SkippedOverlap => None,
        };
        self.tx
            .prepare_cached(
                "INSERT INTO automation_occurrences
                (automation_id,schedule_version,scheduled_at,observed_at,coalesced,run_id)
                VALUES(?1,?2,?3,?4,?5,?6)",
            )
            .map_err(Error::storage)?
            .execute(params![
                occurrence.automation_id,
                occurrence.schedule_version,
                occurrence.scheduled_at_ms,
                occurrence.observed_at_ms,
                occurrence.coalesced,
                run,
            ])
            .map_err(Error::storage)?;
        self.writes += 1;
        unsigned(self.tx.last_insert_rowid())
    }
    fn advance(&mut self, id: &str, next_at: u64, last_run: Option<RunId>) -> Result<()> {
        self.tx
            .prepare_cached(
                "UPDATE automation_cursors SET next_at=?2,last_run=coalesce(?3,last_run)
                WHERE automation_id=?1",
            )
            .map_err(Error::storage)?
            .execute(params![id, next_at, last_run.map(|id| id.to_string())])
            .map_err(Error::storage)?;
        self.writes += 1;
        Ok(())
    }
    fn pause(&mut self, id: &str, revision: u64, error: String) -> Result<()> {
        let next = crate::store::advance(revision, "automation revision")?;
        let changed = self
            .tx
            .prepare_cached(
                "UPDATE automations SET revision=?3,state='paused',last_error=?4
                WHERE id=?1 AND revision=?2",
            )
            .map_err(Error::storage)?
            .execute(params![id, revision, next, error])
            .map_err(Error::storage)?;
        if changed == 0 {
            return Err(Error::Stale);
        }
        self.tx
            .prepare_cached("UPDATE automation_cursors SET enabled=0 WHERE automation_id=?1")
            .map_err(Error::storage)?
            .execute([id])
            .map_err(Error::storage)?;
        self.writes += 2;
        Ok(())
    }
}

/// Run `work` in one transaction, committing and counting it only when it wrote rows. An error
/// rolls back everything it did.
fn transact<T>(
    db: &mut Database,
    work: impl FnOnce(&Transaction<'_>) -> Result<(T, u64)>,
) -> Result<T> {
    let tx = db.connection.transaction().map_err(Error::storage)?;
    let (value, writes) = work(&tx)?;
    if writes > 0 {
        tx.commit().map_err(Error::storage)?;
        db.count(writes);
    }
    Ok(value)
}

#[async_trait]
impl AutomationStore for SqliteStore {
    async fn create_automation(
        &self,
        agent: &Agent,
        id: &str,
        schedule: IntervalAutomation,
    ) -> Result<Automation> {
        let (created, binding) = automation::new_automation(agent, id, schedule)?;
        self.call(move |db| {
            transact(db, |tx| {
                match lookup(tx, &created.id) {
                    Ok((existing, existing_binding)) => {
                        return if automation::same_creation(
                            &existing,
                            &existing_binding,
                            &created,
                            &binding,
                        ) {
                            Ok((existing, 0))
                        } else {
                            Err(Error::IdempotencyConflict)
                        };
                    }
                    Err(Error::NotFound) => (),
                    Err(error) => return Err(error),
                }
                tx.prepare_cached(
                    "INSERT INTO automations(id,agent_id,binding,revision,schedule_version,state,
                    schedule,last_error) VALUES(?1,?2,?3,?4,?5,?6,?7,NULL)",
                )
                .map_err(Error::storage)?
                .execute(params![
                    created.id,
                    created.agent_id,
                    binding,
                    created.revision,
                    created.schedule_version,
                    state_name(created.state),
                    encode(&created.schedule)?,
                ])
                .map_err(Error::storage)?;
                tx.prepare_cached(
                "INSERT INTO automation_cursors(automation_id,agent_id,enabled,next_at,last_run)
                    VALUES(?1,?2,1,?3,NULL)",
            )
            .map_err(Error::storage)?
            .execute(params![created.id, created.agent_id, created.next_at_ms])
            .map_err(Error::storage)?;
                Ok((created, 2))
            })
        })
        .await
    }
    async fn automation(&self, id: &str) -> Result<Automation> {
        let id = id.to_owned();
        self.call(move |db| Ok(lookup(&db.connection, &id)?.0))
            .await
    }
    async fn automations(&self, after: Option<&str>, limit: usize) -> Result<Vec<Automation>> {
        automation::check_page(limit, 0)?;
        let after = after.map(str::to_owned);
        self.call(move |db| {
            let mut stmt = db
                .connection
                .prepare_cached(&format!(
                    "{SELECT} WHERE ?1 IS NULL OR a.id>?1 ORDER BY a.id LIMIT ?2"
                ))
                .map_err(Error::storage)?;
            let rows = stmt
                .query_map(params![after, limit as i64], columns)
                .map_err(Error::storage)?;
            rows.map(|row| Ok(assemble(row.map_err(Error::storage)?)?.0))
                .collect()
        })
        .await
    }
    async fn change_automation(
        &self,
        id: &str,
        revision: u64,
        change: AutomationChange,
    ) -> Result<Automation> {
        let id = id.to_owned();
        self.call(move |db| {
            transact(db, |tx| {
                let (mut automation, mut binding) = lookup(tx, &id)?;
                if !change.apply(&mut automation, &mut binding, revision)? {
                    return Ok((automation, 0));
                }
                let writes = store_managed(tx, &automation, &binding)?;
                Ok((automation, writes))
            })
        })
        .await
    }
    async fn occurrences(&self, id: &str, after: u64, limit: usize) -> Result<Vec<Occurrence>> {
        automation::check_page(limit, after)?;
        let id = id.to_owned();
        self.call(move |db| {
            lookup(&db.connection, &id)?;
            let mut stmt = db
                .connection
                .prepare_cached(
                    "SELECT sequence,schedule_version,scheduled_at,observed_at,coalesced,run_id
                    FROM automation_occurrences WHERE automation_id=?1 AND sequence>?2
                    ORDER BY sequence LIMIT ?3",
                )
                .map_err(Error::storage)?;
            let rows = stmt
                .query_map(params![id, after, limit as i64], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, Option<String>>(5)?,
                    ))
                })
                .map_err(Error::storage)?;
            rows.map(|row| {
                let (sequence, version, scheduled_at, observed_at, coalesced, run) =
                    row.map_err(Error::storage)?;
                Ok(Occurrence {
                    sequence: unsigned(sequence)?,
                    automation_id: id.clone(),
                    schedule_version: unsigned(version)?,
                    scheduled_at_ms: unsigned(scheduled_at)?,
                    observed_at_ms: unsigned(observed_at)?,
                    coalesced: unsigned(coalesced)?,
                    outcome: match run {
                        Some(run) => OccurrenceOutcome::Admitted {
                            run_id: run_id(&run)?,
                        },
                        None => OccurrenceOutcome::SkippedOverlap,
                    },
                })
            })
            .collect()
        })
        .await
    }
    async fn fire_due(
        &self,
        agents: &[Agent],
        now_ms: u64,
        options: AutomationOptions,
    ) -> Result<FiringReport> {
        let agents = agents.to_vec();
        self.call(move |db| {
            transact(db, |tx| {
                let mut firing = SqliteFiring { tx, writes: 0 };
                let report = automation::fire(&mut firing, &agents, now_ms, options)?;
                Ok((report, firing.writes))
            })
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqlite::steering_queue::{self, tests::STATEMENTS_PER_UNSTEERED_COMMIT};
    use crate::{
        Checkpoint, DurableStore, HistoryEntry, ModelResponse, Phase, ScriptedModel, Settlement,
        SteeringStore,
    };
    use rusqlite::trace::{TraceEvent, TraceEventCodes};
    use serde_json::json;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn test_agent() -> Agent {
        Agent::builder("test", Arc::new(ScriptedModel::new(vec![])))
            .instructions("test")
            .build()
            .unwrap()
    }
    fn fired(report: &FiringReport) -> RunId {
        match report.occurrences[..] {
            [
                Occurrence {
                    outcome: OccurrenceOutcome::Admitted { run_id },
                    ..
                },
            ] => run_id,
            _ => panic!("expected one admission: {report:?}"),
        }
    }

    static STATEMENTS: AtomicU64 = AtomicU64::new(0);
    fn count(_: TraceEvent<'_>) {
        STATEMENTS.fetch_add(1, Ordering::Relaxed);
    }
    /// The statements of the three commits the steering guard measures, on this test's own
    /// connection: progress only, the model dispatch, and a completed settlement.
    async fn commit_statements(store: &SqliteStore, id: RunId) -> Vec<u64> {
        let mut state = store.claim(id).await.unwrap();
        store
            .call(|db| {
                db.connection
                    .trace_v2(TraceEventCodes::SQLITE_TRACE_STMT, Some(count));
                Ok(())
            })
            .await
            .unwrap();
        let mut counts = Vec::new();
        for step in 0..3 {
            match step {
                0 => state.usage.model_calls = 1,
                1 => state.phase = Phase::ModelInFlight,
                _ => {
                    state.history.push(HistoryEntry::Exchange {
                        response: ModelResponse::final_output(json!("done")),
                        results: vec![],
                    });
                    state.phase = Phase::Settled(Settlement::Completed(json!("done")));
                }
            }
            STATEMENTS.store(0, Ordering::Relaxed);
            store.commit(&mut state).await.unwrap();
            counts.push(STATEMENTS.load(Ordering::Relaxed));
        }
        store
            .call(|db| {
                db.connection.trace_v2(TraceEventCodes::empty(), None);
                Ok(())
            })
            .await
            .unwrap();
        assert!(state.is_terminal());
        counts
    }

    #[tokio::test]
    async fn automations_add_no_statement_to_a_run_commit() {
        let directory = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(directory.path().join("statements.sqlite"))
            .await
            .unwrap();
        let agent = test_agent();
        let agents = [agent.clone()];
        for (id, first) in [("now", 100), ("later", 1_000_000)] {
            let schedule = IntervalAutomation::new(id, json!("check"), first, 100);
            store.create_automation(&agent, id, schedule).await.unwrap();
        }
        let options = AutomationOptions::default();
        let by_automation = fired(&store.fire_due(&agents, 100, options).await.unwrap());
        let direct = store
            .accept(&agent, "direct", "k", json!("check"), None)
            .await
            .unwrap();
        for id in [direct, by_automation] {
            assert_eq!(
                commit_statements(&store, id).await,
                STATEMENTS_PER_UNSTEERED_COMMIT
            );
        }
        store.close().await.unwrap();
    }

    /// Each feature's additive tables, created by its own schema batch.
    type Create = fn(&Connection) -> Result<()>;
    fn feature_tables() -> Vec<(&'static str, Create)> {
        vec![
            ("automation", create_tables),
            ("steering", steering_queue::create),
            #[cfg(feature = "memory")]
            ("memory", |connection| {
                connection
                    .execute_batch(&format!(
                        "BEGIN IMMEDIATE; {} COMMIT;",
                        crate::sqlite::memory::MEMORY_SCHEMA
                    ))
                    .map_err(Error::storage)
            }),
        ]
    }
    fn orders<T: Copy>(items: &[T]) -> Vec<Vec<T>> {
        if items.is_empty() {
            return vec![vec![]];
        }
        let mut all = Vec::new();
        for (i, first) in items.iter().enumerate() {
            let mut rest = items.to_vec();
            rest.remove(i);
            for mut order in orders(&rest) {
                order.insert(0, *first);
                all.push(order);
            }
        }
        all
    }
    const DROP_FEATURE_TABLES: &str = "
        DROP TABLE IF EXISTS automation_occurrences; DROP TABLE IF EXISTS automation_cursors;
        DROP TABLE IF EXISTS automations; DROP TABLE IF EXISTS steering;
        DROP TABLE IF EXISTS memory_dependencies; DROP TABLE IF EXISTS memory_records;
        DROP TABLE IF EXISTS memory_sources; DROP TABLE IF EXISTS memory_meta;";

    #[tokio::test]
    async fn automation_steering_and_memory_tables_coexist_in_any_creation_order() {
        let agent = test_agent();
        let agents = [agent.clone()];
        let options = AutomationOptions::default();
        for order in orders(&feature_tables()) {
            let names: Vec<_> = order.iter().map(|(name, _)| *name).collect();
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("coexist.sqlite");
            // A version 2 database with none of the feature tables, which then gain theirs in
            // this order, as successive builds would add them.
            SqliteStore::open(&path)
                .await
                .unwrap()
                .close()
                .await
                .unwrap();
            let connection = Connection::open(&path).unwrap();
            connection.execute_batch(DROP_FEATURE_TABLES).unwrap();
            for (_, create) in &order {
                create(&connection).unwrap();
            }
            drop(connection);

            let store = SqliteStore::open(&path).await.unwrap();
            let version: i64 = store
                .call(|db| {
                    db.connection
                        .pragma_query_value(None, "user_version", |r| r.get(0))
                        .map_err(Error::storage)
                })
                .await
                .unwrap();
            assert_eq!(version, 2, "{names:?}");
            let schedule = IntervalAutomation::new("c", json!("check"), 100, 100);
            store
                .create_automation(&agent, "a", schedule)
                .await
                .unwrap();
            let id = fired(&store.fire_due(&agents, 100, options).await.unwrap());
            store.steer(id, "s", "focus".into()).await.unwrap();
            let mut state = store.claim(id).await.unwrap();
            state.phase = Phase::ModelInFlight;
            store.commit(&mut state).await.unwrap();
            assert!(
                matches!(state.history[1], HistoryEntry::Steering { .. }),
                "{names:?}"
            );
            #[cfg(feature = "memory")]
            let source = {
                use crate::memory::{SourceStore, SourceTrust};
                store
                    .upsert_source("scope", "note", "text", SourceTrust::Approved)
                    .await
                    .unwrap()
            };
            store.close().await.unwrap();

            let store = SqliteStore::open(&path).await.unwrap();
            assert_eq!(store.occurrences("a", 0, 10).await.unwrap().len(), 1);
            assert_eq!(store.automation("a").await.unwrap().next_at_ms, 200);
            assert_eq!(store.steering(id).await.unwrap().len(), 1);
            assert_eq!(store.status(id).await.unwrap().history, state.history);
            #[cfg(feature = "memory")]
            {
                use crate::memory::{SourceStore, SourceTrust};
                let again = store
                    .upsert_source("scope", "note", "text", SourceTrust::Approved)
                    .await
                    .unwrap();
                assert_eq!(again.reference, source.reference, "{names:?}");
            }
            store.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn a_failed_cursor_write_rolls_back_the_run_and_occurrence_with_it() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("atomic.sqlite");
        let agent = test_agent();
        let schedule = IntervalAutomation::new("personal", json!("check"), 100, 100);
        let store = SqliteStore::open(&path).await.unwrap();
        store
            .create_automation(&agent, "atomic", schedule)
            .await
            .unwrap();
        // Fail the cursor advance, after the run and occurrence were written.
        store
            .call(|db| {
                db.connection
                    .execute_batch(
                        "CREATE TRIGGER fail_cursor BEFORE UPDATE ON automation_cursors
                        WHEN NEW.next_at<>OLD.next_at
                        BEGIN SELECT RAISE(ABORT,'injected cursor failure'); END;",
                    )
                    .map_err(Error::storage)
            })
            .await
            .unwrap();
        let before = store.metrics().transactions;
        let options = AutomationOptions::default();
        let agents = [agent.clone()];
        assert!(matches!(
            store.fire_due(&agents, 100, options).await,
            Err(Error::Storage(_))
        ));
        assert_eq!(store.metrics().transactions, before);
        assert!(store.due(100, 10).await.unwrap().is_empty());
        assert!(store.history("personal").await.unwrap().is_empty());
        assert!(store.occurrences("atomic", 0, 10).await.unwrap().is_empty());
        assert_eq!(store.automation("atomic").await.unwrap().next_at_ms, 100);
        store.close().await.unwrap();

        let store = SqliteStore::open(&path).await.unwrap();
        store
            .call(|db| {
                db.connection
                    .execute_batch("DROP TRIGGER fail_cursor")
                    .map_err(Error::storage)
            })
            .await
            .unwrap();
        assert_eq!(
            store
                .fire_due(&agents, 100, options)
                .await
                .unwrap()
                .occurrences
                .len(),
            1
        );
        assert!(
            store
                .fire_due(&agents, 100, options)
                .await
                .unwrap()
                .occurrences
                .is_empty()
        );
        store.close().await.unwrap();
    }
}
