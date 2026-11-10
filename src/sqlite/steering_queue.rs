//! SQLite storage for [`SteeringStore`]: the `steering` table, created additively within schema
//! version 2, and the queue reads and writes the delta write makes at a model dispatch.
use super::*;
use crate::steering;
use crate::{SteeringId, SteeringInput, SteeringReceipt, SteeringStore};

/// Created idempotently at every open, so a version 2 database from before steering gains it.
const SCHEMA: &str = "BEGIN IMMEDIATE;
    CREATE TABLE IF NOT EXISTS steering (
        run_id TEXT NOT NULL REFERENCES runs(id), sequence INTEGER NOT NULL,
        id TEXT NOT NULL UNIQUE, idempotency_key TEXT NOT NULL, instruction TEXT NOT NULL,
        bytes INTEGER NOT NULL, applied_revision INTEGER,
        PRIMARY KEY(run_id, sequence), UNIQUE(run_id, idempotency_key));
    CREATE INDEX IF NOT EXISTS steering_queued ON steering(run_id, sequence)
        WHERE applied_revision IS NULL;
    COMMIT;";
pub(super) fn create(connection: &Connection) -> Result<()> {
    connection.execute_batch(SCHEMA).map_err(Error::storage)
}

/// The run's queued instructions in acceptance order. Read only by a dispatch commit that the
/// delta write's first statement found steering queued for.
pub(super) fn queued(tx: &Transaction<'_>, id: &str) -> Result<Vec<(SteeringId, String)>> {
    let mut stmt = tx
        .prepare_cached(
            "SELECT id,instruction FROM steering WHERE run_id=?1 AND applied_revision IS NULL
            ORDER BY sequence",
        )
        .map_err(Error::storage)?;
    let rows = stmt
        .query_map([id], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        })
        .map_err(Error::storage)?;
    rows.map(|row| {
        let (id, instruction) = row.map_err(Error::storage)?;
        Ok((
            SteeringId::parse_str(&id).map_err(Error::storage)?,
            instruction,
        ))
    })
    .collect()
}
/// Appends `queued` to the run's history after the delta's own entries and marks it applied at
/// the delta's revision. Returns the rows written and the entries, for the caller's state.
pub(super) fn apply(
    tx: &Transaction<'_>,
    delta: &Delta,
    queued: Vec<(SteeringId, String)>,
) -> Result<(u64, Vec<HistoryEntry>)> {
    if queued.is_empty() {
        return Ok((0, Vec::new()));
    }
    let applied: Vec<_> = queued
        .into_iter()
        .map(|(id, instruction)| HistoryEntry::Steering { id, instruction })
        .collect();
    let encoded = applied.iter().map(encode).collect::<Result<Vec<_>>>()?;
    let mut writes = insert_history(
        tx,
        &delta.id,
        &delta.conversation,
        delta.history_len,
        &encoded,
    )?;
    writes += tx
        .prepare_cached(
            "UPDATE steering SET applied_revision=?2 WHERE run_id=?1 AND applied_revision IS NULL",
        )
        .map_err(Error::storage)?
        .execute(params![delta.id, delta.revision])
        .map_err(Error::storage)? as u64;
    Ok((writes, applied))
}

#[async_trait]
impl SteeringStore for SqliteStore {
    async fn steer(&self, id: RunId, key: &str, instruction: String) -> Result<SteeringReceipt> {
        steering::check_request(key, &instruction)?;
        let key = key.to_owned();
        self.call(move |db| {
            let tx = db.connection.transaction().map_err(Error::storage)?;
            let run = id.to_string();
            let existing: Option<(String, u32, String)> = tx
                .prepare_cached(
                    "SELECT id,sequence,instruction FROM steering
                    WHERE run_id=?1 AND idempotency_key=?2",
                )
                .map_err(Error::storage)?
                .query_row(params![run, key], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
                .optional()
                .map_err(Error::storage)?;
            if let Some((steering_id, sequence, accepted)) = existing {
                if accepted != instruction {
                    return Err(Error::IdempotencyConflict);
                }
                return Ok(SteeringReceipt {
                    id: SteeringId::parse_str(&steering_id).map_err(Error::storage)?,
                    run_id: id,
                    sequence,
                });
            }
            let state = load(&tx, id)?;
            let (count, bytes): (i64, i64) = tx
                .prepare_cached(
                    "SELECT COUNT(*),COALESCE(SUM(bytes),0) FROM steering WHERE run_id=?1",
                )
                .map_err(Error::storage)?
                .query_row([&run], |r| Ok((r.get(0)?, r.get(1)?)))
                .map_err(Error::storage)?;
            let count = usize::try_from(count).map_err(Error::storage)?;
            let bytes = usize::try_from(bytes).map_err(Error::storage)?;
            let accepted = steering::admit(&state, instruction, count, bytes)?;
            let receipt = SteeringReceipt {
                id: accepted.id,
                run_id: id,
                sequence: count as u32,
            };
            tx.prepare_cached(
                "INSERT INTO steering(run_id,sequence,id,idempotency_key,instruction,bytes)
                VALUES(?1,?2,?3,?4,?5,?6)",
            )
            .map_err(Error::storage)?
            .execute(params![
                run,
                receipt.sequence,
                accepted.id.to_string(),
                key,
                accepted.instruction,
                accepted.bytes as i64,
            ])
            .map_err(Error::storage)?;
            tx.commit().map_err(Error::storage)?;
            db.count(1);
            Ok(receipt)
        })
        .await
    }
    async fn steering(&self, id: RunId) -> Result<Vec<SteeringInput>> {
        self.call(move |db| {
            let run = id.to_string();
            let settled: bool = db
                .connection
                .prepare_cached("SELECT settled FROM checkpoints WHERE run_id=?1")
                .map_err(Error::storage)?
                .query_row([&run], |r| r.get(0))
                .optional()
                .map_err(Error::storage)?
                .ok_or(Error::NotFound)?;
            let mut stmt = db
                .connection
                .prepare_cached(
                    "SELECT id,sequence,instruction,applied_revision FROM steering
                    WHERE run_id=?1 ORDER BY sequence",
                )
                .map_err(Error::storage)?;
            let rows = stmt
                .query_map([&run], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, u32>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, Option<u64>>(3)?,
                    ))
                })
                .map_err(Error::storage)?;
            rows.map(|row| {
                let (steering_id, sequence, instruction, applied) = row.map_err(Error::storage)?;
                Ok(SteeringInput {
                    receipt: SteeringReceipt {
                        id: SteeringId::parse_str(&steering_id).map_err(Error::storage)?,
                        run_id: id,
                        sequence,
                    },
                    instruction,
                    status: steering::status(applied, settled),
                })
            })
            .collect()
        })
        .await
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::{ScriptedModel, Settlement};
    use rusqlite::trace::{TraceEvent, TraceEventCodes};
    use serde_json::json;
    use std::sync::atomic::{AtomicU64, Ordering};

    async fn trigger(db: &SqliteStore, sql: &'static str) {
        db.call(move |db| db.connection.execute_batch(sql).map_err(Error::storage))
            .await
            .unwrap();
    }
    #[tokio::test]
    async fn steering_acceptance_and_application_roll_back_with_their_transactions() {
        let directory = tempfile::tempdir().unwrap();
        let db = SqliteStore::open(directory.path().join("steering.sqlite"))
            .await
            .unwrap();
        let agent = Agent::builder("test", Arc::new(ScriptedModel::new(vec![])))
            .build()
            .unwrap();
        let id = db.accept(&agent, "c", "run", json!(1), None).await.unwrap();
        trigger(&db, "CREATE TRIGGER fail_queue BEFORE INSERT ON steering BEGIN SELECT RAISE(FAIL, 'injected acceptance failure'); END;").await;
        assert!(matches!(
            db.steer(id, "a", "new instruction".into()).await,
            Err(Error::Storage(_))
        ));
        assert!(db.steering(id).await.unwrap().is_empty());
        trigger(&db, "DROP TRIGGER fail_queue;").await;
        let receipt = db.steer(id, "a", "new instruction".into()).await.unwrap();
        assert_eq!(receipt.sequence, 0);

        let mut state = db.claim(id).await.unwrap();
        let position = CheckpointPosition::from(&state);
        state.phase = Phase::ModelInFlight;
        // Fail the queue update, after the checkpoint row and the history rows were written.
        trigger(&db, "CREATE TRIGGER fail_dispatch BEFORE UPDATE ON steering BEGIN SELECT RAISE(FAIL, 'injected dispatch failure'); END;").await;
        assert!(matches!(
            db.commit(&mut state).await,
            Err(Error::Storage(_))
        ));
        assert_eq!(
            state.history.len(),
            1,
            "a failed commit leaves the caller's state"
        );
        assert_eq!(CheckpointPosition::from(&state), position);
        let stored = db.status(id).await.unwrap();
        assert_eq!(stored.phase, Phase::Ready);
        assert_eq!(stored.history.len(), 1);
        assert_eq!(db.history("c").await.unwrap().len(), 1);
        assert_eq!(
            db.steering(id).await.unwrap()[0].status,
            crate::SteeringStatus::Queued
        );
        trigger(&db, "DROP TRIGGER fail_dispatch;").await;
        db.commit(&mut state).await.unwrap();
        assert_eq!(
            db.steering(id).await.unwrap()[0].status,
            crate::SteeringStatus::Applied {
                revision: state.revision
            }
        );
        assert_eq!(state.history.len(), 2);
        assert_eq!(db.status(id).await.unwrap().history, state.history);
        assert_eq!(db.history("c").await.unwrap(), state.history);

        // A reopened completion whose resend fails restores the caller's settlement.
        db.steer(id, "b", "another".into()).await.unwrap();
        state.phase = Phase::Ready;
        db.commit(&mut state).await.unwrap();
        state.history.push(HistoryEntry::Exchange {
            response: ModelResponse::final_output(json!("done")),
            results: vec![],
        });
        let completed = Phase::Settled(Settlement::Completed(json!("done")));
        state.phase = completed.clone();
        trigger(&db, "CREATE TRIGGER fail_reopen BEFORE INSERT ON history BEGIN SELECT RAISE(FAIL, 'injected reopen failure'); END;").await;
        assert!(matches!(
            db.commit(&mut state).await,
            Err(Error::Storage(_))
        ));
        assert_eq!(state.phase, completed);
        trigger(&db, "DROP TRIGGER fail_reopen;").await;
        let before = db.metrics().transactions;
        db.commit(&mut state).await.unwrap();
        assert_eq!(state.phase, Phase::Ready);
        assert_eq!(
            db.metrics().transactions,
            before + 1,
            "the rolled-back attempt is not counted"
        );
        assert_eq!(db.status(id).await.unwrap().history, state.history);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn applied_steering_takes_the_next_ordinals_whatever_the_history_hint() {
        let directory = tempfile::tempdir().unwrap();
        let db = SqliteStore::open(directory.path().join("hint.sqlite"))
            .await
            .unwrap();
        let agent = Agent::builder("test", Arc::new(ScriptedModel::new(vec![])))
            .build()
            .unwrap();
        let id = db.accept(&agent, "c", "k", json!(1), None).await.unwrap();
        let mut state = db.claim(id).await.unwrap();
        let exchange = |n: u64| HistoryEntry::Exchange {
            response: ModelResponse::final_output(json!(n)),
            results: vec![],
        };
        // Ahead of durable history: the dispatch commit is resent from the stored length, and
        // the steering lands after the delta's own entries.
        db.steer(id, "a", "first".into()).await.unwrap();
        db.hint(id, Some(3));
        state.history.extend([exchange(1), exchange(2)]);
        state.phase = Phase::ModelInFlight;
        db.commit(&mut state).await.unwrap();
        assert_eq!(state.history.len(), 4);
        assert!(matches!(state.history[3], HistoryEntry::Steering { .. }));
        // The hint now includes the applied entry, so the next delta starts after it.
        assert_eq!(db.hinted(&state), 4);
        state.phase = Phase::Ready;
        state.history.push(exchange(3));
        db.commit(&mut state).await.unwrap();
        // No hint: the whole history is resent and only the new entries are appended.
        db.steer(id, "b", "second".into()).await.unwrap();
        db.hint(id, None);
        state.phase = Phase::ModelInFlight;
        db.commit(&mut state).await.unwrap();
        assert_eq!(state.history.len(), 6);
        assert_eq!(db.status(id).await.unwrap().history, state.history);
        assert_eq!(db.history("c").await.unwrap(), state.history);
        let ordinals: Vec<i64> = db
            .call(move |db| {
                let mut stmt = db
                    .connection
                    .prepare("SELECT ordinal FROM history WHERE run_id=?1 ORDER BY sequence")
                    .map_err(Error::storage)?;
                let rows = stmt
                    .query_map([id.to_string()], |r| r.get(0))
                    .map_err(Error::storage)?;
                rows.map(|row| row.map_err(Error::storage)).collect()
            })
            .await
            .unwrap();
        assert_eq!(ordinals, [0, 1, 2, 3, 4, 5]);
        db.close().await.unwrap();
    }

    /// SQL statements that each commit of a run without steering executes (exact): a commit that
    /// only changes progress, the model dispatch commit, and a completed settlement that appends
    /// its exchange, BEGIN and COMMIT included. Measured on main before steering: the queue check
    /// rides on the delta write's existing first statement, so steering unused adds none.
    pub(in crate::sqlite) const STATEMENTS_PER_UNSTEERED_COMMIT: [u64; 3] = [4, 4, 5];
    static STATEMENTS: AtomicU64 = AtomicU64::new(0);
    fn count(_: TraceEvent<'_>) {
        STATEMENTS.fetch_add(1, Ordering::Relaxed);
    }
    #[tokio::test]
    async fn unsteered_commits_execute_the_statements_main_does() {
        let directory = tempfile::tempdir().unwrap();
        let db = SqliteStore::open(directory.path().join("statements.sqlite"))
            .await
            .unwrap();
        let agent = Agent::builder("test", Arc::new(ScriptedModel::new(vec![])))
            .build()
            .unwrap();
        let id = db.accept(&agent, "c", "k", json!(1), None).await.unwrap();
        let mut state = db.claim(id).await.unwrap();
        // Only this test traces, and only its own connection.
        db.call(|db| {
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
            db.commit(&mut state).await.unwrap();
            counts.push(STATEMENTS.load(Ordering::Relaxed));
        }
        db.call(|db| {
            db.connection.trace_v2(TraceEventCodes::empty(), None);
            Ok(())
        })
        .await
        .unwrap();
        assert!(state.is_terminal());
        eprintln!("statements per unsteered commit: {counts:?}");
        assert_eq!(counts, STATEMENTS_PER_UNSTEERED_COMMIT);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_version_2_database_from_before_steering_opens_and_gains_the_table() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("v2.sqlite");
        let db = SqliteStore::open(&path).await.unwrap();
        let agent = Agent::builder("test", Arc::new(ScriptedModel::new(vec![])))
            .build()
            .unwrap();
        let id = db.accept(&agent, "c", "k", json!(1), None).await.unwrap();
        db.close().await.unwrap();
        // The schema exactly as a version 2 database had it before this feature.
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch("DROP INDEX steering_queued; DROP TABLE steering;")
            .unwrap();
        drop(connection);
        let db = SqliteStore::open(&path).await.unwrap();
        let version: i64 = db
            .call(|db| {
                db.connection
                    .pragma_query_value(None, "user_version", |r| r.get(0))
                    .map_err(Error::storage)
            })
            .await
            .unwrap();
        assert_eq!(version, 2);
        let receipt = db.steer(id, "a", "first".into()).await.unwrap();
        assert_eq!(db.steering(id).await.unwrap()[0].receipt, receipt);
        let mut state = db.claim(id).await.unwrap();
        state.phase = Phase::ModelInFlight;
        db.commit(&mut state).await.unwrap();
        assert!(matches!(state.history[1], HistoryEntry::Steering { .. }));
        db.close().await.unwrap();
    }
}
