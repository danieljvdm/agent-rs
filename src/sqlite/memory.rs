//! [`SourceStore`] on the store's own worker, database file and ownership lock.
//!
//! The memory tables are created on first use, in one idempotent transaction, and carry their
//! own schema version in `memory_meta`, independent of the run schema's `user_version`. They
//! never touch `runs`, `checkpoints`, `history` or `operations`.
use super::{Database, SqliteStore, encode};
use crate::memory::{
    DERIVED_CONTEXT_VERSION, MemoryRecord, MemorySource, SourceRef, SourceStore, SourceTrust,
    validate_key, validate_limit, validate_source,
};
use crate::{Error, Result, RunId};
use async_trait::async_trait;
use rusqlite::{OptionalExtension, Transaction, params};

const MEMORY_SCHEMA_VERSION: i64 = 1;
pub(super) const MEMORY_SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS memory_meta (
        id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL);
    INSERT INTO memory_meta(id, version) VALUES(1, 1) ON CONFLICT(id) DO NOTHING;
    CREATE TABLE IF NOT EXISTS memory_sources (
        scope TEXT NOT NULL, id TEXT NOT NULL, revision TEXT NOT NULL,
        text TEXT NOT NULL, trust INTEGER NOT NULL, PRIMARY KEY(scope, id));
    CREATE TABLE IF NOT EXISTS memory_records (
        scope TEXT NOT NULL, id TEXT NOT NULL, version INTEGER NOT NULL,
        record TEXT NOT NULL, PRIMARY KEY(scope, id));
    CREATE TABLE IF NOT EXISTS memory_dependencies (
        scope TEXT NOT NULL, record_id TEXT NOT NULL, source_id TEXT NOT NULL,
        revision TEXT NOT NULL, PRIMARY KEY(scope, record_id, source_id),
        FOREIGN KEY(scope, record_id) REFERENCES memory_records(scope, id) ON DELETE CASCADE);
    CREATE INDEX IF NOT EXISTS memory_source_dependents
        ON memory_dependencies(scope, source_id);";

/// Creates the memory tables if they are missing, then checks their schema version.
fn ready(db: &mut Database) -> Result<()> {
    let exists: bool = db
        .connection
        .prepare_cached(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='memory_meta')",
        )
        .and_then(|mut statement| statement.query_row([], |row| row.get(0)))
        .map_err(Error::storage)?;
    if !exists {
        let tx = db.connection.transaction().map_err(Error::storage)?;
        tx.execute_batch(MEMORY_SCHEMA).map_err(Error::storage)?;
        tx.commit().map_err(Error::storage)?;
        db.count(1);
    }
    let version: i64 = db
        .connection
        .prepare_cached("SELECT version FROM memory_meta WHERE id=1")
        .and_then(|mut statement| statement.query_row([], |row| row.get(0)))
        .map_err(Error::storage)?;
    if version != MEMORY_SCHEMA_VERSION {
        return Err(Error::incompatible(format!(
            "memory schema version {version}"
        )));
    }
    Ok(())
}

/// Deletes every record citing source `id`; their dependency rows cascade.
fn invalidate(tx: &Transaction<'_>, scope: &str, id: &str) -> Result<u64> {
    tx.prepare_cached(
        "DELETE FROM memory_records WHERE scope=?1 AND id IN (
            SELECT record_id FROM memory_dependencies WHERE scope=?1 AND source_id=?2)",
    )
    .and_then(|mut statement| statement.execute(params![scope, id]))
    .map(|deleted| deleted as u64)
    .map_err(Error::storage)
}

/// Each method is one worker job, and each write is one transaction.
#[async_trait]
impl SourceStore for SqliteStore {
    async fn upsert_source(
        &self,
        scope: &str,
        id: &str,
        text: &str,
        trust: SourceTrust,
    ) -> Result<MemorySource> {
        validate_source(scope, id, text)?;
        let (scope, id, text) = (scope.to_owned(), id.to_owned(), text.to_owned());
        self.call(move |db| {
            ready(db)?;
            let tx = db.connection.transaction().map_err(Error::storage)?;
            let approved = trust == SourceTrust::Approved;
            let existing: Option<(String, String, bool)> = tx
                .prepare_cached(
                    "SELECT revision, text, trust FROM memory_sources WHERE scope=?1 AND id=?2",
                )
                .and_then(|mut statement| {
                    statement
                        .query_row(params![scope, id], |row| {
                            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                        })
                        .optional()
                })
                .map_err(Error::storage)?;
            if let Some((revision, stored, stored_approved)) = existing
                && stored == text
                && stored_approved == approved
            {
                return Ok(MemorySource {
                    reference: SourceRef {
                        scope,
                        id,
                        revision,
                    },
                    text,
                    trust,
                });
            }
            let writes = invalidate(&tx, &scope, &id)?;
            let source = MemorySource {
                reference: SourceRef {
                    scope,
                    id,
                    revision: RunId::new_v4().to_string(),
                },
                text,
                trust,
            };
            tx.prepare_cached(
                "INSERT INTO memory_sources(scope, id, revision, text, trust) VALUES(?1, ?2, ?3, ?4, ?5)
                ON CONFLICT(scope, id) DO UPDATE SET
                    revision=excluded.revision, text=excluded.text, trust=excluded.trust",
            )
            .and_then(|mut statement| {
                statement.execute(params![
                    source.reference.scope,
                    source.reference.id,
                    source.reference.revision,
                    source.text,
                    approved
                ])
            })
            .map_err(Error::storage)?;
            tx.commit().map_err(Error::storage)?;
            db.count(writes + 1);
            Ok(source)
        })
        .await
    }

    async fn remove_source(&self, scope: &str, id: &str) -> Result<()> {
        validate_key(scope, id)?;
        let (scope, id) = (scope.to_owned(), id.to_owned());
        self.call(move |db| {
            ready(db)?;
            let tx = db.connection.transaction().map_err(Error::storage)?;
            let mut writes = invalidate(&tx, &scope, &id)?;
            writes += tx
                .prepare_cached("DELETE FROM memory_sources WHERE scope=?1 AND id=?2")
                .and_then(|mut statement| statement.execute(params![scope, id]))
                .map_err(Error::storage)? as u64;
            if writes > 0 {
                tx.commit().map_err(Error::storage)?;
                db.count(writes);
            }
            Ok(())
        })
        .await
    }

    async fn publish(&self, record: MemoryRecord) -> Result<()> {
        record.validate()?;
        let encoded = encode(&record)?;
        self.call(move |db| {
            ready(db)?;
            let tx = db.connection.transaction().map_err(Error::storage)?;
            let scope = record.scope();
            for source in &record.context.sources {
                let current: bool = tx
                    .prepare_cached(
                        "SELECT EXISTS(SELECT 1 FROM memory_sources
                        WHERE scope=?1 AND id=?2 AND revision=?3 AND trust=1)",
                    )
                    .and_then(|mut statement| {
                        statement
                            .query_row(params![scope, source.id, source.revision], |row| row.get(0))
                    })
                    .map_err(Error::storage)?;
                if !current {
                    return Err(Error::Stale);
                }
            }
            // Replacing the record replaces its dependency set in the same transaction.
            let run = |sql: &str, values: &[&dyn rusqlite::ToSql]| {
                tx.prepare_cached(sql)
                    .and_then(|mut statement| statement.execute(values))
                    .map_err(Error::storage)
            };
            run(
                "DELETE FROM memory_records WHERE scope=?1 AND id=?2",
                params![scope, record.id],
            )?;
            run(
                "INSERT INTO memory_records(scope, id, version, record) VALUES(?1, ?2, ?3, ?4)",
                params![scope, record.id, DERIVED_CONTEXT_VERSION, encoded],
            )?;
            for source in &record.context.sources {
                run(
                    "INSERT INTO memory_dependencies(scope, record_id, source_id, revision)
                    VALUES(?1, ?2, ?3, ?4)",
                    params![scope, record.id, source.id, source.revision],
                )?;
            }
            tx.commit().map_err(Error::storage)?;
            db.count(1 + record.context.sources.len() as u64);
            Ok(())
        })
        .await
    }

    async fn load(&self, scope: &str, limit: usize) -> Result<Vec<MemoryRecord>> {
        validate_limit(limit)?;
        let scope = scope.to_owned();
        self.call(move |db| {
            ready(db)?;
            // One statement reads one snapshot: the stored format, and every cited source at its
            // cited revision and still approved.
            let mut statement = db
                .connection
                .prepare_cached(
                    "SELECT r.record FROM memory_records r
                    WHERE r.scope=?1 AND r.version=?2
                    AND EXISTS(SELECT 1 FROM memory_dependencies d
                        WHERE d.scope=r.scope AND d.record_id=r.id)
                    AND NOT EXISTS(SELECT 1 FROM memory_dependencies d
                        LEFT JOIN memory_sources s ON s.scope=d.scope AND s.id=d.source_id
                        WHERE d.scope=r.scope AND d.record_id=r.id
                        AND (s.revision IS NULL OR s.revision<>d.revision OR s.trust<>1))
                    ORDER BY r.id LIMIT ?3",
                )
                .map_err(Error::storage)?;
            let rows = statement
                .query_map(
                    params![scope, DERIVED_CONTEXT_VERSION, limit as i64],
                    |row| row.get::<_, String>(0),
                )
                .map_err(Error::storage)?;
            let mut records = Vec::new();
            for row in rows {
                let record: MemoryRecord =
                    serde_json::from_str(&row.map_err(Error::storage)?).map_err(Error::storage)?;
                if record.scope() == scope && record.validate().is_ok() {
                    records.push(record);
                }
            }
            Ok(records)
        })
        .await
    }
}
