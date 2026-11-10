use super::{
    MemoryRecord, MemorySource, SourceRef, SourceTrust, validate_key, validate_limit,
    validate_source,
};
use crate::{Error, Result, RunId};
use async_trait::async_trait;
use std::{
    collections::BTreeMap,
    sync::{Mutex, MutexGuard},
};

/// Source snapshots and the records derived from them, kept apart from authoritative
/// conversation history.
///
/// # Contract
///
/// - [`upsert_source`](Self::upsert_source) with the same text and trust returns the existing
///   snapshot unchanged. Any other change assigns a revision never used before and, in the same
///   atomic step, removes every record that cites the source.
/// - [`remove_source`](Self::remove_source) atomically removes the source and every record that
///   cites it. A source recreated later gets a fresh revision, so a delayed writer cannot
///   resurrect deleted material.
/// - [`publish`](Self::publish) stores a record only if every source it cites is still at the
///   cited revision and approved, checked atomically with the write; otherwise it fails with
///   [`Error::Stale`]. Republishing an ID replaces the record.
/// - [`load`](Self::load) returns at most `limit` (1..=4096) of a scope's records, in record-ID
///   order, each of whose sources is still current and approved.
///
/// Invalid arguments fail with [`Error::Input`], and storage failures with [`Error::Storage`].
/// [`InMemorySourceStore`] and, with the `sqlite` feature, `SqliteStore` implement it.
#[async_trait]
pub trait SourceStore: Send + Sync {
    /// Creates or updates source `id` in `scope` and returns its current snapshot.
    async fn upsert_source(
        &self,
        scope: &str,
        id: &str,
        text: &str,
        trust: SourceTrust,
    ) -> Result<MemorySource>;
    /// Deletes source `id` in `scope` and every record derived from it.
    async fn remove_source(&self, scope: &str, id: &str) -> Result<()>;
    /// Stores `record` if its sources are current.
    async fn publish(&self, record: MemoryRecord) -> Result<()>;
    /// The scope's current records, in record-ID order.
    async fn load(&self, scope: &str, limit: usize) -> Result<Vec<MemoryRecord>>;
}

/// Sources and records by scope, then ID. A published record's sources all share its scope.
#[derive(Default)]
struct State {
    sources: BTreeMap<String, BTreeMap<String, MemorySource>>,
    records: BTreeMap<String, BTreeMap<String, MemoryRecord>>,
}

impl State {
    fn invalidate(&mut self, scope: &str, id: &str) {
        if let Some(records) = self.records.get_mut(scope) {
            records.retain(|_, record| record.context.sources.iter().all(|source| source.id != id));
        }
    }

    fn current(&self, record: &MemoryRecord) -> bool {
        record.context.sources.iter().all(|reference| {
            self.sources
                .get(&reference.scope)
                .and_then(|sources| sources.get(&reference.id))
                .is_some_and(|source| {
                    source.reference == *reference && source.trust == SourceTrust::Approved
                })
        })
    }
}

/// A process-local [`SourceStore`] for small corpora, lost when the process exits. Use
/// `SqliteStore` with the `sqlite` feature to keep memory across restarts.
#[derive(Default)]
pub struct InMemorySourceStore {
    state: Mutex<State>,
}

impl InMemorySourceStore {
    fn state(&self) -> Result<MutexGuard<'_, State>> {
        self.state
            .lock()
            .map_err(|_| Error::storage("memory store lock poisoned"))
    }
}

#[async_trait]
impl SourceStore for InMemorySourceStore {
    async fn upsert_source(
        &self,
        scope: &str,
        id: &str,
        text: &str,
        trust: SourceTrust,
    ) -> Result<MemorySource> {
        validate_source(scope, id, text)?;
        let mut state = self.state()?;
        if let Some(source) = state.sources.get(scope).and_then(|sources| sources.get(id))
            && source.text == text
            && source.trust == trust
        {
            return Ok(source.clone());
        }
        let source = MemorySource {
            reference: SourceRef {
                scope: scope.into(),
                id: id.into(),
                revision: RunId::new_v4().to_string(),
            },
            text: text.into(),
            trust,
        };
        state.invalidate(scope, id);
        state
            .sources
            .entry(scope.into())
            .or_default()
            .insert(id.into(), source.clone());
        Ok(source)
    }

    async fn remove_source(&self, scope: &str, id: &str) -> Result<()> {
        validate_key(scope, id)?;
        let mut state = self.state()?;
        state.invalidate(scope, id);
        if let Some(sources) = state.sources.get_mut(scope) {
            sources.remove(id);
        }
        Ok(())
    }

    async fn publish(&self, record: MemoryRecord) -> Result<()> {
        record.validate()?;
        let mut state = self.state()?;
        if !state.current(&record) {
            return Err(Error::Stale);
        }
        state
            .records
            .entry(record.scope().into())
            .or_default()
            .insert(record.id.clone(), record);
        Ok(())
    }

    async fn load(&self, scope: &str, limit: usize) -> Result<Vec<MemoryRecord>> {
        validate_limit(limit)?;
        let state = self.state()?;
        Ok(state
            .records
            .get(scope)
            .into_iter()
            .flat_map(BTreeMap::values)
            .filter(|record| state.current(record))
            .take(limit)
            .cloned()
            .collect())
    }
}
