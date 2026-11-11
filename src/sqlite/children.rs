//! Children of delegating runs, stored so that a commit writes only what it changed.
//!
//! A child's acceptance is a `runs` row written once, at admission, under its own conversation
//! `child:{root}:{call}`, and its history and operations are `history` and `operations` rows
//! like any run's. Its progress is one `children` row keyed to its root. A child commits only as
//! part of its root's checkpoint, so all of this is written in the root's fenced transaction,
//! and only for the children whose revision advanced. A child has no `checkpoints` row: it is
//! never discovered, claimed or read on its own, and its root's `wake_at` and `recover` columns
//! account for it.
use super::{
    Progress, ProgressRef, decode, encode, insert_history, load_history, sql_time,
    upsert_operations,
};
use crate::types::STATE_VERSION;
use crate::{AttachedChild, Error, ParentRun, Result, RunId, RunState};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use std::collections::HashMap;

/// The revision and history length stored for each child.
pub(super) type Hints = HashMap<RunId, (u64, usize)>;

/// The children of a state just loaded, as stored.
pub(super) fn stored(state: &RunState) -> Hints {
    (state.children.iter())
        .map(|child| {
            (
                child.state.id,
                (child.state.revision, child.state.history.len()),
            )
        })
        .collect()
}

/// A child's acceptance, written with its admission.
pub(super) struct Acceptance {
    conversation: String,
    agent_id: String,
    binding: String,
    input: String,
    limits: String,
    expires_at: Option<i64>,
}
/// One child a commit changed.
pub(super) struct ChildDelta {
    id: String,
    ordinal: usize,
    call_id: String,
    conversation: String,
    revision: u64,
    progress: String,
    history_len: usize,
    /// Encoded history entries from ordinal `history_from` to `history_len`.
    history_from: usize,
    history: Vec<String>,
    operations: Vec<(String, String)>,
    /// Present when the child may not be stored yet.
    acceptance: Option<Acceptance>,
}

/// Encodes every child whose revision differs from what `known` says is stored. A child `known`
/// does not know is encoded whole, acceptance included.
pub(super) fn changed(
    state: &RunState,
    known: impl Fn(RunId) -> Option<(u64, usize)>,
) -> Result<Vec<ChildDelta>> {
    let mut deltas = Vec::new();
    for (ordinal, child) in state.children.iter().enumerate() {
        let (call_id, child) = (&child.call_id, &child.state);
        let known = known(child.id);
        if known.is_some_and(|(revision, _)| revision == child.revision) {
            continue;
        }
        let history_from = known
            .map(|(_, len)| len)
            .filter(|len| *len <= child.history.len())
            .unwrap_or(0);
        let acceptance = match known {
            Some(_) => None,
            None => Some(Acceptance {
                conversation: child.conversation_id.clone(),
                agent_id: child.agent_id.clone(),
                binding: encode(&child.binding)?,
                input: encode(&child.input)?,
                limits: encode(&child.limits)?,
                expires_at: sql_time(child.expires_at_ms),
            }),
        };
        deltas.push(ChildDelta {
            id: child.id.to_string(),
            ordinal,
            call_id: call_id.clone(),
            conversation: child.conversation_id.clone(),
            revision: child.revision,
            progress: encode(&ProgressRef::of(child))?,
            history_len: child.history.len(),
            history_from,
            history: (child.history[history_from..].iter())
                .map(encode)
                .collect::<Result<_>>()?,
            operations: (child.operations.iter())
                .map(|op| Ok((op.call.id.clone(), encode(op)?)))
                .collect::<Result<_>>()?,
            acceptance,
        });
    }
    Ok(deltas)
}

/// Writes the changed children of root `root` within its transaction. `None` means a child is
/// stored differently than it was encoded for; the caller re-encodes it whole.
pub(super) fn write(
    tx: &Transaction<'_>,
    root: &str,
    deltas: &[ChildDelta],
) -> Result<Option<u64>> {
    let mut writes = 0;
    for child in deltas {
        let stored: Option<(i64, i64)> = tx
            .prepare_cached("SELECT revision,history_len FROM children WHERE child_id=?1")
            .map_err(Error::storage)?
            .query_row([&child.id], |r| Ok((r.get(0)?, r.get(1)?)))
            .optional()
            .map_err(Error::storage)?;
        let durable = match stored {
            None => {
                let Some(acceptance) = &child.acceptance else {
                    return Ok(None);
                };
                if child.history_from != 0 {
                    return Ok(None);
                }
                tx.prepare_cached(
                    "INSERT INTO runs(id,conversation,idempotency_key,version,agent_id,binding,
                        input,limits,expires_at,prior_context) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,'[]')",
                )
                .map_err(Error::storage)?
                .execute(params![
                    child.id,
                    acceptance.conversation,
                    child.call_id,
                    STATE_VERSION,
                    acceptance.agent_id,
                    acceptance.binding,
                    acceptance.input,
                    acceptance.limits,
                    acceptance.expires_at,
                ])
                .map_err(Error::storage)?;
                tx.prepare_cached(
                    "INSERT INTO children(child_id,root_id,ordinal,call_id,revision,history_len,
                        progress) VALUES(?1,?2,?3,?4,?5,?6,?7)",
                )
                .map_err(Error::storage)?
                .execute(params![
                    child.id,
                    root,
                    child.ordinal as i64,
                    child.call_id,
                    child.revision,
                    child.history_len as i64,
                    child.progress,
                ])
                .map_err(Error::storage)?;
                writes += 2;
                0
            }
            Some((revision, _)) if revision as u64 == child.revision => continue,
            Some((_, durable)) => {
                let durable = usize::try_from(durable).map_err(Error::storage)?;
                if durable < child.history_from {
                    return Ok(None);
                }
                if child.history_len < durable {
                    return Err(Error::input("history is append-only"));
                }
                tx.prepare_cached(
                    "UPDATE children SET revision=?2,history_len=?3,progress=?4 WHERE child_id=?1",
                )
                .map_err(Error::storage)?
                .execute(params![
                    child.id,
                    child.revision,
                    child.history_len as i64,
                    child.progress,
                ])
                .map_err(Error::storage)?;
                writes += 1;
                durable
            }
        };
        let new = &child.history[durable - child.history_from..];
        writes += insert_history(tx, &child.id, &child.conversation, durable, new)?
            + upsert_operations(tx, &child.id, &child.operations)?;
    }
    Ok(Some(writes))
}

/// The `count` children of `root`, in admission order.
pub(super) fn load(
    connection: &Connection,
    root: &RunState,
    count: usize,
) -> Result<Vec<AttachedChild>> {
    let rows = {
        let mut stmt = connection
            .prepare_cached(
                "SELECT c.child_id,c.call_id,c.revision,c.history_len,c.progress,r.version,
                    r.conversation,r.agent_id,r.binding,r.input,r.limits,r.expires_at
                FROM children c JOIN runs r ON r.id=c.child_id WHERE c.root_id=?1
                ORDER BY c.ordinal",
            )
            .map_err(Error::storage)?;
        let rows = stmt
            .query_map([root.id.to_string()], |r| {
                Ok((
                    (
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, i64>(2)?,
                        r.get::<_, i64>(3)?,
                        r.get::<_, String>(4)?,
                        r.get::<_, i64>(5)?,
                    ),
                    (
                        r.get::<_, String>(6)?,
                        r.get::<_, String>(7)?,
                        r.get::<_, String>(8)?,
                        r.get::<_, String>(9)?,
                        r.get::<_, String>(10)?,
                        r.get::<_, Option<i64>>(11)?,
                    ),
                ))
            })
            .map_err(Error::storage)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Error::storage)?
    };
    if rows.len() != count {
        return Err(Error::storage(format!(
            "run {} records {count} children but {} are stored",
            root.id,
            rows.len()
        )));
    }
    let mut children = Vec::with_capacity(count);
    for (progress_row, acceptance) in rows {
        let (id, call_id, revision, history_len, progress, version) = progress_row;
        let (conversation_id, agent_id, binding, input, limits, expires_at) = acceptance;
        if version != i64::from(STATE_VERSION) {
            return Err(Error::incompatible(format!(
                "child state version {version}"
            )));
        }
        let id = super::run_id(&id)?;
        let progress: Progress = decode(&progress)?;
        let state = RunState {
            version: STATE_VERSION,
            id,
            conversation_id,
            agent_id,
            binding: decode(&binding)?,
            input: decode(&input)?,
            limits: decode(&limits)?,
            expires_at_ms: expires_at
                .map(|at| u64::try_from(at).map_err(Error::storage))
                .transpose()?,
            // Children are fenced by their root, never claimed themselves.
            generation: 0,
            revision: u64::try_from(revision).map_err(Error::storage)?,
            cancel_requested: progress.cancel_requested,
            phase: progress.phase,
            usage: progress.usage,
            prior_context: Vec::new(),
            history: load_history(connection, id, history_len)?,
            pending_response: progress.pending_response,
            operations: progress.operations,
            parent: Some(ParentRun {
                run_id: root.id,
                call_id: call_id.clone(),
            }),
            children: Vec::new(),
            child_usage: progress.child_usage,
        };
        children.push(AttachedChild {
            call_id,
            state: Box::new(state),
        });
    }
    Ok(children)
}
