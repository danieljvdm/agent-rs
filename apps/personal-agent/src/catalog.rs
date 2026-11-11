//! Application metadata and a write-ahead admission journal. Never edits engine tables.
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    path::PathBuf,
    sync::Mutex,
    thread,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;
use yielded_agent::{Error, Result};

type Job = Box<dyn FnOnce(&mut Connection) + Send>;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Conversation {
    pub sequence: i64,
    pub id: Uuid,
    pub title: String,
    pub created_at_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Submission {
    pub key: String,
    pub input: Value,
    pub expires_at_ms: Option<u64>,
}

#[derive(Clone)]
pub(crate) struct Admission {
    pub sequence: i64,
    pub conversation_id: Uuid,
    pub submission: Submission,
    pub binding: Value,
    pub run_id: Option<Uuid>,
    pub rejection: Option<String>,
}

fn admission(row: &rusqlite::Row<'_>) -> rusqlite::Result<Admission> {
    fn json<T: serde::de::DeserializeOwned>(
        row: &rusqlite::Row<'_>,
        n: usize,
    ) -> rusqlite::Result<T> {
        let text: String = row.get(n)?;
        serde_json::from_str(&text).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(n, rusqlite::types::Type::Text, Box::new(e))
        })
    }
    let run: Option<String> = row.get(4)?;
    Ok(Admission {
        sequence: row.get(0)?,
        conversation_id: json(row, 1)?,
        submission: json(row, 2)?,
        binding: json(row, 3)?,
        run_id: run
            .map(|s| serde_json::from_str(&s))
            .transpose()
            .map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    4,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?,
        rejection: row.get(5)?,
    })
}

fn conversation(row: &rusqlite::Row<'_>) -> rusqlite::Result<Conversation> {
    let id: String = row.get(1)?;
    Ok(Conversation {
        sequence: row.get(0)?,
        id: serde_json::from_str(&id).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(1, rusqlite::types::Type::Text, Box::new(e))
        })?,
        title: row.get(2)?,
        created_at_ms: row.get(3)?,
    })
}

fn encode(value: &impl Serialize) -> Result<String> {
    serde_json::to_string(value).map_err(Error::storage)
}

struct Worker {
    sender: Option<mpsc::Sender<Job>>,
    thread: Option<thread::JoinHandle<()>>,
}

pub(crate) struct Catalog(Mutex<Worker>);

impl Catalog {
    pub async fn open(path: PathBuf) -> Result<Self> {
        let (sender, mut receiver) = mpsc::channel::<Job>(64);
        let (ready_tx, ready_rx) = oneshot::channel();
        let thread = thread::Builder::new()
            .name("personal-agent-catalog".into())
            .spawn(move || {
                let opened = (|| -> Result<Connection> {
                    let connection = Connection::open(path).map_err(Error::storage)?;
                    connection.pragma_update(None, "journal_mode", "WAL").map_err(Error::storage)?;
                    connection.pragma_update(None, "synchronous", "FULL").map_err(Error::storage)?;
                    connection.pragma_update(None, "foreign_keys", "ON").map_err(Error::storage)?;
                    let version: i64 = connection.pragma_query_value(None, "user_version", |r| r.get(0)).map_err(Error::storage)?;
                    if version != 0 && version != 1 {
                        return Err(Error::incompatible(format!("application catalogue version {version}")));
                    }
                    connection.execute_batch("BEGIN IMMEDIATE;
                        CREATE TABLE IF NOT EXISTS conversations (
                            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                            id TEXT NOT NULL UNIQUE, title TEXT NOT NULL, created_at_ms INTEGER NOT NULL);
                        CREATE TABLE IF NOT EXISTS admissions (
                            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                            conversation_id TEXT NOT NULL REFERENCES conversations(id),
                            client_key TEXT NOT NULL, submission TEXT NOT NULL, binding TEXT NOT NULL,
                            run_id TEXT UNIQUE, rejection TEXT, retired INTEGER NOT NULL DEFAULT 0,
                            UNIQUE(conversation_id, client_key));
                        CREATE INDEX IF NOT EXISTS admissions_conversation ON admissions(conversation_id, sequence);
                        CREATE INDEX IF NOT EXISTS admissions_unfinished ON admissions(sequence) WHERE retired=0 AND rejection IS NULL;
                        PRAGMA user_version=1; COMMIT;").map_err(Error::storage)?;
                    Ok(connection)
                })();
                match opened {
                    Ok(mut connection) => {
                        if ready_tx.send(Ok(())).is_err() { return; }
                        while let Some(job) = receiver.blocking_recv() { job(&mut connection); }
                    }
                    Err(error) => { let _ = ready_tx.send(Err(error)); }
                }
            }).map_err(Error::storage)?;
        if let Err(error) = ready_rx.await.map_err(Error::storage)? {
            let _ = thread.join();
            return Err(error);
        }
        Ok(Self(Mutex::new(Worker {
            sender: Some(sender),
            thread: Some(thread),
        })))
    }

    async fn call<T: Send + 'static>(
        &self,
        action: impl FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let sender = self
            .0
            .lock()
            .map_err(|_| Error::storage("catalogue lock poisoned"))?
            .sender
            .clone()
            .ok_or_else(|| Error::storage("catalogue is closed"))?;
        let (tx, rx) = oneshot::channel();
        sender
            .send(Box::new(move |connection| {
                let _ = tx.send(action(connection));
            }))
            .await
            .map_err(|_| Error::storage("catalogue is closed"))?;
        rx.await.map_err(Error::storage)?
    }

    pub async fn close(&self) -> Result<()> {
        let thread = {
            let mut worker = self
                .0
                .lock()
                .map_err(|_| Error::storage("catalogue lock poisoned"))?;
            worker.sender.take();
            worker.thread.take()
        };
        if let Some(thread) = thread {
            tokio::task::spawn_blocking(move || thread.join())
                .await
                .map_err(Error::storage)?
                .map_err(|_| Error::storage("catalogue worker panicked"))?;
        }
        Ok(())
    }

    pub async fn create_conversation(&self, id: Uuid, title: String) -> Result<Conversation> {
        let id = encode(&id)?;
        self.call(move |connection| {
            connection.execute("INSERT INTO conversations(id,title,created_at_ms) VALUES(?1,?2,?3) ON CONFLICT(id) DO NOTHING", params![id,title,now_ms()]).map_err(Error::storage)?;
            let existing = connection.query_row("SELECT sequence,id,title,created_at_ms FROM conversations WHERE id=?1", [&id], conversation).map_err(Error::storage)?;
            if existing.title != title { return Err(Error::IdempotencyConflict); }
            Ok(existing)
        }).await
    }

    pub async fn conversation(&self, id: Uuid) -> Result<Conversation> {
        let id = encode(&id)?;
        self.call(move |connection| {
            connection
                .query_row(
                    "SELECT sequence,id,title,created_at_ms FROM conversations WHERE id=?1",
                    [id],
                    conversation,
                )
                .optional()
                .map_err(Error::storage)?
                .ok_or(Error::NotFound)
        })
        .await
    }

    pub async fn conversations(&self, after: i64, limit: usize) -> Result<Vec<Conversation>> {
        self.call(move |connection| {
            let mut statement = connection.prepare("SELECT sequence,id,title,created_at_ms FROM conversations WHERE (?1=0 OR sequence<?1) ORDER BY sequence DESC LIMIT ?2").map_err(Error::storage)?;
            statement.query_map(params![after, limit], conversation).map_err(Error::storage)?.map(|r| r.map_err(Error::storage)).collect()
        }).await
    }

    pub async fn lookup(&self, conversation: Uuid, key: String) -> Result<Option<Admission>> {
        let conversation = encode(&conversation)?;
        self.call(move |connection| {
            connection.query_row("SELECT sequence,conversation_id,submission,binding,run_id,rejection FROM admissions WHERE conversation_id=?1 AND client_key=?2", params![conversation,key], admission).optional().map_err(Error::storage)
        }).await
    }

    /// Commit the request before invoking Host::submit. Replaying it uses the same key.
    pub async fn reserve(
        &self,
        conversation: Uuid,
        submission: Submission,
        binding: Value,
    ) -> Result<Admission> {
        let conversation = encode(&conversation)?;
        let payload = encode(&submission)?;
        let binding = encode(&binding)?;
        self.call(move |connection| {
            connection.execute("INSERT INTO admissions(conversation_id,client_key,submission,binding) VALUES(?1,?2,?3,?4)", params![conversation,submission.key,payload,binding]).map_err(Error::storage)?;
            connection.query_row("SELECT sequence,conversation_id,submission,binding,run_id,rejection FROM admissions WHERE sequence=last_insert_rowid()", [], admission).map_err(Error::storage)
        }).await
    }

    pub async fn accepted(&self, sequence: i64, run_id: Uuid) -> Result<()> {
        let run_id = encode(&run_id)?;
        self.call(move |connection| {
            connection
                .execute(
                    "UPDATE admissions SET run_id=?2 WHERE sequence=?1",
                    params![sequence, run_id],
                )
                .map_err(Error::storage)?;
            Ok(())
        })
        .await
    }

    pub async fn reject(&self, sequence: i64, rejection: String) -> Result<()> {
        self.call(move |connection| {
            connection
                .execute(
                    "UPDATE admissions SET rejection=?2 WHERE sequence=?1",
                    params![sequence, rejection],
                )
                .map_err(Error::storage)?;
            Ok(())
        })
        .await
    }

    pub async fn retire(&self, sequence: i64) -> Result<()> {
        self.call(move |connection| {
            connection
                .execute(
                    "UPDATE admissions SET retired=1 WHERE sequence=?1 AND retired=0",
                    [sequence],
                )
                .map_err(Error::storage)?;
            Ok(())
        })
        .await
    }

    pub async fn by_run(&self, run_id: Uuid) -> Result<Admission> {
        let run_id = encode(&run_id)?;
        self.call(move |connection| {
            connection.query_row("SELECT sequence,conversation_id,submission,binding,run_id,rejection FROM admissions WHERE run_id=?1", [run_id], admission).optional().map_err(Error::storage)?.ok_or(Error::NotFound)
        }).await
    }

    pub async fn list(
        &self,
        conversation: Option<Uuid>,
        after: i64,
        limit: usize,
        pending_only: bool,
    ) -> Result<Vec<Admission>> {
        let conversation = conversation.map(|id| encode(&id)).transpose()?;
        self.call(move |connection| {
            let query = if conversation.is_some() {
                "SELECT sequence,conversation_id,submission,binding,run_id,rejection FROM admissions WHERE (?1=0 OR sequence<?1) AND conversation_id=?3 ORDER BY sequence DESC LIMIT ?2"
            } else if pending_only {
                "SELECT sequence,conversation_id,submission,binding,run_id,rejection FROM admissions WHERE sequence>?1 AND run_id IS NULL AND rejection IS NULL ORDER BY sequence LIMIT ?2"
            } else {
                "SELECT sequence,conversation_id,submission,binding,run_id,rejection FROM admissions WHERE sequence>?1 AND retired=0 AND rejection IS NULL ORDER BY sequence LIMIT ?2"
            };
            let mut statement = connection.prepare(query).map_err(Error::storage)?;
            let rows = if let Some(conversation) = conversation {
                statement.query_map(params![after,limit,conversation], admission)
            } else { statement.query_map(params![after,limit], admission) }.map_err(Error::storage)?;
            rows.map(|r| r.map_err(Error::storage)).collect()
        }).await
    }
}

impl Drop for Catalog {
    fn drop(&mut self) {
        if let Ok(worker) = self.0.get_mut() {
            worker.sender.take();
            if let Some(thread) = worker.thread.take() {
                let _ = thread.join();
            }
        }
    }
}
