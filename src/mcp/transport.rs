//! The stdio connection: a bounded line transport, the task that owns the server process, and
//! the state calls share with it.
use super::{McpOptions, tool_error};
use crate::{Error, Result};
use futures::StreamExt;
use rmcp::{
    Peer, RoleClient, ServiceExt,
    model::{
        CancelledNotificationParam, ClientCapabilities, ClientConfig, Implementation, RequestId,
    },
    service::{RxJsonRpcMessage, TxJsonRpcMessage},
    transport::Transport,
};
use std::{
    collections::HashSet,
    io,
    process::Stdio,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{
    io::AsyncWriteExt,
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::{Mutex as AsyncMutex, Semaphore, oneshot},
    task::JoinHandle,
};
use tokio_util::{
    bytes::BytesMut,
    codec::{Decoder, FramedRead},
    sync::CancellationToken,
};

/// Protocol versions this client accepts from `initialize`.
const PROTOCOLS: [&str; 4] = ["2024-11-05", "2025-03-26", "2025-06-18", "2025-11-25"];

/// What every call, the transport and the owning task share about one connection.
pub(super) struct Core {
    pub options: McpOptions,
    /// Cancelled once the connection is closing, for whatever reason; never reset.
    pub stop: CancellationToken,
    /// Bounds calls in flight on this connection, across runs.
    pub slots: Semaphore,
    changed: AtomicBool,
    reason: Mutex<Option<String>>,
    requests: Mutex<HashSet<RequestId>>,
}
impl Core {
    pub fn new(options: McpOptions) -> Self {
        Self {
            slots: Semaphore::new(options.max_in_flight),
            options,
            stop: CancellationToken::new(),
            changed: AtomicBool::new(false),
            reason: Mutex::new(None),
            requests: Mutex::new(HashSet::new()),
        }
    }
    /// Close the connection, keeping the first reason given.
    pub fn fail(&self, reason: &str) {
        lock(&self.reason).get_or_insert_with(|| reason.into());
        self.stop.cancel();
    }
    /// Close the connection because the server's catalog no longer matches the snapshot, and
    /// refuse every later write to it.
    pub fn catalog_changed(&self) {
        self.changed.store(true, Ordering::Release);
        self.fail("MCP catalog changed; reconnect and register the tools again");
    }
    /// Why the connection closed.
    pub fn reason(&self) -> String {
        lock(&self.reason)
            .clone()
            .unwrap_or_else(|| "MCP connection closed".into())
    }
}
/// The sets behind these locks are only touched briefly and never across an await; a poisoned
/// one still holds consistent data.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Tracks one request. Dropping it before [`finish`](Self::finish), because the request timed
/// out, the transport failed or the engine dropped the call, closes the whole connection: the
/// server may still be acting on it, and MCP cancellation is only advisory. The owning task
/// then notifies the server, kills its process group and reaps it.
pub(super) struct RequestGuard {
    core: Arc<Core>,
    id: Option<RequestId>,
    finished: bool,
}
impl RequestGuard {
    pub fn new(core: Arc<Core>) -> Self {
        Self {
            core,
            id: None,
            finished: false,
        }
    }
    pub fn track(&mut self, id: RequestId) {
        lock(&self.core.requests).insert(id.clone());
        self.id = Some(id);
    }
    pub fn finish(&mut self) {
        if let Some(id) = self.id.take() {
            lock(&self.core.requests).remove(&id);
        }
        self.finished = true;
    }
}
impl Drop for RequestGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.core
                .fail("MCP request interrupted; its remote outcome is unknown");
        }
    }
}

/// rmcp's standard stdio transport buffers an unbounded line in the pinned version. This one
/// caps each frame before decoding it, and treats a malformed or truncated frame as a
/// disconnect.
struct BoundedStdio {
    read: FramedRead<ChildStdout, MessageCodec>,
    write: Arc<AsyncMutex<Option<ChildStdin>>>,
    core: Arc<Core>,
}
impl Transport<RoleClient> for BoundedStdio {
    type Error = io::Error;
    fn send(
        &mut self,
        item: TxJsonRpcMessage<RoleClient>,
    ) -> impl Future<Output = io::Result<()>> + Send + 'static {
        let writer = self.write.clone();
        let core = self.core.clone();
        async move {
            if core.changed.load(Ordering::Acquire) {
                return Err(io::Error::other("MCP catalog changed"));
            }
            let mut bytes = serde_json::to_vec(&item)?;
            if bytes.len() > core.options.max_message_bytes {
                return Err(io::Error::other("MCP request exceeds message limit"));
            }
            bytes.push(b'\n');
            let mut writer = writer.lock().await;
            let writer = writer
                .as_mut()
                .ok_or_else(|| io::Error::other("MCP stdin closed"))?;
            writer.write_all(&bytes).await?;
            writer.flush().await
        }
    }

    async fn receive(&mut self) -> Option<RxJsonRpcMessage<RoleClient>> {
        loop {
            let line = match self.read.next().await {
                Some(Ok(line)) => line,
                Some(Err(_)) => {
                    self.core
                        .fail("MCP frame is invalid or exceeds the message limit");
                    return None;
                }
                None => {
                    self.core.fail("MCP server disconnected");
                    return None;
                }
            };
            if line.is_empty() {
                continue;
            }
            let Ok(value) = serde_json::from_slice::<serde_json::Value>(&line) else {
                self.core.fail("MCP server sent malformed JSON");
                return None;
            };
            if value.get("jsonrpc").and_then(serde_json::Value::as_str) != Some("2.0") {
                self.core
                    .fail("MCP server sent an invalid JSON-RPC version");
                return None;
            }
            if value.get("result").is_some() && value.get("error").is_some() {
                self.core
                    .fail("MCP response contains both a result and an error");
                return None;
            }
            if value.get("method").and_then(serde_json::Value::as_str)
                == Some("notifications/tools/list_changed")
            {
                self.core.catalog_changed();
            }
            // The SDK decodes a CallToolResult without content. The supported protocols require
            // it, so a partial result must not become a success.
            if let Some(result) = value.get("result")
                && !result
                    .get("content")
                    .is_some_and(serde_json::Value::is_array)
                && result.get("tools").is_none()
                && result.get("protocolVersion").is_none()
            {
                self.core.fail("MCP server sent an incomplete response");
                return None;
            }
            match serde_json::from_value(value) {
                Ok(message) => return Some(message),
                Err(_) => {
                    self.core.fail("MCP server sent an invalid message");
                    return None;
                }
            }
        }
    }

    async fn close(&mut self) -> io::Result<()> {
        self.core.stop.cancel();
        self.write.lock().await.take();
        Ok(())
    }
}
impl Drop for BoundedStdio {
    fn drop(&mut self) {
        self.core.stop.cancel();
    }
}

/// Splits newline-delimited frames of at most `max_bytes`, without the newline. Remembers how
/// far it has searched, so a frame arriving in many reads is scanned once.
struct MessageCodec {
    max_bytes: usize,
    scanned: usize,
}
impl Decoder for MessageCodec {
    type Item = BytesMut;
    type Error = io::Error;
    fn decode(&mut self, buffer: &mut BytesMut) -> io::Result<Option<BytesMut>> {
        let newline = buffer[self.scanned..]
            .iter()
            .position(|byte| *byte == b'\n')
            .map(|index| index + self.scanned);
        if newline.unwrap_or(buffer.len()) > self.max_bytes {
            return Err(io::Error::other("MCP message exceeds byte limit"));
        }
        match newline {
            Some(index) => {
                self.scanned = 0;
                let mut message = buffer.split_to(index + 1);
                message.truncate(index);
                Ok(Some(message))
            }
            None => {
                self.scanned = buffer.len();
                Ok(None)
            }
        }
    }
    fn decode_eof(&mut self, buffer: &mut BytesMut) -> io::Result<Option<BytesMut>> {
        match self.decode(buffer)? {
            Some(message) => Ok(Some(message)),
            None if buffer.is_empty() => Ok(None),
            None => Err(io::Error::other("MCP truncated frame")),
        }
    }
}

/// The server's process group, killed when this is dropped unless the server was reaped first.
///
/// The server leads its own group, so killing the group also kills descendants that stayed in
/// it, such as the interpreter a wrapper script starts. The group is signalled only before the
/// leader is reaped: until then the leader's ID cannot be reused, so the signal cannot reach an
/// unrelated process.
struct ProcessGroup {
    #[cfg(unix)]
    leader: Option<rustix::process::Pid>,
}
impl ProcessGroup {
    #[cfg(unix)]
    fn of(child: &Child) -> Self {
        Self {
            leader: child
                .id()
                .and_then(|id| i32::try_from(id).ok())
                .and_then(rustix::process::Pid::from_raw),
        }
    }
    #[cfg(not(unix))]
    fn of(_child: &Child) -> Self {
        Self {}
    }
    /// SIGKILL every process in the group. A group that is already gone is not an error.
    #[cfg(unix)]
    fn kill(&self) -> io::Result<()> {
        use rustix::{io::Errno, process};
        let Some(leader) = self.leader else {
            return Ok(());
        };
        match process::kill_process_group(leader, process::Signal::KILL) {
            // macOS reports EPERM for a group whose remaining members are all zombies.
            Ok(()) | Err(Errno::SRCH) => Ok(()),
            Err(Errno::PERM) if cfg!(target_vendor = "apple") => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
    #[cfg(not(unix))]
    fn kill(&self) -> io::Result<()> {
        Ok(())
    }
    fn reaped(&mut self) {
        #[cfg(unix)]
        {
            self.leader = None;
        }
    }
}
impl Drop for ProcessGroup {
    fn drop(&mut self) {
        // Reached without `reaped` only when the owning task is dropped, for example by a
        // runtime shutting down. The child's own `kill_on_drop` covers the leader.
        let _ = self.kill();
    }
}

type Ready = oneshot::Receiver<Result<Peer<RoleClient>>>;
/// Spawn the server in its own process group and the task that owns it. The task initializes
/// the connection, reports the peer on `Ready`, then waits for `core.stop`; once stopped it
/// notifies the server of requests left open, kills the process group, reaps the server and
/// closes the protocol. Its result is the cleanup's.
pub(super) fn start(
    mut command: Command,
    core: Arc<Core>,
) -> Result<(JoinHandle<Result<()>>, Ready)> {
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    #[cfg(unix)]
    command.process_group(0);
    let mut child = command
        .spawn()
        .map_err(|error| tool_error("MCP server could not be spawned", error))?;
    let mut group = ProcessGroup::of(&child);
    let (Some(stdout), stdin) = (child.stdout.take(), child.stdin.take()) else {
        return Err(Error::tool("MCP server has no stdout"));
    };
    let transport = BoundedStdio {
        read: FramedRead::new(
            stdout,
            MessageCodec {
                max_bytes: core.options.max_message_bytes,
                scanned: 0,
            },
        ),
        write: Arc::new(AsyncMutex::new(stdin)),
        core: core.clone(),
    };
    let (ready, receiver) = oneshot::channel();
    let job = tokio::spawn(async move {
        let info = ClientConfig::new(
            ClientCapabilities::default(),
            Implementation::new("agent-rs", env!("CARGO_PKG_VERSION")),
        );
        let initialized = tokio::select! {
            biased;
            _ = core.stop.cancelled() => Err(Error::Interrupted),
            result = tokio::time::timeout(core.options.request_timeout, info.serve(transport)) => {
                match result {
                    Err(_) => Err(Error::tool("MCP initialization timed out")),
                    Ok(Err(error)) => Err(tool_error("MCP initialization failed", error)),
                    Ok(Ok(service)) => Ok(service),
                }
            }
        };
        let mut service = match initialized {
            Ok(service) => service,
            Err(error) => {
                let _ = ready.send(Err(error));
                return terminate(&mut child, &mut group).await;
            }
        };
        let supported = service.peer_info().is_some_and(|info| {
            PROTOCOLS.contains(&info.protocol_version.to_string().as_str())
                && info.capabilities.tools.is_some()
        });
        if !supported {
            let _ = ready.send(Err(Error::config(
                "MCP server must speak a supported protocol version and offer tools",
            )));
            core.stop.cancel();
        } else if ready.send(Ok(service.peer().clone())).is_err() {
            core.stop.cancel();
        }
        core.stop.cancelled().await;
        let open: Vec<_> = lock(&core.requests).iter().cloned().collect();
        let notifications = open.into_iter().map(|request_id| {
            service
                .peer()
                .notify_cancelled(CancelledNotificationParam::new(
                    Some(request_id),
                    Some("client closing interrupted request".into()),
                ))
        });
        let _ = tokio::time::timeout(
            core.options.cancellation_grace,
            futures::future::join_all(notifications),
        )
        .await;
        service.cancellation_token().cancel();
        // Kill before joining the protocol task, so a blocked pipe write cannot stall cleanup.
        let terminated = terminate(&mut child, &mut group).await;
        let joined = service
            .close()
            .await
            .map_err(|error| tool_error("MCP protocol task failed", error));
        terminated?;
        joined?;
        Ok(())
    });
    Ok((job, receiver))
}

/// Kill the server's process group and the server itself, then reap the server.
async fn terminate(child: &mut Child, group: &mut ProcessGroup) -> Result<()> {
    let signalled = group.kill();
    // Also signal the server directly, in case it left its group. A concurrent exit can make
    // this fail; the wait proves it exited.
    let _ = child.start_kill();
    child
        .wait()
        .await
        .map_err(|error| tool_error("MCP server could not be reaped", error))?;
    group.reaped();
    signalled.map_err(|error| tool_error("MCP process group could not be killed", error))
}
