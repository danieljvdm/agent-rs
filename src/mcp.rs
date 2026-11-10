//! Tools served by [MCP](https://modelcontextprotocol.io) servers over stdio, behind the `mcp`
//! feature.
//!
//! [`McpClient::connect`] starts one server, initializes it and takes a bounded snapshot of its
//! tool catalog. [`McpClient::tool`] registers one discovered tool, under an alias and a policy
//! the application chooses, as an [`McpTool`]: an ordinary [`Tool`] that an
//! [`AgentBuilder`](crate::AgentBuilder) accepts beside local tools, with the same validation,
//! approval, limits and recovery. Nothing starts a server but the application, and the client
//! owns its process: [`close`](McpClient::close) it once the agents using its tools have stopped.
//!
//! ```no_run
//! use tokio::process::Command;
//! use tokio_util::sync::CancellationToken;
//! use yielded_agent::mcp::{McpClient, McpOptions, ServerIdentity, ToolPolicy};
//! use yielded_agent::{Agent, Model, RetryPolicy};
//! # use std::sync::Arc;
//!
//! # async fn example(model: Arc<dyn Model>) -> yielded_agent::Result<()> {
//! let mut command = Command::new("node");
//! command.arg("/opt/mcp/server-filesystem/dist/index.js").arg("/data/workspace");
//! let client = McpClient::connect(
//!     ServerIdentity::new("workspace-files", "filesystem-2.1.0-workspace-v1"),
//!     command,
//!     McpOptions::default(),
//!     CancellationToken::new(),
//! )
//! .await?;
//! let read = client.tool(
//!     "read_text_file",
//!     "files_read",
//!     ToolPolicy::default()
//!         .with_retry(RetryPolicy::ReadOnly)
//!         .with_requires_approval(false),
//! )?;
//! let write = client.tool("write_file", "files_write", ToolPolicy::default())?;
//! let agent = Agent::builder("files", model).tool(read).tool(write).build()?;
//! // ... run or host the agent; stop it before closing the client.
//! client.close().await?;
//! # Ok(()) }
//! ```
//!
//! See `docs/mcp.md` for the recovery contract.

mod transport;

use crate::{
    Error, ErrorDetail, Execution, Result, RetryPolicy, Tool, ToolContext, ToolOutput, ToolSpec,
};
use async_trait::async_trait;
use rmcp::{
    Peer, RoleClient,
    model::{
        CallToolRequest, CallToolRequestParams, ClientRequest, ErrorData, ListToolsRequest,
        PaginatedRequestParams, ServerResult,
    },
    service::{PeerRequestOptions, ServiceError},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::HashSet, error::Error as StdError, sync::Arc, time::Duration};
use tokio::{process::Command, sync::Mutex, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use transport::{Core, RequestGuard};

/// A tool as the server describes it in `tools/list`: the
/// [`rmcp`](https://docs.rs/rmcp) SDK's type, whose version this crate pins exactly.
pub use rmcp::model::Tool as ToolDefinition;

/// The application's name for one deployment of a server. It is part of every registered
/// tool's durable contract, unlike the command and any credentials, which are never recorded.
///
/// Change `revision` whenever the server's executable, configuration, credentials' scope or
/// backing service changes, and give different servers different IDs. A server's self-reported
/// name and version do not identify a deployment.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub struct ServerIdentity {
    /// Which server this is, 1..=256 bytes.
    pub id: String,
    /// Which deployment of it, 1..=256 bytes.
    pub revision: String,
}
impl ServerIdentity {
    /// The deployment `revision` of the server `id`.
    pub fn new(id: impl Into<String>, revision: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            revision: revision.into(),
        }
    }
}

/// Bounds on one connection, shared by every call on it, from every run. Start from
/// [`McpOptions::default`] and set what differs. Each limit must be positive.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct McpOptions {
    /// Bytes of one JSON-RPC message in either direction. Default 1 MiB.
    pub max_message_bytes: usize,
    /// Encoded bytes of the whole tool catalog, over all its pages. Default 1 MiB.
    pub max_catalog_bytes: usize,
    /// Tools in the catalog, and pages it may span. Default 256.
    pub max_tools: usize,
    /// Tool calls in flight on the connection at once. The engine's
    /// `Limits::max_parallel_tools` applies independently. Default 4.
    pub max_in_flight: usize,
    /// Deadline of each request: initialization, each discovery page and each call. The
    /// engine's operation timeout still applies to a call. Default 30 s.
    pub request_timeout: Duration,
    /// How long cancellation notifications may take before the server is killed. Default 100 ms.
    pub cancellation_grace: Duration,
}
impl Default for McpOptions {
    fn default() -> Self {
        Self {
            max_message_bytes: 1_048_576,
            max_catalog_bytes: 1_048_576,
            max_tools: 256,
            max_in_flight: 4,
            request_timeout: Duration::from_secs(30),
            cancellation_grace: Duration::from_millis(100),
        }
    }
}
impl McpOptions {
    /// Sets [`max_message_bytes`](Self::max_message_bytes).
    pub fn with_max_message_bytes(mut self, bytes: usize) -> Self {
        self.max_message_bytes = bytes;
        self
    }
    /// Sets [`max_catalog_bytes`](Self::max_catalog_bytes).
    pub fn with_max_catalog_bytes(mut self, bytes: usize) -> Self {
        self.max_catalog_bytes = bytes;
        self
    }
    /// Sets [`max_tools`](Self::max_tools).
    pub fn with_max_tools(mut self, tools: usize) -> Self {
        self.max_tools = tools;
        self
    }
    /// Sets [`max_in_flight`](Self::max_in_flight).
    pub fn with_max_in_flight(mut self, calls: usize) -> Self {
        self.max_in_flight = calls;
        self
    }
    /// Sets [`request_timeout`](Self::request_timeout).
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }
    /// Sets [`cancellation_grace`](Self::cancellation_grace).
    pub fn with_cancellation_grace(mut self, grace: Duration) -> Self {
        self.cancellation_grace = grace;
        self
    }
}

/// How the engine may run a registered tool. The application chooses it: a server's
/// `readOnlyHint` and `idempotentHint` annotations grant nothing.
///
/// The default is the most cautious: every call waits for approval, runs alone and never runs
/// again once its outcome is unknown. [`RetryPolicy::Idempotent`] is rejected, because MCP has
/// no general way to deduplicate a call.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ToolPolicy {
    /// See [`ToolSpec::retry`]. Default [`RetryPolicy::Never`].
    pub retry: RetryPolicy,
    /// See [`ToolSpec::execution`]. Default [`Execution::Sequential`].
    pub execution: Execution,
    /// See [`ToolSpec::requires_approval`]. Default `true`.
    pub requires_approval: bool,
}
impl Default for ToolPolicy {
    fn default() -> Self {
        Self {
            retry: RetryPolicy::Never,
            execution: Execution::Sequential,
            requires_approval: true,
        }
    }
}
impl ToolPolicy {
    /// Sets [`retry`](Self::retry).
    pub fn with_retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }
    /// Sets [`execution`](Self::execution).
    pub fn with_execution(mut self, execution: Execution) -> Self {
        self.execution = execution;
        self
    }
    /// Sets [`requires_approval`](Self::requires_approval).
    pub fn with_requires_approval(mut self, requires_approval: bool) -> Self {
        self.requires_approval = requires_approval;
        self
    }
}

/// Owns one stdio server and the snapshot of its catalog taken when it connected.
///
/// Registered tools share the connection, but dropping the client still closes it: keep the
/// client alive while its tools are in use, and await [`close`](Self::close) before the Tokio
/// runtime shuts down to be sure the server has been reaped. Dropping it only starts that.
pub struct McpClient {
    session: Arc<Session>,
    identity: ServerIdentity,
    server_info: Value,
    catalog: Arc<Vec<ToolDefinition>>,
}

struct Session {
    peer: Peer<RoleClient>,
    core: Arc<Core>,
    job: Mutex<Job>,
}
/// The task owning the server process, until it has been joined once; then how it ended, for
/// every later join.
enum Job {
    Running(JoinHandle<Result<()>>),
    Joined(Option<String>),
}

impl McpClient {
    /// Spawn `command` as a server, initialize it and take a bounded snapshot of its catalog.
    ///
    /// The server's stdin and stdout carry MCP and its stderr is discarded. On Unix it leads a
    /// new process group, and closing the client kills that whole group. Pass credentials
    /// explicitly on `command`; they are neither logged nor recorded.
    ///
    /// Cancelling `cancel` stops initialization and discovery, kills the server and returns
    /// [`Error::Interrupted`] once it has been reaped. It has no effect after `connect`
    /// returns; use [`close`](Self::close) then.
    ///
    /// Fails with [`Error::Config`] for an identity or option out of bounds, or a server that
    /// speaks no supported protocol version or offers no tools, and with [`Error::Tool`] if the
    /// server cannot be started, fails, times out, or serves a catalog that exceeds the limits
    /// or repeats a name. The server is reaped before any error returns.
    pub async fn connect(
        identity: ServerIdentity,
        command: Command,
        options: McpOptions,
        cancel: CancellationToken,
    ) -> Result<Self> {
        let bounded = |text: &str| !text.is_empty() && text.len() <= 256;
        if !bounded(&identity.id) || !bounded(&identity.revision) {
            return Err(Error::config(
                "MCP server ID and revision must be 1..=256 bytes",
            ));
        }
        if options.max_message_bytes == 0
            || options.max_catalog_bytes == 0
            || options.max_tools == 0
            || options.max_in_flight == 0
            || options.max_in_flight > tokio::sync::Semaphore::MAX_PERMITS
            || options.request_timeout.is_zero()
        {
            return Err(Error::config("MCP limits must be positive"));
        }
        if cancel.is_cancelled() {
            return Err(Error::Interrupted);
        }
        let core = Arc::new(Core::new(options));
        // Dropping this future must still stop the process it started.
        let guard = core.stop.clone().drop_guard();
        let (handle, ready) = transport::start(command, core.clone())?;
        let initialized = tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(Error::Interrupted),
            result = ready => result.unwrap_or_else(|_| Err(Error::tool("MCP connection task ended"))),
        };
        let peer = match initialized {
            Ok(peer) => peer,
            Err(error) => {
                core.stop.cancel();
                let _ = handle.await;
                return Err(error);
            }
        };
        let session = Arc::new(Session {
            peer,
            core,
            job: Mutex::new(Job::Running(handle)),
        });
        let discovered = match serde_json::to_value(session.peer.peer_info()) {
            Ok(server_info) => session
                .discover(&cancel)
                .await
                .map(|catalog| (server_info, catalog)),
            Err(error) => Err(Error::tool(error)),
        };
        let (server_info, catalog) = match discovered {
            Ok(discovered) => discovered,
            Err(error) => {
                let _ = session.close().await;
                return Err(error);
            }
        };
        guard.disarm();
        Ok(Self {
            session,
            identity,
            server_info,
            catalog: Arc::new(catalog),
        })
    }

    /// The identity given to [`connect`](Self::connect).
    pub fn identity(&self) -> &ServerIdentity {
        &self.identity
    }

    /// The catalog snapshot taken by [`connect`](Self::connect), sorted by name.
    pub fn tools(&self) -> &[ToolDefinition] {
        &self.catalog
    }

    /// Whether the connection has closed: by [`close`](Self::close), or because the server
    /// failed, its catalog changed, or a request was interrupted. Reconnect to continue.
    pub fn is_closed(&self) -> bool {
        self.session.core.stop.is_cancelled()
    }

    /// Register the discovered tool `remote_name` for the model to call as `alias`.
    ///
    /// The alias is 1..=64 ASCII letters, digits, `_` or `-`, and must be unique within the
    /// agent, tools of other servers included. The tool's input schema, and any output schema,
    /// must have `type: object` and may use only references bundled in the schema itself.
    ///
    /// The tool's [`ToolSpec::version`] records the complete contract: this adapter's version,
    /// the server's identity and what it reported when initialized, and the tool's whole
    /// definition. A stored run therefore resumes only against the same deployment serving the
    /// same tool; any other difference fails with [`Error::Incompatible`]. Tools the run never
    /// registered do not matter.
    ///
    /// Fails with [`Error::Config`] for an unknown tool, an invalid alias or schema, or a
    /// [`RetryPolicy::Idempotent`] policy, and with [`Error::Tool`] once the connection closed.
    pub fn tool(&self, remote_name: &str, alias: &str, policy: ToolPolicy) -> Result<Arc<McpTool>> {
        if self.is_closed() {
            return Err(Error::tool(self.session.core.reason()));
        }
        if alias.is_empty()
            || alias.len() > 64
            || !alias
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            return Err(Error::config(format!("invalid MCP tool alias {alias:?}")));
        }
        if matches!(policy.retry, RetryPolicy::Idempotent { .. }) {
            return Err(Error::config(
                "MCP tools cannot be idempotent: MCP has no general way to deduplicate a call",
            ));
        }
        let definition = self
            .catalog
            .iter()
            .find(|tool| tool.name == remote_name)
            .ok_or_else(|| Error::config(format!("MCP tool not discovered: {remote_name}")))?;
        let input_schema = Value::Object((*definition.input_schema).clone());
        let input = compile_schema(&input_schema)?;
        let output = definition
            .output_schema
            .as_ref()
            .map(|schema| compile_schema(&Value::Object((**schema).clone())))
            .transpose()?;
        let version = serde_json::to_string(&json!({
            "adapter": "mcp-stdio-v1", "server": self.identity,
            "negotiated": self.server_info, "tool": definition,
        }))
        .map_err(Error::config)?;
        let spec = ToolSpec::new(alias, version)
            .with_description(format!(
                "[MCP server={}, tool={}] {}",
                self.identity.id,
                remote_name,
                definition.description.as_deref().unwrap_or("")
            ))
            .with_input_schema(input_schema)
            .with_retry(policy.retry)
            .with_execution(policy.execution)
            .with_requires_approval(policy.requires_approval);
        Ok(Arc::new(McpTool {
            session: self.session.clone(),
            catalog: self.catalog.clone(),
            identity: self.identity.clone(),
            remote_name: remote_name.into(),
            spec,
            input,
            output,
        }))
    }

    /// Close the connection: stop accepting calls, notify the server of requests in flight,
    /// kill its process group, reap it and join the task that owned it. Idempotent; if this
    /// future is dropped, awaiting `close` again finishes the same cleanup.
    pub async fn close(&self) -> Result<()> {
        self.session.close().await
    }
}
impl Drop for McpClient {
    fn drop(&mut self) {
        self.session.core.fail("MCP client dropped");
    }
}

/// One discovered tool, registered by [`McpClient::tool`].
///
/// # Outcomes
///
/// - The server's result is `Complete`, or `Failed` when it sets `isError`. Either keeps the
///   whole MCP result: content blocks, `structuredContent` and `_meta`. Resource links in it
///   are data, never fetched.
/// - A JSON-RPC error reply to `tools/call` is `Failed`, with the reply as
///   `{"error": {"code", "message", "data"}}`.
/// - A call that was never sent is `Failed` with `{"error": {"message"}}`: the connection had
///   closed, the catalog no longer matched the snapshot, or re-listing it failed.
/// - Once `tools/call` was sent, anything else is an [`Error::Tool`], since the server may have
///   acted: the transport failing, the request timing out, a malformed, truncated, incomplete
///   or oversized reply, or a success whose `structuredContent` violates the declared output
///   schema. For a tool that is not [`RetryPolicy::ReadOnly`], the call becomes
///   [`OperationStatus::Unknown`](crate::OperationStatus::Unknown).
///
/// A failure the server reports is its own account, not proof that nothing changed.
///
/// # Connection
///
/// Before each call the tool lists the server's catalog again, bypassing any cache, and
/// compares it with the snapshot; a change, or a `notifications/tools/list_changed`, closes the
/// connection rather than calling a tool whose contract moved. MCP cannot make the call
/// conditional on the catalog, so a change between that check and the call goes unnoticed.
///
/// A request that times out, or that the engine drops on a timeout, cancellation or shutdown,
/// closes the whole connection, other calls on it included, because the server may still be
/// acting on it. [`cleanup`](Tool::cleanup) then waits until the server's process group has
/// been killed and reaped.
pub struct McpTool {
    session: Arc<Session>,
    catalog: Arc<Vec<ToolDefinition>>,
    identity: ServerIdentity,
    remote_name: String,
    spec: ToolSpec,
    input: jsonschema::Validator,
    output: Option<jsonschema::Validator>,
}
impl McpTool {
    /// The identity of the server serving this tool.
    pub fn server(&self) -> &ServerIdentity {
        &self.identity
    }
    /// The tool's name on its server.
    pub fn remote_name(&self) -> &str {
        &self.remote_name
    }
}

/// A call that was never sent: its failure is known, so the model sees it.
fn not_sent(message: String) -> ToolOutput<Value> {
    ToolOutput::Failed(json!({"error": {"message": message}}))
}

#[async_trait]
impl Tool for McpTool {
    fn spec(&self) -> &ToolSpec {
        &self.spec
    }

    fn validate(&self, arguments: &Value) -> Result<()> {
        self.input.validate(arguments).map_err(|error| {
            Error::model_output(format!(
                "MCP arguments for {} at {}: {}",
                self.spec.name, error.instance_path, error
            ))
        })
    }

    async fn execute(&self, arguments: Value, context: ToolContext) -> Result<ToolOutput<Value>> {
        let core = &self.session.core;
        let Value::Object(arguments) = arguments else {
            return Ok(not_sent("MCP arguments must be a JSON object".into()));
        };
        let _permit = tokio::select! {
            biased;
            _ = context.cancel.cancelled() => return Err(Error::Interrupted),
            _ = core.stop.cancelled() => return Ok(not_sent(core.reason())),
            permit = core.slots.acquire() => match permit {
                Ok(permit) => permit,
                Err(_) => return Ok(not_sent(core.reason())),
            },
        };
        // List again, bypassing SDK caches, before every call: an approval never covers a
        // catalog the application did not register against.
        match self.session.discover(&context.cancel).await {
            Ok(current) if current == *self.catalog => {}
            Ok(_) => {
                core.catalog_changed();
                return Ok(not_sent(core.reason()));
            }
            Err(Error::Interrupted) => return Err(Error::Interrupted),
            Err(error) => return Ok(not_sent(error.to_string())),
        }
        if core.stop.is_cancelled() {
            return Ok(not_sent(core.reason()));
        }
        let request = ClientRequest::CallToolRequest(CallToolRequest::new(
            CallToolRequestParams::new(self.remote_name.clone()).with_arguments(arguments),
        ));
        let result = match self.session.request(request, &context.cancel).await {
            Ok(ServerResult::CallToolResult(result)) => result,
            Ok(_) => {
                core.fail("MCP server answered tools/call with another result");
                return Err(Error::tool(core.reason()));
            }
            Err(RpcError::Remote(error)) => {
                return Ok(ToolOutput::Failed(json!({ "error": error })));
            }
            Err(RpcError::Local(error)) => return Err(error),
        };
        let is_error = result.is_error == Some(true);
        if !is_error && let Some(validator) = &self.output {
            let output = result.structured_content.as_ref().ok_or_else(|| {
                Error::tool("MCP result omits the structuredContent its output schema requires")
            })?;
            validator.validate(output).map_err(|error| {
                Error::tool(format!(
                    "MCP result violates its output schema at {}: {}",
                    error.instance_path, error
                ))
            })?;
        }
        let value = serde_json::to_value(result).map_err(Error::tool)?;
        Ok(if is_error {
            ToolOutput::Failed(value)
        } else {
            ToolOutput::Complete(value)
        })
    }

    async fn cleanup(&self, _context: ToolContext) -> Result<()> {
        if self.session.core.stop.is_cancelled() {
            self.session.join().await
        } else {
            Ok(())
        }
    }
}

/// An error tool with a message and the error that caused it.
fn tool_error(message: &'static str, cause: impl Into<Box<dyn StdError + Send + Sync>>) -> Error {
    Error::Tool(ErrorDetail::caused_by(message, cause))
}

/// Why a request produced no result: the server rejected it, or it failed on this side.
enum RpcError {
    Remote(ErrorData),
    Local(Error),
}

impl Session {
    async fn request(
        &self,
        request: ClientRequest,
        cancel: &CancellationToken,
    ) -> std::result::Result<ServerResult, RpcError> {
        if self.core.stop.is_cancelled() {
            return Err(RpcError::Local(Error::tool(self.core.reason())));
        }
        if cancel.is_cancelled() {
            return Err(RpcError::Local(Error::Interrupted));
        }
        let mut guard = RequestGuard::new(self.core.clone());
        let operation = async {
            let mut handle = self
                .peer
                .send_cancellable_request(request, PeerRequestOptions::no_options())
                .await?;
            guard.track(handle.id.clone());
            (&mut handle.rx)
                .await
                .map_err(|_| ServiceError::TransportClosed)?
        };
        let response = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(RpcError::Local(Error::Interrupted)),
            response = tokio::time::timeout(self.core.options.request_timeout, operation) => response,
            _ = self.core.stop.cancelled() => {
                return Err(RpcError::Local(Error::tool(self.core.reason())));
            }
        };
        match response {
            Ok(Ok(response)) => {
                guard.finish();
                Ok(response)
            }
            Ok(Err(ServiceError::McpError(error))) => {
                guard.finish();
                Err(RpcError::Remote(error))
            }
            // The protocol task ends every open request once the connection closes; report
            // why it closed instead.
            Ok(Err(_)) if self.core.stop.is_cancelled() => {
                Err(RpcError::Local(Error::tool(self.core.reason())))
            }
            Ok(Err(error)) => Err(RpcError::Local(tool_error("MCP transport failed", error))),
            Err(_) => Err(RpcError::Local(Error::tool("MCP request timed out"))),
        }
    }

    /// List the whole catalog, page by page, sorted by name. A catalog that breaks the
    /// connection's limits, repeats a name or never ends closes the connection.
    async fn discover(&self, cancel: &CancellationToken) -> Result<Vec<ToolDefinition>> {
        let limits = &self.core.options;
        let invalid = |reason: &'static str| {
            self.core.fail(reason);
            Error::tool(reason)
        };
        let mut tools: Vec<ToolDefinition> = Vec::new();
        let mut cursors = HashSet::new();
        let mut cursor = None;
        let mut bytes = ByteCount(0);
        loop {
            let request = ClientRequest::ListToolsRequest(ListToolsRequest::with_param(
                PaginatedRequestParams::default().with_cursor(cursor),
            ));
            let page = match self.request(request, cancel).await {
                Ok(ServerResult::ListToolsResult(page)) => page,
                Ok(_) => {
                    return Err(invalid(
                        "MCP server answered tools/list with another result",
                    ));
                }
                Err(RpcError::Remote(error)) => {
                    return Err(tool_error("MCP server rejected tools/list", error));
                }
                Err(RpcError::Local(error)) => return Err(error),
            };
            // Counted, not buffered: the page is already decoded.
            serde_json::to_writer(&mut bytes, &page).map_err(Error::tool)?;
            if tools.len().saturating_add(page.tools.len()) > limits.max_tools
                || bytes.0 > limits.max_catalog_bytes
            {
                return Err(invalid("MCP catalog exceeds the connection's limits"));
            }
            tools.extend(page.tools);
            match page.next_cursor {
                None => break,
                Some(next) => {
                    if cursors.len() >= limits.max_tools || !cursors.insert(next.clone()) {
                        return Err(invalid("MCP catalog pagination does not terminate"));
                    }
                    cursor = Some(next);
                }
            }
        }
        tools.sort_by(|a, b| a.name.cmp(&b.name));
        if tools
            .iter()
            .any(|tool| tool.name.is_empty() || tool.name.len() > 256)
            || tools.windows(2).any(|pair| pair[0].name == pair[1].name)
        {
            return Err(invalid(
                "MCP tool names must be 1..=256 bytes and unique in the catalog",
            ));
        }
        Ok(tools)
    }

    async fn close(&self) -> Result<()> {
        self.core.fail("MCP client closed");
        self.join().await
    }

    /// Wait for the task owning the server to finish. Cancel-safe: the handle stays until it
    /// has been awaited to completion, and every later join reports the same outcome.
    async fn join(&self) -> Result<()> {
        let mut job = self.job.lock().await;
        if let Job::Running(handle) = &mut *job {
            let failure = match handle.await {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(error.to_string()),
                Err(error) => Some(format!("MCP connection task failed: {error}")),
            };
            *job = Job::Joined(failure);
        }
        match &*job {
            Job::Joined(Some(failure)) => Err(Error::tool(failure.clone())),
            _ => Ok(()),
        }
    }
}

/// Counts the bytes written to it.
struct ByteCount(usize);
impl std::io::Write for ByteCount {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0 = self.0.saturating_add(buf.len());
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct NoExternalSchemas;
impl jsonschema::Retrieve for NoExternalSchemas {
    fn retrieve(
        &self,
        _uri: &jsonschema::Uri<String>,
    ) -> std::result::Result<Value, Box<dyn StdError + Send + Sync>> {
        Err("MCP schemas must bundle their references; external retrieval is disabled".into())
    }
}
fn compile_schema(schema: &Value) -> Result<jsonschema::Validator> {
    if schema.get("type").and_then(Value::as_str) != Some("object") {
        return Err(Error::config("MCP tool schemas must have type object"));
    }
    jsonschema::options()
        .with_retriever(NoExternalSchemas)
        .build(schema)
        .map_err(|error| Error::config(format!("invalid MCP tool schema: {error}")))
}
