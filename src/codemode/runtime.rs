//! The interface between code mode and a script interpreter.
use crate::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

/// A script interpreter that code mode runs scripts in, such as [`QuickJs`](super::QuickJs).
///
/// Code mode replays a script from the top on every turn of its run, answering each call it
/// recorded before with the recorded result, so an interpreter must be deterministic: the same
/// code and the same answers must issue the same calls in the same order, and end the same way.
/// It must offer no clock, timers, randomness or I/O, and must not let a script reach the host.
pub trait ScriptRuntime: Send + Sync {
    /// The runtime's name and version, part of the code mode agent's
    /// [`binding`](crate::Agent::binding): a stored run resumes only under the same runtime.
    fn binding(&self) -> Value;
    /// A fresh interpreter holding `code`, not yet run, with one function per entry of
    /// `functions`. `cancel` fires when the turn ends, however it ended; the interpreter must
    /// stop running the script promptly once it has.
    fn start(
        &self,
        code: &str,
        functions: &[ScriptFunction],
        limits: &ScriptLimits,
        cancel: CancellationToken,
    ) -> Result<Box<dyn ScriptSession>>;
}

/// One running script. A session is used on one thread and dropped when its turn ends.
pub trait ScriptSession {
    /// Settles the calls `answers` names, in order, then runs the script until it finishes or
    /// every task waits on a call not yet answered. The first call, with no answers, starts the
    /// script.
    ///
    /// Fails only when the interpreter cannot go on, such as when `cancel` fired; a script's own
    /// failure is [`Step::Threw`].
    fn advance(&mut self, answers: Vec<Answer>) -> Result<Step>;
    /// What the script logged so far, one entry per line, within
    /// [`ScriptLimits::max_log_bytes`].
    fn logs(&mut self) -> Vec<String>;
}

/// Where a script stopped, from [`ScriptSession::advance`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Step {
    /// Every task waits on a call. Lists the calls issued since the previous step, in issue
    /// order; it may be empty when the script waits only on calls issued before.
    Blocked(Vec<ScriptCall>),
    /// The script returned this value, with every call it made answered.
    Returned(Value),
    /// The script failed.
    Threw {
        /// A description of why.
        error: String,
        /// The calls issued since the previous step. None of them is answered or dispatched.
        issued: Vec<ScriptCall>,
    },
}

/// One function a script can call, and the tool it calls.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScriptFunction {
    /// The name the script calls it by, a valid identifier.
    pub identifier: String,
    /// The [`ToolSpec::name`](crate::ToolSpec::name) of the tool it calls.
    pub tool: String,
}

/// One call a script issued. Calls are numbered from zero in the order a session issues them,
/// across the whole session; [`Answer::call`] names one by that number.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScriptCall {
    /// The [`ToolSpec::name`](crate::ToolSpec::name) of the tool called.
    pub tool: String,
    /// The call's arguments.
    pub arguments: Value,
}

/// The result of one call, given to [`ScriptSession::advance`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Answer {
    /// The call's number in issue order.
    pub call: usize,
    /// The call's result, or a description of its failure when `is_error` is set.
    pub value: Value,
    /// Whether the call failed. A failed call throws in the script.
    pub is_error: bool,
}

/// Bounds on one script, beside the limits of the run it belongs to. They are part of the
/// agent's binding.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ScriptLimits {
    /// Bytes the interpreter may allocate. Default 64 MiB.
    pub max_memory_bytes: usize,
    /// Bytes of log lines kept; later lines are dropped and counted. Default 16 KiB.
    pub max_log_bytes: usize,
    /// Bytes of the script's encoded result; a larger result is a failure of the script.
    /// Default 128 KiB.
    pub max_result_bytes: usize,
}
impl Default for ScriptLimits {
    fn default() -> Self {
        Self {
            max_memory_bytes: 64 * 1024 * 1024,
            max_log_bytes: 16 * 1024,
            max_result_bytes: 128 * 1024,
        }
    }
}
impl ScriptLimits {
    /// Sets [`max_memory_bytes`](Self::max_memory_bytes).
    pub fn with_max_memory_bytes(mut self, bytes: usize) -> Self {
        self.max_memory_bytes = bytes;
        self
    }
    /// Sets [`max_log_bytes`](Self::max_log_bytes).
    pub fn with_max_log_bytes(mut self, bytes: usize) -> Self {
        self.max_log_bytes = bytes;
        self
    }
    /// Sets [`max_result_bytes`](Self::max_result_bytes).
    pub fn with_max_result_bytes(mut self, bytes: usize) -> Self {
        self.max_result_bytes = bytes;
        self
    }
}

/// Log lines within a byte cap: once a line does not fit, it and every later line are dropped
/// and counted, and the count is reported as a last line. For runtimes to share.
#[derive(Debug, Default)]
pub struct Logs {
    max_bytes: usize,
    bytes: usize,
    lines: Vec<String>,
    dropped: usize,
}
impl Logs {
    /// An empty log that keeps up to `max_bytes` bytes of lines.
    pub fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            ..Self::default()
        }
    }
    /// Appends one line, or counts it as dropped.
    pub fn push(&mut self, line: String) {
        if self.dropped == 0 && self.bytes.saturating_add(line.len()) <= self.max_bytes {
            self.bytes += line.len();
            self.lines.push(line);
        } else {
            self.dropped += 1;
        }
    }
    /// The kept lines, then a line counting the dropped ones, if any.
    pub fn lines(&self) -> Vec<String> {
        let mut lines = self.lines.clone();
        if self.dropped > 0 {
            lines.push(format!("[{} more log lines dropped]", self.dropped));
        }
        lines
    }
}
