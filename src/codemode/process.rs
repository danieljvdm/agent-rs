//! [`Subprocess`]: scripts run in QuickJS in a separate process, outside the host's address space.
//!
//! Each session is one process, started by [`ScriptRuntime::start`] and killed when the session
//! ends or its turn's `cancel` fires. Host and process exchange one JSON message per line over
//! the process's standard input and output:
//!
//! - the host sends `start` with the code, functions and limits; the process answers `ready`
//!   with its interpreter's binding, which must equal the host's, or `error`;
//! - each `advance` with answers is answered with a `step` or an `error`, and `logs` with the
//!   log lines.
//!
//! The process runs [`serve`], which is all the `agent-rs-quickjs` binary does; an application
//! can call it from its own binary instead.
use super::QuickJs;
use super::runtime::{Answer, ScriptFunction, ScriptLimits, ScriptRuntime, ScriptSession, Step};
use crate::{Error, Result};
use futures::{channel::oneshot, future};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    ffi::OsString,
    io::{BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
    sync::{Arc, Mutex, mpsc},
    thread::JoinHandle,
};
use tokio_util::sync::CancellationToken;

/// One message from the host to a script process.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Request {
    Start {
        code: String,
        functions: Vec<ScriptFunction>,
        limits: ScriptLimits,
    },
    Advance(Vec<Answer>),
    Logs,
}
/// One message from a script process to the host.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Reply {
    Ready { binding: Value },
    Step(Step),
    Logs(Vec<String>),
    Error(String),
}

/// What a process must report to serve this host: the interpreter it runs, and the version of
/// the crate that built it.
fn process_binding() -> Value {
    json!({"interpreter": QuickJs.binding(), "agent-rs": env!("CARGO_PKG_VERSION")})
}

/// Serves one script session over `input` and `output`, as a [`Subprocess`] process: reads the
/// `start` request, runs the script in [`QuickJs`], and answers each request until `input`
/// ends. The process needs nothing else; call it from `main` with the standard input and output.
///
/// `input` is read on a thread of its own. When it ends, because the host closed it or exited
/// without doing so, the interpreter is interrupted, so a script cannot outlive its host. Fails
/// only when `input` or `output` fails or a request is not valid; a script's own failure is
/// reported to the host.
pub fn serve(input: impl Read + Send + 'static, mut output: impl Write) -> Result<()> {
    let mut send = |reply: &Reply| -> Result<()> {
        let mut line = serde_json::to_vec(reply).map_err(Error::provider)?;
        line.push(b'\n');
        output.write_all(&line).map_err(Error::provider)?;
        output.flush().map_err(Error::provider)
    };
    let cancel = CancellationToken::new();
    let (requests, received) = mpsc::channel::<Result<Request>>();
    {
        let cancel = cancel.clone();
        std::thread::spawn(move || {
            for line in BufReader::new(input).lines() {
                let request = (line.map_err(Error::provider))
                    .and_then(|line| serde_json::from_str(&line).map_err(Error::provider));
                let failed = request.is_err();
                if requests.send(request).is_err() || failed {
                    break;
                }
            }
            // The host is gone, or will not be heard from: stop the script.
            cancel.cancel();
        });
    }
    let read = || received.recv().ok().transpose();
    let Some(Request::Start {
        code,
        functions,
        limits,
    }) = read()?
    else {
        return Err(Error::provider("a script process must start with `start`"));
    };
    let mut session = match QuickJs.start(&code, &functions, &limits, cancel) {
        Ok(session) => session,
        Err(error) => return send(&Reply::Error(error.to_string())),
    };
    send(&Reply::Ready {
        binding: process_binding(),
    })?;
    while let Some(request) = read()? {
        let reply = match request {
            Request::Advance(answers) => match session.advance(answers) {
                Ok(step) => Reply::Step(step),
                Err(error) => Reply::Error(error.to_string()),
            },
            Request::Logs => Reply::Logs(session.logs()),
            Request::Start { .. } => {
                return Err(Error::provider("a script process starts only once"));
            }
        };
        send(&reply)?;
    }
    Ok(())
}

/// Runs scripts in [`QuickJs`] in a separate process per session, started from `program`.
///
/// A defect in the interpreter, or a script that exhausts the process, then ends that process
/// and fails the turn, and cannot reach the host's memory. Each session costs a process start.
/// The process is killed when its turn's deadline passes, the run is cancelled, or the session
/// ends; if the host exits first, the process stops once its input closes. It inherits none of
/// the host's environment (see [`env`](Self::env)), and its standard error is discarded.
///
/// `program` is the `agent-rs-quickjs` binary, built with the `codemode` feature (`cargo install
/// agent-rs --features codemode --bin agent-rs-quickjs`), or any binary that calls [`serve`]. It
/// must be built from the same version of this crate: a process that reports another
/// interpreter fails the turn. The program's path is not part of the binding, so it may move
/// between deployments.
#[derive(Clone, Debug)]
pub struct Subprocess {
    program: PathBuf,
    args: Vec<OsString>,
    env: Vec<(OsString, OsString)>,
}
impl Subprocess {
    /// A runtime that starts `program` with no arguments. A bare name, such as
    /// `agent-rs-quickjs` installed in `~/.cargo/bin`, is looked up on the host's `PATH` each
    /// time a process starts, since the process itself gets no environment.
    pub fn new(program: impl Into<PathBuf>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            env: Vec::new(),
        }
    }
    /// Adds an argument to pass to the program, such as a flag that makes an application's own
    /// binary call [`serve`].
    pub fn arg(mut self, arg: impl Into<OsString>) -> Self {
        self.args.push(arg.into());
        self
    }
    /// Sets one environment variable of the process. The process inherits none of the host's,
    /// so its secrets stay out of the process; set here what the program needs.
    pub fn env(mut self, key: impl Into<OsString>, value: impl Into<OsString>) -> Self {
        self.env.push((key.into(), value.into()));
        self
    }
}
impl ScriptRuntime for Subprocess {
    fn binding(&self) -> Value {
        json!({"process": process_binding()})
    }
    fn start(
        &self,
        code: &str,
        functions: &[ScriptFunction],
        limits: &ScriptLimits,
        cancel: CancellationToken,
    ) -> Result<Box<dyn ScriptSession>> {
        let mut session = ProcessSession::spawn(self, limits, cancel)?;
        let reply = session.request(&Request::Start {
            code: code.to_owned(),
            functions: functions.to_vec(),
            limits: limits.clone(),
        })?;
        match reply {
            Reply::Ready { binding } if binding == process_binding() => Ok(Box::new(session)),
            Reply::Ready { binding } => Err(Error::provider(format!(
                "the script process runs {binding}, not {}",
                process_binding()
            ))),
            Reply::Error(error) => Err(Error::provider(error)),
            _ => Err(Error::provider("the script process did not start")),
        }
    }
}

/// `program` as given if it names a path; a bare name, the first executable file of that name in
/// the directories of `path`. The process's environment is cleared, and the lookup a `Command`
/// makes uses the process's `PATH`, so it is done here with the host's instead. A name found
/// nowhere is returned as given, for the spawn to report.
fn resolve(program: &Path, path: Option<OsString>) -> PathBuf {
    let bare = program.components().count() == 1 && !program.is_absolute();
    let Some(path) = path.filter(|_| bare) else {
        return program.to_path_buf();
    };
    let executable = |candidate: &Path| {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            candidate
                .metadata()
                .is_ok_and(|meta| meta.is_file() && meta.permissions().mode() & 0o111 != 0)
        }
        #[cfg(not(unix))]
        {
            candidate.is_file()
        }
    };
    for directory in std::env::split_paths(&path) {
        let candidate = directory.join(program);
        if executable(&candidate) {
            return candidate;
        }
        if cfg!(windows) && program.extension().is_none() {
            let candidate = candidate.with_extension("exe");
            if executable(&candidate) {
                return candidate;
            }
        }
    }
    program.to_path_buf()
}

fn interrupted() -> Error {
    Error::provider("script interrupted: its turn ended")
}

/// One script process. The watcher kills it if `cancel` fires before the session ends.
struct ProcessSession {
    child: Arc<Mutex<Child>>,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    cancel: CancellationToken,
    /// Bytes a reply may take; the process cannot hold a larger value.
    max_reply_bytes: usize,
    ended: Option<oneshot::Sender<()>>,
    watcher: Option<JoinHandle<()>>,
}
impl ProcessSession {
    fn spawn(
        runtime: &Subprocess,
        limits: &ScriptLimits,
        cancel: CancellationToken,
    ) -> Result<Self> {
        let program = resolve(&runtime.program, std::env::var_os("PATH"));
        let mut child = Command::new(&program)
            .args(&runtime.args)
            .env_clear()
            .envs(runtime.env.iter().map(|(key, value)| (key, value)))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| {
                Error::provider(format!(
                    "cannot start the script process {}: {error}",
                    program.display()
                ))
            })?;
        let (Some(stdin), Some(stdout)) = (child.stdin.take(), child.stdout.take()) else {
            let _ = child.kill();
            let _ = child.wait();
            return Err(Error::provider("the script process has no pipes"));
        };
        let child = Arc::new(Mutex::new(child));
        let (ended, end) = oneshot::channel();
        let watcher = {
            let (child, cancel) = (child.clone(), cancel.clone());
            std::thread::spawn(move || {
                let cancelled = Box::pin(cancel.cancelled_owned());
                if let future::Either::Left(_) =
                    futures::executor::block_on(future::select(cancelled, end))
                {
                    let _ = child.lock().unwrap_or_else(|p| p.into_inner()).kill();
                }
            })
        };
        Ok(Self {
            child,
            stdin,
            stdout: BufReader::new(stdout),
            cancel,
            max_reply_bytes: limits.max_memory_bytes.max(1 << 20).saturating_add(1 << 16),
            ended: Some(ended),
            watcher: Some(watcher),
        })
    }
    /// Why the process stopped answering.
    fn lost(&self) -> Error {
        if self.cancel.is_cancelled() {
            interrupted()
        } else {
            Error::provider("the script process exited")
        }
    }
    fn request(&mut self, request: &Request) -> Result<Reply> {
        if self.cancel.is_cancelled() {
            return Err(interrupted());
        }
        let mut line = serde_json::to_vec(request).map_err(Error::provider)?;
        line.push(b'\n');
        if self
            .stdin
            .write_all(&line)
            .and_then(|()| self.stdin.flush())
            .is_err()
        {
            return Err(self.lost());
        }
        let mut reply = Vec::new();
        let limit = self.max_reply_bytes as u64 + 1;
        match (&mut self.stdout).take(limit).read_until(b'\n', &mut reply) {
            Ok(0) | Err(_) => return Err(self.lost()),
            Ok(_) if reply.last() != Some(&b'\n') => {
                return Err(if reply.len() > self.max_reply_bytes {
                    Error::provider("the script process's reply exceeds its limit")
                } else {
                    self.lost()
                });
            }
            Ok(_) => {}
        }
        serde_json::from_slice(&reply).map_err(Error::provider)
    }
}
impl ScriptSession for ProcessSession {
    fn advance(&mut self, answers: Vec<Answer>) -> Result<Step> {
        match self.request(&Request::Advance(answers))? {
            Reply::Step(step) => Ok(step),
            Reply::Error(error) => Err(Error::provider(error)),
            _ => Err(Error::provider("the script process answered out of turn")),
        }
    }
    fn logs(&mut self) -> Vec<String> {
        match self.request(&Request::Logs) {
            Ok(Reply::Logs(lines)) => lines,
            _ => vec!["[the script process did not return its logs]".into()],
        }
    }
}
impl Drop for ProcessSession {
    fn drop(&mut self) {
        if let Some(ended) = self.ended.take() {
            let _ = ended.send(());
        }
        let mut child = self.child.lock().unwrap_or_else(|p| p.into_inner());
        let _ = child.kill();
        let _ = child.wait();
        drop(child);
        if let Some(watcher) = self.watcher.take() {
            let _ = watcher.join();
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::resolve;
    use std::path::{Path, PathBuf};

    #[test]
    fn a_bare_name_is_found_on_the_given_path_and_a_path_is_kept() {
        let path = || Some("/nonexistent:/bin".into());
        assert_eq!(resolve(Path::new("sh"), path()), PathBuf::from("/bin/sh"));
        assert_eq!(
            resolve(Path::new("/bin/sh"), path()),
            PathBuf::from("/bin/sh")
        );
        assert_eq!(resolve(Path::new("./sh"), path()), PathBuf::from("./sh"));
        assert_eq!(
            resolve(Path::new("missing-program"), path()),
            PathBuf::from("missing-program")
        );
        assert_eq!(resolve(Path::new("sh"), None), PathBuf::from("sh"));
        // A directory of the name is not a program.
        assert_eq!(
            resolve(Path::new("bin"), Some("/".into())),
            PathBuf::from("bin")
        );
    }
}
