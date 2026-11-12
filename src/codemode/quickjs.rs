//! [`QuickJs`]: scripts run in an in-process QuickJS interpreter, through `rquickjs`.
//!
//! Each session is a fresh runtime with one context. The context has the language's base objects,
//! `eval`, regular expressions, JSON, maps and sets, promises and proxies, and nothing else: no
//! `Date`, typed arrays, shared memory, `WeakRef`, `FinalizationRegistry` or `performance`, and
//! `Math.random` is removed. A bare context has no timers or I/O. What remains is deterministic,
//! so a replay with the same answers issues the same calls.
//!
//! A small prelude, run before the script, defines one global function per tool and a host object
//! that only Rust holds. It captures every built-in it uses before the script runs, so a script
//! that replaces built-ins or their prototypes cannot change what the bridge reports.
use super::runtime::{
    Answer, Logs, ScriptCall, ScriptFunction, ScriptLimits, ScriptRuntime, ScriptSession, Step,
};
use crate::{Error, Result};
use rquickjs::{
    Context, Ctx, Function, Object, Persistent, Promise, Runtime,
    context::{EvalOptions, intrinsic},
    promise::PromiseState,
};
use serde_json::{Value, json};
use std::{cell::RefCell, rc::Rc};
use tokio_util::sync::CancellationToken;

/// The built-ins a script's context has; see the module documentation.
type Intrinsics = (
    intrinsic::Eval,
    intrinsic::RegExpCompiler,
    intrinsic::RegExp,
    intrinsic::Json,
    intrinsic::MapSet,
    intrinsic::Promise,
    intrinsic::Proxy,
);

/// Runs scripts in QuickJS (quickjs-ng, through `rquickjs`), in the host's address space.
///
/// The interpreter is bounded by [`ScriptLimits::max_memory_bytes`] and stops when its turn's
/// deadline passes or the run is cancelled, but it shares the host's process: a defect in the
/// interpreter is a defect of the host.
#[derive(Clone, Copy, Debug, Default)]
pub struct QuickJs;
impl ScriptRuntime for QuickJs {
    fn binding(&self) -> Value {
        json!({"name": "quickjs-ng", "version": "0.16", "bindings": "rquickjs 0.14", "prelude": 1})
    }
    fn start(
        &self,
        code: &str,
        functions: &[ScriptFunction],
        limits: &ScriptLimits,
        cancel: CancellationToken,
    ) -> Result<Box<dyn ScriptSession>> {
        Ok(Box::new(Session::new(code, functions, limits, cancel)?))
    }
}

/// Defines the tool functions and console, then returns the host object. Runs in strict mode
/// before the script, with the tool functions' identifiers and tool names as JSON text and the
/// function that appends a log line.
const PRELUDE: &str = r#"(function (functionsJson, appendLog) {
  const global = globalThis;
  // `uncurry(method)(target, ...args)` calls `method` on `target`, whatever later replaces it.
  const call = Function.prototype.call;
  const uncurry = (method) => call.bind(method);
  const { stringify, parse } = JSON;
  const { defineProperty, freeze, getOwnPropertyNames } = Object;
  const SafeError = Error, SafeTypeError = TypeError, SafePromise = Promise, SafeMap = Map;
  const SafeString = String;
  const rejected = Promise.reject.bind(Promise);
  const mapGet = uncurry(Map.prototype.get), mapSet = uncurry(Map.prototype.set);
  const mapDelete = uncurry(Map.prototype.delete);
  const isError = (value) => value instanceof SafeError;

  for (const name of ["Date", "WeakRef", "FinalizationRegistry", "performance", "Atomics",
                      "SharedArrayBuffer"]) {
    delete global[name];
  }
  delete Math.random;

  // Calls by number: [resolve, reject, tool] while waiting, and the issued calls not yet taken.
  const settlers = new SafeMap();
  const issued = new SafeMap();
  let count = 0, taken = 0, waiting = 0;

  const define = (name, value) => {
    defineProperty(global, name, { value: freeze(value), writable: false, enumerable: false,
                                   configurable: false });
  };
  const toolError = (tool, value) => {
    let message = typeof value === "string" ? value : stringify(value);
    if (message === undefined) message = "tool call failed";
    const error = new SafeError(message);
    defineProperty(error, "name", { value: "ToolError", writable: true, configurable: true });
    defineProperty(error, "tool", { value: tool, writable: true, enumerable: true,
                                    configurable: true });
    defineProperty(error, "value", { value, writable: true, enumerable: true,
                                     configurable: true });
    return error;
  };
  const tool = (identifier, name, nameJson) => function (input) {
    try {
      if (arguments.length > 1) throw new SafeTypeError(`${identifier} takes one argument`);
      const json = stringify(input === undefined ? {} : input);
      if (json === undefined) throw new SafeTypeError(`${identifier}: the argument is not JSON`);
      return new SafePromise((resolve, reject) => {
        const number = count++;
        mapSet(settlers, number, [resolve, reject, name]);
        mapSet(issued, number, "[" + nameJson + "," + json + "]");
        waiting++;
      });
    } catch (error) {
      return rejected(error);
    }
  };
  for (const [identifier, name] of parse(functionsJson)) {
    define(identifier, tool(identifier, name, stringify(name)));
  }
  const text = (value) => {
    if (typeof value === "string") return value;
    try {
      const json = stringify(value);
      if (json !== undefined) return json;
    } catch {}
    return SafeString(value);
  };
  const log = (...values) => {
    let line = "";
    for (let i = 0; i < values.length; i++) line += (i > 0 ? " " : "") + text(values[i]);
    appendLog(line);
  };
  define("console", { log, info: log, warn: log, error: log, debug: log });

  const describe = (reason) => {
    if (isError(reason)) {
      const head = `${reason.name}: ${reason.message}`;
      const stack = reason.stack;
      return typeof stack === "string" && stack.length > 0 ? `${head}\n${stack}` : head;
    }
    return text(reason);
  };
  return freeze({
    take() {
      let calls = "[";
      for (; taken < count; taken++) {
        calls += (calls.length > 1 ? "," : "") + mapGet(issued, taken);
        mapDelete(issued, taken);
      }
      return calls + "]";
    },
    waiting() {
      return waiting;
    },
    settle(number, ok, json) {
      const settler = mapGet(settlers, number);
      if (settler === undefined) throw new SafeError(`call ${number} is not waiting`);
      mapDelete(settlers, number);
      waiting--;
      const value = parse(json);
      if (ok) settler[0](value); else settler[1](toolError(settler[2], value));
    },
    encode(value) {
      const json = stringify(value);
      return json === undefined ? "null" : json;
    },
    describe(reason) {
      try {
        return describe(reason);
      } catch {
        return "the script threw a value that cannot be described";
      }
    },
  });
})"#;

fn js_error(error: rquickjs::Error) -> Error {
    Error::provider(format!("script runtime: {error}"))
}
fn threw(error: impl Into<String>) -> Step {
    Step::Threw {
        error: error.into(),
        issued: Vec::new(),
    }
}
fn interrupted() -> Error {
    Error::provider("script interrupted: its turn ended")
}

/// One script in its own QuickJS runtime. The JavaScript handles are declared, and so dropped,
/// before the context and runtime they belong to.
struct Session {
    host: Persistent<Object<'static>>,
    main: Option<Persistent<Promise<'static>>>,
    code: String,
    context: Context,
    runtime: Runtime,
    logs: Rc<RefCell<Logs>>,
    cancel: CancellationToken,
    max_result_bytes: usize,
    finished: bool,
}
impl Session {
    fn new(
        code: &str,
        functions: &[ScriptFunction],
        limits: &ScriptLimits,
        cancel: CancellationToken,
    ) -> Result<Self> {
        let runtime = Runtime::new().map_err(js_error)?;
        runtime.set_memory_limit(limits.max_memory_bytes);
        let flag = cancel.clone();
        runtime.set_interrupt_handler(Some(Box::new(move || flag.is_cancelled())));
        let context = Context::custom::<Intrinsics>(&runtime).map_err(js_error)?;
        let logs = Rc::new(RefCell::new(Logs::new(limits.max_log_bytes)));
        let functions: Vec<(&str, &str)> = (functions.iter())
            .map(|f| (f.identifier.as_str(), f.tool.as_str()))
            .collect();
        let functions = serde_json::to_string(&functions).map_err(Error::provider)?;
        let sink = logs.clone();
        let host = context.with(|ctx| -> rquickjs::Result<_> {
            let install: Function = ctx.eval(PRELUDE)?;
            let append = Function::new(ctx.clone(), move |line: String| {
                sink.borrow_mut().push(line);
            })?;
            let host: Object = install.call((functions, append))?;
            Ok(Persistent::save(&ctx, host))
        });
        Ok(Self {
            host: host.map_err(js_error)?,
            main: None,
            code: code.to_owned(),
            context,
            runtime,
            logs,
            cancel,
            max_result_bytes: limits.max_result_bytes,
            finished: false,
        })
    }

    /// A description of the exception pending in `ctx`.
    fn caught<'js>(ctx: &Ctx<'js>, host: &Object<'js>) -> rquickjs::Result<String> {
        let reason = ctx.catch();
        host.get::<_, Function>("describe")?.call((reason,))
    }

    /// Settles `answers`, in order, and on the first step evaluates the script. A script that
    /// fails to evaluate has already ended.
    fn settle_and_start(&mut self, answers: Vec<Answer>) -> Result<Option<Step>> {
        let Self {
            host,
            main,
            code,
            context,
            cancel,
            ..
        } = self;
        context.with(|ctx| -> Result<Option<Step>> {
            let host = host.clone().restore(&ctx).map_err(js_error)?;
            let settle: Function = host.get("settle").map_err(js_error)?;
            for answer in answers {
                let json = serde_json::to_string(&answer.value).map_err(Error::provider)?;
                settle
                    .call::<_, ()>((answer.call, !answer.is_error, json))
                    .map_err(js_error)?;
            }
            if main.is_some() {
                return Ok(None);
            }
            let source = format!("(async () => {{\n{code}\n}})()");
            let mut options = EvalOptions::default();
            options.filename = Some("code.js".into());
            match ctx.eval_with_options::<rquickjs::Value, _>(source, options) {
                Ok(value) => match value.into_promise() {
                    Some(promise) => {
                        *main = Some(Persistent::save(&ctx, promise));
                        Ok(None)
                    }
                    None => Ok(Some(threw(
                        "the code must be the body of an async function",
                    ))),
                },
                Err(_) if cancel.is_cancelled() => Err(interrupted()),
                Err(rquickjs::Error::Exception) => {
                    let message = Self::caught(&ctx, &host).map_err(js_error)?;
                    Ok(Some(threw(message)))
                }
                Err(error) => Err(js_error(error)),
            }
        })
    }

    /// Runs every pending job. A job that fails is the script's failure.
    fn run_jobs(&self) -> Result<Option<Step>> {
        loop {
            if self.cancel.is_cancelled() {
                return Err(interrupted());
            }
            match self.runtime.execute_pending_job() {
                Ok(true) => {}
                Ok(false) => return Ok(None),
                Err(_) if self.cancel.is_cancelled() => return Err(interrupted()),
                Err(job) => {
                    let message = job.0.with(|ctx| {
                        let host = self.host.clone().restore(&ctx)?;
                        Self::caught(&ctx, &host)
                    });
                    return Ok(Some(threw(message.map_err(js_error)?)));
                }
            }
        }
    }

    /// Where the script stands once no job is pending.
    fn inspect(&self) -> Result<Step> {
        let main = self.main.clone().expect("the script was started");
        self.context.with(|ctx| -> Result<Step> {
            let host = self.host.clone().restore(&ctx).map_err(js_error)?;
            let main = main.restore(&ctx).map_err(js_error)?;
            let waiting: usize = (host.get::<_, Function>("waiting"))
                .and_then(|f| f.call(()))
                .map_err(js_error)?;
            let taken: String = (host.get::<_, Function>("take"))
                .and_then(|f| f.call(()))
                .map_err(js_error)?;
            let calls: Vec<(String, Value)> =
                serde_json::from_str(&taken).map_err(Error::provider)?;
            let calls: Vec<ScriptCall> = (calls.into_iter())
                .map(|(tool, arguments)| ScriptCall { tool, arguments })
                .collect();
            let ended = |error: String| Step::Threw {
                error,
                issued: calls.clone(),
            };
            match main.state() {
                PromiseState::Pending if !calls.is_empty() || waiting > 0 => {
                    Ok(Step::Blocked(calls))
                }
                PromiseState::Pending => Ok(ended(
                    "the script is waiting on a promise that never settles".into(),
                )),
                PromiseState::Resolved if waiting > 0 => Ok(ended(format!(
                    "{waiting} calls were not awaited; none of them was dispatched"
                ))),
                PromiseState::Resolved => {
                    let value: rquickjs::Value = match main.result() {
                        Some(Ok(value)) => value,
                        _ => return Err(Error::provider("a resolved promise has no result")),
                    };
                    let encode: Function = host.get("encode").map_err(js_error)?;
                    let json: String = match encode.call((value,)) {
                        Ok(json) => json,
                        Err(_) if self.cancel.is_cancelled() => return Err(interrupted()),
                        Err(rquickjs::Error::Exception) => {
                            let message = Self::caught(&ctx, &host).map_err(js_error)?;
                            return Ok(ended(format!(
                                "the result cannot be encoded as JSON: {message}"
                            )));
                        }
                        Err(error) => return Err(js_error(error)),
                    };
                    if json.len() > self.max_result_bytes {
                        return Ok(ended(format!(
                            "the result is {} bytes, over the limit of {}",
                            json.len(),
                            self.max_result_bytes
                        )));
                    }
                    Ok(Step::Returned(
                        serde_json::from_str(&json).map_err(Error::provider)?,
                    ))
                }
                PromiseState::Rejected => {
                    let _ = main.result::<rquickjs::Value>();
                    Ok(ended(Self::caught(&ctx, &host).map_err(js_error)?))
                }
            }
        })
    }
}
impl ScriptSession for Session {
    fn advance(&mut self, answers: Vec<Answer>) -> Result<Step> {
        if self.finished {
            return Err(Error::provider("the script has already ended"));
        }
        if self.cancel.is_cancelled() {
            return Err(interrupted());
        }
        let step = match self.settle_and_start(answers)? {
            Some(step) => step,
            None => match self.run_jobs()? {
                Some(step) => step,
                None => self.inspect()?,
            },
        };
        self.finished = !matches!(step, Step::Blocked(_));
        Ok(step)
    }
    fn logs(&mut self) -> Vec<String> {
        self.logs.borrow().lines()
    }
}
