//! Replays a script against the rounds its run recorded, independent of the interpreter.
//!
//! A code mode run's history is its input, `{code}`, followed by one exchange per round of calls
//! the script made. Each turn starts the script afresh and answers every call it makes with the
//! recorded result, in the order recorded, until the history runs out. Then the script's next
//! calls are the turn's tool calls, or its end is the turn's final output.
use super::runtime::{Answer, ScriptCall, ScriptSession, Step};
use crate::{Error, HistoryEntry, Result, ToolCall, ToolResult};
use serde::Serialize;
use serde_json::{Value, json};

/// One recorded round: the calls dispatched together, and their results in call order.
pub(crate) struct Round<'a> {
    pub(crate) calls: &'a [ToolCall],
    pub(crate) results: &'a [ToolResult],
}
/// What a code mode run's history records: its code and its rounds.
pub(crate) struct Journal<'a> {
    pub(crate) code: &'a str,
    pub(crate) rounds: Vec<Round<'a>>,
}

/// The ID of the `n`th dispatched call of a run, stable across replays.
pub(crate) fn call_id(n: usize) -> String {
    format!("c{n}")
}

/// Reads the journal from a turn's history: the run's input, then its exchanges, whose call IDs
/// must run on from `c0` without a gap.
pub(crate) fn journal(history: &[HistoryEntry]) -> Result<Journal<'_>> {
    let Some((HistoryEntry::Input(input), exchanges)) = history.split_first() else {
        return Err(Error::incompatible(
            "code mode history does not start with its input",
        ));
    };
    let code = input
        .get("code")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::incompatible("code mode input has no code"))?;
    let mut rounds = Vec::with_capacity(exchanges.len());
    let mut next = 0;
    for entry in exchanges {
        let HistoryEntry::Exchange { response, results } = entry else {
            return Err(Error::incompatible(
                "code mode history holds an entry other than an exchange",
            ));
        };
        if response.output.is_some() || response.tools.len() != results.len() {
            return Err(Error::incompatible(
                "code mode history holds an incomplete round",
            ));
        }
        for (call, result) in response.tools.iter().zip(results) {
            if call.id != call_id(next) || result.call_id != call.id {
                return Err(Error::incompatible(
                    "code mode history has a gap: earlier rounds were dropped",
                ));
            }
            next += 1;
        }
        rounds.push(Round {
            calls: &response.tools,
            results,
        });
    }
    Ok(Journal { code, rounds })
}

/// How one call of a script that threw ended, for its error result.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct CallRecord {
    pub(crate) tool: String,
    /// The call's ID, if it was dispatched.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) id: Option<String>,
    pub(crate) status: CallStatus,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CallStatus {
    /// Dispatched, with a successful result.
    Succeeded,
    /// Dispatched, with an error result.
    Failed,
    /// Refused before dispatch: an unknown tool or invalid arguments.
    Rejected,
    /// Issued but never dispatched, because the script ended first.
    NotDispatched,
}

/// How a turn of a script ends.
#[derive(Debug, PartialEq)]
pub(crate) enum Outcome {
    /// The script waits on these new calls, numbered on from the recorded ones.
    Calls(Vec<ToolCall>),
    /// The script returned.
    Returned { result: Value, logs: Vec<String> },
    /// The script threw.
    Threw {
        error: String,
        logs: Vec<String>,
        calls: Vec<CallRecord>,
        /// Calls made beyond the first `MAX_RECORDED_CALLS`, counted but not listed.
        omitted: usize,
    },
}
impl Outcome {
    /// The final output of a script that ended: `{result, logs}` or `{error, logs, calls}`.
    pub(crate) fn output(self) -> Option<Value> {
        match self {
            Self::Calls(_) => None,
            Self::Returned { result, logs } => Some(json!({"result": result, "logs": logs})),
            Self::Threw {
                error,
                logs,
                calls,
                omitted,
            } => {
                let mut output = json!({"error": error, "logs": logs, "calls": calls});
                if omitted > 0 {
                    output["calls_omitted"] = json!(omitted);
                }
                Some(output)
            }
        }
    }
}

/// Calls listed in a thrown script's result; later ones are only counted.
pub(crate) const MAX_RECORDED_CALLS: usize = 100;
/// Calls refused before dispatch that a script may make. Refusals spend no budget, so a script
/// that makes more ends, rather than loop on them until its deadline.
pub(crate) const MAX_REFUSED_CALLS: usize = 1_000;

/// How each call of a script ended, for the result of a script that throws.
#[derive(Default)]
struct Record {
    calls: Vec<CallRecord>,
    omitted: usize,
}
impl Record {
    fn push(&mut self, tool: String, id: Option<String>, status: CallStatus) {
        if self.calls.len() < MAX_RECORDED_CALLS {
            self.calls.push(CallRecord { tool, id, status });
        } else {
            self.omitted += 1;
        }
    }
    /// The outcome of a script that threw `error`, with the calls still unanswered.
    fn threw(
        mut self,
        error: String,
        logs: Vec<String>,
        unanswered: impl IntoIterator<Item = ScriptCall>,
    ) -> Outcome {
        for call in unanswered {
            self.push(call.tool, None, CallStatus::NotDispatched);
        }
        Outcome::Threw {
            error: bounded(error),
            logs,
            calls: self.calls,
            omitted: self.omitted,
        }
    }
}

/// Bytes of a script's error kept in its result; a deep stack can make it far longer.
const MAX_ERROR_BYTES: usize = 4_096;
fn bounded(mut error: String) -> String {
    if error.len() > MAX_ERROR_BYTES {
        let mut end = MAX_ERROR_BYTES;
        while !error.is_char_boundary(end) {
            end -= 1;
        }
        error.truncate(end);
        error.push_str("… (truncated)");
    }
    error
}
fn diverged() -> Error {
    Error::incompatible("script diverged from its recorded calls")
}

/// Runs `session` against `journal`. Each call the script issues is first `check`ed: a refused
/// call is answered at once with the refusal as an error, and is never dispatched or recorded.
/// The calls still waiting once nothing else can run form a round, which must match the next
/// recorded round by count, tool and arguments; ending while rounds remain also diverges.
pub(crate) fn replay(
    session: &mut dyn ScriptSession,
    journal: &Journal<'_>,
    check: &dyn Fn(&ScriptCall) -> std::result::Result<(), String>,
) -> Result<Outcome> {
    let mut rounds = journal.rounds.iter();
    let mut answers = Vec::new();
    // Calls issued so far, by number, and those waiting for the next round.
    let mut issued = 0;
    let mut waiting: Vec<(usize, ScriptCall)> = Vec::new();
    let mut dispatched = 0;
    let mut record = Record::default();
    let mut refused = 0;
    loop {
        match session.advance(std::mem::take(&mut answers))? {
            Step::Blocked(calls) => {
                for call in calls {
                    let number = issued;
                    issued += 1;
                    match check(&call) {
                        Ok(()) => waiting.push((number, call)),
                        Err(reason) => {
                            answers.push(Answer {
                                call: number,
                                value: Value::String(reason),
                                is_error: true,
                            });
                            record.push(call.tool, None, CallStatus::Rejected);
                            refused += 1;
                        }
                    }
                }
                if refused > MAX_REFUSED_CALLS {
                    if rounds.next().is_some() {
                        return Err(diverged());
                    }
                    let error = format!(
                        "the script made more than {MAX_REFUSED_CALLS} calls that were refused \
                         before dispatch"
                    );
                    let unanswered = waiting.into_iter().map(|(_, call)| call);
                    return Ok(record.threw(error, session.logs(), unanswered));
                }
                if !answers.is_empty() {
                    continue;
                }
                if waiting.is_empty() {
                    return Err(Error::provider(
                        "script runtime blocked without a call to wait on",
                    ));
                }
                let Some(round) = rounds.next() else {
                    let calls = (waiting.into_iter().enumerate())
                        .map(|(offset, (_, call))| ToolCall {
                            id: call_id(dispatched + offset),
                            name: call.tool,
                            arguments: call.arguments,
                        })
                        .collect();
                    return Ok(Outcome::Calls(calls));
                };
                let matches = round.calls.len() == waiting.len()
                    && (round.calls.iter().zip(&waiting)).all(|(recorded, (_, call))| {
                        recorded.name == call.tool && recorded.arguments == call.arguments
                    });
                if !matches {
                    return Err(diverged());
                }
                for ((number, call), (recorded, result)) in
                    waiting.drain(..).zip(round.calls.iter().zip(round.results))
                {
                    answers.push(Answer {
                        call: number,
                        value: result.value.clone(),
                        is_error: result.is_error,
                    });
                    let status = if result.is_error {
                        CallStatus::Failed
                    } else {
                        CallStatus::Succeeded
                    };
                    record.push(call.tool, Some(recorded.id.clone()), status);
                }
                dispatched += round.calls.len();
            }
            step @ (Step::Returned(_) | Step::Threw { .. }) => {
                if rounds.next().is_some() {
                    return Err(diverged());
                }
                let logs = session.logs();
                let (error, issued) = match step {
                    // A runtime must not report a return while calls wait; treat it as the
                    // un-awaited calls it is.
                    Step::Returned(_) if !waiting.is_empty() => {
                        (format!("{} calls were not awaited", waiting.len()), vec![])
                    }
                    Step::Returned(result) => return Ok(Outcome::Returned { result, logs }),
                    Step::Threw { error, issued } => (error, issued),
                    Step::Blocked(_) => unreachable!("matched above"),
                };
                let unanswered = waiting.into_iter().map(|(_, call)| call).chain(issued);
                return Ok(record.threw(error, logs, unanswered));
            }
        }
    }
}
