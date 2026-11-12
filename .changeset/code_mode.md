---
default: minor
---

# Add code mode

With the `codemode` feature, `codemode::CodeMode` builds a subagent the model calls with JavaScript that calls the agent's other tools as async functions and returns one value. Each round of calls is a durable tool batch of the script's child run, so calls keep their authorization, approval, retry policy, budgets and crash recovery; each turn replays the script in a fresh, deterministic QuickJS interpreter against the recorded rounds. See `docs/codemode.md`. `codemode::Subprocess` runs the interpreter in a process of its own per turn, the `agent-rs-quickjs` binary or any binary that calls `codemode::serve`.
