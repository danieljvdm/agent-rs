# Profiling recovery allocations

Recovery after 512 completed runs in one conversation retains 1,024 context entries. The initial allocation profile attributed 76.1 MB of 109.3 MB to decoding checkpoints: every read constructed a complete JSON `Value` tree and then converted it into `RunState`.

The decoder now tries `RunState` directly. It accepts that result only when the state version matches and serializing it reproduces the original bytes exactly. Other representations still use the previous decoder. This preserves handling of duplicate keys, unknown fields, alternate formatting, invalid versions, and malformed data without changing the checkpoint format or transaction protocol. The compatibility tests exercise these behaviors through a reopened SQLite store.

The [retained profiles and matched comparison](../benchmark-results/recovery-2026-10-29/README.md) record the results. Allocation profiles identify where work happens; ordinary uninstrumented benchmarks establish elapsed-time changes.

## Capture an allocation profile

`profile_recovery` is a development-only example using [DHAT](https://docs.rs/dhat/0.3.3/dhat/). Its allocator is confined to that executable. It reuses the existing benchmark fixture and asserts the retained context, final output, four resumed model calls, three tool calls, and their cleanup counts.

```sh
CARGO_PROFILE_RELEASE_DEBUG=1 CARGO_TARGET_DIR=/tmp/agent-rs-profile-target \
  scripts/rust.sh run --locked --release --example profile_recovery -- \
  --prior-runs 512 --out /tmp/agent-rs-profile-512
```

Use `--prior-runs 0` for the short-history control. The output directory must be new and its parent must exist. Separate target directories keep profiling debug information out of normal timing builds.

The profiling window matches the diagnostic benchmark: SQLite open, claim, and engine resume through completed run-owned cleanup. Runtime creation, fixture seeding, database copying, final assertions, reporting, and store shutdown are outside that window. This is one interrupted run resuming after prior completed runs, not 512 simultaneous recoveries.

`dhat.json` contains allocation stacks for the DHAT viewer; `summary.json` records totals and work counts. Total bytes are cumulative allocation/reallocation traffic. Peak/live bytes cover Rust allocations originating inside this profiling window, not whole-process RSS or SQLite's direct C allocations. Profiling timings are intentionally invalid as performance evidence.

## Run a focused matched comparison

The controller's `recovery` suite uses unchanged v2 fixtures for four workloads: minimal in-memory completion, ordinary durable execution, and recovery with 0 or 512 prior runs. Each side must contain identical fixture bytes. The default `all` suite still covers all 19 timed workloads.

```sh
RUSTUP_TOOLCHAIN=1.99.1 PATH="$HOME/.cargo/bin:$PATH" \
  python3 scripts/benchmark.py \
  --base /absolute/base-checkout --candidate /absolute/candidate-checkout \
  --out /tmp/agent-rs-recovery-comparison \
  --suite recovery --cohorts 2 --samples 30 --require-clean
```

Both revisions build before measurement, followed by sequential AB/BA captures. CPU and allocation samples use separate executables. The focused suite retains each filtered diagnostic report before merging it; missing cases, duplicate cases, mismatched metadata and changed work counts fail verification. The manual GitHub workflow exposes the same `suite` option.

Inspect raw normal elapsed-time samples and their tails as well as Criterion medians. Improved medians can coexist with new slow samples. Preserve such samples and reproduce a suspected regression on a fresh matched runner before attributing it to the code or dismissing it as noise.
