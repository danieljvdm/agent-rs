#!/usr/bin/env python3
"""Capture release benchmarks or compare prepared checkouts in alternating AB/BA order."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import shlex
import shutil
import statistics
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_VERSION = "agent-loop-v4"
EXPECTED_BENCHMARK_CASES = {
    "engine/minimal", "engine/four_tool_rounds", "engine/context_entries_0", "engine/context_entries_1024",
    *(f"engine/tools_8_bound_{n}" for n in (1, 4, 8)),
    *(f"engine/stream_chunks_{n}" for n in (1, 64, 1024)),
    *(f"conversations/engine_workers_4/{n}" for n in (1, 16, 64)),
    "sqlite/durable_four_tool_rounds",
    *(f"sqlite/durable_retained_context/{n}" for n in (0, 1024)),
    *(f"sqlite_conversations/host_workers_4/{n}" for n in (1, 16, 64)),
    *(f"recovery/restart_to_completion/{n}" for n in (0, 512)),
}
EXPECTED_DIAGNOSTIC_CASES = {
    "minimal", "four_tool_rounds", "context_entries_0", "context_entries_1024", "durable_four_tool_rounds", "startup_sqlite",
    *(f"tools_8_bound_{n}" for n in (1, 4, 8)),
    *(f"stream_chunks_{n}{suffix}" for n in (1, 64, 1024) for suffix in ("", "_observed")),
    *(f"conversations_{kind}_{n}" for kind in ("engine", "sqlite") for n in (1, 16, 64)),
    *(f"recovery_prior_runs_{n}" for n in (0, 512)),
}
SUITES = {
    "all": {
        "benchmarks": EXPECTED_BENCHMARK_CASES,
        "diagnostics": EXPECTED_DIAGNOSTIC_CASES,
        "criterion_filter": None,
        "diagnostic_filters": (),
    },
    "recovery": {
        "benchmarks": {"engine/minimal", "sqlite/durable_four_tool_rounds",
                       "recovery/restart_to_completion/0", "recovery/restart_to_completion/512"},
        "diagnostics": {"minimal", "durable_four_tool_rounds", "recovery_prior_runs_0", "recovery_prior_runs_512"},
        "criterion_filter": r"^(engine/minimal|sqlite/durable_four_tool_rounds|recovery/restart_to_completion/(0|512))$",
        "diagnostic_filters": ("minimal", "durable_four_tool_rounds", "recovery"),
    },
}
ENVIRONMENT_FIELDS = (
    "rustc", "cargo", "os", "machine", "logical_cpus", "processor", "memory_bytes", "tokio_workers",
    "concurrency", "fixed_conversation_batch", "build_profile", "features", "rustflags", "tracing", "quick",
)

def suite_config(suite):
    if suite not in SUITES:
        raise RuntimeError(f"unknown benchmark suite: {suite}")
    return SUITES[suite]

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def command(argv, cwd, log, env=None):
    with log.open("a") as stream:
        stream.write("$ " + shlex.join(map(str, argv)) + "\n")
        stream.flush()
        process = subprocess.Popen(argv, cwd=cwd, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        output = []
        try:
            for line in process.stdout:
                stream.write(line)
                stream.flush()
                output.append(line)
            status = process.wait()
        except BaseException:
            process.terminate()
            process.wait()
            raise
    if status:
        raise RuntimeError(f"command failed ({status}); see {log}")
    return "".join(output)

def quiet(argv, cwd):
    result = subprocess.run(argv, cwd=cwd, text=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    return result.stdout.strip() if result.returncode == 0 else None

def fingerprint(repo):
    files = [repo / "Cargo.toml", repo / "Cargo.lock", repo / "rust-toolchain.toml"]
    for directory in ("src", "benches", "examples", "scripts"):
        files.extend(path for path in (repo / directory).rglob("*") if path.is_file() and "__pycache__" not in path.parts)
    hashes = {str(path.relative_to(repo)): digest(path) for path in sorted(files) if path.exists()}
    return {"sha256": hashlib.sha256(json.dumps(hashes, sort_keys=True).encode()).hexdigest(), "files": hashes}

def measurement_fingerprint(source):
    """Ignore controller/unrelated examples when deciding whether benchmarked code changed."""
    files = {path: digest for path, digest in source["files"].items()
             if path in ("Cargo.toml", "Cargo.lock", "examples/benchmark_metrics.rs")
             or path.startswith(("src/", "benches/"))}
    return hashlib.sha256(json.dumps(files, sort_keys=True).encode()).hexdigest()

def linux_machine(field):
    try:
        for line in Path("/proc/cpuinfo" if field == "model name" else "/proc/meminfo").read_text().splitlines():
            if line.startswith(field + "\t") or line.startswith(field + ":"):
                value = line.split(":", 1)[1].strip()
                return value if field == "model name" else int(value.split()[0]) * 1024
    except (OSError, ValueError):
        pass
    return None

def metadata(repo, quick):
    return {
        "fixture": FIXTURE_VERSION, "source": fingerprint(repo), "git_revision": quiet(["git", "rev-parse", "HEAD"], repo),
        "git_status": quiet(["git", "status", "--porcelain"], repo), "lockfile_sha256": digest(repo / "Cargo.lock"),
        "rustc": quiet(["rustc", "-Vv"], repo), "cargo": quiet(["cargo", "-V"], repo),
        "os": platform.platform(), "machine": platform.machine(), "logical_cpus": os.cpu_count(),
        "processor": quiet(["sysctl", "-n", "machdep.cpu.brand_string"], repo) if platform.system() == "Darwin" else linux_machine("model name"),
        "memory_bytes": quiet(["sysctl", "-n", "hw.memsize"], repo) if platform.system() == "Darwin" else linux_machine("MemTotal"),
        "github_run_id": os.environ.get("GITHUB_RUN_ID"), "github_run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
        "runner_name": os.environ.get("RUNNER_NAME"), "runner_image": os.environ.get("ImageVersion"),
        "tokio_workers": 4, "concurrency": [1, 16, 64], "fixed_conversation_batch": 64,
        "build_profile": "release", "features": "default", "rustflags": os.environ.get("RUSTFLAGS", ""),
        "tracing": "normal tracing calls; no subscriber", "quick": quick,
        "measurement": "scripted dependencies through public APIs; no network or artificial sleeps",
    }

def executable(output, name):
    for line in reversed(output.splitlines()):
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if item.get("reason") == "compiler-artifact" and item.get("target", {}).get("name") == name and item.get("executable"):
            return Path(item["executable"])
    raise RuntimeError(f"cargo did not report executable {name}")

def prepare(repo, temporary, log, quick, require_clean=False, suite="all"):
    suite_config(suite)
    repo = repo.resolve()
    if require_clean and quiet(["git", "status", "--porcelain"], repo) != "":
        raise RuntimeError(f"benchmark checkout must be a clean Git revision: {repo}")
    before = fingerprint(repo)
    bench = command(["cargo", "bench", "--locked", "--bench", "agent_loop", "--no-run", "--message-format=json"], repo, log)
    normal = command(["cargo", "build", "--locked", "--release", "--example", "benchmark_metrics", "--message-format=json"], repo, log)
    binary_dir = Path(tempfile.mkdtemp(dir=temporary))
    shutil.copy2(executable(bench, "agent_loop"), binary_dir / "agent_loop")
    shutil.copy2(executable(normal, "benchmark_metrics"), binary_dir / "metrics")
    allocated = command(["cargo", "build", "--locked", "--release", "--example", "benchmark_metrics", "--features", "allocation-metrics", "--message-format=json"], repo, log)
    shutil.copy2(executable(allocated, "benchmark_metrics"), binary_dir / "allocations")
    if before != fingerprint(repo):
        raise RuntimeError("source changed during build; captured binaries cannot identify this source")
    info = metadata(repo, quick)
    info["suite"] = suite
    info["executables"] = {path.name: digest(path) for path in sorted(binary_dir.iterdir())}
    info["measurement_source_sha256"] = measurement_fingerprint(info["source"])
    return {"repo": repo, "bin": binary_dir, "metadata": info}

def distribution(values):
    if not values or any(not math.isfinite(value) for value in values):
        raise RuntimeError("missing or non-finite measurement")
    median = statistics.median(values)
    return {"count": len(values), "median": median, "min": min(values), "max": max(values),
            "median_absolute_deviation": statistics.median(abs(value - median) for value in values)}

def summarize(output):
    entries = []
    info = json.loads((output / "metadata.json").read_text())
    expected = suite_config(info.get("suite", "all"))["benchmarks"]
    minimum_samples = 10 if info["quick"] else 30
    for estimates in sorted((output / "criterion").rglob("new/estimates.json")):
        estimate = json.loads(estimates.read_text())
        benchmark = json.loads((estimates.parent / "benchmark.json").read_text())
        elements = (benchmark.get("throughput") or {}).get("Elements", 1)
        samples = json.loads((estimates.parent / "sample.json").read_text())
        if len(samples["times"]) != len(samples["iters"]) or any(value <= 0 for value in samples["iters"]):
            raise RuntimeError("invalid Criterion iteration samples")
        normalized = [elapsed / iterations / elements for elapsed, iterations in zip(samples["times"], samples["iters"])]
        if len(normalized) < minimum_samples or any(value <= 0 for value in normalized):
            raise RuntimeError("Criterion capture has missing or invalid samples for its profile")
        median = estimate["median"]
        entries.append({"case": benchmark["full_id"], "runs_per_iteration": elements,
                        "median_ns_per_run": median["point_estimate"] / elements,
                        "median_interval_lower_ns": median["confidence_interval"]["lower_bound"] / elements,
                        "median_interval_upper_ns": median["confidence_interval"]["upper_bound"] / elements,
                        "normalized_iteration_ns_per_run": distribution(normalized)})
    if len(entries) != len(expected) or {entry["case"] for entry in entries} != expected:
        raise RuntimeError("missing, duplicate, or unexpected Criterion workloads")
    (output / "summary.json").write_text(json.dumps(entries, indent=2) + "\n")
    text = ["Scripted runtime measurement. Timing gates are not calibrated.", "", "| Workload | Median µs/completed run | Median estimate interval µs |", "| --- | ---: | ---: |"]
    for entry in entries:
        text.append(f"| {entry['case']} | {entry['median_ns_per_run']/1000:.3f} | {entry['median_interval_lower_ns']/1000:.3f}–{entry['median_interval_upper_ns']/1000:.3f} |")
    text.extend(["", "Intervals describe Criterion's median estimate, not per-operation tail latency. See metrics.json for raw CPU, latency, first-output, and database samples; allocations.json is separately instrumented. Raw Criterion samples retain outliers."])
    (output / "SUMMARY.md").write_text("\n".join(text) + "\n")
    return entries

def validate_diagnostic_samples(report, allocation_instrumented, expected):
    if report["fixture"] != FIXTURE_VERSION or report["workers"] != 4 or report["allocation_instrumented"] != allocation_instrumented:
        raise RuntimeError("diagnostic fixture, executor, or instrumentation differs")
    samples = report["samples"]
    cases = {sample["case"] for sample in samples}
    if not samples or cases != expected or report["samples_per_case"] < 1:
        raise RuntimeError("missing or unexpected diagnostic workloads")
    for case in cases:
        subset = [sample for sample in samples if sample["case"] == case]
        if len(subset) != report["samples_per_case"]:
            raise RuntimeError("missing or duplicate diagnostic samples")
        signatures = {(sample["completed_runs"], sample["model_calls"], sample["tool_calls"]) for sample in subset}
        if len(signatures) != 1:
            raise RuntimeError("diagnostic samples performed different work")
        for sample in subset:
            if sample["elapsed_ns"] <= 0 or sample["process_cpu_ns"] < 0:
                raise RuntimeError("invalid diagnostic time")
            if allocation_instrumented and (sample["allocations"] is None or sample["allocated_bytes"] is None):
                raise RuntimeError("allocation sample omitted instrumentation")
            if case.startswith("conversations_") and len(sample["operation_latencies_ns"]) != sample["completed_runs"]:
                raise RuntimeError("concurrent sample omitted individual operation latency")
    return samples

def diagnostic_samples(path, allocation_instrumented, suite="all"):
    report = json.loads(path.read_text())
    if report.get("suite", suite) != suite:
        raise RuntimeError("diagnostic suite differs from capture")
    return validate_diagnostic_samples(report, allocation_instrumented, suite_config(suite)["diagnostics"])

def merge_diagnostics(paths, output, allocation_instrumented, suite, requested_samples):
    """Retain each process's raw report; merge only complete, disjoint workloads."""
    expected = suite_config(suite)["diagnostics"]
    if not paths:
        raise RuntimeError("missing filtered diagnostic reports")
    fields = ("fixture", "workers", "allocation_instrumented", "samples_per_case")
    merged = {"suite": suite, "samples": [], "raw_reports": []}
    seen = set()
    for path in paths:
        report = json.loads(path.read_text())
        if report.get("suite", suite) != suite:
            raise RuntimeError("diagnostic suite differs from capture")
        if report["samples_per_case"] != requested_samples or any(
                field in merged and merged[field] != report[field] for field in fields):
            raise RuntimeError("diagnostic report metadata or requested sample count differs")
        cases = {sample["case"] for sample in report["samples"]}
        validate_diagnostic_samples(report, allocation_instrumented, cases)
        if seen & cases:
            raise RuntimeError("duplicate diagnostic workloads across filtered reports")
        seen.update(cases)
        merged.update({field: report[field] for field in fields})
        merged["samples"].extend(report["samples"])
        merged["raw_reports"].append(path.name)
    validate_diagnostic_samples(merged, allocation_instrumented, expected)
    output.write_text(json.dumps(merged, indent=2) + "\n")

def diagnostic_summary(output):
    info = json.loads((output / "metadata.json").read_text())
    suite = info.get("suite", "all")
    timing = diagnostic_samples(output / "metrics.json", False, suite)
    allocations = diagnostic_samples(output / "allocations.json", True, suite)
    requested_samples = info.get("diagnostic_samples_per_case")
    if requested_samples is not None:
        expected_total = requested_samples * len(suite_config(suite)["diagnostics"])
        if len(timing) != expected_total or len(allocations) != expected_total:
            raise RuntimeError("diagnostic capture does not contain the requested sample count")
    groups = {}
    for item in timing:
        groups.setdefault(item["case"], []).append(item)
    allocation_groups = {}
    for item in allocations:
        allocation_groups.setdefault(item["case"], []).append(item)
    rows = []
    for case, samples in sorted(groups.items()):
        signatures = {(sample["completed_runs"], sample["model_calls"], sample["tool_calls"])
                      for sample in samples + allocation_groups[case]}
        if len(signatures) != 1:
            raise RuntimeError("timing and allocation captures performed different work")
        runs = samples[0]["completed_runs"]
        divisor = max(1, runs)
        latencies = sorted(value for sample in samples for value in sample["operation_latencies_ns"])
        row = {"case": case, "samples": len(samples), "runs_per_sample": runs,
               "model_calls_per_sample": samples[0]["model_calls"], "tool_calls_per_sample": samples[0]["tool_calls"],
               "median_elapsed_ns_per_run": statistics.median(s["elapsed_ns"] / divisor for s in samples),
               "median_process_cpu_ns_per_run": statistics.median(s["process_cpu_ns"] / divisor for s in samples),
               "median_runs_per_second": statistics.median(runs * 1e9 / s["elapsed_ns"] for s in samples) if runs else None,
               "median_allocations_per_run": statistics.median(s["allocations"] / divisor for s in allocation_groups[case]),
               "median_allocated_bytes_per_run": statistics.median(s["allocated_bytes"] / divisor for s in allocation_groups[case]),
               "latency_sample_count": len(latencies),
               "latency_p50_ns": statistics.median(latencies) if latencies else None,
               "latency_p95_ns": latencies[round((len(latencies)-1)*0.95)] if latencies else None,
               "median_first_output_ns": statistics.median(s["first_output_ns"] for s in samples) if samples[0]["first_output_ns"] is not None else None,
               "transactions_per_run": [s["transactions"] / divisor for s in samples] if samples[0]["transactions"] is not None else None,
               "writes_per_run": [s["writes"] / divisor for s in samples] if samples[0]["writes"] is not None else None}
        rows.append(row)
    (output / "diagnostics.json").write_text(json.dumps(rows, indent=2) + "\n")
    text = ["Diagnostic intervals include CPU clock reads and concurrency latency bookkeeping; Criterion remains the headline timing source.", "", "| Workload | CPU µs/run | Allocations/run | Requested bytes/run |", "| --- | ---: | ---: | ---: |"]
    for row in rows:
        text.append(f"| {row['case']} | {row['median_process_cpu_ns_per_run']/1000:.3f} | {row['median_allocations_per_run']:.1f} | {row['median_allocated_bytes_per_run']:.1f} |")
    text.extend(["", "Startup rows represent one startup rather than one completed run. Raw latency observations, first-output timing, throughput, database counts and sample sizes are in diagnostics.json; small-sample tails are descriptive, not SLO estimates."])
    (output / "DIAGNOSTICS.md").write_text("\n".join(text) + "\n")

def capture(prepared, output, label, samples, quick):
    output.mkdir(parents=True, exist_ok=False)
    info = dict(prepared["metadata"], label=label, started_at_unix=time.time(), status="running",
                diagnostic_samples_per_case=samples, suite=prepared["metadata"].get("suite", "all"))
    info_path = output / "metadata.json"
    info_path.write_text(json.dumps(info, indent=2) + "\n")
    try:
        if fingerprint(prepared["repo"]) != info["source"]:
            raise RuntimeError("source changed after preparation; refusing stale binary measurement")
        env = dict(os.environ, CRITERION_HOME=str(output / "criterion"))
        env.pop("AGENT_BENCH_QUICK", None)
        if quick:
            env["AGENT_BENCH_QUICK"] = "1"
        selected = suite_config(info["suite"])
        criterion_command = [str(prepared["bin"] / "agent_loop"), "--bench", "--noplot"]
        if selected["criterion_filter"] is not None:
            criterion_command.append(selected["criterion_filter"])
        command(criterion_command, prepared["repo"], output / "criterion.log", env)
        for kind in ("metrics", "allocations"):
            filters = selected["diagnostic_filters"] or (None,)
            reports = []
            for filter_text in filters:
                stem = f"{kind}-{filter_text}" if filter_text else kind
                path = output / f"{stem}.json"
                argv = [str(prepared["bin"] / kind), "--samples", str(samples), "--output", str(path)]
                if filter_text is not None:
                    argv.extend(["--filter", filter_text])
                command(argv, prepared["repo"], output / f"{stem}.log")
                reports.append(path)
            if selected["diagnostic_filters"]:
                merge_diagnostics(reports, output / f"{kind}.json", kind == "allocations", info["suite"], samples)
        summarize(output)
        diagnostic_summary(output)
        if fingerprint(prepared["repo"]) != info["source"]:
            raise RuntimeError("source changed during capture")
        info["status"] = "complete"
    except BaseException as error:
        info.update(status="failed", error=str(error))
        raise
    finally:
        info["finished_at_unix"] = time.time()
        info_path.write_text(json.dumps(info, indent=2) + "\n")

def compare_summary(output, base, candidate, cohorts):
    suite = base["metadata"].get("suite", "all")
    expected = suite_config(suite)["benchmarks"]
    if candidate["metadata"].get("suite", "all") != suite:
        raise RuntimeError("comparison suites differ")
    different_environment = [field for field in ENVIRONMENT_FIELDS
                             if base["metadata"][field] != candidate["metadata"][field]]
    if different_environment:
        raise RuntimeError(f"comparison environments differ: {', '.join(different_environment)}")
    by_label = {}
    work_counts = {}
    for label in ("base", "candidate"):
        cases = {}
        for cohort in range(cohorts):
            capture_path = output / f"{cohort:02d}-{label}"
            info = json.loads((capture_path / "metadata.json").read_text())
            if info["status"] != "complete":
                raise RuntimeError("incomplete capture cannot enter a comparison")
            if info.get("suite", "all") != suite:
                raise RuntimeError("capture suite differs from comparison")
            summary = json.loads((capture_path / "summary.json").read_text())
            if len(summary) != len(expected) or {item["case"] for item in summary} != expected:
                raise RuntimeError("missing, duplicate, or unexpected Criterion workloads in comparison")
            for item in summary:
                cases.setdefault(item["case"], []).append(item["median_ns_per_run"])
            for sample in diagnostic_samples(capture_path / "metrics.json", False, suite):
                signature = (sample["completed_runs"], sample["model_calls"], sample["tool_calls"])
                previous = work_counts.setdefault(sample["case"], signature)
                if signature != previous:
                    raise RuntimeError(f"comparison performed different work: {sample['case']}")
        by_label[label] = cases
    if by_label["base"].keys() != by_label["candidate"].keys():
        raise RuntimeError("comparison has mismatched workloads")
    identical = measurement_fingerprint(base["metadata"]["source"]) == measurement_fingerprint(candidate["metadata"]["source"])
    text = ["Repeated AB/BA comparison of identical benchmarked source. Observed differences estimate noise; they are not improvement evidence." if identical else "Repeated AB/BA comparison. Timing differences remain informational pending independent variance calibration.",
            "", "| Workload | Base median µs/run | Candidate median µs/run | Change | Paired change range |",
            "| --- | ---: | ---: | ---: | ---: |"]
    rows = []
    for case in sorted(by_label["base"]):
        base_values = by_label["base"][case]; candidate_values = by_label["candidate"][case]
        if len(base_values) != cohorts or len(candidate_values) != cohorts:
            raise RuntimeError("comparison omitted or duplicated a workload in a cohort")
        a = statistics.median(base_values); b = statistics.median(candidate_values)
        change = None if identical else (b / a - 1) * 100
        absolute_deltas = [right - left for left, right in zip(base_values, candidate_values)]
        relative_deltas = [(right / left - 1) * 100 for left, right in zip(base_values, candidate_values)]
        row = {"case": case, "base_cohort_medians_ns": base_values, "candidate_cohort_medians_ns": candidate_values,
               "base_distribution_ns": distribution(base_values), "candidate_distribution_ns": distribution(candidate_values),
               "percent_change": change, "paired_absolute_changes_ns": absolute_deltas, "paired_relative_changes_pct": relative_deltas,
               "paired_absolute_change_distribution_ns": distribution(absolute_deltas),
               "paired_relative_change_distribution_pct": distribution(relative_deltas)}
        if identical:
            row["observed_absolute_noise_ns"] = max(abs(delta) for delta in absolute_deltas)
            row["observed_relative_noise_pct"] = max(abs(delta) for delta in relative_deltas)
        rows.append(row)
        text.append(f"| {case} | {a/1000:.3f} | {b/1000:.3f} | {'same source' if identical else f'{change:+.1f}%'} | {min(relative_deltas):+.1f}% to {max(relative_deltas):+.1f}% |")
    text.extend(["", f"Each side has {cohorts} cohort medians from one sequential job. The paired range retains every cohort, including slow ones; it is descriptive, not a confidence interval.",
                 "A timing gate is deliberately disabled. One job cannot establish runner-to-runner variance. Before enabling a workload-specific gate, repeat full same-source jobs, choose both relative and absolute tolerances above observed noise, and verify suspected regressions in a fresh matched job.",
                 "See comparison.json for raw paired deltas and median absolute deviations; each capture retains Criterion samples plus separate CPU/allocation diagnostics. Correctness and work counts are enforced immediately."])
    (output / "COMPARISON.md").write_text("\n".join(text) + "\n")
    (output / "comparison.json").write_text(json.dumps({"suite": suite, "identical_source": identical, "cohorts": cohorts,
        "independent_jobs": 1, "timing_gate_enabled": False, "calibration_status": "independent-job variance not established",
        "work_counts": {case: {"runs": count[0], "model_calls": count[1], "tool_calls": count[2]} for case, count in sorted(work_counts.items())},
        "workloads": rows}, indent=2) + "\n")

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True, help="new output directory; never overwritten")
    parser.add_argument("--base", type=Path, help="base checkout, enables comparison")
    parser.add_argument("--candidate", type=Path, default=ROOT)
    parser.add_argument("--cohorts", type=int, default=2)
    parser.add_argument("--samples", type=int, default=30)
    parser.add_argument("--suite", choices=SUITES, default="all", help="all workloads, or recovery plus minimal/durable controls")
    parser.add_argument("--quick", action="store_true", help="short exploratory windows with 10 Criterion samples")
    parser.add_argument("--require-clean", action="store_true", help="require clean source checkouts; place output outside them")
    parser.add_argument("--label", default="initial")
    args = parser.parse_args()
    if args.samples < 1 or args.cohorts < 2 or args.cohorts % 2:
        parser.error("samples must be positive; use an even number of at least two cohorts for balanced AB/BA order")
    args.out = args.out.resolve()
    if args.out.exists():
        parser.error("output already exists")
    args.out.mkdir(parents=True)
    with tempfile.TemporaryDirectory(prefix="agent-bench-") as temporary:
        candidate = prepare(args.candidate, temporary, args.out / "build.log", args.quick, args.require_clean, args.suite)
        if args.base:
            base = prepare(args.base, temporary, args.out / "build.log", args.quick, args.require_clean, args.suite)
            different_environment = [field for field in ENVIRONMENT_FIELDS
                                     if base["metadata"][field] != candidate["metadata"][field]]
            if different_environment:
                raise RuntimeError(f"comparison environments differ: {', '.join(different_environment)}")
            fixture_files = ("benches/agent_loop.rs", "benches/support/mod.rs", "examples/benchmark_metrics.rs")
            if any(base["metadata"]["source"]["files"].get(path) != candidate["metadata"]["source"]["files"].get(path) for path in fixture_files):
                raise RuntimeError("comparison fixtures differ; use identical fixture bytes in both checkouts")
            for cohort in range(args.cohorts):
                order = [("base", base), ("candidate", candidate)] if cohort % 2 == 0 else [("candidate", candidate), ("base", base)]
                for label, prepared in order:
                    capture(prepared, args.out / f"{cohort:02d}-{label}", label, args.samples, args.quick)
            compare_summary(args.out, base, candidate, args.cohorts)
        else:
            capture(candidate, args.out / "capture", args.label, args.samples, args.quick)
    print(args.out)

if __name__ == "__main__":
    main()
