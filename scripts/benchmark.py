#!/usr/bin/env python3
"""Capture release benchmarks or compare prepared checkouts in alternating AB/BA order."""
import argparse
import hashlib
import json
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
    files = [repo / "Cargo.toml", repo / "Cargo.lock"]
    for directory in ("src", "benches", "examples", "scripts"):
        files.extend(path for path in (repo / directory).rglob("*") if path.is_file() and "__pycache__" not in path.parts)
    hashes = {str(path.relative_to(repo)): digest(path) for path in sorted(files) if path.exists()}
    return {"sha256": hashlib.sha256(json.dumps(hashes, sort_keys=True).encode()).hexdigest(), "files": hashes}

def metadata(repo, quick):
    return {
        "fixture": "agent-loop-v1", "source": fingerprint(repo), "git_revision": quiet(["git", "rev-parse", "HEAD"], repo),
        "git_status": quiet(["git", "status", "--porcelain"], repo), "lockfile_sha256": digest(repo / "Cargo.lock"),
        "rustc": quiet(["rustc", "-Vv"], repo), "cargo": quiet(["cargo", "-V"], repo),
        "os": platform.platform(), "machine": platform.machine(), "logical_cpus": os.cpu_count(),
        "processor": quiet(["sysctl", "-n", "machdep.cpu.brand_string"], repo) if platform.system() == "Darwin" else platform.processor(),
        "memory_bytes": quiet(["sysctl", "-n", "hw.memsize"], repo) if platform.system() == "Darwin" else None,
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

def prepare(repo, temporary, log, quick):
    repo = repo.resolve()
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
    return {"repo": repo, "bin": binary_dir, "metadata": metadata(repo, quick)}

def summarize(output):
    entries = []
    for estimates in sorted((output / "criterion").rglob("new/estimates.json")):
        estimate = json.loads(estimates.read_text())
        benchmark = json.loads((estimates.parent / "benchmark.json").read_text())
        elements = (benchmark.get("throughput") or {}).get("Elements", 1)
        median = estimate["median"]
        entries.append({"case": benchmark["full_id"], "runs_per_iteration": elements,
                        "median_ns_per_run": median["point_estimate"] / elements,
                        "median_interval_lower_ns": median["confidence_interval"]["lower_bound"] / elements,
                        "median_interval_upper_ns": median["confidence_interval"]["upper_bound"] / elements})
    if not entries:
        raise RuntimeError("no Criterion results found")
    (output / "summary.json").write_text(json.dumps(entries, indent=2) + "\n")
    text = ["Scripted runtime measurement. Timing gates are not calibrated.", "", "| Workload | Median µs/completed run | Median estimate interval µs |", "| --- | ---: | ---: |"]
    for entry in entries:
        text.append(f"| {entry['case']} | {entry['median_ns_per_run']/1000:.3f} | {entry['median_interval_lower_ns']/1000:.3f}–{entry['median_interval_upper_ns']/1000:.3f} |")
    text.extend(["", "Intervals describe Criterion's median estimate, not per-operation tail latency. See metrics.json for raw CPU, latency, first-output, and database samples; allocations.json is separately instrumented. Raw Criterion samples retain outliers."])
    (output / "SUMMARY.md").write_text("\n".join(text) + "\n")
    return entries

def diagnostic_summary(output):
    timing = json.loads((output / "metrics.json").read_text())["samples"]
    allocations = json.loads((output / "allocations.json").read_text())["samples"]
    groups = {}
    for item in timing:
        groups.setdefault(item["case"], []).append(item)
    allocation_groups = {}
    for item in allocations:
        allocation_groups.setdefault(item["case"], []).append(item)
    rows = []
    for case, samples in sorted(groups.items()):
        runs = samples[0]["completed_runs"]
        divisor = max(1, runs)
        latencies = sorted(value for sample in samples for value in sample["operation_latencies_ns"])
        row = {"case": case, "samples": len(samples), "runs_per_sample": runs,
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
    info = dict(prepared["metadata"], label=label, started_at_unix=time.time(), status="running")
    info_path = output / "metadata.json"
    info_path.write_text(json.dumps(info, indent=2) + "\n")
    try:
        if fingerprint(prepared["repo"]) != info["source"]:
            raise RuntimeError("source changed after preparation; refusing stale binary measurement")
        env = dict(os.environ, CRITERION_HOME=str(output / "criterion"))
        env.pop("AGENT_BENCH_QUICK", None)
        if quick:
            env["AGENT_BENCH_QUICK"] = "1"
        command([str(prepared["bin"] / "agent_loop"), "--bench", "--noplot"], prepared["repo"], output / "criterion.log", env)
        command([str(prepared["bin"] / "metrics"), "--samples", str(samples), "--output", str(output / "metrics.json")], prepared["repo"], output / "metrics.log")
        command([str(prepared["bin"] / "allocations"), "--samples", str(samples), "--output", str(output / "allocations.json")], prepared["repo"], output / "allocations.log")
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
    by_label = {}
    for label in ("base", "candidate"):
        cases = {}
        for cohort in range(cohorts):
            for item in json.loads((output / f"{cohort:02d}-{label}" / "summary.json").read_text()):
                cases.setdefault(item["case"], []).append(item["median_ns_per_run"])
        by_label[label] = cases
    if by_label["base"].keys() != by_label["candidate"].keys():
        raise RuntimeError("comparison has mismatched workloads")
    identical = base["metadata"]["source"]["sha256"] == candidate["metadata"]["source"]["sha256"]
    text = ["Repeated AB/BA comparison. Identical-source runs calibrate variation; they are not improvement evidence." if identical else "Repeated AB/BA comparison. Timing differences remain informational pending variance calibration.", "", "| Workload | Base median µs/run | Candidate median µs/run | Change |", "| --- | ---: | ---: | ---: |"]
    rows = []
    for case in sorted(by_label["base"]):
        a = statistics.median(by_label["base"][case]); b = statistics.median(by_label["candidate"][case])
        change = None if identical else (b / a - 1) * 100
        rows.append({"case": case, "base_cohort_medians_ns": by_label["base"][case], "candidate_cohort_medians_ns": by_label["candidate"][case], "percent_change": change})
        text.append(f"| {case} | {a/1000:.3f} | {b/1000:.3f} | {'same source' if identical else f'{change:+.1f}%'} |")
    (output / "COMPARISON.md").write_text("\n".join(text) + "\n")
    (output / "comparison.json").write_text(json.dumps({"identical_source": identical, "cohorts": cohorts, "workloads": rows}, indent=2) + "\n")

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True, help="new output directory; never overwritten")
    parser.add_argument("--base", type=Path, help="base checkout, enables comparison")
    parser.add_argument("--candidate", type=Path, default=ROOT)
    parser.add_argument("--cohorts", type=int, default=2)
    parser.add_argument("--samples", type=int, default=30)
    parser.add_argument("--quick", action="store_true", help="short exploratory windows with 10 Criterion samples")
    parser.add_argument("--label", default="initial")
    args = parser.parse_args()
    if args.samples < 1 or args.cohorts < 2:
        parser.error("samples must be positive; comparison requires at least two alternating cohorts")
    args.out = args.out.resolve()
    if args.out.exists():
        parser.error("output already exists")
    args.out.mkdir(parents=True)
    with tempfile.TemporaryDirectory(prefix="agent-bench-") as temporary:
        candidate = prepare(args.candidate, temporary, args.out / "build.log", args.quick)
        if args.base:
            base = prepare(args.base, temporary, args.out / "build.log", args.quick)
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
