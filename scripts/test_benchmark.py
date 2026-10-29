"""Report integrity checks using retained, real benchmark artifacts (no timing runs)."""
import importlib.util
import json
from pathlib import Path
import re
import shutil
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("benchmark", Path(__file__).with_name("benchmark.py"))
benchmark = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(benchmark)
EVIDENCE = Path(__file__).resolve().parents[1] / "benchmark-results" / "initial-2026-10-29"


class BenchmarkReportTests(unittest.TestCase):
    def setUp(self):
        # Retained evidence is v1. This tests its parser without claiming it is v2 data.
        version = patch.object(benchmark, "FIXTURE_VERSION", "agent-loop-v1")
        version.start()
        self.addCleanup(version.stop)
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.output = Path(self.temporary.name) / "evidence"
        shutil.copytree(EVIDENCE, self.output)

    def prepared(self, label):
        return {"metadata": json.loads((self.output / f"00-{label}" / "metadata.json").read_text())}

    def capture_recovery(self, output):
        """Replay real raw data through the controller's command/filter contract."""
        prepared = self.prepared("base")
        prepared["metadata"]["suite"] = "recovery"
        prepared.update(repo=self.output, bin=self.output / "binaries")

        def replay(argv, cwd, log, env=None):
            kind = Path(argv[0]).name
            if kind == "agent_loop":
                criterion = Path(env["CRITERION_HOME"])
                shutil.copytree(EVIDENCE / "00-base" / "criterion", criterion)
                filter_text = argv[3] if len(argv) > 3 else ""
                for path in criterion.rglob("new/benchmark.json"):
                    if not re.search(filter_text, json.loads(path.read_text())["full_id"]):
                        shutil.rmtree(path.parent)
            else:
                report = json.loads((EVIDENCE / "00-base" / f"{kind}.json").read_text())
                self.assertEqual(int(argv[argv.index("--samples") + 1]), report["samples_per_case"])
                filter_text = argv[argv.index("--filter") + 1] if "--filter" in argv else ""
                report["samples"] = [sample for sample in report["samples"] if filter_text in sample["case"]]
                Path(argv[argv.index("--output") + 1]).write_text(json.dumps(report))
            return ""

        with patch.object(benchmark, "command", side_effect=replay), patch.object(
                benchmark, "fingerprint", return_value=prepared["metadata"]["source"]):
            benchmark.capture(prepared, output, "recovery-replay", 5, True)
        return prepared

    def test_real_evidence_reanalyzes_without_claiming_speedup_or_a_gate(self):
        for label in ("00-base", "00-candidate", "01-base", "01-candidate"):
            benchmark.summarize(self.output / label)
            benchmark.diagnostic_summary(self.output / label)
        base, candidate = self.prepared("base"), self.prepared("candidate")
        # Controller and unrelated application examples are not runtime changes.
        candidate["metadata"]["source"]["files"]["examples/personal_eval.rs"] = "new-example"
        candidate["metadata"]["source"]["sha256"] = "changed-controller-only"
        benchmark.compare_summary(self.output, base, candidate, 2)
        report = json.loads((self.output / "comparison.json").read_text())
        self.assertTrue(report["identical_source"])
        self.assertFalse(report["timing_gate_enabled"])
        self.assertEqual(len(report["workloads"]), 19)
        self.assertTrue(all(row["percent_change"] is None for row in report["workloads"]))
        self.assertTrue(all(len(row["paired_relative_changes_pct"]) == 2 for row in report["workloads"]))

    def test_missing_workloads_and_samples_fail_instead_of_looking_faster(self):
        capture = self.output / "00-base"
        missing = next((capture / "criterion").rglob("new/estimates.json"))
        missing.unlink()
        with self.assertRaisesRegex(RuntimeError, "Criterion workloads"):
            benchmark.summarize(capture)
        path = capture / "metrics.json"
        data = json.loads(path.read_text())
        data["samples"].pop()
        path.write_text(json.dumps(data))
        with self.assertRaisesRegex(RuntimeError, "diagnostic samples"):
            benchmark.diagnostic_samples(path, False)

    def test_v1_observer_measurements_are_not_accepted_as_v2_evidence(self):
        with patch.object(benchmark, "FIXTURE_VERSION", "agent-loop-v2"):
            with self.assertRaisesRegex(RuntimeError, "fixture, executor, or instrumentation"):
                benchmark.diagnostic_samples(self.output / "00-base" / "metrics.json", False)

    def test_changed_work_counts_incomplete_runs_and_environments_reject_comparison(self):
        base, candidate = self.prepared("base"), self.prepared("candidate")
        candidate["metadata"]["rustc"] = "different compiler"
        with self.assertRaisesRegex(RuntimeError, "environments differ"):
            benchmark.compare_summary(self.output, base, candidate, 2)
        candidate = self.prepared("candidate")
        status_path = self.output / "01-candidate" / "metadata.json"
        metadata = json.loads(status_path.read_text())
        metadata["status"] = "failed"
        status_path.write_text(json.dumps(metadata))
        with self.assertRaisesRegex(RuntimeError, "incomplete capture"):
            benchmark.compare_summary(self.output, base, candidate, 2)
        metadata["status"] = "complete"
        status_path.write_text(json.dumps(metadata))
        path = self.output / "01-candidate" / "metrics.json"
        data = json.loads(path.read_text())
        for sample in data["samples"]:
            if sample["case"] == "minimal":
                sample["model_calls"] += 1
        path.write_text(json.dumps(data))
        with self.assertRaisesRegex(RuntimeError, "different work: minimal"):
            benchmark.compare_summary(self.output, base, candidate, 2)

    def test_recovery_capture_filters_real_data_and_retains_each_raw_report(self):
        capture = self.output / "recovery-capture"
        self.capture_recovery(capture)
        metadata = json.loads((capture / "metadata.json").read_text())
        self.assertEqual((metadata["suite"], metadata["status"]), ("recovery", "complete"))
        summary = json.loads((capture / "summary.json").read_text())
        self.assertEqual({row["case"] for row in summary}, {
            "engine/minimal", "sqlite/durable_four_tool_rounds",
            "recovery/restart_to_completion/0", "recovery/restart_to_completion/512",
        })
        for kind, instrumented in (("metrics", False), ("allocations", True)):
            merged = json.loads((capture / f"{kind}.json").read_text())
            self.assertEqual(len(merged["raw_reports"]), 3)
            raw_samples = []
            for name in merged["raw_reports"]:
                raw = json.loads((capture / name).read_text())
                self.assertIn("runtime_startup_ns", raw)
                self.assertEqual(raw["allocation_instrumented"], instrumented)
                raw_samples.extend(raw["samples"])
            self.assertEqual(merged["samples"], raw_samples)
            self.assertEqual(len(raw_samples), 20)
        self.assertEqual(len(json.loads((capture / "diagnostics.json").read_text())), 4)

    def test_recovery_comparison_requires_compatible_suite_in_every_capture(self):
        output = self.output / "recovery-comparison"
        output.mkdir()
        for label in ("00-base", "00-candidate", "01-base", "01-candidate"):
            prepared = self.capture_recovery(output / label)
        benchmark.compare_summary(output, prepared, prepared, 2)
        report = json.loads((output / "comparison.json").read_text())
        self.assertEqual(report["suite"], "recovery")
        self.assertEqual(len(report["workloads"]), 4)
        self.assertEqual(len(report["work_counts"]), 4)
        self.assertFalse(report["timing_gate_enabled"])
        other = {"metadata": dict(prepared["metadata"], suite="all")}
        with self.assertRaisesRegex(RuntimeError, "comparison suites differ"):
            benchmark.compare_summary(output, prepared, other, 2)
        path = output / "01-candidate" / "metadata.json"
        data = json.loads(path.read_text())
        data["suite"] = "all"
        path.write_text(json.dumps(data))
        with self.assertRaisesRegex(RuntimeError, "capture suite differs"):
            benchmark.compare_summary(output, prepared, prepared, 2)

    def test_recovery_rejects_missing_or_unexpected_cases(self):
        capture = self.output / "recovery-capture"
        self.capture_recovery(capture)
        for path in (capture / "criterion").rglob("new/benchmark.json"):
            if json.loads(path.read_text())["full_id"] == "recovery/restart_to_completion/512":
                path.with_name("estimates.json").unlink()
        with self.assertRaisesRegex(RuntimeError, "Criterion workloads"):
            benchmark.summarize(capture)
        path = capture / "metrics.json"
        original = path.read_text()
        for case, replacement in (("recovery_prior_runs_512", None), ("minimal", "four_tool_rounds")):
            with self.subTest(case=case, replacement=replacement):
                data = json.loads(original)
                if replacement is None:
                    data["samples"] = [sample for sample in data["samples"] if sample["case"] != case]
                else:
                    for sample in data["samples"]:
                        if sample["case"] == case:
                            sample["case"] = replacement
                path.write_text(json.dumps(data))
                with self.assertRaisesRegex(RuntimeError, "diagnostic workloads"):
                    benchmark.diagnostic_samples(path, False, "recovery")

    def test_filtered_reports_reject_duplicates_and_mixed_metadata(self):
        capture = self.output / "recovery-capture"
        self.capture_recovery(capture)
        merged = json.loads((capture / "metrics.json").read_text())
        paths = [capture / name for name in merged["raw_reports"]]
        destination = capture / "invalid-merge.json"
        with self.assertRaisesRegex(RuntimeError, "duplicate diagnostic workloads"):
            benchmark.merge_diagnostics(paths + paths[:1], destination, False, "recovery", 5)
        with self.assertRaisesRegex(RuntimeError, "diagnostic workloads"):
            benchmark.merge_diagnostics(paths[:-1], destination, False, "recovery", 5)
        original = paths[1].read_text()
        for field, value in (("fixture", "agent-loop-other"), ("workers", 1),
                             ("allocation_instrumented", True), ("samples_per_case", 6), ("suite", "all")):
            with self.subTest(field=field):
                data = json.loads(original)
                data[field] = value
                paths[1].write_text(json.dumps(data))
                with self.assertRaisesRegex(RuntimeError, "diagnostic"):
                    benchmark.merge_diagnostics(paths, destination, False, "recovery", 5)
        self.assertFalse(destination.exists())


if __name__ == "__main__":
    unittest.main()
