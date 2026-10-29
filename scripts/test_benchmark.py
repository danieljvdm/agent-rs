"""Report integrity checks using retained, real benchmark artifacts (no timing runs)."""
import importlib.util
import json
from pathlib import Path
import shutil
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location("benchmark", Path(__file__).with_name("benchmark.py"))
benchmark = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(benchmark)
EVIDENCE = Path(__file__).resolve().parents[1] / "benchmark-results" / "initial-2026-10-29"


class BenchmarkReportTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.output = Path(self.temporary.name) / "evidence"
        shutil.copytree(EVIDENCE, self.output)

    def prepared(self, label):
        return {"metadata": json.loads((self.output / f"00-{label}" / "metadata.json").read_text())}

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


if __name__ == "__main__":
    unittest.main()
