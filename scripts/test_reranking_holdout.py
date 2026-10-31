"""Offline holdout protocol tests; do not call a model or production API."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


SCRIPT = Path(__file__).with_name("eval-reranking-holdout.py")
SPEC = importlib.util.spec_from_file_location("reranking_holdout", SCRIPT)
evaluation = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(evaluation)


class FakeClient:
    def __init__(self, output, failure=None):
        self.output, self.failure = Path(output), failure
        self.fixture = evaluation.load_fixture()
        self.uploads, self.searches, self.calls = {}, [], []
        self.deleted, self.token = False, ""

    def upload(self, workspace, document):
        number = len(self.uploads) + 1
        entry = {"document_id": f"{number:032x}", "version_id": f"{number + 100:032x}",
                 "index_id": f"{number + 200:032x}", "generation": f"{number + 300:032x}",
                 "source_sha256": hashlib.sha256(document["text"].encode()).hexdigest()}
        self.uploads[document["id"]] = entry
        return {"document": {"id": entry["document_id"]}, "version": {"id": entry["version_id"]}}, entry["source_sha256"]

    def request(self, method, path, data=None, media=None, expected=200):
        self.calls.append((method, path))
        if path == "/auth/register":
            assert (self.output / "fixture.freeze.json").is_file()
            assert (self.output / "model.freeze.json").is_file()
            return {"token": "SECRET-NEVER-IN-ARTIFACTS", "workspaces": [{"id": "a" * 32}]}
        if method == "DELETE":
            self.deleted = True
            if self.failure == "cleanup":
                raise evaluation.CheckError("cleanup failed")
            return None
        if path.endswith("/index"):
            row = next(row for row in self.uploads.values() if "/versions/" + row["version_id"] + "/" in path)
            return {"id": row["index_id"], "generation": row["generation"], "status": "ready",
                    "source_sha256": row["source_sha256"], "profile_id": "a" * 64}
        if method == "GET":
            row = next(row for row in self.uploads.values() if path.endswith("/" + row["document_id"]))
            return {"current_version_id": row["version_id"]}
        if path.endswith("/search"):
            case = next(row for row in self.fixture["queries"] if row["text"] == data["query"])
            if case["split"] == "holdout":
                assert (self.output / "decision.freeze.json").is_file(), "holdout before choice frozen"
            self.searches.append((case["id"], data["rerank"]))
            relevant = case["relevant"]
            ranking = list(relevant) if data["rerank"] else case["hard_negatives"][:1] + relevant
            if self.failure == "dev_regression" and data["rerank"] and case["split"] == "dev":
                ranking = []
            if self.failure == "holdout_regression" and data["rerank"] and case["split"] == "holdout":
                ranking = [] if relevant else case["hard_negatives"][:1]
            outcome = ("ok" if ranking else "all_below_threshold") if data["rerank"] else "disabled"
            applied = data["rerank"]
            if self.failure == "model_error" and applied:
                outcome, applied = "model_error", False
            hits = []
            for number, key in enumerate(ranking):
                row = self.uploads[key]
                chunk = {name: row[name] for name in ("document_id", "version_id", "index_id", "source_sha256")}
                chunk["id"] = f"{number + 500:032x}"
                hits.append({"chunk": chunk, "score": .9 - number * .1})
            policy = "rerank-policy" if data["rerank"] else "baseline-policy"
            if self.failure == "policy" and len(self.searches) > 2:
                policy += "changed"
            threshold = .3 if self.failure == "threshold" else .2
            return {"mode": "hybrid", "execution": "qdrant_native", "profile_id": "a" * 64,
                    "retrieval_policy_id": policy, "hits": hits,
                    "rerank": {"applied": applied, "outcome": outcome, "threshold": threshold,
                               "candidate_count": 10, "result_count": len(hits)}}
        raise AssertionError("unexpected route")


class FixtureTests(unittest.TestCase):
    def test_new_topics_and_positive_negative_coverage(self):
        fixture = evaluation.load_fixture()
        self.assertEqual(evaluation.fixture_counts(fixture), {
            "dev": {"documents": 12, "answerable": 6, "no_answer": 3},
            "holdout": {"documents": 12, "answerable": 6, "no_answer": 3}})

    def test_cross_split_labels_and_topic_leakage_are_rejected(self):
        for failure in ("topic", "label", "duplicate_query", "no_negatives"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                fixture = evaluation.load_fixture()
                if failure == "topic":
                    fixture["documents"][-1]["topic"] = "archive"
                elif failure == "label":
                    fixture["queries"][0]["relevant"] = ["garden-school"]
                elif failure == "duplicate_query":
                    fixture["queries"][-1]["text"] = fixture["queries"][0]["text"]
                else:
                    fixture["queries"][0]["hard_negatives"] = []
                path = Path(directory) / "fixture.json"
                path.write_text(json.dumps(fixture))
                with self.assertRaises(evaluation.CheckError):
                    evaluation.load_fixture(path)


class ModelInfoTests(unittest.TestCase):
    def test_expected_model_and_safe_runtime_metadata_are_verified(self):
        info = {"model_id": "BAAI/bge-reranker-base", "model_sha": "a" * 40,
                "model_type": {"reranker": {}}, "auto_truncate": True, "max_input_length": 512,
                "max_batch_tokens": 1024, "max_client_batch_size": 200,
                "device": "cuda", "requested_device": "", "private_unknown_field": "do-not-copy"}

        class Response:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self, limit):
                return json.dumps(info).encode()

        class Opener:
            def open(self, request, timeout):
                self.request = request
                return Response()

        with patch.object(evaluation.urllib.request, "build_opener", return_value=Opener()):
            value = evaluation.model_info("http://127.0.0.1:18091/info", "k" * 32, "BAAI/bge-reranker-base")
            self.assertEqual(value["device"], "cuda")
            self.assertEqual(value["model_sha"], "a" * 40)
            self.assertNotIn("private_unknown_field", value)
            with self.assertRaises(evaluation.CheckError):
                evaluation.model_info("http://127.0.0.1:18091/info", "k" * 32, "unexpected-model")
            info["model_sha"] = ""
            with self.assertRaises(evaluation.CheckError):
                evaluation.model_info("http://127.0.0.1:18091/info", "k" * 32, "BAAI/bge-reranker-base")


class ProtocolTests(unittest.TestCase):
    def evaluate(self, directory, failure=None, info_reader=None):
        client = FakeClient(directory, failure)
        report = evaluation.run(client, directory, wait_timeout=1, poll_interval=0,
                                info_reader=info_reader or (lambda: {"model_id": "frozen-test-model"}))
        return client, report

    def test_dev_choice_precedes_holdout_and_warm_does_not_inflate_quality_count(self):
        with tempfile.TemporaryDirectory() as directory:
            client, report = self.evaluate(directory)
            self.assertEqual(report["status"], "completed")
            self.assertEqual(report["decision"]["selected"], "rerank")
            self.assertEqual(report["conclusion"], "rerank_confirmed_on_synthetic_holdout")
            self.assertTrue(client.deleted)
            self.assertEqual(len(client.searches), 72)
            for split in ("dev", "holdout"):
                stats = report[split + "_summary"]["rerank"]
                self.assertEqual(stats["answerable_queries"], 6)
                self.assertEqual(stats["no_answer_queries"], 3)
                self.assertEqual(stats["warm_latency_ms"]["samples"], 9)
                self.assertEqual(stats["first_pass_latency_ms"]["samples"], 9)
                self.assertEqual(stats["metrics"]["Recall@5"], 1)
                self.assertEqual(stats["no_answer_false_return_rate"], 0)
            self.assertNotIn("SECRET-NEVER-IN-ARTIFACTS", (Path(directory) / "results.json").read_text())

    def test_dev_rejection_keeps_holdout_baseline_and_never_tunes_on_it(self):
        with tempfile.TemporaryDirectory() as directory:
            client, report = self.evaluate(directory, "dev_regression")
            self.assertEqual(report["status"], "completed")
            self.assertEqual(report["decision"]["selected"], "baseline")
            self.assertEqual(report["conclusion"], "retain_baseline_rerank_rejected_on_dev")
            self.assertTrue(all(not rerank for query, rerank in client.searches if query.startswith("test-")))
            self.assertEqual(set(report["holdout_summary"]), {"baseline"})

    def test_holdout_regression_is_reported_without_new_policy_selection(self):
        with tempfile.TemporaryDirectory() as directory:
            client, report = self.evaluate(directory, "holdout_regression")
            self.assertEqual(report["status"], "completed")
            self.assertEqual(report["decision"]["selected"], "rerank")
            self.assertEqual(report["conclusion"], "rerank_not_confirmed_on_holdout_no_retuning")
            self.assertEqual(len(client.searches), 72)

    def test_model_error_policy_change_and_wrong_threshold_never_pass(self):
        for failure in ("model_error", "policy", "threshold"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                client, report = self.evaluate(directory, failure)
                self.assertEqual(report["status"], "failed")
                self.assertFalse(report["holdout_started"])
                self.assertTrue(client.deleted)

    def test_model_change_and_cleanup_error_fail_completion(self):
        with tempfile.TemporaryDirectory() as directory:
            identities = iter([{"model_id": "first"}, {"model_id": "second"}])
            client, report = self.evaluate(directory, info_reader=lambda: next(identities))
            self.assertEqual(report["status"], "failed")
            self.assertFalse(report["holdout_started"])
            self.assertTrue(client.deleted)
        with tempfile.TemporaryDirectory() as directory:
            _, report = self.evaluate(directory, "cleanup")
            self.assertEqual(report["status"], "failed")
            self.assertEqual(report["cleanup"], "failed")

    def test_existing_artifacts_are_not_overwritten_or_resumed(self):
        with tempfile.TemporaryDirectory() as directory:
            self.evaluate(directory)
            client = FakeClient(directory)
            with self.assertRaises(evaluation.CheckError):
                evaluation.run(client, directory, info_reader=lambda: {})
            self.assertEqual(client.calls, [])

    def test_latency_budget_can_reject_otherwise_better_candidate(self):
        with tempfile.TemporaryDirectory() as directory:
            _, report = self.evaluate(directory)
            summary = copy.deepcopy(report["dev_summary"])
            summary["rerank"]["warm_latency_ms"]["p95"] = 5001
            decision = evaluation.select_on_dev(summary)
            self.assertEqual(decision["selected"], "baseline")
            self.assertFalse(decision["dev_gates"]["warm_latency_budget"])


if __name__ == "__main__":
    unittest.main()
