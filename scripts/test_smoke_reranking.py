"""Protocol and metric regressions; no Docker, provider, or real API calls."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("smoke-reranking.py")
SPEC = importlib.util.spec_from_file_location("smoke_reranking", SCRIPT)
smoke = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(smoke)


class FixtureClient:
    """Protocol fixture, with failure injection at externally visible stages."""
    def __init__(self, output, failure=None):
        self.output, self.failure = output, failure
        self.uploads, self.searches, self.deleted = {}, [], False
        self.profile = "a" * 64
        self.token = ""
        self.calls = []

    def upload(self, workspace, document):
        if self.failure == "upload":
            raise smoke.CheckError("fixture upload failed")
        number = len(self.uploads) + 1
        entry = {"document_id": f"{number:032x}", "version_id": f"{number + 100:032x}",
                 "index_id": f"{number + 200:032x}", "generation": f"{number + 300:032x}",
                 "source_sha256": hashlib.sha256(document["text"].encode()).hexdigest()}
        self.uploads[document["id"]] = entry
        return {"document": {"id": entry["document_id"]}, "version": {"id": entry["version_id"]}}, entry["source_sha256"]

    def request(self, method, path, data=None, media=None, expected=200):
        self.calls.append((method, path))
        if path == "/auth/register":
            frozen = json.loads((self.output / "fixture.json").read_text())
            if frozen["fixture_sha256"] != smoke.digest(smoke.FIXTURE):
                raise AssertionError("fixture was not frozen before the first HTTP call")
            return {"token": "DO-NOT-LOG-TOKEN", "workspaces": [{"id": "1" * 32}]}
        if method == "DELETE":
            self.deleted = True
            if self.failure == "cleanup":
                raise smoke.CheckError("cleanup failure")
            return None
        if path.endswith("/index"):
            entry = next(row for row in self.uploads.values() if "/versions/" + row["version_id"] + "/" in path)
            return {"id": entry["index_id"], "status": "ready",
                    "profile_id": self.profile, "generation": entry["generation"],
                    "source_sha256": entry["source_sha256"]}
        if method == "GET":
            entry = next(row for row in self.uploads.values() if path.endswith("/" + row["document_id"]))
            return {"current_version_id": entry["version_id"]}
        if path.endswith("/search"):
            self.searches.append(data)
            applied = data["rerank"]
            outcome = "ok" if applied else "disabled"
            if self.failure == "model_error" and applied:
                applied, outcome = False, "model_error"
            policy = "rerank-policy" if data["rerank"] else "baseline-policy"
            if self.failure == "policy" and len(self.searches) > 2:
                policy += "-changed"
            if self.failure == "profile":
                profile = "b" * 64
            else:
                profile = self.profile
            case = next(row for row in smoke.FIXTURE["queries"] + smoke.FIXTURE["no_answer"]
                        if row["text"] == data["query"])
            ids = case.get("relevant", [])
            hits = []
            for number, key in enumerate(ids):
                entry = self.uploads[key]
                chunk = {field: entry[field] for field in ("document_id", "version_id", "index_id", "source_sha256")}
                chunk["id"] = f"{number + 500:032x}"
                hits.append({"chunk": chunk, "title": "unlogged title", "score": 0.9 - number * 0.1})
            if not ids and data["rerank"]:
                outcome = "all_below_threshold"
            if self.failure == "foreign":
                hits[0]["chunk"]["document_id"] = "f" * 32
            return {"mode": "hybrid", "profile_id": profile, "retrieval_policy_id": policy,
                    "rerank": {"applied": applied, "outcome": outcome, "candidate_count": 16,
                               "result_count": len(hits), "threshold": 0.2},
                    "hits": hits}
        raise AssertionError("unexpected API route")


class QualityTests(unittest.TestCase):
    def test_document_metrics_use_all_positive_judgments(self):
        metrics = smoke.quality(["wrong", "one", "also-wrong", "two", "third-wrong"], ["one", "two", "missing"])
        self.assertAlmostEqual(metrics["Recall@5"], 2 / 3)
        self.assertEqual(metrics["Hit@5"], 1)
        self.assertEqual(metrics["MRR@5"], 0.5)
        self.assertAlmostEqual(metrics["nDCG@5"], (1 / 1.584962500721156 + 1 / 2.321928094887362)
                               / (1 + 1 / 1.584962500721156 + 0.5))
        self.assertEqual(smoke.quality(["wrong"], ["one"])["Hit@5"], 0)
        with self.assertRaises(smoke.CheckError):
            smoke.quality([], [])
        with self.assertRaises(smoke.CheckError):
            smoke.quality(["one", "one"], ["one"])

    def test_chinese_tokens_do_not_bridge_spaces_and_nfkc_words_match(self):
        self.assertIn("内存", smoke.body_terms("查看内存"))
        self.assertNotIn("据部", smoke.body_terms("数据 部署"))
        self.assertEqual(smoke.body_terms("ＭｙＳＱＬ"), smoke.body_terms("MySQL"))

    def test_redundancy_does_not_include_common_titles(self):
        documents = {"a": {"title": "same title", "text": "内存部署"},
                     "b": {"title": "same title", "text": "七天退款"},
                     "copy": {"title": "different title", "text": "内存部署"}}
        self.assertEqual(smoke.redundancy(["a", "b"], documents)["max_pairwise_jaccard"], 0)
        self.assertEqual(smoke.redundancy(["a", "copy"], documents)["pairs_at_least_0_8"], 1)
        self.assertEqual(smoke.redundancy(["a"], documents)["pair_count"], 0)

    def test_fixture_is_fixed_and_judgments_are_complete(self):
        docs = {row["id"] for row in smoke.FIXTURE["documents"]}
        self.assertEqual(len(docs), 16)
        self.assertEqual(len(smoke.FIXTURE["queries"]), 10)
        self.assertEqual(len(smoke.FIXTURE["no_answer"]), 1)
        for row in smoke.FIXTURE["queries"]:
            self.assertTrue(set(row["relevant"]).issubset(docs))
            self.assertTrue(row["relevant"])


class ProtocolTests(unittest.TestCase):
    def run_fixture(self, directory, failure=None):
        output = Path(directory)
        client = FixtureClient(output, failure)
        result = smoke.run(client, output, wait_timeout=1, poll_interval=0)
        return client, result

    def test_same_frozen_corpus_paired_queries_no_answer_and_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            client, report = self.run_fixture(directory)
            self.assertEqual(report["status"], "passed")
            self.assertTrue(client.deleted)
            self.assertEqual(len(client.searches), 22)
            for before, after in zip(client.searches[::2], client.searches[1::2]):
                self.assertEqual(before["query"], after["query"])
                self.assertFalse(before["rerank"])
                self.assertTrue(after["rerank"])
            self.assertEqual(report["summary"]["rerank"]["queries"], 10)
            self.assertEqual(report["summary"]["rerank"]["metrics"]["Recall@5"], 1)
            self.assertEqual(len(report["no_answer"]), 1)
            self.assertFalse(report["no_answer"][0]["rerank"]["returned_any"])
            self.assertEqual(report["summary"]["rerank"]["model_error_fallbacks"], 0)
            self.assertNotEqual(report["retrieval_policy_ids"]["baseline"], report["retrieval_policy_ids"]["rerank"])
            serialized = (Path(directory) / "results.json").read_text()
            self.assertNotIn("DO-NOT-LOG-TOKEN", serialized)
            self.assertNotIn("unlogged title", serialized)

    def test_errors_never_pass_and_owned_workspace_is_cleaned(self):
        for failure in ("model_error", "profile", "policy", "foreign", "upload"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                client, report = self.run_fixture(directory, failure)
                self.assertEqual(report["status"], "failed")
                self.assertTrue(client.deleted)
                self.assertEqual(report["cleanup"], "temporary_workspace_deleted")

    def test_cleanup_failure_fails_the_check(self):
        with tempfile.TemporaryDirectory() as directory:
            _, report = self.run_fixture(directory, "cleanup")
            self.assertEqual(report["status"], "failed")
            self.assertEqual(report["cleanup"], "failed")

    def test_completed_output_cannot_be_silently_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            self.run_fixture(directory)
            client = FixtureClient(Path(directory))
            with self.assertRaises(smoke.CheckError):
                smoke.run(client, directory)
            self.assertEqual(client.calls, [])

    def test_no_credentials_or_redirects_in_base_url(self):
        for url in ("http://user:password@localhost", "http://localhost?token=private",
                    "file:///private", "http://localhost/wrong"):
            with self.subTest(url=url), self.assertRaises(smoke.CheckError):
                smoke.Client(url)

    def test_enabled_empty_is_valid_but_skipped_and_nonfinite_are_not(self):
        mapping = {"one": {"document_id": "1" * 32, "version_id": "2" * 32,
                           "index_id": "3" * 32, "source_sha256": "4" * 64}}
        base = {"mode": "hybrid", "profile_id": "a" * 64, "retrieval_policy_id": "policy",
                "rerank": {"applied": True, "outcome": "all_below_threshold"}, "hits": []}
        result = smoke.validate_response(base, "rerank", mapping, "a" * 64, {})
        self.assertEqual(result["ranking"], [])
        for change in ({"applied": False, "outcome": "disabled"}, {"applied": True, "outcome": "ok", "top_score": float("nan")}):
            response = copy.deepcopy(base)
            response["rerank"] = change
            with self.assertRaises(smoke.CheckError):
                smoke.validate_response(response, "rerank", mapping, "a" * 64, {})


if __name__ == "__main__":
    unittest.main()
