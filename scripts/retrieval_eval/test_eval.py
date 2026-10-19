import hashlib
import http.server
import http.client
import concurrent.futures
import json
import math
import os
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from .api import APIClient, atomic_json
from .data import CONTENT_POLICY, EvaluationError, corpus_content, dataset_info, load_dataset, query_for_api
from .metrics import deduplicate_documents, paired_compare, query_metrics, score_records
from .runner import compare_runs, environment_freeze, evaluator_source_sha256, evaluate, ingest, load_records, load_state


class MetricTests(unittest.TestCase):
    def test_known_binary_and_graded_metrics(self):
        metrics = query_metrics(["x", "a", "b"], {"a": 2, "b": 1, "c": 0})
        self.assertAlmostEqual(metrics["nDCG@10"], (2 / math.log2(3) + 1 / 2) / (2 + 1 / math.log2(3)))
        self.assertEqual(metrics["Recall@5"], 1)
        self.assertEqual(metrics["MRR@10"], .5)
        self.assertAlmostEqual(metrics["MAP@10"], (.5 + 2 / 3) / 2)
        self.assertEqual(metrics["Precision@10"], .2)

    def test_map_cut_denominator_is_all_relevant(self):
        relevant = {str(i): 1 for i in range(20)}
        self.assertEqual(query_metrics(list(relevant)[:10], relevant)["MAP@10"], .5)

    def test_deduplication_preserves_first_hit(self):
        hits = [{"chunk": {"document_id": doc}} for doc in ["two", "one", "two"]]
        self.assertEqual(deduplicate_documents(hits, {"one": "a", "two": "b"}), ["b", "a"])
        with self.assertRaises(EvaluationError):
            deduplicate_documents(hits, {})

    def test_coverage_errors_never_drop_queries(self):
        qrels = {"q1": {"a": 1}, "q2": {"b": 1}}
        records = [{"query_id": "q1", "status": "ok", "ranking": ["a"], "latency_ms": 10},
                   {"query_id": "q2", "status": "error", "ranking": [], "latency_ms": 1}]
        summary = score_records(records, qrels)
        self.assertEqual(summary["metrics"]["nDCG@10"], .5)
        self.assertEqual(summary["coverage"]["failed"], 1)
        for bad in (records[:1], records + records[:1], records + [{"query_id": "foreign"}]):
            with self.assertRaises(EvaluationError):
                score_records(bad, qrels)
        with self.assertRaises(EvaluationError):
            paired_compare(records, records, qrels, 100)

    def test_paired_bootstrap_is_reproducible_and_signed(self):
        qrels = {"q1": {"a": 1}, "q2": {"b": 1}}
        baseline = [{"query_id": q, "status": "ok", "ranking": [], "latency_ms": 10} for q in qrels]
        candidate = [{"query_id": q, "status": "ok", "ranking": list(rel), "latency_ms": 5} for q, rel in qrels.items()]
        result = paired_compare(baseline, candidate, qrels, 100, 123)
        self.assertEqual(result, paired_compare(baseline, candidate, qrels, 100, 123))
        self.assertEqual(result["delta_candidate_minus_baseline"]["nDCG@10"], {"mean": 1, "ci95": [1, 1]})

    def test_invalid_latency_and_ranking_fail_closed(self):
        for latency in (-1, float("nan"), float("inf"), True, "10"):
            with self.assertRaises(EvaluationError):
                score_records([{"query_id": "q", "status": "ok", "ranking": ["a"], "latency_ms": latency}], {"q": {"a": 1}})
        with self.assertRaises(EvaluationError):
            score_records([{"query_id": "q", "status": "ok", "ranking": ["a", "a"], "latency_ms": 1}], {"q": {"a": 1}})

    def test_query_length_policy_is_explicit_and_utf8_safe(self):
        with self.assertRaises(EvaluationError):
            query_for_api("中" * 171, "fail")
        query, truncated = query_for_api("中" * 171, "truncate_utf8")
        self.assertEqual(len(query), 170)
        self.assertTrue(truncated)
        self.assertLessEqual(len(query.encode()), 512)
        for value in ("a", "ab\ncd", "abc\x7f"):
            with self.assertRaises(EvaluationError):
                query_for_api(value, "fail")


def make_dataset(root):
    root = Path(root)
    (root / "qrels").mkdir()
    corpus = [{"_id": "a", "title": "A title", "text": "Alpha body"}, {"_id": "b", "title": "B title", "text": "Beta body"}]
    queries = [{"_id": "q1", "text": "Alpha claim"}, {"_id": "q2", "text": "Beta claim"}, {"_id": "training", "text": "not evaluated"}]
    for name, records in (("corpus", corpus), ("queries", queries)):
        (root / (name + ".jsonl")).write_text("".join(json.dumps(row) + "\n" for row in records))
    (root / "qrels" / "test.tsv").write_text("query-id\tcorpus-id\tscore\nq1\ta\t1\nq2\tb\t1\n")
    return load_dataset(root, "test")


class DatasetTests(unittest.TestCase):
    def test_content_policy_replaces_disallowed_cc_preserving_boundaries(self):
        row = {"title": "A\u0096B", "text": "C\u0082D\tE\nF\rG\fH\x00I\vJ\x7fK\u200dL"}
        self.assertEqual(corpus_content(row).decode(), "A B\n\nC D\tE\nF\rG\fH I J K\u200dL")
        with self.assertRaises(EvaluationError):
            query_for_api("query\u0096control", "fail")

    def test_clean_content_is_byte_identical_to_previous_serialization(self):
        for row in ({"title": "中文标题", "text": "正文\n换行\r回车\t制表\f换页"},
                    {"title": "  spaced title  ", "text": "  spaced body  "},
                    {"text": "  no title 🙂\u200d  "}):
            expected = ((row.get("title") or "") + "\n\n" + row["text"]).strip().encode()
            self.assertEqual(corpus_content(row), expected)

    def test_normalization_provenance_keeps_input_files_and_ids(self):
        with tempfile.TemporaryDirectory() as directory:
            make_dataset(directory)
            path = Path(directory) / "corpus.jsonl"
            rows = [json.loads(line) for line in path.read_text().splitlines()]
            rows[0]["text"] = "Alpha\u0096body\u0082with\u0094controls"
            path.write_text("".join(json.dumps(row) + "\n" for row in rows))
            original = path.read_bytes()
            dataset = load_dataset(directory, "test")
            info = dataset_info(dataset)
            self.assertEqual(path.read_bytes(), original)
            self.assertEqual([row["_id"] for row in dataset["corpus"]], ["a", "b"])
            self.assertEqual(info["input_sha256"]["corpus"], hashlib.sha256(original).hexdigest())
            self.assertEqual(info["content_policy"], CONTENT_POLICY)
            self.assertEqual(info["control_normalization"], {"documents": 1, "characters": 3})
            content_hashes = {row["_id"]: hashlib.sha256(corpus_content(row)).hexdigest() for row in rows}
            expected = hashlib.sha256(json.dumps(content_hashes, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()
            self.assertEqual(info["normalized_content_sha256"], expected)

    def test_split_selection_hashes_and_duplicate_rejection(self):
        with tempfile.TemporaryDirectory() as directory:
            dataset = make_dataset(directory)
            self.assertEqual(dataset_info(dataset)["counts"]["evaluated_queries"], 2)
            self.assertEqual(len(dataset["hashes"]["corpus"]), 64)
            with open(Path(directory) / "queries.jsonl", "a") as stream:
                stream.write('{"_id":"q1","text":"duplicate"}\n')
            with self.assertRaises(EvaluationError):
                load_dataset(directory, "test")

    def test_invalid_qrels_and_empty_content_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            make_dataset(directory)
            target = Path(directory) / "qrels" / "test.tsv"
            for content in ("bad header\n", "query-id\tcorpus-id\tscore\nq1\tunknown\t1\n", "query-id\tcorpus-id\tscore\nq1\ta\t0\n"):
                target.write_text(content)
                with self.assertRaises(EvaluationError):
                    load_dataset(directory, "test")


class StateIntegrityTests(unittest.TestCase):
    def state_for(self, dataset):
        return {"schema": 1, "corpus_sha256": dataset["hashes"]["corpus"], "workspace_id": "workspace",
                "documents": {row["_id"]: {"document_id": "doc-" + row["_id"], "version_id": "version-" + row["_id"],
                                            "source_sha256": hashlib.sha256(corpus_content(row)).hexdigest(), "status": "uploaded"}
                              for row in dataset["corpus"]}}

    def test_correct_full_partial_and_empty_mappings_are_allowed(self):
        with tempfile.TemporaryDirectory() as directory:
            dataset = make_dataset(directory)
            path = Path(directory) / "state.json"
            state = self.state_for(dataset)
            for identifiers in (("a", "b"), ("a",), ()):
                selected = dict(state, documents={identifier: state["documents"][identifier] for identifier in identifiers})
                atomic_json(path, selected)
                self.assertEqual(set(load_state(path, dataset)["documents"]), set(identifiers))

    def test_different_document_mappings_cannot_be_swapped(self):
        with tempfile.TemporaryDirectory() as directory:
            dataset = make_dataset(directory)
            state = self.state_for(dataset)
            state["documents"]["a"], state["documents"]["b"] = state["documents"]["b"], state["documents"]["a"]
            path = Path(directory) / "state.json"
            atomic_json(path, state)
            with self.assertRaises(EvaluationError):
                load_state(path, dataset)

    def test_legacy_cc_original_digest_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            make_dataset(directory)
            corpus = Path(directory) / "corpus.jsonl"
            rows = [json.loads(line) for line in corpus.read_text().splitlines()]
            rows[0]["text"] = "Alpha\u0096body"
            corpus.write_text("".join(json.dumps(row) + "\n" for row in rows))
            dataset = load_dataset(directory, "test")
            state = self.state_for(dataset)
            path = Path(directory) / "state.json"
            atomic_json(path, state)
            self.assertEqual(load_state(path, dataset), state)
            raw = (rows[0]["title"] + "\n\n" + rows[0]["text"]).strip().encode()
            state["documents"]["a"]["source_sha256"] = hashlib.sha256(raw).hexdigest()
            atomic_json(path, state)
            with self.assertRaises(EvaluationError):
                load_state(path, dataset)

    def test_malformed_state_entries_fail_with_safe_evaluation_error(self):
        with tempfile.TemporaryDirectory() as directory:
            dataset = make_dataset(directory)
            path = Path(directory) / "state.json"
            for entry in (None, {}, "invalid", {"source_sha256": 42}):
                state = self.state_for(dataset)
                state["documents"]["a"] = entry
                atomic_json(path, state)
                with self.assertRaises(EvaluationError):
                    load_state(path, dataset)
            atomic_json(path, [])
            with self.assertRaises(EvaluationError):
                load_state(path, dataset)


class FakeAPI:
    base = "http://127.0.0.1:8080/api/v1"
    workspace_id = "b" * 32
    profile_id = "a" * 64

    def __init__(self, dataset):
        self.dataset, self.documents, self.search_calls = dataset, {}, 0
        self.fail_query = None
        self.execution = "baseline"

    def request(self, method, path, data=None, media="application/json", envelope=False):
        if method == "POST" and path == "/workspaces":
            return {"id": self.workspace_id}
        if method == "GET" and path == "/workspaces/" + self.workspace_id:
            return {"id": self.workspace_id}
        if "/documents?page=" in path:
            return {"data": list(self.documents.values()), "total": len(self.documents)}
        if method == "GET" and "/versions/" in path:
            doc = self.documents[path.split("/")[4]]
            if path.endswith("/index"):
                return {"id": "index-" + doc["id"], "status": "ready", "source_sha256": doc["sha256"],
                        "profile_id": self.profile_id, "generation": "generation", "chunk_count": 1}
            return {"id": doc["current_version_id"], "sha256": doc["sha256"]}
        if method == "POST" and path.endswith("/search"):
            self.search_calls += 1
            if self.fail_query == data["query"]:
                raise EvaluationError("injected unavailable dependency")
            docs = list(self.documents.values())
            if data["query"].startswith("Beta"):
                docs.reverse()
            return {"profile_id": self.profile_id, "mode": data["mode"], "execution": self.execution, "hits": [
                {"chunk": {"document_id": doc["id"], "version_id": doc["current_version_id"], "index_id": "index-" + doc["id"],
                           "source_sha256": doc["sha256"]}} for doc in docs]}
        raise AssertionError((method, path))

    def upload(self, workspace, title, content):
        document_id = "doc" + str(len(self.documents))
        document = {"id": document_id, "title": title, "current_version_id": "version-" + document_id,
                    "sha256": hashlib.sha256(content).hexdigest()}
        self.documents[document_id] = document
        return {"document": document, "version": {"id": document["current_version_id"], "sha256": document["sha256"]}}


class PipelineTests(unittest.TestCase):
    def test_identical_content_swapped_ids_fail_remote_title_verification(self):
        with tempfile.TemporaryDirectory() as directory:
            make_dataset(directory)
            corpus = Path(directory) / "corpus.jsonl"
            rows = [json.loads(line) for line in corpus.read_text().splitlines()]
            for row in rows:
                row.update(title="Same title", text="Identical document content")
            corpus.write_text("".join(json.dumps(row) + "\n" for row in rows))
            dataset = load_dataset(directory, "test")
            client = FakeAPI(dataset)
            path = Path(directory) / "state.json"
            ingest(client, dataset, path, progress=lambda _: None)
            state = load_state(path, dataset)
            state["documents"]["a"], state["documents"]["b"] = state["documents"]["b"], state["documents"]["a"]
            atomic_json(path, state)
            # Equal bytes satisfy per-ID SHA checks; the deterministic title
            # still distinguishes the two real document identities.
            load_state(path, dataset)
            with self.assertRaises(EvaluationError):
                evaluate(client, dataset, path, Path(directory) / "results", ["hybrid"], "swapped", progress=lambda _: None)
            self.assertEqual(client.search_calls, 0)

    def test_full_fixture_pipeline_resume_and_manifest_guard(self):
        with tempfile.TemporaryDirectory() as directory:
            dataset = make_dataset(directory)
            client = FakeAPI(dataset)
            state = Path(directory) / "state.json"
            result = ingest(client, dataset, state, progress=lambda _: None)
            self.assertTrue(result["complete"])
            ingest(client, dataset, state, progress=lambda _: None)
            self.assertEqual(len(client.documents), 2)
            output = Path(directory) / "results"
            summaries = evaluate(client, dataset, state, output, ["lexical", "vector", "hybrid"], "baseline", progress=lambda _: None)
            self.assertEqual(summaries["hybrid"]["metrics"]["nDCG@10"], 1)
            self.assertEqual(client.search_calls, 6)
            evaluate(client, dataset, state, output, ["lexical", "vector", "hybrid"], "baseline", progress=lambda _: None)
            self.assertEqual(client.search_calls, 6)
            self.assertEqual(len(load_records(output / "baseline.hybrid.rankings.jsonl")), 2)
            with self.assertRaises(EvaluationError):
                evaluate(client, dataset, state, output, ["lexical"], "baseline", progress=lambda _: None)

    def test_query_failure_recorded_and_counted(self):
        with tempfile.TemporaryDirectory() as directory:
            dataset = make_dataset(directory)
            client = FakeAPI(dataset)
            state = Path(directory) / "state.json"
            ingest(client, dataset, state, progress=lambda _: None)
            client.fail_query = "Beta claim"
            summary = evaluate(client, dataset, state, Path(directory) / "results", ["hybrid"], "failed", progress=lambda _: None)["hybrid"]
            self.assertFalse(summary["coverage"]["complete"])
            self.assertEqual(summary["coverage"]["recorded"], 2)
            self.assertEqual(summary["metrics"]["nDCG@10"], .5)
            client.fail_query = None
            summary = evaluate(client, dataset, state, Path(directory) / "results", ["hybrid"], "failed",
                               progress=lambda _: None, retry_errors=True)["hybrid"]
            self.assertTrue(summary["coverage"]["complete"])
            failures = load_records(Path(directory) / "results" / "failed.hybrid.failed-attempts.jsonl")
            self.assertEqual(len(failures), 1)

    def test_remote_document_head_change_is_rejected_before_queries(self):
        with tempfile.TemporaryDirectory() as directory:
            dataset = make_dataset(directory)
            client = FakeAPI(dataset)
            state = Path(directory) / "state.json"
            ingest(client, dataset, state, progress=lambda _: None)
            next(iter(client.documents.values()))["current_version_id"] = "changed-head"
            with self.assertRaises(EvaluationError):
                evaluate(client, dataset, state, Path(directory) / "results", ["hybrid"], "frozen", progress=lambda _: None)
            self.assertEqual(client.search_calls, 0)

    def test_head_changes_during_queries_prevent_success_summary(self):
        with tempfile.TemporaryDirectory() as directory:
            dataset = make_dataset(directory)
            client = FakeAPI(dataset)
            state = Path(directory) / "state.json"
            ingest(client, dataset, state, progress=lambda _: None)
            original = client.request
            def mutate_after_query(method, path, data=None, media="application/json", envelope=False):
                response = original(method, path, data, media, envelope)
                if path.endswith("/search"):
                    if client.search_calls == 1:
                        next(iter(client.documents.values()))["current_version_id"] = "changed-head"
                    else:
                        response["hits"] = []
                return response
            client.request = mutate_after_query
            output = Path(directory) / "results"
            with self.assertRaises(EvaluationError):
                evaluate(client, dataset, state, output, ["hybrid"], "changing", progress=lambda _: None)
            self.assertEqual(len(load_records(output / "changing.hybrid.rankings.jsonl")), 2)
            self.assertFalse((output / "changing.hybrid.summary.json").exists())

    def test_index_generation_changes_are_detected_by_strict_verification(self):
        with tempfile.TemporaryDirectory() as directory:
            dataset = make_dataset(directory)
            client = FakeAPI(dataset)
            state = Path(directory) / "state.json"
            ingest(client, dataset, state, progress=lambda _: None)
            original = client.request
            def changed_index(method, path, data=None, media="application/json", envelope=False):
                response = original(method, path, data, media, envelope)
                if path.endswith("/index"):
                    response["generation"] = "changed-generation"
                return response
            client.request = changed_index
            with self.assertRaises(EvaluationError):
                evaluate(client, dataset, state, Path(directory) / "results", ["hybrid"], "changed", progress=lambda _: None)
            self.assertEqual(client.search_calls, 0)

    def test_hit_index_and_digest_must_match_frozen_state(self):
        for field in ("index_id", "source_sha256"):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory:
                dataset = make_dataset(directory)
                client = FakeAPI(dataset)
                state = Path(directory) / "state.json"
                ingest(client, dataset, state, progress=lambda _: None)
                original = client.request
                def changed_hit(method, path, data=None, media="application/json", envelope=False):
                    response = original(method, path, data, media, envelope)
                    if path.endswith("/search"):
                        response["hits"][0]["chunk"][field] = "changed"
                    return response
                client.request = changed_hit
                summary = evaluate(client, dataset, state, Path(directory) / "results", ["hybrid"], "changed", progress=lambda _: None)["hybrid"]
                self.assertEqual(summary["coverage"]["failed"], 2)

    def test_execution_strategy_is_reported_and_enforced(self):
        with tempfile.TemporaryDirectory() as directory:
            dataset = make_dataset(directory)
            client = FakeAPI(dataset)
            state = Path(directory) / "state.json"
            ingest(client, dataset, state, progress=lambda _: None)
            summary = evaluate(client, dataset, state, Path(directory) / "results", ["hybrid"], "wrong-label",
                               progress=lambda _: None, expected_execution="optimized")["hybrid"]
            self.assertEqual(summary["coverage"]["failed"], 2)
            self.assertIsNone(summary["observed_execution"])


class ComparisonTests(unittest.TestCase):
    def prepare(self, directory):
        dataset = make_dataset(directory)
        client = FakeAPI(dataset)
        state = Path(directory) / "state.json"
        ingest(client, dataset, state, progress=lambda _: None)
        output = Path(directory) / "results"
        evaluate(client, dataset, state, output, ["hybrid"], "baseline", progress=lambda _: None, expected_execution="baseline")
        client.execution = "optimized"
        evaluate(client, dataset, state, output, ["hybrid"], "optimized", progress=lambda _: None, expected_execution="optimized")
        return dataset, output

    def compare(self, dataset, output):
        return compare_runs(output / "baseline.hybrid.rankings.jsonl", output / "optimized.hybrid.rankings.jsonl", dataset, 100, 42)

    def test_verified_comparison_allows_distinct_execution_strategies(self):
        with tempfile.TemporaryDirectory() as directory:
            dataset, output = self.prepare(directory)
            result = self.compare(dataset, output)
            self.assertTrue(result["ranking_parity"])
            self.assertEqual(result["execution"], {"baseline": "baseline", "candidate": "optimized"})

    def test_missing_one_or_both_manifests_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            dataset, output = self.prepare(directory)
            for label in ("baseline", "optimized"):
                (output / (label + ".manifest.json")).unlink()
                with self.assertRaises(EvaluationError):
                    self.compare(dataset, output)

    def test_incomplete_or_tampered_manifests_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            dataset, output = self.prepare(directory)
            path = output / "optimized.manifest.json"
            original = json.loads(path.read_text())
            required = ("schema", "dataset", "label", "modes", "chunk_limit", "query_policy", "truncated_queries",
                        "workspace_id", "profile_id", "mapping_sha256", "api_base", "python", "query_order",
                        "latency_note", "quality_protocol", "corpus_verification", "expected_execution",
                        "evaluator_source_sha256", "environment_freeze")
            for key in required:
                with self.subTest(missing=key):
                    changed = dict(original)
                    changed.pop(key)
                    atomic_json(path, changed)
                    with self.assertRaises(EvaluationError):
                        self.compare(dataset, output)
            for key, value in (("profile_id", "not-a-hash"), ("mapping_sha256", "f" * 64), ("schema", True)):
                with self.subTest(tampered=key):
                    changed = dict(original)
                    changed[key] = value
                    atomic_json(path, changed)
                    with self.assertRaises(EvaluationError):
                        self.compare(dataset, output)
            changed = json.loads(json.dumps(original))
            changed["dataset"]["input_sha256"]["qrels"] = "f" * 64
            atomic_json(path, changed)
            with self.assertRaises(EvaluationError):
                self.compare(dataset, output)

    def test_comparison_requires_same_evaluator_source(self):
        with tempfile.TemporaryDirectory() as directory:
            dataset, output = self.prepare(directory)
            client = FakeAPI(dataset)
            # The existing mapping is deterministic in this isolated fixture.
            state = Path(directory) / "state.json"
            ingest(client, dataset, state, progress=lambda _: None)
            client.execution = "optimized"
            with patch("scripts.retrieval_eval.runner.evaluator_source_sha256", return_value="f" * 64):
                evaluate(client, dataset, state, output, ["hybrid"], "new-evaluator", progress=lambda _: None)
            with self.assertRaises(EvaluationError):
                compare_runs(output / "baseline.hybrid.rankings.jsonl", output / "new-evaluator.hybrid.rankings.jsonl", dataset, 100, 42)

    def test_content_policy_provenance_is_required_for_comparison(self):
        with tempfile.TemporaryDirectory() as directory:
            dataset, output = self.prepare(directory)
            path = output / "optimized.manifest.json"
            original = json.loads(path.read_text())
            for key in ("content_policy", "normalized_content_sha256", "control_normalization"):
                with self.subTest(missing=key):
                    changed = json.loads(json.dumps(original))
                    changed["dataset"].pop(key)
                    atomic_json(path, changed)
                    with self.assertRaises(EvaluationError):
                        self.compare(dataset, output)

    def test_final_verification_summary_and_ranking_digest_are_required(self):
        with tempfile.TemporaryDirectory() as directory:
            dataset, output = self.prepare(directory)
            summary_path = output / "optimized.hybrid.summary.json"
            summary = json.loads(summary_path.read_text())
            summary_path.unlink()
            with self.assertRaises(EvaluationError):
                self.compare(dataset, output)
            atomic_json(summary_path, summary)
            with open(output / "optimized.hybrid.rankings.jsonl", "a") as stream:
                stream.write('{"query_id":"forged"}\n')
            with self.assertRaises(EvaluationError):
                self.compare(dataset, output)


class SourceEnvironmentTests(unittest.TestCase):
    def write_environment(self, path, model="model-one"):
        atomic_json(path, {"schema": 1, "source_sha256": "c" * 64,
                           "measured_environment": {"model_digest": model, "profile_id": "a" * 64}})

    def test_production_hash_excludes_tests_and_changes_with_source(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "api.py").write_text("value = 1\n")
            (root / "test_api.py").write_text("test = 1\n")
            before = evaluator_source_sha256(root)
            (root / "test_api.py").write_text("test = 2\n")
            self.assertEqual(before, evaluator_source_sha256(root))
            (root / "api.py").write_text("value = 2\n")
            self.assertNotEqual(before, evaluator_source_sha256(root))

    def test_changed_evaluator_refuses_resume_without_new_queries(self):
        with tempfile.TemporaryDirectory() as directory:
            dataset = make_dataset(directory)
            client = FakeAPI(dataset)
            state, output = Path(directory) / "state.json", Path(directory) / "results"
            ingest(client, dataset, state, progress=lambda _: None)
            evaluate(client, dataset, state, output, ["hybrid"], "baseline", progress=lambda _: None)
            with patch("scripts.retrieval_eval.runner.evaluator_source_sha256", return_value="f" * 64), self.assertRaises(EvaluationError):
                evaluate(client, dataset, state, output, ["hybrid"], "baseline", progress=lambda _: None)
            self.assertEqual(client.search_calls, 2)

    def test_environment_file_changes_refuse_resume(self):
        with tempfile.TemporaryDirectory() as directory:
            dataset = make_dataset(directory)
            client = FakeAPI(dataset)
            state, output = Path(directory) / "state.json", Path(directory) / "results"
            frozen = Path(directory) / "environment.freeze.json"
            self.write_environment(frozen)
            ingest(client, dataset, state, progress=lambda _: None)
            evaluate(client, dataset, state, output, ["hybrid"], "baseline", progress=lambda _: None, environment_file=frozen)
            frozen.write_bytes(frozen.read_bytes() + b"\n")
            with self.assertRaises(EvaluationError):
                evaluate(client, dataset, state, output, ["hybrid"], "baseline", progress=lambda _: None, environment_file=frozen)
            self.write_environment(frozen, "model-two")
            with self.assertRaises(EvaluationError):
                evaluate(client, dataset, state, output, ["hybrid"], "baseline", progress=lambda _: None, environment_file=frozen)
            self.assertEqual(client.search_calls, 2)

    def test_same_external_freeze_allows_distinct_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            dataset = make_dataset(directory)
            client = FakeAPI(dataset)
            state, output = Path(directory) / "state.json", Path(directory) / "results"
            frozen = Path(directory) / "environment.freeze.json"
            self.write_environment(frozen)
            ingest(client, dataset, state, progress=lambda _: None)
            evaluate(client, dataset, state, output, ["hybrid"], "baseline", progress=lambda _: None, environment_file=frozen)
            client.execution = "optimized"
            evaluate(client, dataset, state, output, ["hybrid"], "optimized", progress=lambda _: None, environment_file=frozen)
            compared = compare_runs(output / "baseline.hybrid.rankings.jsonl", output / "optimized.hybrid.rankings.jsonl", dataset, 100, 42)
            self.assertTrue(compared["environment_frozen"])
            self.assertEqual(compared["execution"], {"baseline": "baseline", "candidate": "optimized"})

    def test_different_environment_files_cannot_be_compared(self):
        with tempfile.TemporaryDirectory() as directory:
            dataset = make_dataset(directory)
            client = FakeAPI(dataset)
            state, output = Path(directory) / "state.json", Path(directory) / "results"
            frozen = Path(directory) / "environment.freeze.json"
            self.write_environment(frozen)
            ingest(client, dataset, state, progress=lambda _: None)
            evaluate(client, dataset, state, output, ["hybrid"], "baseline", progress=lambda _: None, environment_file=frozen)
            self.write_environment(frozen, "model-two")
            client.execution = "optimized"
            evaluate(client, dataset, state, output, ["hybrid"], "optimized", progress=lambda _: None, environment_file=frozen)
            with self.assertRaises(EvaluationError):
                compare_runs(output / "baseline.hybrid.rankings.jsonl", output / "optimized.hybrid.rankings.jsonl", dataset, 100, 42)

    def test_environment_schema_and_credential_fields_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "environment.freeze.json"
            for value in ({"schema": 1}, {"schema": 1, "source_sha256": "invalid", "measured_environment": {"model": "test"}},
                          {"schema": 1, "source_sha256": "a" * 64, "measured_environment": {"api_key": "not-a-real-key"}}):
                atomic_json(path, value)
                with self.assertRaises(EvaluationError):
                    environment_freeze(path)

class HTTPClientTests(unittest.TestCase):
    def test_response_read_failures_are_safe_and_queries_remain_counted(self):
        failures = (TimeoutError("secret timeout"), ConnectionResetError("secret reset"), http.client.IncompleteRead(b"secret partial"))
        for failure in failures:
            with self.subTest(kind=type(failure).__name__), tempfile.TemporaryDirectory() as directory:
                dataset = make_dataset(directory)
                fixture = FakeAPI(dataset)
                state = Path(directory) / "state.json"
                ingest(fixture, dataset, state, progress=lambda _: None)
                response = Mock()
                response.__enter__ = Mock(return_value=response)
                response.__exit__ = Mock(return_value=False)
                response.headers = {"X-Request-ID": "c" * 32}
                response.status = 200
                response.read.side_effect = failure
                opener = Mock()
                opener.open.return_value = response
                with patch.dict(os.environ, {"EVAL_TEST_TOKEN": "private"}):
                    real_client = APIClient(fixture.base, "EVAL_TEST_TOKEN")
                original = fixture.request
                def wired_search(method, path, data=None, media="application/json", envelope=False):
                    if path.endswith("/search"):
                        return real_client.request(method, path, data, media, envelope)
                    return original(method, path, data, media, envelope)
                fixture.request = wired_search
                with patch("urllib.request.build_opener", return_value=opener):
                    summary = evaluate(fixture, dataset, state, Path(directory) / "results", ["hybrid"], "read-failure", progress=lambda _: None)["hybrid"]
                self.assertEqual(summary["coverage"]["failed"], 2)
                records = load_records(Path(directory) / "results" / "read-failure.hybrid.rankings.jsonl")
                self.assertTrue(all(record["error"] == "API response read failed; no response body logged" for record in records))
                self.assertNotIn("secret", json.dumps(records))

    def test_error_envelope_error_field_may_not_be_an_object(self):
        with patch.dict(os.environ, {"EVAL_TEST_TOKEN": "private"}):
            client = APIClient("http://127.0.0.1:8080", "EVAL_TEST_TOKEN")
        for error_data in (None, "secret", ["secret"], 42):
            with self.subTest(kind=type(error_data).__name__):
                response = Mock()
                response.__enter__ = Mock(return_value=response)
                response.__exit__ = Mock(return_value=False)
                response.headers = {}
                response.status = 503
                response.read.return_value = json.dumps({"error": error_data}).encode()
                opener = Mock()
                opener.open.return_value = response
                with patch("urllib.request.build_opener", return_value=opener), self.assertRaises(EvaluationError) as caught:
                    client.request("GET", "/test")
                self.assertIn("HTTP 503, code unknown", str(caught.exception))
                self.assertNotIn("secret", str(caught.exception))

    def test_error_payload_never_logged_and_no_redirect(self):
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(503)
                self.end_headers()
                self.wfile.write(b'{"error":{"code":"safe_code","message":"secret-token"}}')
            def log_message(self, *args):
                pass
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with patch.dict(os.environ, {"EVAL_TEST_TOKEN": "private"}):
                client = APIClient(f"http://127.0.0.1:{server.server_port}", "EVAL_TEST_TOKEN")
                with self.assertRaises(EvaluationError) as caught:
                    client.request("GET", "/foo")
                self.assertIn("safe_code", str(caught.exception))
                self.assertNotIn("secret-token", str(caught.exception))
                self.assertNotIn("private", str(caught.exception))
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def test_redirect_does_not_forward_authorization(self):
        requests = []
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                requests.append(self.path)
                self.send_response(302)
                self.send_header("Location", "/should-not-follow")
                self.end_headers()
            def log_message(self, *args):
                pass
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with patch.dict(os.environ, {"EVAL_TEST_TOKEN": "private"}):
                client = APIClient(f"http://127.0.0.1:{server.server_port}", "EVAL_TEST_TOKEN")
                with self.assertRaises(EvaluationError):
                    client.request("GET", "/original")
                self.assertEqual(requests, ["/api/v1/original"])
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def test_concurrent_expired_tokens_refresh_once_and_persist_privately(self):
        refreshes = []
        class Handler(http.server.BaseHTTPRequestHandler):
            def reply(self, status, body):
                self.send_response(status)
                self.end_headers()
                self.wfile.write(json.dumps(body).encode())
            def do_GET(self):
                if self.headers.get("Authorization") == "Bearer renewed":
                    self.reply(200, {"success": True, "data": {"id": "workspace"}})
                else:
                    self.reply(401, {"error": {"code": "unauthorized"}})
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                refreshes.append(body["refresh_token"])
                self.reply(200, {"success": True, "data": {"token": "renewed", "refresh_token": "renewed-refresh"}})
            def log_message(self, *args):
                pass
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as directory:
                auth = Path(directory) / "auth.json"
                atomic_json(auth, {"token": "expired", "refresh_token": "original-refresh"}, private=True)
                client = APIClient(f"http://127.0.0.1:{server.server_port}", auth_file=auth)
                with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                    futures = [pool.submit(client.request, "GET", "/workspaces/workspace") for _ in range(2)]
                    self.assertEqual([future.result() for future in futures], [{"id": "workspace"}] * 2)
                self.assertEqual(refreshes, ["original-refresh"])
                self.assertEqual(json.loads(auth.read_text())["token"], "renewed")
                self.assertEqual(auth.stat().st_mode & 0o077, 0)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def test_auth_file_requires_private_permissions(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "auth.json"
            atomic_json(path, {"token": "test", "refresh_token": "refresh"})
            path.chmod(0o644)
            with self.assertRaises(EvaluationError):
                APIClient("http://localhost", auth_file=path)
            path.chmod(0o600)
            self.assertEqual(APIClient("http://localhost", auth_file=path).token, "test")


if __name__ == "__main__":
    unittest.main()
