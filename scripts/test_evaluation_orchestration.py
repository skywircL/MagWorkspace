"""Regression checks for orchestration resumes; no Docker or live API calls."""
import copy
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import call, patch


SCRIPT = Path(__file__).resolve().with_name("run-scifact.py")


def load_orchestrator():
    spec = importlib.util.spec_from_file_location("scifact_orchestration_test", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def complete_state():
    profile = "a" * 64
    return {
        "schema": 1,
        "ready_at": "2026-10-16T09:00:00+00:00",
        "corpus_sha256": "b" * 64,
        "profile_id": profile,
        "documents": {
            "claim-paper-1": {"status": "ready", "profile_id": profile, "index_id": "1" * 32, "generation": "2" * 32},
            "claim-paper-2": {"status": "ready", "profile_id": profile, "index_id": "3" * 32, "generation": "4" * 32},
        },
    }


def frozen_environment():
    return {
        "schema": 1,
        "source_sha256": "a" * 64,
        "measured_environment": {
            "source_files": {"internal/indexing/service.go": "b" * 64},
            "embedding_model": {"name": "bge-m3:567m", "digest": "sha256:" + "c" * 64},
            "index_settings": {"number_of_shards": "1", "number_of_replicas": "0", "refresh_interval": "1s"},
            "physical_index": {"uuid": "physical-index-uuid", "preparation": "single_segment_before_queries",
                               "primary_segments": {"0": {"_1": {"generation": 1, "num_docs": 7, "deleted_docs": 0}}}},
            "corpus_sha256": "d" * 64,
            "profile_id": "e" * 64,
        },
    }


class IngestionCompletionTests(unittest.TestCase):
    def setUp(self):
        self.orchestrator = load_orchestrator()
        self.corpus_ids = {"claim-paper-1", "claim-paper-2"}

    def check_incomplete(self, state):
        before = copy.deepcopy(state)
        self.assertFalse(self.orchestrator.ingestion_complete(state, self.corpus_ids))
        self.assertEqual(state, before, "readiness checks must not repair or modify ingestion state")

    def test_complete_state_is_ready(self):
        state = complete_state()
        self.assertTrue(self.orchestrator.ingestion_complete(state, self.corpus_ids))
        state["ingestion_failures"] = 0
        self.assertTrue(self.orchestrator.ingestion_complete(state, self.corpus_ids))

    def test_old_ready_at_does_not_promote_resumed_uploads(self):
        for status in ("uploaded", "queued", "running"):
            with self.subTest(status=status):
                state = complete_state()
                state["documents"]["claim-paper-2"]["status"] = status
                self.check_incomplete(state)

    def test_missing_or_extra_mapping_is_incomplete(self):
        missing = complete_state()
        del missing["documents"]["claim-paper-2"]
        extra = complete_state()
        extra["documents"]["foreign-paper"] = copy.deepcopy(extra["documents"]["claim-paper-1"])
        for name, state in (("missing", missing), ("extra", extra)):
            with self.subTest(mapping=name):
                self.check_incomplete(state)

    def test_mixed_profiles_are_incomplete(self):
        state = complete_state()
        state["documents"]["claim-paper-2"]["profile_id"] = "f" * 64
        self.check_incomplete(state)

    def test_recorded_failures_are_incomplete(self):
        state = complete_state()
        state["ingestion_failures"] = 1
        self.check_incomplete(state)
        state = complete_state()
        state["documents"]["claim-paper-2"]["status"] = "failed"
        self.check_incomplete(state)

    def test_unpublished_or_unfinished_state_is_incomplete(self):
        for field in ("index_id", "generation", "profile_id"):
            with self.subTest(missing_entry_field=field):
                state = complete_state()
                del state["documents"]["claim-paper-2"][field]
                self.check_incomplete(state)
        for field in ("ready_at", "profile_id"):
            with self.subTest(missing_state_field=field):
                state = complete_state()
                del state[field]
                self.check_incomplete(state)


class PersistedFreezeTests(unittest.TestCase):
    def setUp(self):
        self.orchestrator = load_orchestrator()
        self.directory = tempfile.TemporaryDirectory(prefix="mw-evaluation-freeze-")
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "reports" / "environment.freeze.json"

    def test_same_freeze_is_reentrant_across_processes(self):
        value = frozen_environment()
        self.orchestrator.persist_freeze(self.path, value)
        before, stat = self.path.read_bytes(), self.path.stat()
        self.orchestrator.persist_freeze(self.path, copy.deepcopy(value))
        # A fresh interpreter loads the production module and attempts the
        # resume. No in-memory state from the first writer can make it pass.
        child = subprocess.run([
            sys.executable, "-B", "-c",
            "import importlib.util,json,sys; from pathlib import Path; "
            "spec=importlib.util.spec_from_file_location('scifact_resume_child',sys.argv[1]); "
            "module=importlib.util.module_from_spec(spec); spec.loader.exec_module(module); "
            "module.persist_freeze(Path(sys.argv[2]),json.loads(sys.argv[3]))",
            str(SCRIPT), str(self.path), json.dumps(value),
        ], capture_output=True, text=True, timeout=15, check=False)
        self.assertEqual(child.returncode, 0, child.stderr)
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual(self.path.stat().st_mtime_ns, stat.st_mtime_ns, "resume overwrote an existing freeze")
        self.assertEqual(json.loads(before), value)
        self.assertEqual(list(self.path.parent.glob("*.tmp")), [])

    def test_changed_source_or_model_is_rejected_without_overwrite(self):
        original = frozen_environment()
        self.orchestrator.persist_freeze(self.path, original)
        before = self.path.read_bytes()
        changed_source = copy.deepcopy(original)
        changed_source["source_sha256"] = "f" * 64
        changed_source["measured_environment"]["source_files"]["internal/indexing/service.go"] = "0" * 64
        changed_model = copy.deepcopy(original)
        changed_model["measured_environment"]["embedding_model"]["digest"] = "sha256:" + "1" * 64
        changed_settings = copy.deepcopy(original)
        changed_settings["measured_environment"]["index_settings"]["refresh_interval"] = "2s"
        changed_index = copy.deepcopy(original)
        changed_index["measured_environment"]["physical_index"]["primary_segments"]["0"]["_1"]["num_docs"] += 1
        for kind, candidate in (("source", changed_source), ("model", changed_model),
                                ("settings", changed_settings), ("physical_index", changed_index)):
            with self.subTest(changed=kind):
                with self.assertRaises(ValueError):
                    load_orchestrator().persist_freeze(self.path, candidate)
                self.assertEqual(self.path.read_bytes(), before)
                self.assertEqual(list(self.path.parent.glob("*.tmp")), [])

    def test_bad_existing_freeze_is_rejected_without_overwrite(self):
        self.path.parent.mkdir(parents=True)
        for raw in (b'{"schema":', b"null\n", b"[]\n", b'{"schema":999}\n'):
            with self.subTest(existing=raw):
                self.path.write_bytes(raw)
                with self.assertRaises(ValueError):
                    self.orchestrator.persist_freeze(self.path, frozen_environment())
                self.assertEqual(self.path.read_bytes(), raw)
                self.assertEqual(list(self.path.parent.glob("*.tmp")), [])


class PhysicalIndexPreparationTests(unittest.TestCase):
    def setUp(self):
        self.orchestrator = load_orchestrator()
        self.state = complete_state()
        self.index_name = "mw_" + self.state["profile_id"]
        self.directory = tempfile.TemporaryDirectory(prefix="mw-index-preparation-")
        self.addCleanup(self.directory.cleanup)
        self.output = Path(self.directory.name) / "reports"

    def segments(self):
        # Match the ES response contract, retaining deliberately volatile fields
        # so the test can prove that cache/size changes are not frozen.
        return {"_shards": {"total": 1, "successful": 1, "failed": 0}, "indices": {self.index_name: {
            "shards": {"0": [{"routing": {"primary": True, "state": "STARTED", "node": "volatile-node"},
                              "num_search_segments": 1, "num_committed_segments": 1,
                              "segments": {"_a": {"generation": 10, "num_docs": 7, "deleted_docs": 0,
                                                   "search": True, "committed": True, "size_in_bytes": 4096}}}]}}}}

    def statistics(self, current=0):
        return {"_shards": {"total": 1, "successful": 1, "failed": 0}, "indices": {self.index_name: {
            "primaries": {"merges": {"current": current, "total": 43}, "docs": {"count": 7}}}}}

    def test_existing_freeze_never_mutates_or_requests_preparation(self):
        self.output.mkdir()
        # Even an invalid freeze must be left to persist_freeze's validation;
        # a preparation retry must never silently repair the physical graph.
        for frozen in (json.dumps(frozen_environment()).encode(), b"invalid freeze\n"):
            with self.subTest(freeze=frozen):
                (self.output / "environment.freeze.json").write_bytes(frozen)
                (self.output / "baseline.hybrid.rankings.jsonl").write_text("old result\n")
                (self.output / "index-preparation.json").write_text("old preparation\n")
                before = {path.name: (path.read_bytes(), path.stat().st_mtime_ns) for path in self.output.iterdir()}
                with patch.object(self.orchestrator, "index_request") as request, \
                        patch.object(self.orchestrator, "index_shape") as shape:
                    self.orchestrator.prepare_index(self.state, self.output)
                request.assert_not_called()
                shape.assert_not_called()
                after = {path.name: (path.read_bytes(), path.stat().st_mtime_ns) for path in self.output.iterdir()}
                self.assertEqual(after, before)

    def test_old_rankings_or_manifest_without_freeze_rejects_before_mutation(self):
        self.output.mkdir()
        for name in ("baseline.hybrid.rankings.jsonl", "baseline.hybrid.manifest.json"):
            with self.subTest(existing=name):
                path = self.output / name
                path.write_text("existing result\n")
                before = path.read_bytes()
                with patch.object(self.orchestrator, "index_request") as request, \
                        patch.object(self.orchestrator, "index_shape") as shape:
                    with self.assertRaisesRegex(ValueError, "existing results"):
                        self.orchestrator.prepare_index(self.state, self.output)
                request.assert_not_called()
                shape.assert_not_called()
                self.assertEqual(path.read_bytes(), before)
                self.assertEqual({file.name for file in self.output.iterdir()}, {name})
                path.unlink()

    def test_preparation_waits_for_merge_then_refresh_and_validates_graph(self):
        replies = [{"_shards": {"failed": 0}}, {"_shards": {"failed": 0}}, self.segments(), self.statistics()]
        with patch.object(self.orchestrator, "index_request", side_effect=replies) as request:
            self.orchestrator.prepare_index(self.state, self.output)
        self.assertEqual(request.call_args_list, [
            call(self.state, "/_forcemerge?max_num_segments=1&wait_for_completion=true", "POST", 900),
            call(self.state, "/_refresh", "POST"), call(self.state, "/_segments"), call(self.state, "/_stats"),
        ])
        record = json.loads((self.output / "index-preparation.json").read_text())
        self.assertEqual(record["primary_segments"], {"0": {"_a": {"generation": 10, "num_docs": 7, "deleted_docs": 0}}})
        self.assertGreaterEqual(record["duration_seconds"], 0)
        self.assertEqual(list(self.output.glob("*.rankings.jsonl")), [])
        self.assertFalse((self.output / "environment.freeze.json").exists())

    def test_failed_merge_or_refresh_never_records_completed_preparation(self):
        for phase, replies in (("merge", [{"_shards": {"failed": 1}}]),
                               ("refresh", [{"_shards": {"failed": 0}}, {"_shards": {"failed": 1}}])):
            with self.subTest(phase=phase):
                with patch.object(self.orchestrator, "index_request", side_effect=replies) as request, \
                        patch.object(self.orchestrator, "index_shape") as shape:
                    with self.assertRaises(ValueError):
                        self.orchestrator.prepare_index(self.state, self.output)
                self.assertEqual(request.call_count, len(replies))
                shape.assert_not_called()
                self.assertFalse(self.output.exists())

    def test_shape_freezes_only_primary_segment_identity_and_doc_counts(self):
        segments = self.segments()
        # A replica is not part of the primary graph fingerprint.
        replica = copy.deepcopy(segments["indices"][self.index_name]["shards"]["0"][0])
        replica["routing"]["primary"] = False
        replica["num_search_segments"] = 2
        segments["indices"][self.index_name]["shards"]["0"].insert(0, replica)
        with patch.object(self.orchestrator, "index_request", side_effect=[segments, self.statistics()]):
            self.assertEqual(self.orchestrator.index_shape(self.state),
                             {"0": {"_a": {"generation": 10, "num_docs": 7, "deleted_docs": 0}}})

    def test_shape_rejects_missing_or_ambiguous_searchable_primary(self):
        for invalid in ("empty_shards", "no_primary", "two_primaries", "two_search_segments", "two_segments", "not_searchable"):
            with self.subTest(shape=invalid):
                segments = self.segments()
                shards = segments["indices"][self.index_name]["shards"]
                primary = shards["0"][0]
                if invalid == "empty_shards":
                    shards.clear()
                elif invalid == "no_primary":
                    primary["routing"]["primary"] = False
                elif invalid == "two_primaries":
                    shards["0"].append(copy.deepcopy(primary))
                elif invalid == "two_search_segments":
                    primary["num_search_segments"] = 2
                elif invalid == "two_segments":
                    primary["segments"]["_b"] = copy.deepcopy(primary["segments"]["_a"])
                else:
                    primary["segments"]["_a"]["search"] = False
                with patch.object(self.orchestrator, "index_request", side_effect=[segments, self.statistics()]) as request:
                    with self.assertRaises(ValueError):
                        self.orchestrator.index_shape(self.state)
                self.assertEqual(request.call_count, 1, "invalid segment state must not continue to statistics")

    def test_active_merge_rejects_preparation_without_completion_record(self):
        replies = [{"_shards": {"failed": 0}}, {"_shards": {"failed": 0}}, self.segments(), self.statistics(current=1)]
        with patch.object(self.orchestrator, "index_request", side_effect=replies):
            with self.assertRaisesRegex(ValueError, "still merging"):
                self.orchestrator.prepare_index(self.state, self.output)
        self.assertFalse(self.output.exists())

    def test_invalid_profile_is_rejected_before_endpoint_or_network_access(self):
        for profile in ("", "a" * 63, "a" * 65, "A" * 64, "*" * 64, "a" * 61 + "/..", "a" * 63 + "?"):
            with self.subTest(profile=profile):
                with patch.object(Path, "read_text") as read, \
                        patch.object(self.orchestrator.urllib.request, "build_opener") as opener:
                    with self.assertRaisesRegex(ValueError, "invalid physical index profile"):
                        self.orchestrator.index_request({"profile_id": profile}, "/_forcemerge", "POST")
                read.assert_not_called()
                opener.assert_not_called()

    def test_index_requests_target_only_the_validated_profile(self):
        runtime = Path(self.directory.name) / "runtime"
        runtime.mkdir()
        (runtime / "endpoints.json").write_text(json.dumps({"elasticsearch:9200": "http://127.0.0.1:19200"}))
        with patch.object(self.orchestrator, "RUNTIME", runtime), \
                patch.object(self.orchestrator.urllib.request, "build_opener") as opener:
            opener.return_value.open.return_value = io.BytesIO(b'{"_shards":{"failed":0}}')
            self.orchestrator.index_request(self.state, "/_forcemerge?max_num_segments=1&wait_for_completion=true", "POST", 900)
        arguments, options = opener.return_value.open.call_args
        self.assertEqual(arguments[0].full_url,
                         "http://127.0.0.1:19200/" + self.index_name + "/_forcemerge?max_num_segments=1&wait_for_completion=true")
        self.assertEqual(arguments[0].get_method(), "POST")
        self.assertEqual(options["timeout"], 900)


class OrchestrationRuntimeContractTests(unittest.TestCase):
    def test_qdrant_stack_and_orchestrator_share_a_new_runtime(self):
        spec = importlib.util.spec_from_file_location("qdrant_stack_test", SCRIPT.with_name("evaluation-stack.py"))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.assertEqual(module.RUNTIME, load_orchestrator().RUNTIME)
        self.assertEqual(module.RUNTIME.name, "retrieval-evaluation-qdrant")

    def test_source_freeze_includes_build_inputs_and_excludes_test_code(self):
        orchestrator = load_orchestrator()
        sources = orchestrator.source_fingerprint()
        for required in (".dockerignore", "Dockerfile", "compose.yaml", "scripts/run-scifact.py",
                         "scripts/evaluation-stack.py", "scripts/download-scifact.py",
                         "scripts/retrieval_eval/runner.py", "internal/indexing/service.go"):
            self.assertIn(required, sources)
        self.assertTrue(any(name.endswith(".sql") for name in sources))
        self.assertTrue(any(name.startswith("docker/") and name.endswith(".xml") for name in sources))
        self.assertFalse(any(name.endswith("_test.go") for name in sources))
        self.assertFalse(any(name.startswith("scripts/retrieval_eval/test") for name in sources))
        self.assertFalse(any("/.deps/" in name or "/__pycache__/" in name for name in sources))

    def test_orchestrator_freeze_matches_evaluator_contract(self):
        from scripts.retrieval_eval.runner import environment_freeze

        orchestrator = load_orchestrator()
        sources = orchestrator.source_fingerprint()
        freeze = frozen_environment()
        freeze["source_sha256"] = hashlib.sha256(json.dumps(sources, sort_keys=True).encode()).hexdigest()
        freeze["measured_environment"]["source_files"] = sources
        freeze["measured_environment"]["application_sha256"] = "f" * 64
        with tempfile.TemporaryDirectory(prefix="mw-environment-contract-") as temporary:
            path = Path(temporary) / "environment.freeze.json"
            orchestrator.persist_freeze(path, freeze)
            accepted = environment_freeze(path)
            self.assertEqual(accepted["value"], freeze)
            self.assertEqual(accepted["file_sha256"], hashlib.sha256(path.read_bytes()).hexdigest())
            self.assertEqual(len(accepted["content_sha256"]), 64)

    def test_application_hash_reads_only_the_configured_stack_container(self):
        orchestrator = load_orchestrator()
        with tempfile.TemporaryDirectory(prefix="mw-application-hash-") as temporary:
            runtime = Path(temporary)
            (runtime / "stack.env").write_text("COMPOSE_PROJECT_NAME=mw-eval-unit-test\n")
            with patch.object(orchestrator, "RUNTIME", runtime), patch.object(orchestrator.subprocess, "run") as execute:
                execute.return_value.stdout = "a" * 64 + "  /app/magworkspace\n"
                self.assertEqual(orchestrator.application_sha256(), "a" * 64)
                arguments = execute.call_args.args[0]
                self.assertEqual(arguments, ["docker", "exec", "mw-eval-unit-test-backend-1", "sha256sum", "/app/magworkspace"])
                self.assertEqual(execute.call_args.kwargs["timeout"], 30)
                for invalid in ("", "A" * 64, "a" * 63, "a" * 64 + " extra"):
                    with self.subTest(digest=invalid):
                        execute.return_value.stdout = invalid
                        with self.assertRaises(ValueError):
                            orchestrator.application_sha256()


class QdrantEnvironmentTests(unittest.TestCase):
    def setUp(self):
        self.module = load_orchestrator()
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.runtime = Path(self.directory.name)
        (self.runtime / 'endpoints.json').write_text(json.dumps({'qdrant:6333':'http://127.0.0.1:16333','ollama:11434':'http://127.0.0.1:11435'}))
        (self.runtime / 'stack.env').write_text('QDRANT_API_KEY=synthetic-only\n')
        self.info = {'status':'green', 'optimizer_status':'ok', 'points_count':2,
                     'config':{'vectors':{'dense':{'size':3}}}, 'segments_count':1, 'indexed_vectors_count':0}
        self.points = [{'id':'a', 'payload':{'generation_id':'x'}, 'vector':{'dense':[1,0,0]}},
                       {'id':'b', 'payload':{'generation_id':'x'}, 'vector':{'dense':[0,1,0]}}]

    def environment(self, info=None, points=None):
        points = points or self.points
        replies = [info or self.info, {'points':points[:1], 'next_page_offset':'b'}, {'points':points[1:], 'next_page_offset':None}]
        with patch.object(self.module, 'RUNTIME', self.runtime), \
                patch.object(self.module, 'qdrant_request', side_effect=replies) as request, \
                patch.object(self.module.urllib.request, 'build_opener') as opener:
            opener.return_value.open.return_value = io.BytesIO(b'{"models":[{"name":"bge-m3:567m","digest":"fixed-model"}]}')
            result = self.module.qdrant_environment(complete_state())
            self.assertEqual(request.call_args_list[2].args[2]['offset'], 'b')
            return result

    def test_point_vectors_and_payload_are_frozen_not_only_count(self):
        baseline = self.environment()
        for field in ('vector', 'payload'):
            points = copy.deepcopy(self.points)
            points[0][field] = {'changed':True}
            self.assertNotEqual(baseline['points_sha256'], self.environment(points=points)['points_sha256'])
        self.assertIn('not a byte-identical', baseline['scope'])

    def test_incomplete_count_or_unready_collection_rejects(self):
        for key, value in [('points_count',3), ('status','yellow'), ('optimizer_status',{'error':'failure'})]:
            with self.subTest(key=key):
                info = {**self.info, key:value}
                with self.assertRaises(ValueError):
                    self.environment(info=info)

    def test_request_sends_private_key_and_validated_collection(self):
        with patch.object(self.module, 'RUNTIME', self.runtime), patch.object(self.module.urllib.request, 'build_opener') as opener:
            opener.return_value.open.return_value = io.BytesIO(b'{"status":"ok","result":{}}')
            self.module.qdrant_request(complete_state())
            request = opener.return_value.open.call_args.args[0]
            self.assertEqual(request.get_header('Api-key'), 'synthetic-only')
            self.assertEqual(request.full_url, 'http://127.0.0.1:16333/collections/mw_' + 'a'*64)
        with patch.object(Path, 'read_text') as read:
            with self.assertRaises(ValueError):
                self.module.qdrant_request({'profile_id':'../bad'})
            read.assert_not_called()

if __name__ == "__main__":
    unittest.main()
