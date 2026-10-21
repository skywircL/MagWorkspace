"""Regression checks for orchestration resumes; no Docker or live API calls."""
import copy
import argparse
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import runpy
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


def load_stack():
    spec = importlib.util.spec_from_file_location("evaluation_stack_test", SCRIPT.with_name("evaluation-stack.py"))
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
        for required in (".dockerignore", ".env.example", "Dockerfile", "compose.yaml", "scripts/run-scifact.py",
                         "scripts/evaluation-stack.py", "scripts/download-scifact.py",
                         "scripts/retrieval_eval/runner.py", "internal/indexing/service.go",
                         "docker/reranker/entrypoint.sh"):
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


class RerankStackOrchestrationTests(unittest.TestCase):
    def setUp(self):
        self.stack = load_stack()
        self.directory = tempfile.TemporaryDirectory(prefix="mw-rerank-orchestration-")
        self.addCleanup(self.directory.cleanup)
        self.runtime = Path(self.directory.name)
        self.addCleanup(patch.stopall)
        patch.object(self.stack, "RUNTIME", self.runtime).start()
        self.execute = patch.object(self.stack.subprocess, "run", side_effect=self.fake_execute).start()
        response = io.BytesIO(b"")
        response.status = 200
        patch("urllib.request.urlopen", return_value=response).start()
        self.output = io.StringIO()
        patch("sys.stdout", self.output).start()

    def fake_execute(self, arguments, **kwargs):
        if "port" in arguments:
            return subprocess.CompletedProcess(arguments, 0, "127.0.0.1:18080\n", "")
        return subprocess.CompletedProcess(arguments, 0, "", "")

    def invoke(self, *arguments):
        with patch.object(sys, "argv", ["evaluation-stack.py", *arguments]):
            self.stack.main()

    def commands(self):
        return [item.args[0] for item in self.execute.call_args_list]

    def test_new_baseline_is_explicitly_disabled_despite_parent_environment(self):
        with patch.dict(os.environ, {"RERANK_ENABLED": "true", "RERANK_API_KEY": "caller-secret"}):
            self.invoke("up")
        settings = json.loads((self.runtime / "settings.json").read_text())
        self.assertFalse(settings["rerank"])
        self.assertEqual(settings["chunking_strategy"], "parent_child_v1")
        override = (self.runtime / "override.yaml").read_text()
        self.assertIn("RERANK_ENABLED: 'false'", override)
        self.assertIn("CHUNKING_STRATEGY: 'parent_child_v1'", override)
        self.assertFalse(any("up" in command and command[-1] == "reranker" for command in self.commands()))
        self.assertNotIn("RERANK_API_KEY", settings["rerank_configuration"])
        private = dict(line.split("=", 1) for line in (self.runtime / "stack.env").read_text().splitlines())
        self.assertGreaterEqual(len(private["RERANK_API_KEY"]), 32)
        self.assertNotEqual(private["RERANK_API_KEY"], "caller-secret")
        self.assertEqual((self.runtime / "stack.env").stat().st_mode & 0o777, 0o600)
        self.assertNotIn(private["RERANK_API_KEY"], self.output.getvalue())

    def test_enabled_model_is_healthy_before_backend_and_cache_is_external(self):
        self.invoke("up", "--rerank", "--model-ready-timeout", "42")
        commands = self.commands()
        model = next(i for i, command in enumerate(commands) if "up" in command and command[-1] == "reranker")
        backend = next(i for i, command in enumerate(commands) if "up" in command and command[-1] == "backend")
        self.assertLess(model, backend)
        self.assertEqual(commands[model][-7:], ["up", "-d", "--build", "--wait", "--wait-timeout", "42", "reranker"])
        self.assertIn(["docker", "volume", "create", "magworkspace_reranker-models"], commands)
        override = (self.runtime / "override.yaml").read_text()
        self.assertIn("start_period: 42s", override)
        self.assertIn("reranker-models:\n    external: true", override)
        self.assertIn("RERANK_ENABLED: 'true'", override)
        self.assertIn("reranker:8091", json.loads((self.runtime / "endpoints.json").read_text()))
        self.assertTrue(json.loads((self.runtime / "settings.json").read_text())["rerank"])
        self.assertIn("Waiting up to 42s", self.output.getvalue())

    def test_model_readiness_failure_never_starts_backend(self):
        def fail_model(arguments, **kwargs):
            if "up" in arguments and arguments[-1] == "reranker":
                raise subprocess.CalledProcessError(1, arguments)
            return self.fake_execute(arguments, **kwargs)
        self.execute.side_effect = fail_model
        with self.assertRaises(subprocess.CalledProcessError):
            self.invoke("up", "--rerank")
        self.assertFalse(any("up" in command and command[-1] == "backend" for command in self.commands()))
        self.assertTrue((self.runtime / "stack.env").exists())
        self.assertFalse(any("down" in command for command in self.commands()))

    def test_obsolete_or_unknown_chunking_state_cannot_be_reused(self):
        state = self.runtime / "scifact-state.json"
        frozen = json.dumps(complete_state())
        state.write_text(frozen)
        for arguments in [("up",), ("up", "--chunking-strategy", "parent_child_v1"),
                          ("up", "--chunking-strategy", "legacy_v1")]:
            with patch("sys.stderr", io.StringIO()), self.assertRaises(SystemExit):
                self.invoke(*arguments)
            self.execute.assert_not_called()
            self.assertEqual(state.read_text(), frozen)
        (self.runtime / "settings.json").write_text(json.dumps({"observability": False,
            "execution": "qdrant_native", "chunking_strategy": "legacy_v1"}))
        with patch("sys.stderr", io.StringIO()), self.assertRaises(SystemExit):
            self.invoke("up", "--chunking-strategy", "parent_child_v1")
        self.execute.assert_not_called()
        self.assertEqual(state.read_text(), frozen)


    def test_explicit_disable_stops_previous_model_without_removing_cache(self):
        self.invoke("up", "--infra-only", "--rerank")
        self.execute.reset_mock()
        self.invoke("up", "--infra-only", "--no-rerank")
        self.assertTrue(any(command[-2:] == ["stop", "reranker"] for command in self.commands()))
        self.assertFalse(any("rm" in command or "down" in command for command in self.commands()))
        self.assertFalse(json.loads((self.runtime / "settings.json").read_text())["rerank"])

    def test_invalid_ready_timeout_rejected_before_mutation(self):
        with patch("sys.stderr", io.StringIO()), self.assertRaises(SystemExit):
            self.invoke("up", "--rerank", "--model-ready-timeout", "0")
        self.execute.assert_not_called()
        self.assertEqual(list(self.runtime.iterdir()), [])


class RerankerEnvironmentTests(unittest.TestCase):
    def setUp(self):
        self.module, self.stack = load_orchestrator(), load_stack()
        self.directory = tempfile.TemporaryDirectory(prefix="mw-rerank-freeze-")
        self.addCleanup(self.directory.cleanup)
        self.runtime = Path(self.directory.name)
        self.config = dict(self.stack.RERANK_DEFAULTS)
        self.key = "private-synthetic-key-" + "a" * 32
        (self.runtime / "settings.json").write_text(json.dumps({
            "rerank": True, "chunking_strategy": "parent_child_v1", "rerank_configuration": self.config}))
        (self.runtime / "stack.env").write_text("RERANK_API_KEY=" + self.key + "\n" +
                                               "".join(key + "=" + value + "\n" for key, value in self.config.items()))
        (self.runtime / "endpoints.json").write_text(json.dumps({"reranker:8091": "http://127.0.0.1:18091"}))
        self.info = {"model_id": self.config["RERANK_MODEL"], "model_sha": self.config["RERANK_MODEL_REVISION"],
                     "model_type": {"reranker": {}}, "auto_truncate": True,
                     "max_input_length": 512, "max_batch_tokens": 1024, "max_client_batch_size": 200,
                     "unexpected_private_debug": self.key}

    def read(self, info=None):
        with patch.object(self.module, "RUNTIME", self.runtime), \
                patch.object(self.module.urllib.request, "build_opener") as opener:
            opener.return_value.open.return_value = io.BytesIO(json.dumps(info or self.info).encode())
            result = self.module.reranker_environment(True)
            request = opener.return_value.open.call_args.args[0]
            self.assertEqual(request.get_header("Authorization"), "Bearer " + self.key)
            self.assertEqual(request.full_url, "http://127.0.0.1:18091/info")
            return result

    def test_loaded_identity_and_limits_frozen_without_credentials(self):
        result = self.read()
        self.assertEqual(result["configuration"], self.config)
        self.assertEqual(result["loaded_model"]["model_sha"], self.info["model_sha"])
        self.assertNotIn(self.key, json.dumps(result))
        self.assertNotIn("unexpected_private_debug", result["loaded_model"])

    def test_wrong_identity_limits_or_settings_reject_before_scoring(self):
        for field, value in (("model_id", "other"), ("model_sha", "other"), ("auto_truncate", False),
                             ("model_type", {}), ("max_input_length", 1025),
                             ("max_batch_tokens", 2048),
                             ("max_client_batch_size", int(self.config["RERANK_MAX_CANDIDATES"]) - 1)):
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.read({**self.info, field: value})
        with patch.object(self.module, "RUNTIME", self.runtime), \
                patch.object(self.module.urllib.request, "build_opener") as opener:
            with self.assertRaises(ValueError):
                self.module.reranker_environment(False)
            opener.assert_not_called()

    def test_baseline_does_not_contact_model(self):
        (self.runtime / "settings.json").write_text(json.dumps({"rerank": False, "chunking_strategy": "parent_child_v1"}))
        with patch.object(self.module, "RUNTIME", self.runtime), \
                patch.object(self.module.urllib.request, "build_opener") as opener:
            self.assertEqual(self.module.reranker_environment(False), {"enabled": False, "chunking_strategy": "parent_child_v1"})
            opener.assert_not_called()

    def test_scifact_forwards_explicit_mode_and_optional_chunking(self):
        args = argparse.Namespace(rerank=False, chunking_strategy=None, model_ready_timeout=3600)
        baseline = self.module.stack_arguments(args)
        self.assertIn("--no-rerank", baseline)
        self.assertNotIn("--chunking-strategy", baseline)
        args.rerank, args.chunking_strategy = True, "parent_child_v1"
        enabled = self.module.stack_arguments(args)
        self.assertIn("--rerank", enabled)
        self.assertEqual(enabled[-2:], ["--chunking-strategy", "parent_child_v1"])

    def test_full_orchestrator_forwards_both_rebuilds_without_starting_real_work(self):
        root = self.runtime / "project"
        dataset = root / ".cache/datasets/scifact"
        dataset.mkdir(parents=True)
        state = complete_state()
        (dataset / "corpus.jsonl").write_text("".join(json.dumps({"_id": key}) + "\n" for key in state["documents"]))
        (self.runtime / "scifact-state.json").write_text(json.dumps(state))
        for enabled in (False, True):
            with self.subTest(rerank=enabled):
                output = root / ("enabled" if enabled else "baseline")
                argv = ["run-scifact.py", "--output", str(output), "--chunking-strategy", "parent_child_v1"]
                if enabled:
                    argv.append("--rerank")
                with patch.object(self.module, "ROOT", root), patch.object(self.module, "RUNTIME", self.runtime), \
                        patch.object(sys, "argv", argv), patch.object(self.module, "run") as run, \
                        patch.object(self.module, "base_url", return_value="http://127.0.0.1:18080"), \
                        patch.object(self.module, "source_fingerprint", return_value={"source.go": "a" * 64}), \
                        patch.object(self.module, "application_sha256", return_value="b" * 64), \
                        patch.object(self.module, "qdrant_environment", return_value={"engine": "qdrant_native"}), \
                        patch.object(self.module, "reranker_environment", return_value={"enabled": enabled}) as model, \
                        patch("sys.stdout", io.StringIO()):
                    self.module.main()
                startups = [item.args for item in run.call_args_list
                            if item.args[:2] == ("scripts/evaluation-stack.py", "up")]
                self.assertEqual(len(startups), 2)
                for startup in startups:
                    self.assertIn("--rerank" if enabled else "--no-rerank", startup)
                    self.assertEqual(startup[-2:], ("--chunking-strategy", "parent_child_v1"))
                evaluation = next(item.args for item in run.call_args_list if item.args[:4] == ("-u", "-m", "scripts.retrieval_eval", "run"))
                self.assertEqual(evaluation[-2:], ("--request-timeout", "125"))
                self.assertEqual(model.call_args_list, [call(enabled), call(enabled)])
                self.assertEqual(json.loads((self.runtime / "scifact-state.json").read_text()), state)


class AcceptanceBaselineCompatibilityTests(unittest.TestCase):
    def test_rebuild_old_private_stack_explicitly_disables_model(self):
        with tempfile.TemporaryDirectory(prefix="mw-acceptance-baseline-") as directory:
            runtime = Path(directory)
            (runtime / "private-state.json").write_text(json.dumps({"project": "mw-old-synthetic"}))
            (runtime / "private.env").write_text("REDIS_PASSWORD=synthetic\nMAGENT_SERVICE_API_KEY=" + "k" * 32 + "\n")
            (runtime / "override.yaml").write_text("volumes:\n  ollama-models:\n    external: true\n")
            response = io.BytesIO(b"")
            response.status = 200
            with patch.dict(os.environ, {"MAGWORKSPACE_ACCEPTANCE_DIR": directory, "RERANK_ENABLED": "true"}), \
                    patch.object(sys, "argv", ["acceptance-stack.py", "rebuild"]), \
                    patch("subprocess.run") as execute, \
                    patch("subprocess.check_output", return_value="127.0.0.1:18080\n"), \
                    patch("urllib.request.build_opener") as opener, patch("sys.stdout", io.StringIO()):
                opener.return_value.open.return_value = response
                runpy.run_path(str(SCRIPT.with_name("acceptance-stack.py")), run_name="acceptance_baseline_test")
            override = (runtime / "baseline-rerank.yaml").resolve()
            self.assertIn("RERANK_ENABLED: 'false'", override.read_text())
            for invocation in execute.call_args_list:
                self.assertIn(str(override), invocation.args[0])
                self.assertNotIn("reranker", invocation.args[0])


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
