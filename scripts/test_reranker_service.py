#!/usr/bin/env python3
"""Offline transfer-integrity regressions; not a model-quality evaluation."""
import hashlib
import importlib.util
import io
import pathlib
import subprocess
import tempfile
import unittest
from unittest import mock

ROOT = pathlib.Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("download_reranker_service", ROOT / "scripts/download-reranker_service.py")
DOWNLOAD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(DOWNLOAD)


class Response(io.BytesIO):
    def __init__(self, content, status=200, headers=None):
        super().__init__(content)
        self.status, self.headers = status, headers or {}


class Opener:
    def __init__(self, response):
        self.response, self.requests = response, []

    def open(self, request, timeout):
        self.requests.append(request)
        return self.response


class DownloadTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.target = pathlib.Path(self.directory.name) / "model.safetensors"
        self.content = b"verified model file"
        self.metadata = {
            "size": len(self.content),
            "lfs": {"sha256": hashlib.sha256(self.content).hexdigest()},
        }

    def test_verified_cache_does_not_download(self):
        self.target.write_bytes(self.content)
        opener = Opener(None)
        DOWNLOAD.download(self.target, self.metadata, opener)
        self.assertEqual(opener.requests, [])

    def test_alternate_model_uses_only_the_selected_pinned_revision(self):
        opener = Opener(Response(self.content))
        model = "cross-encoder/mmarco-mMiniLMv2-L12-H384-v1"
        revision = "1427fd652930e4ba29e8149678df786c240d8825"
        DOWNLOAD.download(self.target, self.metadata, opener, model=model, revision=revision)
        self.assertEqual(opener.requests[0].full_url,
                         f"https://huggingface.co/{model}/resolve/{revision}/model.safetensors")
        self.assertTrue(DOWNLOAD.valid_file(self.target, self.metadata))

    def test_partial_file_resumes_and_publishes_only_verified_bytes(self):
        part = self.target.with_suffix(".safetensors.part")
        part.write_bytes(self.content[:5])
        opener = Opener(Response(self.content[5:], 206, {"Content-Range": f"bytes 5-{len(self.content)-1}/{len(self.content)}"}))
        DOWNLOAD.download(self.target, self.metadata, opener)
        self.assertEqual(opener.requests[0].get_header("Range"), "bytes=5-")
        self.assertEqual(self.target.read_bytes(), self.content)
        self.assertFalse(part.exists())

    def test_range_ignored_replaces_partial_instead_of_appending(self):
        self.target.with_suffix(".safetensors.part").write_bytes(b"wrong")
        DOWNLOAD.download(self.target, self.metadata, Opener(Response(self.content)))
        self.assertEqual(self.target.read_bytes(), self.content)

    def test_clean_eof_before_expected_size_resumes_remaining_bytes(self):
        class InterruptedOpener:
            requests = []
            def open(inner, request, timeout):
                inner.requests.append(request)
                if len(inner.requests) == 1:
                    return Response(self.content[:5])
                return Response(self.content[5:], 206, {"Content-Range": f"bytes 5-{len(self.content)-1}/{len(self.content)}"})
        opener = InterruptedOpener()
        with mock.patch.object(DOWNLOAD.time, "sleep"):
            DOWNLOAD.download(self.target, self.metadata, opener)
        self.assertEqual(opener.requests[1].get_header("Range"), "bytes=5-")
        self.assertEqual(self.target.read_bytes(), self.content)

    def test_wrong_range_does_not_publish_or_overwrite_partial(self):
        part = self.target.with_suffix(".safetensors.part")
        part.write_bytes(self.content[:5])
        with self.assertRaisesRegex(RuntimeError, "partial content range"):
            DOWNLOAD.download(self.target, self.metadata, Opener(Response(self.content[5:], 206, {"Content-Range": "bytes 0-99/100"})))
        self.assertFalse(self.target.exists())
        self.assertEqual(part.read_bytes(), self.content[:5])

    def test_corrupt_bytes_never_become_ready_snapshot(self):
        with self.assertRaisesRegex(RuntimeError, "checksum mismatch"):
            DOWNLOAD.download(self.target, self.metadata, Opener(Response(b"x" * len(self.content))))
        self.assertFalse(self.target.exists())

    def test_git_blob_metadata_uses_git_object_hash(self):
        self.target.write_bytes(self.content)
        metadata = {
            "size": len(self.content),
            "blobId": hashlib.sha1(f"blob {len(self.content)}\0".encode() + self.content).hexdigest(),
        }
        self.assertTrue(DOWNLOAD.valid_file(self.target, metadata))
        self.target.write_bytes(b"x" * len(self.content))
        self.assertFalse(DOWNLOAD.valid_file(self.target, metadata))

    def test_unauthenticated_service_fails_closed(self):
        for environment in ({}, {"API_KEY": "short"}):
            result = subprocess.run(["/bin/sh", str(ROOT / "docker/reranker/entrypoint.sh")],
                                    env=environment, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("at least 32", result.stderr)
            self.assertNotIn("short", result.stderr)

    def test_wrapper_disables_implicit_device_fallback_and_progress(self):
        directory = pathlib.Path(self.directory.name)
        router = directory / "python3"
        router.write_text("#!/bin/sh\nprintf '%s %s %s %s' \"$PYTORCH_ENABLE_MPS_FALLBACK\" \"$HF_HUB_DISABLE_PROGRESS_BARS\" \"$TOKENIZERS_PARALLELISM\" \"$1\"\n")
        router.chmod(0o755)
        result = subprocess.run(
            ["/bin/sh", str(ROOT / "docker/reranker/entrypoint.sh")],
            env={"PATH": str(directory), "API_KEY": "x" * 32, "PYTORCH_ENABLE_MPS_FALLBACK": "1"},
            capture_output=True, text=True, check=True,
        )
        self.assertEqual(result.stdout, "0 1 false /opt/reranker/service.py")


if __name__ == "__main__":
    unittest.main()
