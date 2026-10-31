#!/usr/bin/env python3
"""Real HTTP contract and device/batch regressions with an offline fake model."""
from __future__ import annotations

import contextlib
import dataclasses
import http.client
import importlib.util
import io
import json
import math
import pathlib
import sys
import threading
import types
import unittest
from unittest import mock

ROOT = pathlib.Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("cross_encoder_service", ROOT / "docker/reranker/service.py")
SERVICE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = SERVICE
SPEC.loader.exec_module(SERVICE)
KEY = "offline-synthetic-api-key-1234567890"


def torch_devices(cuda=False, mps=False, built=True):
    return types.SimpleNamespace(
        cuda=types.SimpleNamespace(is_available=lambda: cuda),
        backends=types.SimpleNamespace(mps=types.SimpleNamespace(is_available=lambda: mps, is_built=lambda: built)),
        inference_mode=contextlib.nullcontext,
    )


class Tensor:
    def __init__(self, rows):
        self.rows, self.device = rows, None
        self.shape = (len(rows), len(rows[0]))

    def to(self, device):
        self.device = device
        return self

    def float(self):
        return self

    def cpu(self):
        return self

    def reshape(self, *_):
        return self

    def tolist(self):
        return [item for row in self.rows for item in row]


class Tokenizer:
    model_max_length = 512

    def __call__(self, query, text, *, truncation, max_length, **_):
        length = len(query) + len(text) + 4
        ids = [1] * (min(length, max_length) if truncation else length)
        # Preserve a deterministic passage score independently of padding.
        ids[0] = float(text.split(":", 1)[0]) if ":" in text else len(text)
        return {"input_ids": ids, "attention_mask": [1] * len(ids)}

    def pad(self, encoded, **_):
        length = max(len(pair["input_ids"]) for pair in encoded)
        return {name: Tensor([pair[name] + [0] * (length - len(pair[name])) for pair in encoded])
                for name in ("input_ids", "attention_mask")}


class Model:
    config = types.SimpleNamespace(num_labels=1, max_position_embeddings=514,
                                   model_type="xlm-roberta", pad_token_id=1)

    def __init__(self):
        self.batches = []
        self.failure = None
        self.entered = threading.Event()
        self.release = None

    def __call__(self, **batch):
        self.batches.append(batch)
        self.entered.set()
        if self.release:
            if not self.release.wait(5):
                raise RuntimeError("test gate timed out")
        if self.failure:
            raise self.failure
        return types.SimpleNamespace(logits=Tensor([[row[0]] for row in batch["input_ids"].rows]))


class DeviceTests(unittest.TestCase):
    def test_automatic_priority_and_forced_cpu(self):
        for cuda, mps, expected in ((True, True, "cuda"), (True, False, "cuda"),
                                    (False, True, "mps"), (False, False, "cpu")):
            with self.subTest(cuda=cuda, mps=mps):
                torch = torch_devices(cuda, mps)
                self.assertEqual(SERVICE.select_device(torch, ""), expected)
                self.assertEqual(SERVICE.select_device(torch, "cpu"), "cpu")
        self.assertEqual(SERVICE.select_device(torch_devices(False, True, False), ""), "cpu")

    def test_explicit_device_never_falls_back(self):
        self.assertEqual(SERVICE.select_device(torch_devices(True, True), "mps"), "mps")
        self.assertEqual(SERVICE.select_device(torch_devices(True), "cuda"), "cuda")
        for target, torch in (("cuda", torch_devices(False, True)),
                              ("mps", torch_devices(True, False)),
                              ("mps", torch_devices(False, True, False))):
            with self.subTest(target=target), self.assertRaisesRegex(SERVICE.ConfigurationError, "unavailable"):
                SERVICE.select_device(torch, target)
        with self.assertRaises(SERVICE.ConfigurationError):
            SERVICE.select_device(torch_devices(), "cuda:0")

    def test_config_limits_are_fail_closed_without_echoing_values(self):
        for variable, value in (("CROSS_ENCODER_DEVICE", "secret-invalid"), ("REVISION", "main"),
                                ("MAX_CLIENT_BATCH_SIZE", "201"), ("MAX_BATCH_TOKENS", "0"),
                                ("MAX_CONCURRENT_REQUESTS", "0"), ("PAYLOAD_LIMIT", "2000001"),
                                ("AUTO_TRUNCATE", "maybe"), ("DTYPE", "float16")):
            with self.subTest(variable=variable), self.assertRaises(SERVICE.ConfigurationError) as error:
                SERVICE.Settings.from_env({"API_KEY": KEY, variable: value})
            self.assertNotIn(KEY, str(error.exception))
            self.assertNotIn("secret-invalid", str(error.exception))
        settings = SERVICE.Settings.from_env({"API_KEY": KEY})
        self.assertEqual(settings.requested_device, "")
        self.assertEqual(settings.model_id, "BAAI/bge-reranker-base")
        self.assertEqual(len(settings.revision), 40)

    def test_host_defaults_to_loopback_and_rejects_unsupported_addresses(self):
        self.assertEqual(SERVICE.Settings.from_env({"API_KEY": KEY}).host, "127.0.0.1")
        for host in ("127.0.0.1", "0.0.0.0"):
            self.assertEqual(SERVICE.Settings.from_env({"API_KEY": KEY, "HOST": host}).host, host)
        for host in ("", "localhost", "::1", "192.168.1.10"):
            with self.subTest(host=host), self.assertRaises(SERVICE.ConfigurationError):
                SERVICE.Settings.from_env({"API_KEY": KEY, "HOST": host})

    def test_main_binds_the_configured_host(self):
        for host in ("127.0.0.1", "0.0.0.0"):
            with self.subTest(host=host), \
                    mock.patch.dict(SERVICE.os.environ, {"API_KEY": KEY, "HOST": host, "PORT": "19091"}, clear=True), \
                    mock.patch.object(SERVICE.CrossEncoder, "load", return_value=types.SimpleNamespace(device="cpu")), \
                    mock.patch.object(SERVICE, "Server") as server, \
                    contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(SERVICE.main(), 0)
                self.assertEqual(server.call_args.args[0], (host, 19091))
                server.return_value.serve_forever.assert_called_once_with()
                server.return_value.server_close.assert_called_once_with()

    def test_startup_error_does_not_expose_model_error_or_key(self):
        stderr = io.StringIO()
        with mock.patch.dict(SERVICE.os.environ, {"API_KEY": KEY}, clear=True), \
                mock.patch.object(SERVICE.CrossEncoder, "load", side_effect=RuntimeError(KEY + " document")), \
                contextlib.redirect_stderr(stderr):
            self.assertEqual(SERVICE.main(), 1)
        self.assertIn("startup failed (RuntimeError)", stderr.getvalue())
        self.assertNotIn(KEY, stderr.getvalue())
        self.assertNotIn("document", stderr.getvalue())

    def test_loading_binds_pinned_model_to_selected_device_and_never_falls_back(self):
        torch = torch_devices(True, True)
        torch.float32 = "float32"
        model = Model()
        model.to, model.eval = mock.Mock(), mock.Mock()
        model_loader, tokenizer_loader = mock.Mock(return_value=model), mock.Mock(return_value=Tokenizer())
        modules = {
            "torch": torch,
            "transformers": types.SimpleNamespace(
                AutoModelForSequenceClassification=types.SimpleNamespace(from_pretrained=model_loader),
                AutoTokenizer=types.SimpleNamespace(from_pretrained=tokenizer_loader)),
            "transformers.utils": types.SimpleNamespace(logging=types.SimpleNamespace(set_verbosity_error=lambda: None)),
            "huggingface_hub.utils": types.SimpleNamespace(disable_progress_bars=lambda: None),
        }
        settings = SERVICE.Settings(api_key=KEY, requested_device="mps")
        with mock.patch.dict(sys.modules, modules), mock.patch.dict(SERVICE.os.environ, {}, clear=True):
            encoder = SERVICE.CrossEncoder.load(settings)
            self.assertEqual(encoder.device, "mps")
            model.to.assert_called_once_with("mps")
            model.eval.assert_called_once_with()
            self.assertTrue(model.batches, "startup must run inference before becoming healthy")
            self.assertEqual(model_loader.call_args.kwargs["revision"], SERVICE.REVISION)
            self.assertTrue(model_loader.call_args.kwargs["use_safetensors"])
            self.assertFalse(model_loader.call_args.kwargs["trust_remote_code"])
            self.assertEqual(SERVICE.os.environ["PYTORCH_ENABLE_MPS_FALLBACK"], "0")
            model.to.reset_mock()
            model.to.side_effect = RuntimeError("device allocation failed")
            with self.assertRaises(RuntimeError):
                SERVICE.CrossEncoder.load(settings)
            model.to.assert_called_once_with("mps")


class HTTPTests(unittest.TestCase):
    def setUp(self):
        self.settings = SERVICE.Settings(api_key=KEY)
        self.model = Model()
        self.encoder = SERVICE.CrossEncoder(self.settings, torch_devices(), Tokenizer(), self.model, "cpu")
        self.application = SERVICE.Application(self.settings, self.encoder)
        self.server = SERVICE.Server(("127.0.0.1", 0), self.application)
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": 0.01})
        self.thread.start()
        self.addCleanup(self.close_server)

    def close_server(self):
        if self.model.release:
            self.model.release.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(5)

    def request(self, payload=None, *, path="/rerank", key=KEY, headers=None, body=None):
        connection = http.client.HTTPConnection(*self.server.server_address, timeout=5)
        request_headers = {"Content-Type": "application/json"}
        if key is not None:
            request_headers["Authorization"] = "Bearer " + key
        request_headers.update(headers or {})
        if body is None and payload is not None:
            body = json.dumps(payload).encode()
        connection.request("GET" if payload is None and body is None else "POST", path, body=body, headers=request_headers)
        response = connection.getresponse()
        raw = response.read()
        status = response.status
        connection.close()
        return status, json.loads(raw) if raw else None

    def payload(self, **updates):
        return {"query": "q", "texts": ["-2:first", "2:second", "0:third"], **updates}

    def test_health_info_and_authentication_use_real_http(self):
        self.assertEqual(self.request(path="/health", key=None)[0], 200)
        for key in (None, "wrong-secret"):
            self.assertEqual(self.request(path="/info", key=key)[0], 401)
            self.assertEqual(self.request(self.payload(), key=key)[0], 401)
        self.assertEqual(self.model.batches, [])
        status, info = self.request(path="/info")
        self.assertEqual(status, 200)
        self.assertEqual(info["model_id"], SERVICE.MODEL_ID)
        self.assertEqual(info["model_sha"], SERVICE.REVISION)
        self.assertEqual(info["model_type"], {"reranker": {}})
        self.assertEqual(info["device"], "cpu")
        self.assertEqual(info["max_input_length"], 512)
        self.assertNotIn(KEY, json.dumps(info))

    def test_normalized_raw_and_return_text_contract(self):
        status, rows = self.request(self.payload())
        self.assertEqual(status, 200)
        self.assertEqual([row["index"] for row in rows], [1, 2, 0])
        self.assertTrue(all(set(row) == {"index", "score"} for row in rows))
        normalized = {row["index"]: row["score"] for row in rows}
        status, raw = self.request(self.payload(raw_scores=True, return_text=True))
        self.assertEqual(status, 200)
        for row in raw:
            self.assertAlmostEqual(normalized[row["index"]], SERVICE.sigmoid(row["score"]))
            self.assertEqual(row["text"], self.payload()["texts"][row["index"]])
        status, extreme = self.request(self.payload(texts=["-10000:a", "10000:b"]))
        self.assertEqual(status, 200)
        self.assertEqual([row["score"] for row in extreme], [1, 0])

    def test_truncation_and_padded_batch_budget_are_enforced(self):
        payload = self.payload(texts=["x" * 1000] * 5)
        self.assertEqual(self.request(dict(payload, truncate=False))[0], 422)
        self.assertEqual(self.model.batches, [])
        status, rows = self.request(payload)
        self.assertEqual(status, 200)
        self.assertEqual(len(rows), 5)
        self.assertEqual([len(batch["input_ids"].rows) for batch in self.model.batches], [2, 2, 1])
        for batch in self.model.batches:
            count, length = batch["input_ids"].shape
            self.assertLessEqual(count, 4)
            self.assertLessEqual(count * length, 1024)
            self.assertEqual(batch["input_ids"].device, "cpu")

    def test_disabled_auto_truncation_and_lower_token_budget(self):
        settings = dataclasses.replace(self.settings, auto_truncate=False, max_batch_tokens=32)
        self.application.settings = settings
        self.encoder.settings = settings
        self.encoder.max_input_length = 32
        payload = self.payload(texts=["x" * 100])
        self.assertEqual(self.request(payload)[0], 422)
        self.assertEqual(self.request(dict(payload, truncate=True))[0], 200)

    def test_bad_inputs_are_refused_before_model_execution(self):
        cases = [(self.payload(texts=[]), 400), (self.payload(texts=["x"] * 201), 422),
                 (self.payload(query=""), 422), (self.payload(texts=[None]), 422),
                 (self.payload(truncate="true"), 422), (self.payload(raw_scores=1), 422),
                 (self.payload(return_text=None), 422), (self.payload(model="other"), 422),
                 (self.payload(query="\ud800"), 422)]
        for payload, expected in cases:
            with self.subTest(payload=payload):
                self.assertEqual(self.request(payload)[0], expected)
        self.assertEqual(self.request(body=b"{")[0], 400)
        self.assertEqual(self.request(self.payload(), headers={"Content-Type": "text/plain"})[0], 415)
        self.assertEqual(self.model.batches, [])

    def test_payload_limit_is_checked_before_reading_or_tokenizing(self):
        # Declare a large body, but send none: rejection must not wait for it.
        status, body = self.request(body=b"", headers={"Content-Length": "2000001"})
        self.assertEqual(status, 413)
        self.assertNotIn(KEY, json.dumps(body))
        self.assertEqual(self.model.batches, [])

    def test_failed_nonfinite_and_incomplete_results_fail_closed_and_release_capacity(self):
        self.model.failure = RuntimeError(KEY + " private document")
        status, body = self.request(self.payload())
        self.assertEqual(status, 503)
        self.assertEqual(body, {"error": "inference unavailable"})
        self.model.failure = None
        for scores in ([float("nan")] * 3, [float("inf")] * 3, [0], [True] * 3):
            with mock.patch.object(self.encoder, "score", return_value=scores):
                self.assertEqual(self.request(self.payload())[0], 503)
        self.assertEqual(self.request(self.payload())[0], 200)

    def test_inflight_pair_limit_and_health_during_inference(self):
        self.application.settings = dataclasses.replace(self.settings, max_concurrent_requests=3)
        self.model.release = threading.Event()
        results = []
        worker = threading.Thread(target=lambda: results.append(self.request(self.payload())))
        worker.start()
        self.assertTrue(self.model.entered.wait(2))
        self.assertEqual(self.request(self.payload(texts=["x"]))[0], 429)
        self.assertEqual(self.request(path="/health", key=None)[0], 200)
        self.model.release.set()
        worker.join(5)
        self.assertFalse(worker.is_alive())
        self.assertEqual(results[0][0], 200)
        self.assertEqual(self.request(self.payload())[0], 200)

    def test_http_worker_limit_refuses_excess_connection_and_recovers(self):
        self.server._http_slots = threading.BoundedSemaphore(2)
        self.model.release = threading.Event()
        entered_second = threading.Event()
        original = self.application.rerank
        counter = 0
        counter_lock = threading.Lock()

        def tracked(payload):
            nonlocal counter
            with counter_lock:
                counter += 1
                if counter == 2:
                    entered_second.set()
            return original(payload)

        results = []
        with mock.patch.object(self.application, "rerank", side_effect=tracked):
            workers = [threading.Thread(target=lambda: results.append(self.request(self.payload()))) for _ in range(2)]
            try:
                workers[0].start()
                self.assertTrue(self.model.entered.wait(2))
                workers[1].start()
                self.assertTrue(entered_second.wait(2))
                self.assertEqual(self.request(path="/health", key=None)[0], 503)
            finally:
                self.model.release.set()
                for worker in workers:
                    if worker.ident is not None:
                        worker.join(5)
        self.assertEqual([result[0] for result in results], [200, 200])
        self.assertEqual(self.request(path="/health", key=None)[0], 200)


if __name__ == "__main__":
    unittest.main()
