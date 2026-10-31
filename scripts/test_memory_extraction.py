"""Real FastAPI entry + child Harness + controlled HTTP provider; no paid calls."""
from __future__ import annotations

import asyncio
from copy import deepcopy
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import sys
import threading
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bridges/magent"))
import service
import httpx
import memory_extraction

KEY = "memory-extraction-test-service-key-0123456789"
HEADERS = {"Authorization": "Bearer " + KEY, "X-Magent-Protocol": "1", "Content-Type": "application/json"}
QUERY = "我喜欢中文回答。"
ITEM = {"kind": "preference", "content": "喜欢中文回答", "topic": "language", "importance": 3, "evidence": "我喜欢中文回答"}


class ExtractionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.requests = []
        self.result = {"items": [deepcopy(ITEM)]}
        self.finish = "stop"
        self.tool_calls = None
        self.block = False
        self.release = threading.Event()
        owner = self

        class Provider(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_POST(self):
                owner.requests.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
                if owner.block:
                    owner.release.wait(10)
                message = {"role": "assistant", "content": json.dumps(owner.result, ensure_ascii=False)}
                if owner.tool_calls is not None:
                    message["tool_calls"] = owner.tool_calls
                payload = json.dumps({"choices": [{"message": message, "finish_reason": owner.finish}],
                                      "usage": {"prompt_tokens": 20, "completion_tokens": 20}}).encode()
                try:
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                except (BrokenPipeError, ConnectionResetError):
                    pass

        self.provider = ThreadingHTTPServer(("127.0.0.1", 0), Provider)
        self.thread = threading.Thread(target=self.provider.serve_forever, daemon=True)
        self.thread.start()
        harness = ROOT.parent / "Magent Harness/src"
        self.env = patch.dict(os.environ, {
            "MAGENT_SERVICE_API_KEY": KEY, "MAGENT_MODE": "live", "MAGENT_CALLBACK_ORIGIN": "http://backend:8081",
            "MAGENT_SERVICE_MAX_CONCURRENCY": "1", "MAGENT_BASE_URL": f"http://127.0.0.1:{self.provider.server_port}",
            "MAGENT_API_KEY": "synthetic-provider-key", "MAGENT_MODEL": "test-model", "PYTHONPATH": str(harness),
        }, clear=True)
        self.env.start()
        self.app = service.create_app()
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url="http://test")
        self.processes = []
        real_spawn = asyncio.create_subprocess_exec

        async def capture(*args, **kwargs):
            proc = await real_spawn(*args, **kwargs)
            self.processes.append(proc)
            return proc

        self.spawn = patch.object(service.asyncio, "create_subprocess_exec", capture)
        self.spawn.start()

    async def asyncTearDown(self):
        self.release.set()
        await self.client.aclose()
        self.spawn.stop()
        self.env.stop()
        await asyncio.to_thread(self.provider.shutdown)
        self.provider.server_close()
        self.thread.join()
        self.assertFalse(self.app.state.extractions)
        self.assertFalse(self.app.state.responses)
        self.assertFalse(self.app.state.active)
        for proc in self.processes:
            self.assertIsNotNone(proc.returncode)
            with self.assertRaises(ProcessLookupError):
                os.kill(proc.pid, 0)

    async def post(self, **kwargs):
        return await self.client.post("/v1/memories/extract", headers=kwargs.pop("headers", HEADERS),
                                      json=kwargs.pop("json", {"query": QUERY}), **kwargs)

    async def wait_requests(self, count):
        async with asyncio.timeout(5):
            while len(self.requests) < count:
                await asyncio.sleep(.01)

    async def test_real_provider_and_private_contract(self):
        response = await self.post()
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json(), self.result)
        self.assertEqual(response.headers["X-Magent-Protocol"], "1")
        self.assertEqual(len(self.requests), 1)
        request = self.requests[0]
        self.assertEqual([m["role"] for m in request["messages"]], ["system", "user"])
        self.assertEqual(request["messages"][1]["content"], QUERY)
        self.assertIn("untrusted source data", request["messages"][0]["content"])
        self.assertFalse(request.get("tools"))
        self.assertEqual(request["max_tokens"], 2048)
        self.assertNotIn("synthetic-provider-key", response.text)

    async def test_auth_bounds_and_strict_request_before_provider(self):
        for headers, expected in [({}, 401), (dict(HEADERS, Authorization="Bearer wrong"), 401),
                                  (dict(HEADERS, **{"X-Magent-Protocol": "2"}), 400),
                                  (dict(HEADERS, **{"X-Magent-Timeout-Ms": "0"}), 400),
                                  (dict(HEADERS, Accept="text/event-stream"), 406),
                                  (dict(HEADERS, **{"Content-Type": "text/plain"}), 415),
                                  (list(HEADERS.items()) + [("Authorization", "Bearer " + KEY)], 400)]:
            self.assertEqual((await self.post(headers=headers)).status_code, expected)
        for data in ({"query": "x", "history": []}, {"query": "x", "user_id": "other"},
                     {"query": ""}, {"query": "文" * 5334}, {"query": None}, []):
            self.assertEqual((await self.post(json=data)).status_code, 400)
        response = await self.client.post("/v1/memories/extract", headers=HEADERS, content=b'{"query":"x","query":"y"}')
        self.assertEqual(response.status_code, 400)
        response = await self.client.post("/v1/memories/extract", headers=HEADERS, content=b"x" * (memory_extraction.MAX_REQUEST_BYTES + 1))
        self.assertEqual(response.status_code, 413)
        self.assertFalse(self.processes)
        self.assertFalse(self.requests)

    async def test_invalid_model_results_fail_closed(self):
        invalid = [dict(ITEM, kind="secret"), dict(ITEM, evidence="imaginary unsupported fact"),
                   dict(ITEM, evidence=""), dict(ITEM, importance=True), dict(ITEM, content="文" * 501),
                   dict(ITEM, topic="文" * 101), dict(ITEM, user_id="other"), dict(ITEM, content="\ud800")]
        missing = deepcopy(ITEM)
        del missing["topic"]
        invalid.append(missing)
        for item in invalid:
            # A surrogate is serialized escaped by the local provider fixture.
            if item.get("content") == "\ud800":
                continue
            self.result = {"items": [item]}
            response = await self.post()
            self.assertEqual(response.status_code, 502)
            self.assertEqual(response.json(), {"code": "agent_failed"})
        for result in ({"items": None}, {"items": [ITEM] * 6}, {"items": [], "private": "synthetic-provider-key"}):
            self.result = result
            self.assertEqual((await self.post()).status_code, 502)
        self.result = {"items": []}
        self.finish = "length"
        self.assertEqual((await self.post()).status_code, 502)
        self.finish = "tool_calls"
        self.tool_calls = [{"id": "call-1", "type": "function", "function": {"name": "save_memory", "arguments": "{}"}}]
        self.assertEqual((await self.post()).status_code, 502)

    async def test_background_lane_does_not_consume_interactive_capacity(self):
        self.block = True
        extraction = asyncio.create_task(self.post())
        await self.wait_requests(1)
        self.assertEqual((await self.post()).status_code, 429)
        run_data = {"run_id": "run", "session_id": "session", "workspace_id": "workspace", "user_id": "user",
                    "role": "owner", "query": "hello", "history": []}
        interactive = asyncio.create_task(self.client.post("/v1/runs", headers=HEADERS, json=run_data))
        try:
            await self.wait_requests(2)
            self.assertEqual(len(self.processes), 2)
            self.assertEqual(len(self.app.state.active), 1)
            self.assertEqual(len(self.app.state.extractions), 1)
        finally:
            for task in (extraction, interactive):
                task.cancel()
            await asyncio.gather(extraction, interactive, return_exceptions=True)
        self.assertTrue(all(proc.returncode == -9 for proc in self.processes))

    async def test_timeout_and_cancellation_reap_child(self):
        self.block = True
        response = await self.post(headers=dict(HEADERS, **{"X-Magent-Timeout-Ms": "300"}))
        self.assertEqual(response.status_code, 502)
        self.assertEqual(response.json(), {"code": "agent_failed"})
        self.assertEqual(self.processes[0].returncode, -9)
        task = asyncio.create_task(self.post())
        await self.wait_requests(2)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertFalse(self.app.state.extractions)
        self.assertEqual(self.processes[1].returncode, -9)

    async def test_demo_is_deterministic_without_model(self):
        with patch.dict(os.environ, {"MAGENT_MODE": "demo"}):
            app = service.create_app()
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post("/v1/memories/extract", headers=HEADERS, json={"query": QUERY})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"items": []})
        self.assertFalse(self.requests)


class SchemaTests(unittest.TestCase):
    def test_surrogate_and_duplicate_fields_are_rejected(self):
        with self.assertRaises((ValueError, UnicodeError)):
            memory_extraction.validate_result({"items": [dict(ITEM, content="\ud800")]}, QUERY)
        with self.assertRaises(ValueError):
            memory_extraction.parse_request(b'{"query":"x","query":"y"}')


if __name__ == "__main__":
    unittest.main()
