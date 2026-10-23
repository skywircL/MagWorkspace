"""Private HTTP entry, resource limits and real child cleanup (no model calls)."""
from __future__ import annotations

import asyncio
import contextlib
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bridges/magent"))
import service
import httpx
from callback_endpoint import validate_callback

KEY = "test-service-capability-" + "a" * 32
HEADERS = {"Authorization": "Bearer " + KEY, "X-Magent-Protocol": "1", "Content-Type": "application/json"}
BODY = {"run_id": "run", "session_id": "session", "workspace_id": "workspace", "user_id": "user",
        "role": "owner", "query": "ok", "history": []}
CHILD = '''
import json, os, sys, time
data=json.load(sys.stdin)
q=data["query"]
stream="--stream" in sys.argv
result={"run_id":data["run_id"],"session_id":data["session_id"],"status":"completed","answer":"ok","messages":[],"mode":"demo"}
def emit(value):
    print(json.dumps(value),flush=True)
if q=="unavailable": sys.exit(78)
if q=="stderr":
    sys.stderr.write("private-provider-token"*2000)
    sys.stderr.flush()
    time.sleep(60)
if q=="oversize":
    sys.stdout.write("x"*(3<<20))
    sys.stdout.flush()
    time.sleep(60)
if q=="many_events":
    for _ in range(10000):
        emit({"event":{"type":"message.delta","run_id":data["run_id"],"session_id":data["session_id"],"round":1,"delta":"x"*1000}})
    time.sleep(60)
if q=="environment":
    assert "MAGENT_SERVICE_API_KEY" not in os.environ
    assert "DATABASE_URL" not in os.environ
    assert "HTTP_PROXY" not in os.environ
    assert os.environ["MAGENT_HTTP_TIMEOUT"]=="120.0"
if stream:
    emit({"event":{"type":"message.start","run_id":data["run_id"],"session_id":data["session_id"],"round":1}})
if q=="hang": time.sleep(60)
emit({"result":result} if stream else result)
if q=="fail_after_result": sys.exit(1)
'''


class ConfigurationTests(unittest.TestCase):
    def test_settings_and_legacy_duration_compatibility(self):
        env = {"MAGENT_SERVICE_API_KEY": KEY, "MAGENT_MODE": "demo", "MAGENT_CALLBACK_ORIGIN": "http://backend:8081", "MAGENT_TIMEOUT": "1m30s",
               "MAGENT_SUMMARY_TIMEOUT": "1500ms", "MAGENT_RESERVE_TOKENS": "3000"}
        with patch.dict(os.environ, env, clear=True):
            value = service.Settings()
            self.assertEqual(value.timeout, 90)
            self.assertEqual(value.worker_env["MAGENT_RESERVE_TOKENS"], "4096")
            self.assertEqual(value.worker_env["MAGENT_SUMMARY_TIMEOUT_S"], "1.5")
        for key, value in [("MAGENT_SERVICE_API_KEY", "short"), ("MAGENT_TIMEOUT", "0s"),
                           ("MAGENT_SUMMARY_TIMEOUT", "0.1ms"), ("MAGENT_SUMMARY_TIMEOUT_S", "nan"),
                           ("MAGENT_MAX_CONTEXT_TOKENS", "2000001"), ("MAGENT_MAX_CONTEXT_TOKENS", "1000"),
                           ("MAGENT_MAX_COMPLETION_TOKENS", "0"), ("MAGENT_MODE", "unknown"), ("MAGENT_CALLBACK_ORIGIN", "")]:
            with self.subTest(key=key, value=value), patch.dict(os.environ, dict(env, **{key: value}), clear=True):
                with self.assertRaises(ValueError):
                    service.Settings()

    def test_live_provider_configuration_matches_current_native_model(self):
        from bridge import ConfigurationError
        harness = Path(os.environ.get("MAGENT_TEST_HARNESS_PATH", ROOT.parent / "Magent Harness"))
        with patch.object(sys, "path", [str(harness / "src"), *sys.path]):
            from magent_harness import DeepSeekChatModel

        env = {"MAGENT_SERVICE_API_KEY": KEY, "MAGENT_MODE": "live",
               "MAGENT_CALLBACK_ORIGIN": "http://backend:8081",
               "MAGENT_BASE_URL": "https://provider.example/v1",
               "MAGENT_API_KEY": "synthetic-provider-key", "MAGENT_MODEL": "test-model"}
        for name, value in [
            ("MAGENT_API_KEY", "synthetic key"), ("MAGENT_API_KEY", "synthetic\tkey"),
            ("MAGENT_API_KEY", "synthetic\x7fkey"), ("MAGENT_API_KEY", "synthetic-密钥"),
            ("MAGENT_MODEL", "test\tmodel"), ("MAGENT_MODEL", "test\x01model"),
            ("MAGENT_BASE_URL", "http://127.0.0.1:99999"),
            ("MAGENT_BASE_URL", "https://provider.example:invalid"),
            ("MAGENT_BASE_URL", "https://provider.example /v1"),
            ("MAGENT_BASE_URL", "https://provider.example/\nv1"),
            ("MAGENT_BASE_URL", "https://"),
        ]:
            values = dict(env, **{name: value})
            with self.subTest(name=name, value=value), patch.dict(os.environ, values, clear=True):
                with self.assertRaises(ConfigurationError) as failure:
                    service.create_app()
                self.assertEqual(str(failure.exception), "invalid live configuration")
                # Instantiate the actual adjacent Harness adapter, not a copied
                # validator. Invalid configurations fail before creating I/O.
                with self.assertRaises(ValueError):
                    DeepSeekChatModel(api_key=values["MAGENT_API_KEY"], model=values["MAGENT_MODEL"],
                                      base_url=values["MAGENT_BASE_URL"])

        async def valid_native(values):
            async with httpx.AsyncClient(trust_env=False) as client:
                async with DeepSeekChatModel(api_key=values["MAGENT_API_KEY"], model=values["MAGENT_MODEL"],
                                            base_url=values["MAGENT_BASE_URL"], client=client) as native:
                    self.assertEqual(native.model, values["MAGENT_MODEL"])

        for url in ("https://provider.example/v1", "http://127.0.0.1:18000", "http://[::1]:18000"):
            values = dict(env, MAGENT_BASE_URL=url)
            with self.subTest(url=url), patch.dict(os.environ, values, clear=True):
                self.assertEqual(service.create_app().state.settings.mode, "live")
                asyncio.run(valid_native(values))

        # The deployment boundary intentionally keeps stricter HTTP origins.
        with patch.dict(os.environ, dict(env, MAGENT_BASE_URL="http://provider.example/v1"), clear=True):
            with self.assertRaises(ConfigurationError) as failure:
                service.create_app()
            self.assertEqual(str(failure.exception), "invalid live configuration")

    def test_callbacks_bind_to_exact_configured_origin(self):
        token = "a" * 64
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(validate_callback({"url": "http://127.0.0.1:1234/tools", "token": token}, "/tools")["token"], token)
            with self.assertRaises(ValueError):
                validate_callback({"url": "http://backend:8081/tools", "token": token}, "/tools")
        with patch.dict(os.environ, {"MAGENT_CALLBACK_ORIGIN": "http://backend:8081"}, clear=True):
            for path in ("/tools", "/journal", "/memory"):
                validate_callback({"url": "http://backend:8081" + path, "token": token}, path)
                for bad in ("http://127.0.0.1:1234" + path, "http://backend:8081" + path + "?x=1",
                            "http://backend:8081" + path + "#", "http://user@backend:8081" + path,
                            "http://other:8081" + path, "http://backend:8081/tools/../" + path):
                    with self.subTest(url=bad), self.assertRaises(ValueError):
                        validate_callback({"url": bad, "token": token}, path)


class HTTPTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="magent-http-test-")
        Path(self.temp.name, "bridge.py").write_text(CHILD)
        self.env = patch.dict(os.environ, {"MAGENT_SERVICE_API_KEY": KEY, "MAGENT_MODE": "demo",
                                         "MAGENT_SERVICE_MAX_CONCURRENCY": "1", "MAGENT_CALLBACK_ORIGIN": "http://backend:8081",
                                         "DATABASE_URL": "private-database", "HTTP_PROXY": "private-proxy"}, clear=True)
        self.env.start()
        self.root = patch.object(service, "ROOT", Path(self.temp.name))
        self.root.start()
        self.app = service.create_app()
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url="http://test")
        self.processes = []
        self.spawn_real = asyncio.create_subprocess_exec

        async def capture(*args, **kwargs):
            proc = await self.spawn_real(*args, **kwargs)
            self.processes.append(proc)
            return proc

        self.spawn = patch.object(service.asyncio, "create_subprocess_exec", capture)
        self.spawn.start()

    async def asyncTearDown(self):
        await self.client.aclose()
        self.spawn.stop()
        self.root.stop()
        self.env.stop()
        self.temp.cleanup()
        self.assertFalse(self.app.state.active)
        self.assertFalse(self.app.state.responses)
        for proc in self.processes:
            self.assertIsNotNone(proc.returncode, "child not reaped")
            with self.assertRaises(ProcessLookupError):
                os.kill(proc.pid, 0)

    async def post(self, query="ok", **kwargs):
        return await self.client.post("/v1/runs", headers=kwargs.pop("headers", HEADERS),
                                      json=dict(BODY, query=query, **kwargs.pop("body", {})), **kwargs)

    async def test_auth_protocol_and_strict_json_before_spawn(self):
        self.assertEqual((await self.client.get("/healthz")).json(), {"status": "ok"})
        for path in ("/docs", "/openapi.json", "/v1/runs/"):
            self.assertEqual((await self.client.get(path)).status_code, 404)
        for headers, status in [({}, 401), (dict(HEADERS, Authorization="Bearer wrong"), 401),
                                (dict(HEADERS, **{"X-Magent-Protocol": "2"}), 400),
                                (dict(HEADERS, **{"X-Magent-Timeout-Ms": "0"}), 400),
                                (dict(HEADERS, **{"Content-Type": "text/plain"}), 415),
                                (list(HEADERS.items()) + [("Authorization", "Bearer " + KEY)], 400)]:
            with self.subTest(status=status):
                self.assertEqual((await self.post(headers=headers)).status_code, status)
        raw = json.dumps(BODY)[:-1] + ',"run_id":"duplicated"}'
        response = await self.client.post("/v1/runs", headers=HEADERS, content=raw)
        self.assertEqual(response.status_code, 400)
        self.assertEqual((await self.client.post("/v1/runs", headers=HEADERS, content=b"x"*(service.MAX_REQUEST+1))).status_code, 413)
        self.assertFalse(self.processes)

    async def test_disabled_mode_and_successful_json_environment(self):
        with patch.dict(os.environ, {"MAGENT_MODE": ""}):
            disabled = service.create_app()
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=disabled), base_url="http://test") as client:
            self.assertEqual((await client.get("/healthz")).status_code, 200)
            self.assertEqual((await client.post("/v1/runs", headers=HEADERS, json=BODY)).status_code, 503)
        response = await self.post("environment")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["X-Magent-Protocol"], "1")
        self.assertEqual(response.json()["answer"], "ok")

    async def test_sse_terminal_requires_successful_exit(self):
        headers = dict(HEADERS, Accept="text/event-stream")
        success = await self.post(headers=headers)
        self.assertIn("event: message.start\n", success.text)
        self.assertIn("event: result\n", success.text)
        failure = await self.post("fail_after_result", headers=headers)
        self.assertNotIn("event: result\n", failure.text)
        self.assertIn('event: error\ndata: {"code":"agent_failed"}', failure.text)

    async def test_timeout_oversize_stderr_and_unavailable_are_safe(self):
        for query in ("hang", "oversize", "stderr", "unavailable"):
            with self.subTest(query=query):
                response = await self.post(query, headers=dict(HEADERS, **{"X-Magent-Timeout-Ms": "150"}))
                expected = "agent_unavailable" if query == "unavailable" else "agent_failed"
                self.assertEqual(response.json(), {"code": expected})
                self.assertNotIn("private", response.text)
                self.assertEqual(response.status_code, 503 if query == "unavailable" else 502)

    async def test_stream_limits_include_wire_encoding_and_reap(self):
        for query in ("oversize", "many_events"):
            with self.subTest(query=query):
                # A short limit checks SSE overhead without generating megabytes.
                with patch.object(service, "MAX_STREAM", 4096):
                    response = await asyncio.wait_for(self.post(query, headers=dict(HEADERS, Accept="text/event-stream")), 3)
                self.assertLessEqual(len(response.content), 4096)
                self.assertIn('event: error\ndata: {"code":"agent_failed"}', response.text)
                self.assertNotIn("event: result", response.text)

    async def test_asgi_cancellation_reaps_before_return(self):
        request = asyncio.create_task(self.post("hang"))
        async with asyncio.timeout(3):
            while not self.processes:
                await asyncio.sleep(.005)
        request.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await request
        self.assertFalse(self.app.state.active)
        self.assertEqual(self.processes[0].returncode, -9)

    async def test_request_body_time_counts_toward_execution_deadline(self):
        observed = []

        async def inspect_execution(response, send):
            observed.append(response.timeout)
            await response.start(send)
            await send({"type": "http.response.body", "body": b"{}", "more_body": False})

        async def slow_body():
            yield b" "
            await asyncio.sleep(.07)
            yield json.dumps(BODY).encode()

        with patch.object(service.RunResponse, "execute", inspect_execution):
            response = await self.client.post("/v1/runs", headers=dict(HEADERS, **{"X-Magent-Timeout-Ms": "200"}), content=slow_body())
        self.assertEqual(response.status_code, 200)
        self.assertGreater(observed[0], 0)
        self.assertLess(observed[0], .15)

    async def test_cancellation_while_spawn_completes_still_reaps(self):
        self.spawn.stop()

        async def delayed_spawn(*args, **kwargs):
            proc = await self.spawn_real(*args, **kwargs)
            self.processes.append(proc)
            await asyncio.sleep(.05)
            return proc

        with patch.object(service.asyncio, "create_subprocess_exec", delayed_spawn):
            request = asyncio.create_task(self.post("hang"))
            async with asyncio.timeout(3):
                while not self.processes:
                    await asyncio.sleep(.005)
            request.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await request
        self.assertEqual(self.processes[0].returncode, -9)

    async def test_busy_and_duplicate_runs_are_not_retried(self):
        first = asyncio.create_task(self.post("hang", headers=dict(HEADERS, **{"X-Magent-Timeout-Ms": "300"})))
        async with asyncio.timeout(2):
            while not self.app.state.active:
                await asyncio.sleep(.005)
        self.assertEqual((await self.post()).status_code, 409)
        self.assertEqual((await self.post(body={"run_id": "other"})).status_code, 429)
        await first
        self.assertEqual(len(self.processes), 1)

    async def test_real_tcp_stream_disconnect_kills_and_reaps_child(self):
        import uvicorn
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        address = "http://127.0.0.1:" + str(sock.getsockname()[1])
        server = uvicorn.Server(uvicorn.Config(self.app, log_config=None, access_log=False, lifespan="on", ws="none", http="h11"))
        task = asyncio.create_task(server.serve(sockets=[sock]))
        try:
            async with asyncio.timeout(3):
                while not server.started:
                    await asyncio.sleep(.005)
            async with httpx.AsyncClient(trust_env=False) as client:
                async with client.stream("POST", address + "/v1/runs", headers=dict(HEADERS, Accept="text/event-stream"), json=dict(BODY, query="hang")) as response:
                    self.assertEqual(response.status_code, 200)
                    async for line in response.aiter_lines():
                        if line.startswith("data:"):
                            break
            async with asyncio.timeout(3):
                while self.app.state.active:
                    await asyncio.sleep(.005)
            self.assertEqual(len(self.processes), 1)
            self.assertEqual(self.processes[0].returncode, -9)
        finally:
            server.should_exit = True
            await task
            sock.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
