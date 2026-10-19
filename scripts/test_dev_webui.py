"""Local frontend server tests; stdlib only, no Docker or model calls.

Run: python3 -m unittest scripts.test_dev_webui -v
"""
import argparse
import contextlib
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
from pathlib import Path
import socket
import tempfile
import threading
import unittest
from unittest import mock


SPEC = importlib.util.spec_from_file_location(
    "dev_webui", Path(__file__).with_name("dev-webui.py")
)
DEV = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(DEV)


@contextlib.contextmanager
def serving(server):
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


class Backend(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def do_GET(self):
        content = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        self.server.requests.append((self.command, self.path, dict(self.headers), content))
        if self.path == "/api/events":
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(b"event: run.started\ndata: {}\n\n")
            self.wfile.flush()
            self.server.event_started.set()
            self.server.finish_event.wait(3)
            try:
                self.wfile.write(b"event: run.completed\ndata: {}\n\n")
                self.wfile.flush()
            except OSError:
                pass
            self.close_connection = True
            return
        if self.path == "/api/idle-events":
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            self.wfile.write(b"data: {}\n\n")
            self.wfile.flush()
            self.connection.settimeout(2)
            try:
                if not self.connection.recv(1):
                    self.server.disconnected.set()
            except OSError:
                pass
            self.close_connection = True
            return
        body = b"backend response"
        self.send_response(307 if self.path == "/api/redirect" else 409)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Run-ID", "run-1")
        if self.path == "/api/redirect":
            self.send_header("Location", "http://192.0.2.1/private")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    do_HEAD = do_POST = do_PUT = do_PATCH = do_DELETE = do_GET


class DevWebUITests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.assets = Path(self.temp.name)
        for name, _ in DEV.ASSETS.values():
            (self.assets / name).write_text("initial " + name)
        self.backend = ThreadingHTTPServer(("127.0.0.1", 0), Backend)
        self.backend.daemon_threads = True
        self.backend.requests = []
        self.backend.event_started = threading.Event()
        self.backend.finish_event = threading.Event()
        self.backend.disconnected = threading.Event()
        self.enterContext(serving(self.backend))
        backend_url = f"http://127.0.0.1:{self.backend.server_port}"
        self.dev = self.enterContext(serving(DEV.DevServer(
            ("127.0.0.1", 0), backend_url, assets=self.assets
        )))

    def request(self, method, path, body=None, headers=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.dev.server_port, timeout=2)
        self.addCleanup(connection.close)
        connection.request(method, path, body=body, headers=headers or {})
        response = connection.getresponse()
        return response, response.read()

    def test_assets_are_exact_fresh_and_protected(self):
        for path, (name, _) in DEV.ASSETS.items():
            response, body = self.request("GET", path)
            self.assertEqual(response.status, 200)
            self.assertEqual(body, ("initial " + name).encode())
            self.assertEqual(response.getheader("Content-Security-Policy"), DEV.CSP)
            self.assertEqual(response.getheader("Cache-Control"), "no-store")
            self.assertEqual(response.getheader("X-Frame-Options"), "DENY")
        (self.assets / "app.js").write_text("edited without restart")
        self.assertEqual(self.request("GET", "/app.js?v=2")[1], b"edited without restart")
        response, body = self.request("HEAD", "/app.js")
        self.assertEqual(response.status, 200)
        self.assertEqual(body, b"")
        self.assertEqual(response.getheader("Content-Length"), "22")
        self.assertEqual(self.request("POST", "/app.js")[0].status, 405)

    def test_no_unknown_static_paths_or_other_proxy_targets(self):
        for path in ("/.env", "/index.html", "/missing.js", "/api", "/other",
                     "/../app.js", "/api/../.env", "/api/%2e%2e/.env",
                     "http://192.0.2.1/api/private", "//example.test/api/private"):
            with self.subTest(path=path):
                self.assertEqual(self.request("GET", path)[0].status, 404)
        self.assertEqual(self.backend.requests, [])

    def test_proxy_preserves_method_body_query_bearer_and_status(self):
        body = '{"content":"私有内容"}'.encode()
        path = "/api/v1/messages?query=%E7%9F%A5%E8%AF%86&mode=hybrid"
        response, result = self.request("POST", path, body, {
            "Authorization": "Bearer private-token", "Content-Type": "application/json",
            "Connection": "keep-alive, X-Private-Hop", "X-Private-Hop": "remove",
        })
        self.assertEqual((response.status, result), (409, b"backend response"))
        self.assertEqual(response.getheader("X-Run-ID"), "run-1")
        method, target, headers, received = self.backend.requests[-1]
        self.assertEqual((method, target, received), ("POST", path, body))
        self.assertEqual(headers["Authorization"], "Bearer private-token")
        self.assertEqual(headers["Content-Type"], "application/json")
        self.assertNotIn("X-Private-Hop", headers)
        for path in ("/healthz", "/readyz"):
            self.assertEqual(self.request("GET", path)[0].status, 409)

    def test_redirect_is_returned_without_following(self):
        response, _ = self.request("GET", "/api/redirect")
        self.assertEqual(response.status, 307)
        self.assertEqual(response.getheader("Location"), "http://192.0.2.1/private")
        self.assertEqual(len(self.backend.requests), 1)

    def test_backend_failure_and_ambiguous_body_fail_without_retry(self):
        response, _ = self.request("POST", "/api/private", b"", {"Transfer-Encoding": "chunked"})
        self.assertEqual(response.status, 400)
        self.assertEqual(self.backend.requests, [])
        self.backend.shutdown()
        self.backend.server_close()
        response, body = self.request("GET", "/api/private?query=secret")
        self.assertEqual(response.status, 502)
        self.assertNotIn(b"secret", body)

    def test_sse_first_frame_arrives_before_backend_finishes(self):
        connection = http.client.HTTPConnection("127.0.0.1", self.dev.server_port, timeout=1)
        self.addCleanup(connection.close)
        connection.request("POST", "/api/events", b"{}", {"Accept": "text/event-stream"})
        response = connection.getresponse()
        try:
            self.assertEqual(response.getheader("Content-Type"), "text/event-stream")
            self.assertEqual(response.fp.readline(), b"event: run.started\n")
            self.assertTrue(self.backend.event_started.is_set())
            self.assertFalse(self.backend.finish_event.is_set())
        finally:
            self.backend.finish_event.set()
        self.assertIn(b"event: run.completed", response.read())

    def test_sse_disconnect_closes_idle_upstream(self):
        connection = socket.create_connection(("127.0.0.1", self.dev.server_port), timeout=2)
        self.addCleanup(connection.close)
        connection.sendall(b"GET /api/idle-events HTTP/1.1\r\nHost: localhost\r\n\r\n")
        received = b""
        while b"data: {}\n\n" not in received:
            received += connection.recv(4096)
        connection.close()
        self.assertTrue(self.backend.disconnected.wait(1), "upstream kept idle SSE alive")

    def test_browser_auth_origin_is_verified_before_upstream_translation(self):
        origin = f"http://127.0.0.1:{self.dev.server_port}"
        path = "/api/v1/auth/browser/refresh"
        headers = {"Content-Type": "application/json", "Origin": origin,
                   "X-Magworkspace-Browser": "1", "Sec-Fetch-Site": "same-origin"}
        response, _ = self.request("POST", path, b"{}", headers)
        self.assertEqual(response.status, 409)
        forwarded = self.backend.requests[-1][2]
        self.assertEqual(forwarded["Origin"], f"http://127.0.0.1:{self.backend.server_port}")
        self.assertEqual(forwarded["X-Magworkspace-Browser"], "1")
        self.backend.requests.clear()
        for changed in ({"Origin": "http://evil.example"}, {"Origin": "null"},
                        {"X-Magworkspace-Browser": ""}, {"Sec-Fetch-Site": "same-site"},
                        {"Host": "evil.example"}):
            with self.subTest(changed=changed):
                response, _ = self.request("POST", path, b"{}", headers | changed)
                self.assertEqual(response.status, 403)
        self.assertEqual(self.backend.requests, [])

    def test_browser_auth_default_port_origin_matches_automatic_host(self):
        backend_port = self.backend.server_port
        original_connection = http.client.HTTPConnection

        class RedirectDefaultPort(original_connection):
            def connect(connection):
                if connection.port == 80:
                    # Keep http.client's real default-port Host generation,
                    # while routing its socket to the temporary test backend.
                    connection.sock = socket.create_connection(("127.0.0.1", backend_port), timeout=2)
                else:
                    super().connect()

        self.dev.backend = ("127.0.0.1", 80)
        headers = {"Content-Type": "application/json",
                   "Origin": f"http://127.0.0.1:{self.dev.server_port}",
                   "X-Magworkspace-Browser": "1"}
        with mock.patch.object(DEV.http.client, "HTTPConnection", RedirectDefaultPort):
            response, _ = self.request("POST", "/api/v1/auth/browser/refresh", b"{}", headers)
        self.assertEqual(response.status, 409)
        forwarded = self.backend.requests[-1][2]
        self.assertEqual(forwarded["Origin"], "http://" + forwarded["Host"])
        self.assertEqual(forwarded["Origin"], "http://127.0.0.1")

    def test_backend_requires_numeric_loopback_http_origin(self):
        for value in ("https://127.0.0.1", "http://example.test", "http://192.0.2.1",
                      "http://localhost:8080", "http://user:pass@127.0.0.1", "http://127.0.0.1/api",
                      "http://127.0.0.1?token=secret", "http://127.0.0.1#fragment",
                      "http://127.0.0.1:0", "http://127.0.0.1:70000"):
            with self.subTest(value=value):
                with self.assertRaises(argparse.ArgumentTypeError):
                    DEV.backend_address(value)
        self.assertEqual(DEV.backend_address("http://127.0.0.1:8080"), ("127.0.0.1", 8080))
        self.assertEqual(DEV.backend_address("http://[::1]:8080/"), ("::1", 8080))


if __name__ == "__main__":
    unittest.main()
