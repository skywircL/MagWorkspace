"""Serve editable frontend assets with a same-origin local API proxy.

Run: python3 scripts/dev-webui.py --backend http://127.0.0.1:8080 --port 5173
Open http://127.0.0.1:5173 and refresh after editing internal/webui/assets.
The backend must already be running. No credentials are stored or logged.
"""
import argparse
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import ipaddress
from pathlib import Path
import select
import socket
import threading
from urllib.parse import unquote, urlsplit


CSP = "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' blob:; base-uri 'none'; frame-ancestors 'none'; form-action 'self'"
ASSETS = {
    "/": ("index.html", "text/html; charset=utf-8"),
    "/app.js": ("app.js", "text/javascript; charset=utf-8"),
    "/core.js": ("core.js", "text/javascript; charset=utf-8"),
    "/style.css": ("style.css", "text/css; charset=utf-8"),
}
HOP_HEADERS = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailer", "transfer-encoding", "upgrade", "proxy-connection",
}


def backend_address(value):
    """Accept only an HTTP origin with a numeric loopback address."""
    try:
        parts = urlsplit(value)
        if (parts.scheme != "http" or parts.username is not None
                or parts.password is not None or parts.path not in ("", "/")
                or parts.query or parts.fragment or not parts.hostname
                or not ipaddress.ip_address(parts.hostname).is_loopback):
            raise ValueError()
        port = 80 if parts.port is None else parts.port
        if not 1 <= port <= 65535:
            raise ValueError()
        return parts.hostname, port
    except ValueError:
        raise argparse.ArgumentTypeError(
            "backend must be an HTTP loopback origin, e.g. http://127.0.0.1:8080"
        ) from None


def hop_headers(headers):
    return HOP_HEADERS | {
        item.strip().lower()
        for value in headers.get_all("Connection", [])
        for item in value.split(",")
    }


class DevServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address, backend, assets=None):
        self.backend = backend_address(backend)
        self.assets = Path(assets) if assets else (
            Path(__file__).resolve().parents[1] / "internal/webui/assets"
        )
        super().__init__(address, DevHandler)


class DevHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        # Request targets and headers may contain private queries or credentials.
        pass

    def do_GET(self):
        try:
            parts = urlsplit(self.path)
        except ValueError:
            self.send_error(404)
            return
        path = parts.path
        decoded = unquote(path)
        if (not self.path.startswith("/") or parts.scheme or parts.netloc
                or "\\" in decoded
                or any(segment in (".", "..") for segment in decoded.split("/"))):
            self.send_error(404)
            return
        if path.startswith("/api/") or path in ("/healthz", "/readyz"):
            self.proxy()
        elif path in ASSETS:
            self.asset(*ASSETS[path])
        else:
            self.send_error(404)

    do_HEAD = do_POST = do_PUT = do_PATCH = do_DELETE = do_OPTIONS = do_GET

    def asset(self, name, content_type):
        if self.command not in ("GET", "HEAD"):
            self.send_error(405)
            return
        try:
            content = (self.server.assets / name).read_bytes()
        except OSError:
            self.send_error(404)
            return
        self.send_response(200)
        for key, value in {
            "Content-Type": content_type,
            "Content-Length": str(len(content)),
            "Content-Security-Policy": CSP,
            "Referrer-Policy": "no-referrer",
            "X-Frame-Options": "DENY",
            "X-Content-Type-Options": "nosniff",
            "Cache-Control": "no-store",
        }.items():
            self.send_header(key, value)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(content)

    def proxy(self):
        browser_auth = urlsplit(self.path).path.startswith("/api/v1/auth/browser/")
        if browser_auth:
            # Authenticate the browser's origin here before translating it for
            # the explicit loopback upstream, whose Host differs from this port.
            try:
                host = self.headers.get("Host", "")
                origin = urlsplit("http://" + host)
                valid_host = (origin.hostname == "localhost" or ipaddress.ip_address(origin.hostname).is_loopback)
                if (not valid_host or origin.port != self.server.server_port
                        or self.headers.get("Origin") != "http://" + host
                        or self.headers.get("X-Magworkspace-Browser") != "1"
                        or self.headers.get("Sec-Fetch-Site", "same-origin") != "same-origin"):
                    raise ValueError()
            except (ValueError, TypeError):
                self.close_connection = True
                self.send_error(403, "Same-origin browser request required")
                return
        # Browser fetch sends a length-delimited body. Reject ambiguous framing.
        lengths = self.headers.get_all("Content-Length", [])
        try:
            if self.headers.get("Transfer-Encoding") or len(lengths) > 1:
                raise ValueError()
            length = int(lengths[0]) if lengths else 0
            if length < 0:
                raise ValueError()
        except ValueError:
            self.close_connection = True
            self.send_error(400)
            return
        upstream = http.client.HTTPConnection(*self.server.backend, timeout=60)
        finished = threading.Event()
        watcher = None
        response = None
        started = False
        self.close_connection = True
        try:
            upstream.putrequest(self.command, self.path, skip_accept_encoding=True)
            excluded = hop_headers(self.headers) | {"host", "content-length"}
            if browser_auth:
                excluded.add("origin")
                host, port = self.server.backend
                hostname = f"[{host}]" if ":" in host else host
                authority = hostname if port == 80 else f"{hostname}:{port}"
                upstream.putheader("Origin", "http://" + authority)
            for key, value in self.headers.items():
                if key.lower() not in excluded:
                    upstream.putheader(key, value)
            upstream.putheader("Connection", "close")
            if lengths:
                upstream.putheader("Content-Length", str(length))
            upstream.endheaders()
            watcher = threading.Thread(
                target=self.watch_disconnect, args=(upstream.sock, finished), daemon=True
            )
            watcher.start()
            while length:
                chunk = self.rfile.read(min(length, 64 * 1024))
                if not chunk:
                    return
                upstream.send(chunk)
                length -= len(chunk)
            # HTTPConnection returns redirects unchanged; it never follows them.
            response = upstream.getresponse()
            self.send_response(response.status, response.reason)
            excluded = hop_headers(response.headers)
            for key, value in response.getheaders():
                if key.lower() not in excluded:
                    self.send_header(key, value)
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.flush()
            started = True
            if self.command != "HEAD":
                while chunk := response.read1(64 * 1024):
                    self.wfile.write(chunk)
                    self.wfile.flush()
        except (OSError, http.client.HTTPException):
            if not started:
                try:
                    self.send_error(502, "Local backend unavailable")
                except OSError:
                    pass
        finally:
            finished.set()
            if response:
                response.close()
            upstream.close()
            if watcher:
                watcher.join(timeout=0.5)

    def watch_disconnect(self, upstream_socket, finished):
        # Abort an idle SSE upstream too, without waiting for its next heartbeat.
        while not finished.wait(0.1):
            try:
                readable, _, _ = select.select([self.connection], [], [], 0)
                if readable and not self.connection.recv(1, socket.MSG_PEEK):
                    upstream_socket.shutdown(socket.SHUT_RDWR)
                    return
            except OSError:
                return


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", default="http://127.0.0.1:8080")
    parser.add_argument("--port", type=int, default=5173)
    args = parser.parse_args()
    try:
        server = DevServer(("127.0.0.1", args.port), args.backend)
    except (argparse.ArgumentTypeError, OSError, OverflowError) as error:
        parser.error(str(error))
    print(f"Frontend: http://127.0.0.1:{server.server_port} (refresh to load edits)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
