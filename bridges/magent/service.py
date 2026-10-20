"""Private FastAPI transport for isolated, cancellable Harness executions.

The Go backend owns authentication, tools, journals and final business state.
One ASGI process owns admission; each accepted execution owns one bridge child.
"""
from __future__ import annotations

import asyncio
import contextlib
import hmac
import json
import math
import os
from pathlib import Path
import re
import signal
import sys
from contextlib import asynccontextmanager

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / ".deps"))

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from starlette.requests import ClientDisconnect

from bridge import MAX_REQUEST, MAX_RESPONSE, parse_request, provider_config
from callback_endpoint import callback_origin
from retrieval import strict_json
import memory_extraction

MAX_STREAM = 8 << 20
MAX_STDERR = 16 << 10
PROTOCOL = "1"
WORKER_ENV = {
    "PATH", "PYTHONPATH", "SYSTEMROOT", "LANG", "LC_ALL",
    "MAGENT_MODE", "MAGENT_BASE_URL", "MAGENT_API_KEY", "MAGENT_MODEL",
    "MAGENT_HTTP_TIMEOUT", "MAGENT_MAX_CONTEXT_TOKENS", "MAGENT_RESERVE_TOKENS",
    "MAGENT_KEEP_RECENT_TOKENS", "MAGENT_MAX_SUMMARY_TOKENS",
    "MAGENT_SUMMARY_TIMEOUT_S", "MAGENT_MAX_COMPLETION_TOKENS",
    "MAGENT_CALLBACK_ORIGIN",
}


class WorkerError(Exception):
    def __init__(self, code="agent_failed"):
        self.code = code


def encode(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")


def error_response(status, code):
    return JSONResponse({"code": code}, status_code=status, headers={"X-Magent-Protocol": PROTOCOL})


def one_header(request, name, default=None):
    values = request.headers.getlist(name)
    if len(values) > 1:
        raise ValueError("duplicate header")
    return values[0] if values else default


def duration_seconds(value):
    units = {"h": 3600, "m": 60, "s": 1, "ms": .001, "us": .000001, "µs": .000001, "ns": .000000001}
    pieces = re.findall(r"([0-9]+(?:\.[0-9]+)?)(ms|us|µs|ns|h|m|s)", value)
    if not pieces or "".join(number + unit for number, unit in pieces) != value:
        raise ValueError("invalid duration configuration")
    return sum(float(number) * units[unit] for number, unit in pieces)


class Settings:
    def __init__(self):
        self.key = os.environ.get("MAGENT_SERVICE_API_KEY", "").encode("utf-8")
        if len(self.key) < 32 or any(c < 33 or c > 126 for c in self.key):
            raise ValueError("invalid service key configuration")
        self.timeout = (float(os.environ["MAGENT_HTTP_TIMEOUT"]) if "MAGENT_HTTP_TIMEOUT" in os.environ
                        else duration_seconds(os.environ.get("MAGENT_TIMEOUT", "120s")))
        self.concurrency = int(os.environ.get("MAGENT_SERVICE_MAX_CONCURRENCY", "4"))
        if not math.isfinite(self.timeout) or not 1 <= self.timeout <= 600 or not 1 <= self.concurrency <= 64:
            raise ValueError("invalid service limit configuration")
        origin = callback_origin()
        self.mode = os.environ.get("MAGENT_MODE", "")
        if self.mode not in ("", "demo", "live"):
            raise ValueError("invalid mode configuration")
        if self.mode and not origin:
            raise ValueError("callback origin is required for the HTTP service")
        self.worker_env = {key: value for key, value in os.environ.items() if key in WORKER_ENV}
        self.worker_env["PYTHONUNBUFFERED"] = "1"
        self.worker_env["MAGENT_HTTP_TIMEOUT"] = str(self.timeout)
        if self.mode:
            values = {}
            for key, default in (("MAX_CONTEXT_TOKENS", 128000), ("RESERVE_TOKENS", 16384),
                                 ("KEEP_RECENT_TOKENS", 20000), ("MAX_SUMMARY_TOKENS", 2048),
                                 ("MAX_COMPLETION_TOKENS", 4096)):
                value = int(os.environ.get("MAGENT_" + key, str(default)))
                if not 1 <= value <= 2000000:
                    raise ValueError("invalid context configuration")
                values[key] = value
            values["RESERVE_TOKENS"] = max(values["RESERVE_TOKENS"], values["MAX_COMPLETION_TOKENS"])
            if (values["RESERVE_TOKENS"] >= values["MAX_CONTEXT_TOKENS"]
                    or values["MAX_SUMMARY_TOKENS"] >= values["MAX_CONTEXT_TOKENS"] - values["RESERVE_TOKENS"]):
                raise ValueError("invalid context configuration")
            summary = (float(os.environ["MAGENT_SUMMARY_TIMEOUT_S"]) if "MAGENT_SUMMARY_TIMEOUT_S" in os.environ
                       else duration_seconds(os.environ.get("MAGENT_SUMMARY_TIMEOUT", "60s")))
            if not math.isfinite(summary) or not .001 <= summary <= 600:
                raise ValueError("invalid summary timeout configuration")
            self.worker_env.update({"MAGENT_" + key: str(value) for key, value in values.items()})
            self.worker_env["MAGENT_SUMMARY_TIMEOUT_S"] = str(summary)
        if self.mode == "live":
            provider_config()


class RunResponse(Response):
    """Keep process lifetime, disconnect handling and admission in one scope."""

    output_limit = MAX_RESPONSE

    def __init__(self, request_data, raw, streaming, timeout, state):
        super().__init__(content=b"")
        self.data, self.raw, self.streaming = request_data, raw, streaming
        self.timeout, self.state = timeout, state
        self.proc = None
        self.spawn = None
        self.tasks = []
        self.started = False
        self.stream_bytes = 0
        self.cleanup_lock = asyncio.Lock()
        self.cleaned = False

    async def start(self, send, status=200):
        media = b"text/event-stream" if self.streaming and status == 200 else b"application/json"
        await send({"type": "http.response.start", "status": status, "headers": [
            (b"content-type", media), (b"x-magent-protocol", b"1"),
            (b"cache-control", b"no-store"), (b"x-accel-buffering", b"no"),
        ]})
        self.started = True

    async def frame(self, send, kind, data):
        raw = b"event: " + kind.encode("ascii") + b"\ndata: " + encode(data) + b"\n\n"
        self.stream_bytes += len(raw)
        # Reserve space for a final safe error if the child exceeds its budget.
        if self.stream_bytes > MAX_STREAM - 128:
            raise WorkerError()
        await send({"type": "http.response.body", "body": raw, "more_body": True})

    async def write_input(self):
        self.proc.stdin.write(self.raw)
        await self.proc.stdin.drain()
        self.proc.stdin.close()
        await self.proc.stdin.wait_closed()

    async def discard_stderr(self):
        total = 0
        while chunk := await self.proc.stderr.read(4096):
            total += len(chunk)
            if total > MAX_STDERR:
                raise WorkerError()

    def check_identity(self, value):
        if (not isinstance(value, dict) or value.get("run_id") != self.data["run_id"]
                or value.get("session_id") != self.data["session_id"]):
            raise WorkerError()

    async def read_output(self, send):
        if not self.streaming:
            raw = bytearray()
            while chunk := await self.proc.stdout.read(65536):
                raw.extend(chunk)
                if len(raw) > self.output_limit:
                    raise WorkerError()
            return bytes(raw)
        result = None
        total = 0
        while line := await self.proc.stdout.readline():
            total += len(line)
            if total > MAX_STREAM or len(line) > MAX_RESPONSE or not line.endswith(b"\n") or result is not None:
                raise WorkerError()
            record = strict_json(line)
            if not isinstance(record, dict) or len(record) != 1:
                raise WorkerError()
            if "event" in record:
                event = record["event"]
                self.check_identity(event)
                kind = event.get("type")
                if kind not in ("message.start", "message.delta", "message.discard"):
                    raise WorkerError()
                await self.frame(send, kind, event)
            elif "result" in record:
                result = record["result"]
                self.check_identity(result)
            else:
                raise WorkerError()
        return result

    def command(self):
        command = [sys.executable, "-S", str(ROOT / "bridge.py")]
        if self.streaming:
            command.append("--stream")
        return command

    async def execute(self, send):
        env = self.state.settings.worker_env
        self.spawn = asyncio.create_task(asyncio.create_subprocess_exec(
            *self.command(), stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE, env=env, cwd=str(ROOT),
            start_new_session=True, limit=MAX_RESPONSE + 1,
        ))
        try:
            # If cancellation races spawn, cleanup still awaits and reaps it.
            self.proc = await asyncio.shield(self.spawn)
        except OSError:
            raise WorkerError("agent_unavailable") from None
        if self.streaming:
            await self.start(send)
        self.tasks = [asyncio.create_task(self.write_input()), asyncio.create_task(self.discard_stderr()),
                      asyncio.create_task(self.read_output(send)), asyncio.create_task(self.proc.wait())]
        # FIRST_EXCEPTION interrupts blocked output when stderr or stdin fails.
        done, _ = await asyncio.wait(self.tasks, return_when=asyncio.FIRST_EXCEPTION)
        for task in done:
            task.result()
        if self.proc.returncode:
            raise WorkerError("agent_unavailable" if self.proc.returncode == 78 else "agent_failed")
        result = self.tasks[2].result()
        if not self.streaming:
            result = strict_json(result)
        self.check_identity(result)
        # A terminal record is provisional until successful exit and pipe EOF.
        if self.streaming:
            await self.frame(send, "result", result)
            await send({"type": "http.response.body", "body": b"", "more_body": False})
        else:
            await self.start(send)
            await send({"type": "http.response.body", "body": encode(result), "more_body": False})

    async def cleanup(self):
        async with self.cleanup_lock:
            if not self.cleaned:
                await self._cleanup()
                self.cleaned = True

    async def _cleanup(self):
        if self.spawn is not None:
            with contextlib.suppress(Exception, asyncio.CancelledError):
                self.proc = await self.spawn
        if self.proc is not None:
            # Kill the entire owned process group, including any remaining child.
            with contextlib.suppress(ProcessLookupError):
                os.killpg(self.proc.pid, signal.SIGKILL)
            for task in self.tasks:
                task.cancel()
            await asyncio.gather(*self.tasks, return_exceptions=True)
            # Drain abandoned pipes: asyncio wait() can otherwise stay blocked
            # when its pipe transport paused at the stream buffer limit.
            async def drain(reader):
                while await reader.read(65536):
                    pass
            await asyncio.gather(drain(self.proc.stdout), drain(self.proc.stderr), self.proc.wait())
        self.state.active.discard(self.data["run_id"])
        self.state.responses.discard(self)

    async def __call__(self, scope, receive, send):
        async def disconnected():
            while True:
                if (await receive())["type"] == "http.disconnect":
                    return

        run = asyncio.create_task(self.execute(send))
        disconnect = asyncio.create_task(disconnected())
        try:
            async with asyncio.timeout(self.timeout):
                done, _ = await asyncio.wait((run, disconnect), return_when=asyncio.FIRST_COMPLETED)
                if disconnect in done:
                    return
                run.result()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            code = exc.code if isinstance(exc, WorkerError) else "agent_failed"
            run.cancel()
            await asyncio.gather(run, return_exceptions=True)
            await self.cleanup()
            with contextlib.suppress(Exception):
                async with asyncio.timeout(1):
                    if not self.started:
                        await self.start(send, 503 if code == "agent_unavailable" else 502)
                        await send({"type": "http.response.body", "body": encode({"code": code}), "more_body": False})
                    else:
                        raw = b'event: error\ndata: ' + encode({"code": code}) + b'\n\n'
                        await send({"type": "http.response.body", "body": raw, "more_body": False})
        finally:
            run.cancel()
            disconnect.cancel()
            await asyncio.gather(run, disconnect, return_exceptions=True)
            # Reaping is not abandoned if the ASGI server cancels this request.
            cleanup = asyncio.create_task(self.cleanup())
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                await cleanup
                raise


class ExtractionResponse(RunResponse):
    output_limit = memory_extraction.MAX_RESPONSE_BYTES

    def __init__(self, data, raw, timeout, state):
        # The synthetic identifier is local bookkeeping, never a user identity.
        super().__init__(dict(data, run_id=object()), raw, False, timeout, state)

    def command(self):
        return [sys.executable, "-S", str(ROOT / "memory_extraction.py")]

    def check_identity(self, value):
        memory_extraction.validate_result(value, self.data["query"])
        if len(encode(value)) > memory_extraction.MAX_RESPONSE_BYTES:
            raise WorkerError()

    async def _cleanup(self):
        await super()._cleanup()
        self.state.extractions.discard(self)


def create_app():
    settings = Settings()

    @asynccontextmanager
    async def lifespan(app):
        yield
        await asyncio.gather(*(item.cleanup() for item in tuple(app.state.responses)))

    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None, lifespan=lifespan)
    app.router.redirect_slashes = False
    app.state.active = set()
    app.state.responses = set()
    # One dedicated background lane cannot consume interactive run admission.
    app.state.extractions = set()
    app.state.settings = settings

    @app.get("/healthz")
    async def health():
        return {"status": "ok"}

    @app.post("/v1/memories/extract")
    async def extract_memory(request: Request):
        started_at = asyncio.get_running_loop().time()
        try:
            auth = one_header(request, "authorization", "").encode("utf-8")
            if not hmac.compare_digest(auth, b"Bearer " + settings.key):
                return error_response(401, "unauthorized")
            if one_header(request, "x-magent-protocol") != PROTOCOL:
                return error_response(400, "invalid_protocol")
            if one_header(request, "content-type", "").lower() not in ("application/json", "application/json; charset=utf-8"):
                return error_response(415, "unsupported_media_type")
            if one_header(request, "accept", "application/json") not in ("application/json", "*/*"):
                return error_response(406, "unsupported_media_type")
            timeout = min(settings.timeout, memory_extraction.MAX_TIMEOUT)
            timeout_ms = one_header(request, "x-magent-timeout-ms")
            if timeout_ms is not None:
                if not re.fullmatch(r"[1-9][0-9]{0,12}", timeout_ms):
                    raise ValueError("invalid timeout")
                timeout = min(timeout, int(timeout_ms) / 1000)
            length = one_header(request, "content-length")
            if length is not None and (not length.isdecimal() or int(length) > memory_extraction.MAX_REQUEST_BYTES):
                return error_response(413, "request_too_large")
            raw = bytearray()
            async with asyncio.timeout(timeout):
                async for chunk in request.stream():
                    raw.extend(chunk)
                    if len(raw) > memory_extraction.MAX_REQUEST_BYTES:
                        return error_response(413, "request_too_large")
            data = memory_extraction.parse_request(bytes(raw))
        except (ValueError, TypeError, UnicodeError, RecursionError, TimeoutError, ClientDisconnect):
            return error_response(400, "invalid_request")
        if not settings.mode:
            return error_response(503, "agent_unavailable")
        timeout -= asyncio.get_running_loop().time() - started_at
        if timeout <= 0:
            return error_response(502, "agent_failed")
        if app.state.extractions:
            return error_response(429, "agent_busy")
        response = ExtractionResponse(data, bytes(raw), timeout, app.state)
        app.state.responses.add(response)
        app.state.extractions.add(response)
        return response

    @app.post("/v1/runs")
    async def run(request: Request):
        started_at = asyncio.get_running_loop().time()
        try:
            auth = one_header(request, "authorization", "").encode("utf-8")
            if not hmac.compare_digest(auth, b"Bearer " + settings.key):
                return error_response(401, "unauthorized")
            if one_header(request, "x-magent-protocol") != PROTOCOL:
                return error_response(400, "invalid_protocol")
            content_type = one_header(request, "content-type", "").lower()
            if content_type not in ("application/json", "application/json; charset=utf-8"):
                return error_response(415, "unsupported_media_type")
            accept = one_header(request, "accept", "application/json")
            if accept not in ("application/json", "text/event-stream", "*/*"):
                return error_response(406, "unsupported_media_type")
            timeout_ms = one_header(request, "x-magent-timeout-ms")
            timeout = settings.timeout
            if timeout_ms is not None:
                if not re.fullmatch(r"[1-9][0-9]{0,12}", timeout_ms):
                    raise ValueError("invalid timeout")
                timeout = min(timeout, int(timeout_ms) / 1000)
            length = one_header(request, "content-length")
            if length is not None and (not length.isdecimal() or int(length) > MAX_REQUEST):
                return error_response(413, "request_too_large")
            raw = bytearray()
            async with asyncio.timeout(timeout):
                async for chunk in request.stream():
                    raw.extend(chunk)
                    if len(raw) > MAX_REQUEST:
                        return error_response(413, "request_too_large")
            data = parse_request(bytes(raw))
        except (ValueError, TypeError, UnicodeError, RecursionError, TimeoutError, ClientDisconnect):
            return error_response(400, "invalid_request")
        if not settings.mode:
            return error_response(503, "agent_unavailable")
        timeout -= asyncio.get_running_loop().time() - started_at
        if timeout <= 0:
            return error_response(502, "agent_failed")
        if data["run_id"] in app.state.active:
            return error_response(409, "run_active")
        if len(app.state.active) >= settings.concurrency:
            return error_response(429, "agent_busy")
        app.state.active.add(data["run_id"])
        response = RunResponse(data, bytes(raw), accept == "text/event-stream", timeout, app.state)
        app.state.responses.add(response)
        return response

    return app


def main():
    try:
        import uvicorn
        app = create_app()
        port = int(os.environ.get("MAGENT_SERVICE_PORT", "8090"))
        if not 1 <= port <= 65535:
            raise ValueError("invalid port")
        uvicorn.run(app, host=os.environ.get("MAGENT_SERVICE_HOST", "0.0.0.0"), port=port,
                    access_log=False, log_config=None, server_header=False,
                    timeout_graceful_shutdown=5, ws="none", http="h11")
    except Exception:
        sys.stderr.write("Magent HTTP service unavailable; check runtime configuration.\n")
        return 78
    return 0


if __name__ == "__main__":
    sys.exit(main())
