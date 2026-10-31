"""Packaged automatic-memory acceptance with a disposable Docker stack.

Uses current Compose services, the real cmd/server worker and a controlled local
provider. Never reads the project's .env or connects to a paid model. Cleanup is
limited to the uniquely named project, its volumes, images and temporary files.
Run: python3 scripts/smoke-memory-stack.py --output-dir artifacts/acceptance/043
Existing receipts are never overwritten; choose a fresh output directory to rerun.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import socket
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "artifacts/acceptance/030"
QUERY = "我偏好简短的中文回答，先给结论。"
MEMORY = "用户偏好简短中文回答，先给结论。"
OUTAGE_QUERY = "我偏好深色编辑器主题。"
OUTAGE_MEMORY = "用户偏好深色编辑器主题。"
PROVIDER = r'''import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Event, Lock

lock = Lock()
stats = {"chat_calls": 0, "extraction_calls": 0, "recalled_calls": 0,
         "held_calls": 0, "hold_timeouts": 0, "outage_extraction_calls": 0}
query = "我偏好简短的中文回答，先给结论。"
memory = "用户偏好简短中文回答，先给结论。"
outage_query = "我偏好深色编辑器主题。"
outage_memory = "用户偏好深色编辑器主题。"
hold_query = None
gate = None

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass
    def emit(self, value, status=200):
        raw = json.dumps(value, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)
    def do_GET(self):
        if self.path == "/healthz":
            self.emit({"status": "ok"})
        elif self.path == "/stats":
            with lock:
                self.emit(dict(stats))
        else:
            self.emit({}, 404)
    def do_POST(self):
        global hold_query, gate
        if self.path not in ("/v1/chat/completions", "/control"):
            self.emit({}, 404)
            return
        size = int(self.headers.get("Content-Length", "0"))
        if not 0 < size <= 2 * 1024 * 1024:
            self.emit({}, 400)
            return
        request = json.loads(self.rfile.read(size))
        if self.path == "/control":
            with lock:
                if request.get("hold") in ("automatic", "outage"):
                    if gate is not None:
                        gate.set()
                    hold_query = query if request["hold"] == "automatic" else outage_query
                    gate = Event()
                elif request.get("release") is True:
                    if gate is not None:
                        gate.set()
                    gate = None
                    hold_query = None
                else:
                    self.emit({}, 400)
                    return
            self.emit({"ok": True})
            return
        messages = request["messages"]
        extracting = bool(messages and "MEMORY_EXTRACTION_V1" in messages[0].get("content", ""))
        if extracting and (len(messages) != 2 or messages[1].get("role") != "user"):
            self.emit({}, 400)
            return
        waiting = None
        with lock:
            if extracting:
                stats["extraction_calls"] += 1
                if messages[1]["content"] == outage_query:
                    stats["outage_extraction_calls"] += 1
                if messages[1]["content"] == hold_query and gate is not None:
                    waiting = gate
                    stats["held_calls"] += 1
            else:
                stats["chat_calls"] += 1
                if any(m.get("role") == "system" and memory in m.get("content", "") for m in messages):
                    stats["recalled_calls"] += 1
        # Hold only the synthetic extraction, never the answer request. This
        # exposes a real Asynq active task for independent SQL/Redis inspection.
        if waiting is not None and not waiting.wait(25):
            with lock:
                stats["hold_timeouts"] += 1
            self.emit({}, 503)
            return
        if extracting:
            items = []
            if messages[1]["content"] == query:
                items = [{"kind": "preference", "content": memory, "topic": "response_style", "importance": 4, "evidence": query}]
            elif messages[1]["content"] == outage_query:
                items = [{"kind": "preference", "content": outage_memory, "topic": "editor_theme", "importance": 4, "evidence": outage_query}]
            answer = json.dumps({"items": items}, ensure_ascii=False)
        else:
            answer = "本轮已回答，记忆保存结果以后台状态为准。"
        message = {"role": "assistant", "content": answer}
        if request.get("stream"):
            raw = json.dumps({"choices": [{"index": 0, "delta": message, "finish_reason": "stop"}]}, ensure_ascii=False).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            self.wfile.write(b"data: " + raw + b"\n\ndata: [DONE]\n\n")
        else:
            self.emit({"choices": [{"message": message, "finish_reason": "stop"}]})

ThreadingHTTPServer(("0.0.0.0", 8999), Handler).serve_forever()
'''
HTTP_CLIENT = r'''import base64,json,sys,urllib.error,urllib.request
spec=json.load(sys.stdin)
body=base64.b64decode(spec["body"]) if spec["body"] is not None else None
request=urllib.request.Request(spec["url"],data=body,headers=spec["headers"],method=spec["method"])
opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
try:
    response=opener.open(request,timeout=spec["timeout"])
except urllib.error.HTTPError as error:
    response=error
with response:
    json.dump({"status":response.status,"body":base64.b64encode(response.read()).decode()},sys.stdout)
'''


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=OUT, help="fresh receipt directory (default: artifacts/acceptance/030)")
    args = parser.parse_args()
    out = args.output_dir if args.output_dir.is_absolute() else ROOT / args.output_dir
    if any((out / name).exists() for name in ("packaged-build.log", "packaged-results.json", "packaged-runtime.log", "packaged-source-sha256.json")):
        parser.error("output directory already contains packaged acceptance files; choose a fresh --output-dir")
    out.mkdir(parents=True, exist_ok=True)
    (ROOT / ".cache").mkdir(exist_ok=True)
    private = Path(tempfile.mkdtemp(prefix="memory-stack-", dir=ROOT / ".cache"))
    project = "mw-memory-" + secrets.token_hex(5)
    image_names = [project + "-backend:latest", project + "-harness:latest"]
    report = {"scope": "real Docker cmd/server + FastAPI Harness + MySQL + Redis Asynq + MinIO; controlled provider; document indexing disabled", "checks": [], "observations": {}, "status": "running"}
    started = time.monotonic()
    log = (out / "packaged-build.log").open("x")
    # Explicitly exclude application/provider values inherited from the shell.
    environment = {key: os.environ[key] for key in ("PATH", "HOME", "DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_CONFIG", "TMPDIR") if key in os.environ}
    compose = []
    token = ""

    def save():
        (out / "packaged-results.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")

    def check(value, label):
        if not value:
            raise RuntimeError(label)
        report["checks"].append(label)
        save()
        print("PASS " + label, flush=True)

    def command(args, timeout=180, capture=False, input_text=None):
        # No command contains credentials. Docker output goes to a dedicated log.
        completed = subprocess.run(args, cwd=ROOT, env=environment, stdout=subprocess.PIPE if capture else log,
                                   stderr=log, input=input_text.encode() if input_text is not None else None, timeout=timeout, check=False)
        if completed.returncode:
            raise RuntimeError("command failed: " + " ".join(args[:4]))
        return completed.stdout.decode() if capture else ""

    def call(base, method, path, data=None, expected=200, sse=False):
        headers = {}
        if token and base == backend:
            headers["Authorization"] = "Bearer " + token
        body = None
        if data is not None:
            body = json.dumps(data, ensure_ascii=False).encode()
            headers["Content-Type"] = "application/json"
        if sse:
            headers["Accept"] = "text/event-stream"
        # HTTP runs inside the fixture network because an internal Docker
        # network intentionally need not expose published ports to the host.
        # Synthetic auth travels over stdin, never command arguments or logs.
        spec = {"url": base + path, "method": method, "headers": headers,
                "body": base64.b64encode(body).decode() if body is not None else None,
                "timeout": 5 if path in ("/healthz", "/readyz") else 90}
        response = subprocess.run(["docker", "exec", "-i", project + "-provider-1", "python3", "-c", HTTP_CLIENT],
                                  input=json.dumps(spec).encode(), stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                  cwd=ROOT, env=environment, timeout=100, check=False)
        if response.returncode:
            raise RuntimeError("fixture HTTP client failed")
        wire = json.loads(response.stdout)
        raw = base64.b64decode(wire["body"])
        if wire["status"] != expected:
            raise RuntimeError("HTTP " + method + " status " + str(wire["status"]) + " expected " + str(expected))
        if sse:
            events = []
            for block in raw.decode().replace("\r\n", "\n").split("\n\n"):
                name, values = "", []
                for line in block.splitlines():
                    if line.startswith("event:"):
                        name = line[6:].strip()
                    elif line.startswith("data:"):
                        values.append(line[5:].lstrip())
                if name and values:
                    events.append((name, json.loads("\n".join(values))))
            check(bool(events) and events[0][0] == "run.started" and events[-1][0] == "run.completed", "SSE reaches durable completed terminal")
            return events[-1][1]
        if not raw:
            return None
        value = json.loads(raw)
        return value.get("data", value)

    def wait_ready(base):
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            try:
                if call(base, "GET", "/healthz"):
                    return
            except (OSError, RuntimeError):
                pass
            time.sleep(1)
        raise RuntimeError("service readiness deadline")

    def wait_capture(session, run, status="done"):
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            value = call(backend, "GET", session + "/runs/" + run + "/memory")
            if value["status"] == status:
                return value
            if value["status"] in ("failed", "cancelled"):
                raise RuntimeError("capture unexpected terminal: " + value["status"] + " " + value.get("error_code", ""))
            time.sleep(.25)
        raise RuntimeError("automatic worker deadline")

    def capture_state(run):
        # Only synthetic IDs enter SQL; credentials are expanded inside the
        # disposable MySQL container and never passed in process arguments.
        if len(run) != 32 or any(ch not in "0123456789abcdef" for ch in run):
            raise RuntimeError("invalid fixture run ID")
        query = ("SELECT JSON_OBJECT('status',j.status,'attempt',j.attempt,'generation',j.generation,"
                 "'saved_count',j.saved_count,'run_status',r.status,'delivery_id',o.id,"
                 "'source_memory_count',(SELECT COUNT(*) FROM user_memories m WHERE m.source_run_id=j.run_id)) "
                 "FROM memory_capture_jobs j JOIN runs r ON r.id=j.run_id "
                 "LEFT JOIN memory_capture_outbox o ON o.run_id=j.run_id WHERE j.run_id='" + run + "';\n")
        raw = command(compose + ["exec", "-T", "mysql", "sh", "-ec",
                                'MYSQL_PWD="$MYSQL_PASSWORD" exec mysql --protocol=TCP -h127.0.0.1 -umagworkspace -Dmagworkspace --batch --raw --skip-column-names'],
                      capture=True, input_text=query, timeout=10)
        if not raw.strip():
            raise RuntimeError("capture SQL row missing")
        return json.loads(raw)

    def redis(*args):
        raw = command(compose + ["exec", "-T", "redis", "sh", "-ec",
                                'REDISCLI_AUTH="$REDIS_PASSWORD" exec redis-cli --json "$@"', "fixture-redis", *args],
                      capture=True, timeout=10)
        return json.loads(raw)

    def observe_active(run, label, held_count, delivery=None):
        deadline = time.monotonic() + 40
        while time.monotonic() < deadline:
            counts = call(provider, "GET", "/stats")
            if counts["held_calls"] == held_count:
                break
            time.sleep(.25)
        else:
            raise RuntimeError("controlled extraction did not reach pause point")
        state = capture_state(run)
        check(state["status"] == "running" and state["run_status"] == "completed" and state["attempt"] == 1 and
              state["delivery_id"] and (delivery is None or state["delivery_id"] == delivery),
              label + ": completed answer has one claimed SQL delivery")
        task_key = "asynq:{mw-memory-capture}:t:" + state["delivery_id"]
        payload = json.dumps({"job_id": run, "delivery_id": state["delivery_id"]}, separators=(",", ":"))
        # Asynq's message is protobuf. Inspect the state and exact known type /
        # ID-only payload inside Redis instead of dumping the encoded message.
        transport = redis("EVAL", "local m=redis.call('HGET',KEYS[1],'msg') or ''; return {redis.call('HGET',KEYS[1],'state') or '', string.find(m,ARGV[1],1,true) and 1 or 0, string.find(m,ARGV[2],1,true) and 1 or 0}",
                          "1", task_key, "magworkspace:memory-capture", payload)
        check(transport == ["active", 1, 1], label + ": real memory Asynq task matches SQL delivery and task type")
        report["observations"][label] = {"sql": state, "redis_state": transport[0], "task_type_matches": True, "payload_matches": True}
        save()
        return state["delivery_id"]

    def observe_done(run, delivery, label):
        state = capture_state(run)
        check(state["status"] == "done" and state["run_status"] == "completed" and state["attempt"] == 1 and
              state["saved_count"] == 1 and state["source_memory_count"] == 1 and state["delivery_id"] is None,
              label + ": one memory committed and durable delivery removed")
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if redis("EXISTS", "asynq:{mw-memory-capture}:t:" + delivery) == 0:
                break
            time.sleep(.25)
        else:
            raise RuntimeError("completed Asynq task was not acknowledged")
        check(True, label + ": Asynq ACK removes completed transport task")
        report["observations"][label] = state
        save()

    try:
        values = {key: secrets.token_hex(24) for key in ("MYSQL_PASSWORD", "MYSQL_ROOT_PASSWORD", "REDIS_PASSWORD", "MINIO_SECRET_KEY", "QDRANT_API_KEY", "MAGENT_SERVICE_API_KEY")}
        values.update({"MINIO_ACCESS_KEY": "memory-fixture", "MINIO_BUCKET": "memory-fixture", "MAGENT_MODE": "live",
                       "MAGENT_BASE_URL": "http://127.0.0.1:8999/v1", "MAGENT_API_KEY": "fixture-only-no-external-model", "MAGENT_MODEL": "memory-fixture",
                       "DOCUMENT_INDEXING_ENABLED": "false", "ALLOW_REGISTRATION": "true", "TRACING_ENABLED": "false", "MAGENT_TIMEOUT": "45s",
                       "MAGENT_HARNESS_PATH": str(ROOT.parent / "Magent Harness")})
        for key in ("MYSQL_PORT", "REDIS_PORT", "MINIO_PORT", "MINIO_CONSOLE_PORT", "HTTP_PORT", "MAGENT_SERVICE_PORT", "QDRANT_PORT", "OLLAMA_PORT", "TIKA_PORT"):
            values[key] = str(free_port())
        backend = "http://backend:8080"
        harness = "http://127.0.0.1:8090"
        provider_port = free_port()
        provider = "http://127.0.0.1:8999"
        env_file = private / "fixture.env"
        env_file.write_text("\n".join(key + "=" + value for key, value in values.items()) + "\n")
        env_file.chmod(0o600)
        fixture_file = private / "provider.py"
        fixture_file.write_text(PROVIDER)
        fixture_file.chmod(0o644)
        context = private / "backend-context"
        context.mkdir()
        for folder in ("cmd", "internal"):
            shutil.copytree(ROOT / folder, context / folder)
        for name in ("go.mod", "go.sum"):
            shutil.copy2(ROOT / name, context / name)
        cache = Path(os.environ.get("MAGWORKSPACE_GO_DOWNLOAD_CACHE", str(ROOT / ".cache/gomod/cache/download")))
        if not cache.is_dir():
            raise RuntimeError("verified Go download cache is required")
        shutil.copytree(cache, context / "gomod-cache", ignore=shutil.ignore_patterns("*.lock", "*.tmp"))
        dockerfile = (ROOT / "Dockerfile").read_text()
        if dockerfile.count("RUN go mod download") != 1:
            raise RuntimeError("backend Dockerfile download step changed")
        (context / "Dockerfile").write_text(dockerfile.replace("RUN go mod download", "COPY gomod-cache /module-cache\nRUN GOPROXY=file:///module-cache GOSUMDB=off go mod download"))
        hashes = {}
        for path in [ROOT / name for name in ("Dockerfile", "compose.yaml", "go.mod", "go.sum", "scripts/smoke-memory-stack.py")] + [p for folder in ("cmd", "internal", "bridges") for p in (ROOT / folder).rglob("*") if p.is_file() and ".deps" not in p.parts and "__pycache__" not in p.parts]:
            relative = path.relative_to(ROOT)
            copied = context / relative
            build_source = copied if relative.parts[0] in ("cmd", "internal", "go.mod", "go.sum") else path
            hashes[str(relative)] = hashlib.sha256(build_source.read_bytes()).hexdigest()
            if hashes[str(relative)] != hashlib.sha256(path.read_bytes()).hexdigest():
                raise RuntimeError("source changed while preparing copied build context")
        (out / "packaged-source-sha256.json").write_text(json.dumps(hashes, indent=2) + "\n")
        # Sharing only this disposable provider's network namespace preserves
        # the production rule that plaintext model endpoints must be loopback.
        # The provider owns the published ports; its alias keeps backend->Harness
        # and Harness->backend callbacks identical to normal Compose networking.
        override_file = private / "compose.fixture.yaml"
        override_file.write_text("\n".join([
            "services:", "  backend:", "    image: " + image_names[0], "    build:", "      context: " + json.dumps(str(context)),
            "  harness:", "    image: " + image_names[1], "    network_mode: service:provider", "    ports: !reset []",
            "  provider:", "    image: " + image_names[1], '    entrypoint: ["python3", "-B", "/fixture/provider.py"]',
            "    ports:", '      - "127.0.0.1:' + str(provider_port) + ':8999"', '      - "127.0.0.1:' + values["MAGENT_SERVICE_PORT"] + ':8090"',
            "    volumes:", "      - " + json.dumps(str(fixture_file) + ":/fixture/provider.py:ro"),
            "    networks:", "      default:", "        aliases: [harness]", "    healthcheck:",
            "      test: " + json.dumps(["CMD", "python3", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8999/healthz',timeout=2).close()"]),
            "      interval: 2s", "      timeout: 3s", "      retries: 30", "networks:", "  default:", "    internal: true", "",
        ]))
        compose = ["docker", "compose", "--project-directory", str(ROOT), "--env-file", str(env_file), "-f", str(ROOT / "compose.yaml"), "-f", str(override_file), "-p", project, "--profile", "app"]
        command(compose + ["build", "backend", "harness"], timeout=900)
        command(compose + ["up", "-d", "--no-build", "--no-deps", "--wait", "--wait-timeout", "100", "mysql", "redis", "minio", "provider", "harness"], timeout=140)
        command(compose + ["up", "-d", "--no-build", "--no-deps", "backend"], timeout=45)
        wait_ready(harness)
        wait_ready(provider)
        wait_ready(backend)
        call(backend, "GET", "/readyz")
        check(True, "isolated current Compose backend Harness MySQL Redis MinIO and provider ready")
        account = call(backend, "POST", "/api/v1/auth/register", {"name": "Memory packaged fixture", "email": "memory-" + secrets.token_hex(5) + "@example.test", "password": secrets.token_hex(24)}, expected=201)
        token = account["token"]
        workspace = "/api/v1/workspaces/" + account["workspaces"][0]["id"]
        unavailable = call(backend, "POST", workspace + "/search", {"query": "fixture", "mode": "keyword", "limit": 1}, expected=503)
        check(unavailable.get("error", {}).get("code") == "indexing_disabled", "real search endpoint confirms document indexing is disabled")

        def session(title):
            return workspace + "/sessions/" + call(backend, "POST", workspace + "/sessions", {"title": title}, expected=201)["id"]

        first = session("Automatic capture")
        call(provider, "POST", "/control", {"hold": "automatic"})
        answer = call(backend, "POST", first + "/messages", {"content": QUERY}, expected=201)
        try:
            first_delivery = observe_active(answer["run"]["id"], "indexing_disabled_memory_active", held_count=1)
        finally:
            call(provider, "POST", "/control", {"release": True})
        receipt = wait_capture(first, answer["run"]["id"])
        items = call(backend, "GET", workspace + "/memories")
        check(receipt["saved_count"] == 1 and len(items) == 1 and items[0]["origin"] == "automatic" and items[0]["source_run_id"] == answer["run"]["id"] and items[0]["content"] == MEMORY,
              "real cmd/server background worker automatically persists scoped source-bound memory")
        check(call(provider, "GET", "/stats")["extraction_calls"] == 1, "automatic extraction invokes only controlled local provider once")
        observe_done(answer["run"]["id"], first_delivery, "indexing_disabled_memory_done")
        queues = redis("SMEMBERS", "asynq:queues")
        check("mw-memory-capture" in queues and not any(name.startswith("mw-index-") for name in queues),
              "independent memory queue operates without a document index queue")
        report["observations"]["registered_queues"] = sorted(queues)
        second = session("Cross-session recall")
        recalled = call(backend, "POST", second + "/messages", {"content": "介绍一下工作台。"}, sse=True)
        wait_capture(second, recalled["run_id"])
        check(call(provider, "GET", "/stats")["recalled_calls"] == 1, "fresh-session SSE includes previously captured memory in real Harness context")
        before = call(provider, "GET", "/stats")
        explicit = call(backend, "POST", second + "/messages", {"content": "请记住：我的项目使用 MySQL 数据库。"}, expected=201)
        explicit_receipt = wait_capture(second, explicit["run"]["id"])
        check(explicit_receipt["saved_count"] == 1 and call(provider, "GET", "/stats")["extraction_calls"] == before["extraction_calls"], "explicit chat directive persists without extraction model call")
        items = call(backend, "GET", workspace + "/memories")
        check(any(item["origin"] == "explicit" and item["source_run_id"] == explicit["run"]["id"] for item in items), "explicit provenance is visible through management API")
        call(backend, "PUT", workspace + "/memory-settings", {"enabled": False})
        before = call(provider, "GET", "/stats")
        third = session("Disabled capture and recall")
        disabled = call(backend, "POST", third + "/messages", {"content": "我长期使用 Go 编写服务。"}, expected=201)
        wait_capture(third, disabled["run"]["id"], "cancelled")
        after = call(provider, "GET", "/stats")
        check(after["extraction_calls"] == before["extraction_calls"] and after["recalled_calls"] == before["recalled_calls"], "off switch disables capture and recall while completed answer remains available")
        item_ids = sorted(item["id"] for item in items)
        command(compose + ["restart", "backend"], timeout=60)
        wait_ready(backend)
        check(sorted(item["id"] for item in call(backend, "GET", workspace + "/memories")) == item_ids and not call(backend, "GET", workspace + "/memory-settings")["enabled"], "container restart retains memories and private switch")
        call(backend, "PUT", workspace + "/memory-settings", {"enabled": True})
        fourth = session("Recall after restart")
        final = call(backend, "POST", fourth + "/messages", {"content": "说明你的回答风格。"}, sse=True)
        wait_capture(fourth, final["run_id"])
        check(call(provider, "GET", "/stats")["recalled_calls"] == after["recalled_calls"] + 1, "re-enabled memory is recalled after backend container restart")

        # Redis is a transport dependency, not permission to fall back to SQL
        # polling. A completed JSON answer must still atomically retain its
        # queued SQL intent without starting extraction while Redis is down.
        fifth = session("Redis outage delivery")
        before_outage = call(provider, "GET", "/stats")
        call(provider, "POST", "/control", {"hold": "outage"})
        command(compose + ["stop", "--timeout", "10", "redis"], timeout=25)
        outage = call(backend, "POST", fifth + "/messages", {"content": OUTAGE_QUERY}, expected=201)
        outage_run = outage["run"]["id"]
        queued = capture_state(outage_run)
        check(queued["run_status"] == "completed" and queued["status"] == "queued" and queued["attempt"] == 0 and
              queued["delivery_id"] is not None and queued["source_memory_count"] == 0,
              "Redis outage preserves completed answer and queued SQL outbox before extraction")
        # Observe twice across several two-second dispatch intervals. Docker
        # exec itself can take longer than a fixed observation window on a busy
        # host; keep both samples and the minimum separation deterministic.
        outage_started = time.monotonic()
        samples = 0
        for sample in range(2):
            if sample:
                time.sleep(6)
            state = capture_state(outage_run)
            public = call(backend, "GET", fifth + "/runs/" + outage_run + "/memory")
            counts = call(provider, "GET", "/stats")
            if state != queued or public["status"] != "queued" or counts["extraction_calls"] != before_outage["extraction_calls"]:
                raise RuntimeError("Redis outage unexpectedly consumed or changed durable memory intent")
            samples += 1
        check(samples == 2, "Redis outage leaves attempt zero, same delivery and no model call across dispatch intervals")
        report["observations"]["redis_outage"] = {"sql": queued, "samples": samples, "seconds": round(time.monotonic() - outage_started, 1),
                                                     "extraction_calls_before": before_outage["extraction_calls"], "extraction_calls_after": counts["extraction_calls"]}
        save()
        command(compose + ["up", "-d", "--no-build", "--no-deps", "--wait", "--wait-timeout", "30", "redis"], timeout=45)
        try:
            recovered_delivery = observe_active(outage_run, "redis_recovered_memory_active", held_count=before_outage["held_calls"] + 1, delivery=queued["delivery_id"])
        finally:
            call(provider, "POST", "/control", {"release": True})
        recovered = wait_capture(fifth, outage_run)
        items = call(backend, "GET", workspace + "/memories")
        check(recovered["saved_count"] == 1 and any(item["origin"] == "automatic" and item["source_run_id"] == outage_run and item["content"] == OUTAGE_MEMORY for item in items),
              "Redis recovery automatically republishes retained SQL intent and exposes saved memory through HTTP")
        observe_done(outage_run, recovered_delivery, "redis_recovered_memory_done")
        counts = call(provider, "GET", "/stats")
        check(counts["extraction_calls"] == before_outage["extraction_calls"] + 1 and counts["outage_extraction_calls"] == 1 and counts["hold_timeouts"] == 0,
              "recovered delivery invokes extraction once without pause timeout or duplicate memory")
        check(all(hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == digest for name, digest in hashes.items()), "tested source matches current source snapshot")
        report["provider_counts"] = call(provider, "GET", "/stats")
        report["images"] = {name: command(["docker", "image", "inspect", "--format", "{{.Id}}", name], capture=True).strip() for name in image_names}
        report["status"] = "passed"
    except Exception as error:
        report["status"] = "failed"
        report["error"] = str(error)
        if compose:
            with (out / "packaged-runtime.log").open("w") as runtime_log:
                subprocess.run(compose + ["logs", "--no-color", "--tail", "60", "backend", "harness", "provider"], cwd=ROOT, env=environment,
                               stdout=runtime_log, stderr=runtime_log, timeout=30, check=False)
        raise
    finally:
        cleanup_ok = True
        if compose:
            try:
                command(compose + ["down", "--volumes", "--remove-orphans", "--timeout", "15"], timeout=90)
            except Exception:
                cleanup_ok = False
        if cleanup_ok:
            for name in image_names:
                subprocess.run(["docker", "image", "rm", name], cwd=ROOT, env=environment, stdout=log, stderr=log, check=False)
            shutil.rmtree(private)
        report["cleanup"] = "complete" if cleanup_ok else "failed; owned context retained at " + str(private)
        report["elapsed_seconds"] = round(time.monotonic() - started, 1)
        save()
        log.close()


if __name__ == "__main__":
    main()
