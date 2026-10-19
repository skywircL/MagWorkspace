#!/usr/bin/env python3
"""Opt-in live SSE smoke; only a synthetic question leaves this workspace."""

import http.client
import json
import os
from pathlib import Path
import secrets
import signal
import subprocess
import sys
import time
import tempfile
from urllib.parse import quote, urlsplit


ROOT = Path(__file__).resolve().parents[1]
QUESTION = "请用三句话解释知识库，每句话少于20个字。"
MAX_BODY = 1 << 20


class SmokeFailure(Exception):
    pass


def require(condition, code):
    if not condition:
        raise SmokeFailure(code)


def request(port, method, path, data=None, token=None):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    try:
        connection.request(method, path, json.dumps(data) if data is not None else None, headers)
        response = connection.getresponse()
        raw = response.read(MAX_BODY + 1)
        require(len(raw) <= MAX_BODY, "http_body_limit")
        return response.status, json.loads(raw) if raw else {}
    finally:
        connection.close()


def component(value):
    require(isinstance(value, str) and 0 < len(value) <= 256, "invalid_identity")
    return quote(value, safe="")


def events(response, deadline):
    total = 0
    while True:
        lines = []
        while True:
            require(time.monotonic() < deadline, "stream_deadline")
            line = response.readline(MAX_BODY + 1)
            require(bool(line), "stream_ended_before_terminal")
            total += len(line)
            require(len(line) <= MAX_BODY and total <= 8 << 20, "stream_body_limit")
            if line in (b"\n", b"\r\n"):
                break
            if not line.startswith(b":"):
                lines.append(line.decode("utf-8").rstrip("\r\n"))
        if not lines:
            continue
        require(
            len(lines) == 2 and lines[0].startswith("event: ") and lines[1].startswith("data: "),
            "invalid_sse_frame",
        )
        data = json.loads(lines[1][6:])
        require(isinstance(data, dict), "invalid_sse_data")
        yield lines[0][7:], data


def exercise(port):
    status, registered = request(
        port, "POST", "/api/v1/auth/register",
        {"name": "Live SSE smoke", "email": "smoke-" + secrets.token_hex(8) + "@example.com", "password": secrets.token_hex(24)},
    )
    require(status == 201 and registered.get("success") is True, "registration_failed")
    token = registered["data"]["token"]
    workspace_id = registered["data"]["workspaces"][0]["id"]
    sessions = "/api/v1/workspaces/" + component(workspace_id) + "/sessions"
    status, session = request(port, "POST", sessions, {"title": "Synthetic live SSE smoke"}, token)
    require(status == 201 and session.get("success") is True, "session_creation_failed")
    session_id = session["data"]["id"]
    session_path = sessions + "/" + component(session_id)
    messages_path = session_path + "/messages"
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=135)
    started_at = time.monotonic()
    delta_count, current_round = 0, 0
    first_delta_ms = None
    draft, discarded, started = "", False, False
    try:
        # This is the script's only model-triggering request. Do not retry it.
        connection.request(
            "POST", messages_path, json.dumps({"content": QUESTION}),
            {"Authorization": "Bearer " + token, "Content-Type": "application/json", "Accept": "text/event-stream"},
        )
        response = connection.getresponse()
        require(response.status == 200, "stream_http_status")
        require(response.getheader("Content-Type", "").split(";", 1)[0] == "text/event-stream", "stream_content_type")
        run_id = response.getheader("X-Run-ID")
        component(run_id)
        for kind, data in events(response, started_at + 135):
            require(data.get("run_id") == run_id and data.get("session_id") == session_id, "stream_identity_mismatch")
            if not started:
                require(kind == "run.started" and data.get("run", {}).get("status") == "running", "stream_start_order")
                require(data["run"].get("id") == run_id and data["run"].get("session_id") == session_id, "run_identity_mismatch")
                started = True
            elif kind == "message.start":
                require(type(data.get("round")) is int and data["round"] == current_round + 1 and data["round"] <= 32 and not data.get("delta"), "message_start_order")
                current_round, draft, discarded = data["round"], "", False
            elif kind == "message.delta":
                delta = data.get("delta")
                require(current_round > 0 and not discarded and data.get("round") == current_round and isinstance(delta, str) and bool(delta), "message_delta_order")
                if first_delta_ms is None:
                    first_delta_ms = (time.monotonic() - started_at) * 1000
                delta_count += 1
                draft += delta
                require(len(draft.encode("utf-8")) <= 64000, "message_size_limit")
            elif kind == "message.discard":
                require(current_round > 0 and not discarded and data.get("round") == current_round and not data.get("delta"), "message_discard_order")
                draft, discarded = "", True
            elif kind == "run.completed":
                answer, run = data.get("answer"), data.get("run", {})
                require(delta_count > 0 and not discarded and isinstance(answer, str) and bool(answer.strip()) and answer == draft.strip(), "completed_answer_mismatch")
                require(run.get("id") == run_id and run.get("session_id") == session_id and run.get("status") == "completed" and run.get("mode") == "live", "completed_run_mismatch")
                completed_ms = (time.monotonic() - started_at) * 1000
                require(response.readline(MAX_BODY + 1) == b"", "events_after_terminal")
                break
            elif kind == "run.failed":
                raise SmokeFailure("agent_run_failed")
            else:
                raise SmokeFailure("unexpected_sse_event")
    finally:
        connection.close()
    status, persisted = request(port, "GET", session_path + "/runs/" + component(run_id), token=token)
    require(status == 200 and persisted.get("success") is True, "run_read_failed")
    run = persisted["data"]
    require(run.get("id") == run_id and run.get("session_id") == session_id and run.get("status") == "completed" and run.get("mode") == "live", "run_not_durable")
    status, persisted = request(port, "GET", messages_path, token=token)
    require(status == 200 and persisted.get("success") is True and persisted.get("total") == 2, "messages_read_failed")
    messages = persisted["data"]
    require(len(messages) == 2, "message_count_mismatch")
    for message, role, content in zip(messages, ("user", "assistant"), (QUESTION, answer)):
        require(message.get("role") == role and message.get("content") == content and message.get("status") == "completed" and message.get("run_id") == run_id and message.get("session_id") == session_id, "messages_not_durable")
    return {"success": True, "mode": "live", "delta_count": delta_count, "first_delta_ms": round(first_delta_ms, 2), "completed_ms": round(completed_ms, 2), "answer_bytes": len(answer.encode("utf-8"))}


def live():
    # The shared acceptance runner builds both services and owns isolated data.
    # --live explicitly opts into reading the project model configuration.
    with tempfile.TemporaryDirectory(prefix="mw-live-http-") as directory:
        environment = dict(os.environ, MAGWORKSPACE_ACCEPTANCE_DIR=directory)
        runner = [sys.executable, str(ROOT / "scripts/acceptance-stack.py")]
        outcome = {"success": False, "code": "smoke_execution_failed"}
        try:
            subprocess.run(runner + ["start"], cwd=ROOT, env=environment,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
            state = json.loads((Path(directory) / "private-state.json").read_text())
            port = urlsplit(state["base_url"]).port
            require(port is not None, "backend_port_missing")
            outcome = exercise(port)
        except SmokeFailure as error:
            outcome = {"success": False, "code": error.args[0]}
        except (Exception, KeyboardInterrupt):
            pass
        finally:
            if (Path(directory) / "private-state.json").exists():
                try:
                    subprocess.run(runner + ["stop"], cwd=ROOT, env=environment,
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
                except Exception:
                    outcome = {"success": False, "code": "cleanup_failed"}
        return outcome


def main():
    if sys.argv[1:] != ["--live"]:
        print(json.dumps({"success": False, "code": "live_opt_in_required"}))
        return 2
    def interrupted(_signal, _frame):
        raise SmokeFailure("interrupted")
    signal.signal(signal.SIGTERM, interrupted)
    try:
        result = live()
    except SmokeFailure as error:
        result = {"success": False, "code": error.args[0]}
    except (Exception, KeyboardInterrupt):
        result = {"success": False, "code": "smoke_execution_failed"}
    print(json.dumps(result, separators=(",", ":")), flush=True)
    return 0 if result.get("success") else 1


if __name__ == "__main__":
    sys.exit(main())
