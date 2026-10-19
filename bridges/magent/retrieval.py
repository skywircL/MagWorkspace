"""Private run-scoped transport for the backend's run-scoped read-only tools.

No model argument chooses identity, permissions, endpoint, token or scope.
"""

from __future__ import annotations

from callback_endpoint import validate_callback

import asyncio
import json
import math
import sys
from pathlib import Path
import re
import urllib.error
import urllib.request

MAX_REQUEST = 256 << 10
MAX_RESPONSE = 1 << 20


class RetrievalError(ValueError):
    pass


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON field")
        result[key] = value
    return result


def _invalid_constant(value):
    raise ValueError("invalid JSON number")


def strict_json(raw):
    return json.loads(
        raw, object_pairs_hook=_unique_object, parse_constant=_invalid_constant
    )


def validate_endpoint(value):
    return validate_callback(value, "/tools")


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise urllib.error.HTTPError(
            req.full_url, code, "redirect rejected", headers, fp
        )


class Backend:
    """EvidenceBackend adapter; server authorization is bound to the capability."""

    def __init__(self, endpoint: dict, timeout: float = 30.0):
        checked = validate_endpoint(endpoint)
        self._url = checked["url"]
        self._token = checked["token"]
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("invalid retrieval timeout")
        self._timeout = min(30.0, timeout)
        self._client = None

    async def __aenter__(self):
        # Same pinned runtime as the native model adapter, including demo mode.
        sys.path.insert(0, str(Path(__file__).resolve().parent / ".deps"))
        import httpx

        self._client = httpx.AsyncClient(
            trust_env=False, follow_redirects=False, timeout=self._timeout,
            limits=httpx.Limits(max_connections=8, max_keepalive_connections=8),
        )
        await self._client.__aenter__()
        return self

    async def __aexit__(self, *exc):
        if self._client is not None:
            await self._client.__aexit__(*exc)
            self._client = None

    async def call(self, tool: str, arguments: dict, *, tool_call_id: str = ""):
        import httpx

        if self._client is None:
            raise RetrievalError("retrieval transport is closed")
        try:
            encoded = json.dumps(
                {"tool": tool, "arguments": arguments, **({"tool_call_id": tool_call_id} if tool_call_id else {})},
                ensure_ascii=False, allow_nan=False, separators=(",", ":"),
            ).encode("utf-8")
            if len(encoded) > MAX_REQUEST:
                raise RetrievalError("invalid retrieval request")
            # The deadline covers pool acquisition and the entire streamed body,
            # including a peer that keeps sending small chunks indefinitely.
            async with asyncio.timeout(self._timeout):
                async with self._client.stream(
                    "POST", self._url, content=encoded,
                    headers={"Authorization": "Bearer " + self._token,
                             "Content-Type": "application/json", "Accept-Encoding": "identity"},
                ) as response:
                    if response.headers.get("content-encoding", "identity") != "identity":
                        raise RetrievalError("invalid retrieval encoding")
                    limit = MAX_RESPONSE if response.status_code == 200 else 1024
                    raw = bytearray()
                    async for chunk in response.aiter_raw():
                        if len(raw) + len(chunk) > limit:
                            raise RetrievalError("retrieval result exceeds limit")
                        raw.extend(chunk)
                    if response.status_code != 200:
                        code = "tool_unavailable"
                        try:
                            candidate = strict_json(raw)["error"]["code"]
                            if isinstance(candidate, str) and candidate in {
                                "indexing_unavailable", "index_not_ready", "index_failed",
                                "evidence_limit", "not_found", "forbidden", "invalid_arguments",
                                "tool_unavailable", "write_conflict", "approval_required",
                            }:
                                code = candidate
                        except (ValueError, TypeError, KeyError):
                            pass
                        raise RetrievalError(code)
                    return strict_json(raw)
        except RetrievalError:
            raise
        except (httpx.HTTPError, OSError, TimeoutError, ValueError, TypeError):
            # Never reflect network errors, private capabilities or raw bodies.
            # CancelledError is deliberately allowed to close the socket upstream.
            raise RetrievalError("retrieval unavailable") from None

    async def search(self, ctx, query: str, mode: str = "hybrid", limit: int = 5):
        from magent_harness.retrieval import Evidence

        result = await self.call(
            "search_documents", {"query": query, "mode": mode, "limit": limit}
        )
        if not isinstance(result, dict) or set(result) != {"evidence"}:
            raise RetrievalError("invalid retrieval result")
        items = result["evidence"]
        if not isinstance(items, list) or len(items) > limit:
            raise RetrievalError("invalid retrieval result")
        return [Evidence.from_dict(item) for item in items]

    async def read(self, ctx, evidence_id: str):
        from magent_harness.retrieval import Evidence

        result = await self.call(
            "read_evidence", {"evidence_id": evidence_id}
        )
        return Evidence.from_dict(result)


def demo_answer(messages, tools):
    """Deterministic offline answer using only the actual tool result."""
    from magent_harness.testing import reply

    for message in reversed(messages):
        if message.role != "tool":
            continue
        try:
            result = strict_json(message.content)
            evidence = result.get("evidence", [])
            if not evidence:
                break
            first = evidence[0]
            marker = first["citation"]
            text = first["content"]
            if not re.fullmatch(r"\[E:[0-9a-f]{32}\]", marker) or not isinstance(text, str):
                break
            return reply("演示检索结果（未调用真实模型）：\n" + text[:2048] + " " + marker)
        except (ValueError, TypeError, KeyError, AttributeError):
            break
    return reply("演示模式（未调用真实模型）：未找到可用的文档证据。")
