"""Personal memory read transport; MySQL and current authority remain in Go.

Only explicit user statements are stored. A source session is audit metadata,
not a channel to learn document content or copy historical messages.
"""
from __future__ import annotations

from callback_endpoint import validate_callback
import json
import math
import re
import urllib.error
import urllib.request
from retrieval import strict_json, _NoRedirect

MAX_RESPONSE = 16 << 20


def validate_endpoint(value):
    return validate_callback(value, "/memory")


class RemoteMemoryStore:
    def __init__(self, endpoint, request):
        self.endpoint = validate_endpoint(endpoint)
        self.identity = request["workspace_id"], "user:" + request["user_id"]
        self._opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())

    def _scope(self, scope):
        if (scope.workspace_id, scope.subject_id) != self.identity:
            raise ValueError("memory identity mismatch")

    def _call(self, operation, arguments):
        from magent_harness.file_store import PersistenceError
        try:
            encoded = json.dumps({"operation": operation, "arguments": arguments}, allow_nan=False, separators=(",", ":")).encode("utf-8")
            if len(encoded) > 64 << 10:
                raise ValueError("memory request exceeds limit")
            request = urllib.request.Request(self.endpoint["url"], data=encoded, method="POST", headers={"Authorization": "Bearer " + self.endpoint["token"], "Content-Type": "application/json"})
            with self._opener.open(request, timeout=30.0) as response:
                raw = response.read(MAX_RESPONSE + 1)
            if len(raw) > MAX_RESPONSE:
                raise ValueError("memory response exceeds limit")
            return strict_json(raw)
        except (OSError, urllib.error.URLError, ValueError, TypeError, UnicodeError, RecursionError):
            raise PersistenceError("authenticated memory operation failed") from None

    @staticmethod
    def _items(values):
        from magent_harness.memory import MemoryItem, MemoryKind, MemoryStatus
        fields = {"id", "kind", "content", "topic", "normalized_key", "importance", "status", "created_at", "expires_at", "superseded_by", "source_session_id", "use_count", "last_used_at"}
        if not isinstance(values, list) or len(values) > 1000:
            raise ValueError("invalid memory response")
        out, seen = [], set()
        for value in values:
            if not isinstance(value, dict) or set(value) != fields:
                raise ValueError("invalid memory item")
            for field in ("id", "content", "topic", "normalized_key", "superseded_by", "source_session_id"):
                if not isinstance(value[field], str) or len(value[field]) > 1000:
                    raise ValueError("invalid memory text")
            if not re.fullmatch(r"[0-9a-f]{32}", value["id"]) or value["id"] in seen:
                raise ValueError("invalid memory ID")
            if type(value["importance"]) is not int or not 1 <= value["importance"] <= 5 or type(value["use_count"]) is not int or value["use_count"] < 0:
                raise ValueError("invalid memory counters")
            for name in ("created_at", "expires_at", "last_used_at"):
                item = value[name]
                if item is None and name != "created_at":
                    continue
                if type(item) not in (float, int) or not math.isfinite(item):
                    raise ValueError("invalid memory timestamp")
            item = dict(value, kind=MemoryKind(value["kind"]), status=MemoryStatus(value["status"]))
            out.append(MemoryItem(**item))
            seen.add(value["id"])
        return out

    def list_items(self, scope, status=None):
        from magent_harness.memory import MemoryStatus
        self._scope(scope)
        status = MemoryStatus.ACTIVE if status is None else MemoryStatus(status)
        items = self._items(self._call("list", {"status": status.value}))
        if any(item.status is not status for item in items):
            raise ValueError("memory response status mismatch")
        return items

    def get_items(self, scope, item_ids):
        from magent_harness.memory import MemoryStatus
        self._scope(scope)
        ids = list(item_ids)
        items = self._items(self._call("get_items", {"item_ids": ids}))
        if any(item.id not in ids or item.status is not MemoryStatus.ACTIVE for item in items):
            raise ValueError("memory response differs from requested IDs")
        return items

    def touch(self, scope, item_ids, now):
        self._scope(scope)
        if self._call("touch", {"item_ids": list(item_ids), "now": now}) != {}:
            raise ValueError("invalid memory usage receipt")

    def add(self, scope, item):
        self._scope(scope)
        raise NotImplementedError("memory writes require the authenticated management API")

    def update(self, scope, item):
        self._scope(scope)
        raise NotImplementedError("memory writes require the authenticated management API")
