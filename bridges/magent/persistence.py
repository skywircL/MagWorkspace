"""Run-scoped journal transport; business authority remains in the Go backend.

The capability comes only from authenticated stdin. No path or caller is ever
selected by a model tool. Journal commits and generation fencing are enforced by
the backend, while this adapter independently verifies the complete hash chain.
"""
from __future__ import annotations

from callback_endpoint import validate_callback

import hashlib
import json
import math
import urllib.error
import urllib.request

from retrieval import strict_json, _NoRedirect

MAX_RECORD = 2 << 20
MAX_REQUEST = 3 << 20
MAX_RESPONSE = 17 << 20  # 16 MiB journal plus the response wrapper.


def validate_state(data, state):
    """Bind replayed authority to the backend's independently stored run DTO."""
    from magent_harness import AccessLevel
    from prompts import ALLOWED_TOOLS
    wanted_scope = None if data.get("document_ids") is None else frozenset(data["document_ids"])
    text_terminal = (
        state.status in {"completed", "failed", "canceled"}
        and state.scope.document_ids == frozenset()
        and state.policy.allowed_tools == frozenset()
    )
    if (
        state.run_id != data["run_id"] or state.session_id != data["session_id"]
        or state.query != data["query"] or state.caller.workspace_id != data["workspace_id"]
        or state.caller.user_id != data["user_id"] or state.scope.workspace_id != data["workspace_id"]
        or (state.scope.document_ids != wanted_scope and not text_terminal)
        or state.policy.access not in {AccessLevel.READ_ONLY, AccessLevel.CONFIRM} or state.policy.allowed_tools is None
        or not state.policy.allowed_tools <= ALLOWED_TOOLS
    ):
        raise ValueError("persisted run differs from authenticated backend state")


def validate_endpoint(value):
    return validate_callback(value, "/journal")


def canonical(value):
    def check(item):
        if item is None or type(item) in (bool, int, str):
            return
        if type(item) is float and math.isfinite(item):
            return
        if isinstance(item, list):
            for child in item:
                check(child)
            return
        if isinstance(item, dict) and all(isinstance(key, str) for key in item):
            for child in item.values():
                check(child)
            return
        raise ValueError("invalid journal JSON")
    check(value)
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def copy_json(value):
    return strict_json(canonical(value))


class RemoteJournal:
    def __init__(self, endpoint):
        self.endpoint = validate_endpoint(endpoint)
        self._opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())
        self._records = []
        self._open = False
        self._broken = False

    def _call(self, operation, arguments):
        from magent_harness.file_store import PersistenceError
        try:
            encoded = canonical({"operation": operation, "arguments": arguments})
            if len(encoded) > MAX_REQUEST:
                raise ValueError("journal request exceeds limit")
            request = urllib.request.Request(
                self.endpoint["url"], data=encoded, method="POST",
                headers={"Authorization": "Bearer " + self.endpoint["token"], "Content-Type": "application/json"},
            )
            with self._opener.open(request, timeout=30.0) as response:
                raw = response.read(MAX_RESPONSE + 1)
            if len(raw) > MAX_RESPONSE:
                raise ValueError("journal response exceeds limit")
            return strict_json(raw)
        except (OSError, urllib.error.URLError, ValueError, TypeError, UnicodeError, RecursionError):
            raise PersistenceError("authenticated journal operation failed") from None

    def __enter__(self):
        from magent_harness.file_store import CorruptJournalError, PersistenceError
        if self._open:
            raise PersistenceError("journal is already open")
        loaded = self._call("load", {})
        try:
            if not isinstance(loaded, dict) or set(loaded) != {"records"} or not isinstance(loaded["records"], list):
                raise ValueError("invalid journal load")
            previous = ""
            for index, record in enumerate(loaded["records"], 1):
                self._validate_record(record, index, previous)
                previous = record["digest"]
            self._records = copy_json(loaded["records"])
        except (ValueError, KeyError, TypeError, UnicodeError, RecursionError):
            raise CorruptJournalError("invalid authenticated journal facts") from None
        self._open, self._broken = True, False
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._open = False

    def _require_open(self):
        from magent_harness.file_store import PersistenceError
        if not self._open or self._broken:
            raise PersistenceError("journal must be reopened before use")

    @staticmethod
    def _validate_record(record, seq, previous):
        if not isinstance(record, dict) or set(record) != {"version", "seq", "kind", "data", "prev_hash", "digest"}:
            raise ValueError("invalid journal envelope")
        if type(record["version"]) is not int or record["version"] != 1 or type(record["seq"]) is not int or record["seq"] != seq:
            raise ValueError("invalid journal sequence")
        if not isinstance(record["kind"], str) or not record["kind"] or not isinstance(record["data"], dict) or record["prev_hash"] != previous:
            raise ValueError("invalid journal content")
        content = {key: value for key, value in record.items() if key != "digest"}
        if record["digest"] != hashlib.sha256(canonical(content)).hexdigest() or len(canonical(record)) > MAX_RECORD:
            raise ValueError("invalid journal digest or size")

    @property
    def records(self):
        self._require_open()
        return copy_json(self._records)

    @property
    def seq(self):
        return len(self._records)

    @property
    def digest(self):
        return self._records[-1]["digest"] if self._records else ""

    def append(self, kind, data):
        from magent_harness.file_store import PersistenceError
        self._require_open()
        try:
            if not isinstance(kind, str) or not kind or not isinstance(data, dict):
                raise ValueError("invalid journal record")
            content = {"version": 1, "seq": self.seq + 1, "kind": kind, "data": copy_json(data), "prev_hash": self.digest}
            record = {**content, "digest": hashlib.sha256(canonical(content)).hexdigest()}
            self._validate_record(record, self.seq + 1, self.digest)
        except (ValueError, TypeError, UnicodeError, RecursionError):
            raise PersistenceError("journal record is not valid JSON") from None
        try:
            committed = self._call("append", {"record": record})
            if canonical(committed) != canonical(record):
                raise PersistenceError("journal commit receipt does not match")
        except BaseException:
            self._broken = True  # A lost receipt must never cause a blind retry.
            raise
        self._records.append(record)
        return copy_json(record)

    def read_checkpoint(self):
        from magent_harness.file_store import PersistenceError
        self._require_open()
        try:
            result = self._call("checkpoint_read", {})
        except PersistenceError:
            return None  # Derived checkpoints must not override available facts.
        if not isinstance(result, dict) or set(result) != {"version", "seq", "digest", "data"}:
            return None
        if type(result["version"]) is not int or result["version"] != 1 or type(result["seq"]) is not int or result["seq"] != self.seq or result["digest"] != self.digest or not isinstance(result["data"], dict):
            return None
        return copy_json(result["data"])

    def write_checkpoint(self, data):
        from magent_harness.file_store import PersistenceError
        self._require_open()
        if not isinstance(data, dict):
            raise PersistenceError("invalid checkpoint data")
        self._call("checkpoint_write", {"data": {"version": 1, "seq": self.seq, "digest": self.digest, "data": data}})


class RemoteRunStore:
    def __init__(self, endpoint, request, prompt_store):
        self.endpoint = validate_endpoint(endpoint)
        self.identity = (request["workspace_id"], request["user_id"], request["run_id"])
        self.prompts = prompt_store
        self._records = []

    def journal(self, caller, run_id):
        from magent_harness.file_store import PersistenceError
        if (caller.workspace_id, caller.user_id, run_id) != self.identity:
            raise PersistenceError("journal identity mismatch")
        return RemoteJournal(self.endpoint)

    def state(self, caller, run_id):
        from magent_harness.runs import RunLog
        with self.journal(caller, run_id) as journal:
            state = RunLog(journal).state
            self._records = journal.records
            return state

    def committed_answer(self, prompt_id):
        from magent_harness.file_store import prompt_from_dict
        for record in reversed(self._records):
            if record["kind"] == "resume_requested" and record["data"]["prompt"]["id"] == prompt_id:
                return prompt_from_dict(record["data"]["prompt"])
        return None

    def initial_history(self):
        """Original input turns, never assistant output produced by this run.

        state() has authenticated and validated the full journal before this is
        called. Recovery does not receive ordinary history again from HTTP.
        Legacy imported prompt snapshots are not an initial request transcript.
        """
        if not self._records or self._records[0]["kind"] != "created":
            return []
        created = self._records[0]["data"]
        if "prompt" in created:
            return []
        state = created["state"]
        messages = state["messages"]
        if not messages or messages[-1]["role"] != "user" or messages[-1]["content"] != state["query"]:
            return []
        return copy_json(messages[:-1])

    def initial_messages(self):
        """Authenticated original context, including private routing metadata.

        Unlike citation history, this includes imported prompt snapshots. The
        initial record survives context compaction; never infer routing from a
        later model summary or generate a new plan while recovering.
        """
        if not self._records or self._records[0]["kind"] != "created":
            return []
        return copy_json(self._records[0]["data"]["state"]["messages"])
