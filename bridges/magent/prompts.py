"""Validate server-owned native prompt snapshots before cross-process resume."""
from __future__ import annotations

import math
import re
import time

from retrieval import strict_json
from workspace_tools import NAMES, LEGACY_NAMES

from document_writes import NAMES as WRITE_NAMES

MAX_SNAPSHOT = 512 << 10
ALLOWED_TOOLS = NAMES | WRITE_NAMES | LEGACY_NAMES | {"ask_user", "search_memory"}


def _text(value, limit, nonempty=True):
    if not isinstance(value, str) or len(value.encode("utf-8")) > limit or (nonempty and not value.strip()):
        raise ValueError("invalid prompt text")


def validate_snapshot(data, prompt):
    from magent_harness import AccessLevel, RunSnapshot
    _text(prompt.snapshot, MAX_SNAPSHOT)
    strict_json(prompt.snapshot)  # Native decoder accepts duplicates; our boundary does not.
    snapshot = RunSnapshot.decode(prompt.snapshot)
    wanted_scope = None if data.get("document_ids") is None else frozenset(data["document_ids"])
    if (
        snapshot.run_id != data["run_id"] or snapshot.session_id != data["session_id"]
        or snapshot.query != data["query"]
        or snapshot.caller.workspace_id != data["workspace_id"] or snapshot.caller.user_id != data["user_id"]
        or snapshot.scope.workspace_id != data["workspace_id"] or snapshot.scope.document_ids != wanted_scope
        or snapshot.policy.access not in {AccessLevel.READ_ONLY, AccessLevel.CONFIRM} or snapshot.policy.allowed_tools is None
        or not snapshot.policy.allowed_tools <= ALLOWED_TOOLS
    ):
        raise ValueError("snapshot identity, scope or policy mismatch")
    blocked = snapshot.pending.tool_calls[snapshot.pending.blocked_index]
    if blocked.id != prompt.tool_call_id:
        raise ValueError("snapshot does not wait for this prompt")
    args = strict_json(blocked.arguments)
    if prompt.kind.value == "tool_approval":
        if snapshot.policy.access is not AccessLevel.CONFIRM or blocked.name not in WRITE_NAMES or blocked.name != prompt.request["tool"] or args != prompt.request["arguments"]:
            raise ValueError("approval does not match pending write")
    elif blocked.name != "ask_user" or not isinstance(args, dict) or set(args)-{"question", "options"} or args.get("question") != prompt.request["question"] or args.get("options", []) != prompt.request["options"]:
        raise ValueError("question does not match pending call")
    return snapshot


def public_internal_prompt(data, prompt):
    from magent_harness.hitl import PromptKind, PromptStatus
    if prompt.kind not in {PromptKind.ASK_USER, PromptKind.TOOL_APPROVAL} or prompt.status is not PromptStatus.SUSPENDED:
        raise ValueError("unsupported prompt kind")
    _validate_fields(data, prompt)
    validate_snapshot(data, prompt)
    if prompt.kind is PromptKind.TOOL_APPROVAL:
        return {"id": prompt.id, "kind": prompt.kind.value, "tool_call_id": prompt.tool_call_id, "question": "请确认文档写入操作", "options": [], "request": prompt.request, "snapshot": prompt.snapshot, "expires_at": prompt.expires_at}
    return {"id": prompt.id, "tool_call_id": prompt.tool_call_id, "question": prompt.request["question"], "options": prompt.request["options"], "snapshot": prompt.snapshot, "expires_at": prompt.expires_at}


def _validate_fields(data, prompt):
    if not re.fullmatch(r"[0-9a-f]{32}", prompt.id) or prompt.run_id != data["run_id"] or prompt.session_id != data["session_id"] or prompt.workspace_id != data["workspace_id"] or prompt.user_id != data["user_id"]:
        raise ValueError("prompt identity mismatch")
    _text(prompt.tool_call_id, 256)
    if prompt.kind.value == "tool_approval":
        if not isinstance(prompt.request, dict) or set(prompt.request) != {"tool", "risk", "description", "arguments", "reason"} or prompt.request["tool"] not in WRITE_NAMES or prompt.request["risk"] != "write" or not isinstance(prompt.request["arguments"], dict):
            raise ValueError("invalid approval fields")
        _text(prompt.request["description"], 2048)
        _text(prompt.request["reason"], 1024, nonempty=False)
    else:
        if not isinstance(prompt.request, dict) or set(prompt.request) != {"question", "options"}:
            raise ValueError("invalid question fields")
        _text(prompt.request["question"], 4096)
        options = prompt.request["options"]
        if not isinstance(options, list) or len(options)>10:
            raise ValueError("invalid question options")
        for option in options:
            _text(option, 512)
    if type(prompt.expires_at) not in (int, float) or not math.isfinite(prompt.expires_at) or prompt.expires_at <= time.time():
        raise ValueError("prompt is expired")


def restore_prompt(data, broker, original=None, *, resuming=False, install=True, completed=False):
    from dataclasses import replace
    from magent_harness.hitl import AgentPrompt, PromptKind, PromptStatus
    raw = data["resume"]
    if not isinstance(raw, dict) or set(raw)!={"prompt"} or not isinstance(raw["prompt"], dict):
        raise ValueError("invalid resume shape")
    fields = dict(raw["prompt"])
    fields["kind"] = PromptKind(fields["kind"])
    fields["status"] = PromptStatus(fields["status"])
    prompt = AgentPrompt(**fields)
    if prompt.kind not in {PromptKind.ASK_USER, PromptKind.TOOL_APPROVAL} or prompt.status is not PromptStatus.ANSWERED or prompt.resolved_by != data["user_id"]:
        raise ValueError("prompt has no owned answer")
    _validate_fields(data, prompt)
    if prompt.kind is PromptKind.TOOL_APPROVAL:
        if not isinstance(prompt.response, dict) or set(prompt.response)!={"approved"} or type(prompt.response["approved"]) is not bool:
            raise ValueError("invalid approval decision")
    else:
        if not isinstance(prompt.response, dict) or set(prompt.response)!={"answer"}:
            raise ValueError("invalid prompt answer")
        _text(prompt.response["answer"], 4096)
    validate_snapshot(data, prompt)
    if original is not None:
        # Go owns answer/CAS authority. Its row timestamps have database
        # precision and its creation time differs from the original native
        # prompt; retain the journal's immutable prompt rather than replacing it.
        for name in ("id", "kind", "workspace_id", "user_id", "session_id", "run_id", "tool_call_id", "request", "snapshot"):
            if getattr(prompt, name) != getattr(original, name):
                raise ValueError("answer does not match the persisted question")
        if original.status not in (PromptStatus.SUSPENDED, PromptStatus.ANSWERED):
            raise ValueError("persisted question is not answerable")
        if original.status is PromptStatus.ANSWERED and original.response != prompt.response:
            raise ValueError("answer differs from committed resume request")
        prompt = replace(original, status=PromptStatus.ANSWERED, response=prompt.response,
                         resolved_at=prompt.resolved_at, resolved_by=prompt.resolved_by)
        _validate_fields(data, prompt)
        if resuming:
            # ASK_USER claim is deterministic and has no remote side effect.
            # Its committed resume_requested fact plus Go's claimed answer
            # reconstructs DONE even if the old Python process disappeared.
            prompt = replace(prompt, status=(PromptStatus.DONE if prompt.kind is PromptKind.ASK_USER or completed else PromptStatus.EXECUTING))
    if install:
        broker.store.create(prompt)
    return prompt
