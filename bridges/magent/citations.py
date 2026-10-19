"""Canonicalize historical markers only through backend-proven passage aliases.

This is a private delivery step, not a model tool. The immutable Harness journal
keeps the model's original answer; delivery and recovery repeat this validation.
"""
import re

_MARKER = re.compile(r"\[E:([0-9a-f]{32})\]")


async def canonicalize_citations(registry, backend, ctx, answer, history):
    from magent_harness.retrieval import EvidenceError

    if "[E:" in _MARKER.sub("", answer):
        raise EvidenceError("malformed evidence citation")
    missing = []
    for evidence_id in dict.fromkeys(_MARKER.findall(answer)):
        try:
            registry.require(ctx, evidence_id)
        except EvidenceError:
            missing.append(evidence_id)
    if missing:
        historical = {evidence_id for message in history if message["role"] == "assistant"
                      for evidence_id in _MARKER.findall(message["content"])}
        if not set(missing) <= historical:
            raise EvidenceError("citation was not present in supplied assistant history")
        result = await backend.call("resolve_citation_aliases", {"evidence_ids": missing})
        if not isinstance(result, dict) or set(result) != {"aliases"}:
            raise EvidenceError("invalid citation aliases")
        aliases = result["aliases"]
        if not isinstance(aliases, dict) or set(aliases) != set(missing):
            raise EvidenceError("incomplete citation aliases")
        for target in aliases.values():
            registry.require(ctx, target)
        answer = _MARKER.sub(lambda match: "[E:" + aliases.get(match[1], match[1]) + "]", answer)
    registry.citations(ctx, answer)
    return answer
