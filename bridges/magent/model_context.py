"""Model-only citation projection; durable messages and journals stay exact.

Historical references are separate from current evidence: old markers provide
context, never usable citation IDs. The Harness keeps its E
protocol; only its verified current-run registry can preserve a marker here.
"""
from dataclasses import replace
import re

_SOURCE = re.compile(r"\[(E|historical evidence):([0-9a-f]{32})\]")
HISTORICAL_SOURCE = "[historical source; retrieve again]"


class CitationModelView:
    def __init__(self, model, registry, context, *, summary=False):
        self.model = model
        self.registry = registry
        self.context = context
        self.summary = summary

    def messages(self, messages):
        from magent_harness.retrieval import EvidenceError

        def marker(match):
            if match[1] == "E":
                try:
                    self.registry.require(self.context, match[2])
                    return match[0]
                except EvidenceError:
                    pass
            return HISTORICAL_SOURCE

        return [replace(message, content=_SOURCE.sub(marker, message.content))
                if self.summary or message.role in {"assistant", "tool"}
                or message.kind == "compaction_summary" else message
                for message in messages]

    async def complete(self, messages, tools, *, max_tokens=None):
        return await self.model.complete(self.messages(messages), tools, max_tokens=max_tokens)

    async def complete_stream(self, messages, tools, *, on_delta, max_tokens=None):
        return await self.model.complete_stream(
            self.messages(messages), tools, on_delta=on_delta, max_tokens=max_tokens,
        )
