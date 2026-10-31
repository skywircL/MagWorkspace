"""Bounded lexical recall for stored personal memories; no model calls."""
from __future__ import annotations

import unicodedata
from magent_harness.memory import MemoryService, MemoryScope

_STOP = frozenset("a an and are as at be by can could did do does for from had has have how i if in is it its me my of on or our should that the their this to was we were what when where which who why will with would you your 什么 我的 一下 怎么 请问 是否 可以 如何 这个 那个".split())


def _han(char):
    return "\u3400" <= char <= "\u9fff" or "\U00020000" <= char <= "\U0002ebef"


def terms(text):
    tokens, run, kind = set(), "", None

    def flush():
        if kind == "han":
            tokens.update(run[i:i + 2] for i in range(len(run) - 1))
            if len(run) == 1:
                tokens.add(run)
        elif len(run) > 1:
            tokens.add(run)

    for char in unicodedata.normalize("NFKC", text).casefold():
        next_kind = "han" if _han(char) else "word" if char.isalnum() else None
        if next_kind != kind or next_kind is None:
            flush()
            run = ""
        kind = next_kind
        if kind is not None:
            run += char
    flush()
    return tokens - _STOP


def ranked(items, query):
    wanted = terms(query)
    if not wanted:
        return []
    matches = []
    for item in items:
        content, topic = terms(item.content), terms(item.topic)
        coverage = len(wanted & (content | topic)) / len(wanted)
        if coverage < 0.2:
            continue
        # Importance breaks ties; it cannot make unrelated content relevant.
        matches.append((coverage, len(wanted & topic), item))
    matches.sort(key=lambda row: (-row[0], -row[1], -row[2].importance, -row[2].created_at, row[2].id))
    return [row[2] for row in matches]


def budget(items, limit, runes, *, include_kind=False):
    selected, seen, used = [], set(), 0
    for item in items:
        key = (item.kind, item.content)
        cost = len(item.content) + (len(item.kind.value) + 6 if include_kind else 3)
        if key in seen or used + cost > runes:
            continue
        if len(selected) >= limit:
            break
        seen.add(key)
        selected.append(item)
        used += cost
    return selected


class WorkspaceMemoryService(MemoryService):
    @staticmethod
    def _select(active, query):
        resident = sorted((i for i in active if i.kind.resident),
                          key=lambda i: (-i.importance, -i.created_at, i.id))
        return (budget(resident, 20, 600),
                budget(ranked([i for i in active if not i.kind.resident], query), 5, 600))

    def _search(self, caller, query, limit):
        if not isinstance(query, str) or len(query) > 2000 or type(limit) is not int:
            return []
        scope = MemoryScope.for_caller(caller)
        limit = min(max(limit, 0), 20)
        selected = budget(ranked(self._active(scope), query), limit, 1600, include_kind=True)
        if not selected:
            return []
        self.store.touch(scope, [i.id for i in selected], self.clock())
        # Final read occurs after statistics; disabled/forgotten/replaced rows
        # are removed before rendering, including when a worker had old IDs.
        return budget(ranked(self._current(scope, selected), query), limit, 1600, include_kind=True)
