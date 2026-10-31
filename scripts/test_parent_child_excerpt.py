"""Model-visible parent excerpts through the real bridge/Harness tool contract."""
from __future__ import annotations

import copy
import asyncio
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import sys
import threading
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "bridges/magent"), str(Path(os.environ.get(
    "MAGENT_TEST_HARNESS_PATH", ROOT.parent / "Magent Harness")) / "src")]

from magent_harness.identity import Caller, ResourceScope
from magent_harness.retrieval import EvidenceError
from magent_harness.tools import ToolContext
from retrieval import Backend as TransportBackend, MAX_REQUEST, MAX_RESPONSE, RetrievalError
from workspace_tools import WorkspaceTools


def identifier(number):
    return f"{number:032x}"


def evidence(number, content, start=123):
    encoded = content.encode("utf-8")
    return {
        "evidence_id": identifier(number), "run_id": identifier(10),
        "workspace_id": identifier(11), "document_id": identifier(12),
        "version_id": identifier(13), "index_id": identifier(14),
        "generation": identifier(15), "chunk_id": identifier(100 + number),
        "source_sha256": "a" * 64, "sha256": hashlib.sha256(encoded).hexdigest(),
        "title": "父块", "start_byte": start, "end_byte": start + len(encoded),
        "content": content,
    }


def focus(item, start, end):
    return {"start_byte": item["start_byte"] + len(item["content"][:start].encode("utf-8")),
            "end_byte": item["start_byte"] + len(item["content"][:end].encode("utf-8"))}


class Backend:
    def __init__(self, response):
        self.response = response

    async def call(self, name, args):
        return self.response


class ParentExcerptTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.ctx = ToolContext(
            caller=Caller(workspace_id=identifier(11), user_id=identifier(16), role="owner"),
            scope=ResourceScope(workspace_id=identifier(11), document_ids=frozenset({identifier(12)})),
            session_id=identifier(17), run_id=identifier(10), tool_call_id="excerpt-test",
        )

    async def execute(self, response, name="search_knowledge"):
        workspace = WorkspaceTools(Backend(response))
        tool = next(tool for tool in workspace.tools if tool.spec.name == name)
        args = {"query": "TAIL_EVIDENCE", "limit": 5}
        if name == "read_document":
            args["document_id"] = identifier(12)
        result = await tool.execute(self.ctx, args)
        return workspace, result

    def assert_excerpt(self, item, shown):
        start = shown["excerpt_start_byte"] - item["start_byte"]
        end = shown["excerpt_end_byte"] - item["start_byte"]
        self.assertGreaterEqual(start, 0)
        self.assertGreaterEqual(end, start)
        self.assertLessEqual(end, item["end_byte"] - item["start_byte"])
        self.assertEqual(item["content"].encode("utf-8")[start:end].decode("utf-8"), shown["content"])
        self.assertEqual(shown["start_byte"], item["start_byte"])
        self.assertEqual(shown["end_byte"], item["end_byte"])
        self.assertEqual(shown["content_is_excerpt"], shown["content"] != item["content"])

    async def test_five_tail_matches_survive_search_and_document_display_budget(self):
        for name in ("search_knowledge", "read_document"):
            with self.subTest(tool=name):
                items = []
                spans = {}
                for number in range(1, 6):
                    marker = f"TAIL_EVIDENCE_{number}"
                    content = "中🙂é" * 1350 + marker + "末尾" * 10
                    item = evidence(number, content, 123 + number * 20000)
                    items.append(item)
                    spans[item["evidence_id"]] = focus(item, 4000, len(content))
                response = {"evidence": items, "evidence_focus": spans}
                if name == "read_document":
                    response.update({
                        "document": {"id": identifier(12), "workspace_id": identifier(11)},
                        "version": {"id": identifier(13), "document_id": identifier(12)},
                        "index": {"id": identifier(14), "document_id": identifier(12), "version_id": identifier(13)},
                    })
                original = copy.deepcopy(response)
                workspace, result = await self.execute(response, name)
                self.assertTrue(result.success, result.error)
                self.assertLessEqual(len(result.output), 14000)
                shown = json.loads(result.output)["evidence"]
                self.assertEqual(len(shown), 5)
                for number, (item, entry) in enumerate(zip(items, shown), 1):
                    self.assertIn(f"TAIL_EVIDENCE_{number}", entry["content"])
                    self.assertTrue(entry["content_is_excerpt"])
                    self.assert_excerpt(item, entry)
                    acquired = workspace.registry.require(self.ctx, item["evidence_id"])
                    self.assertEqual(acquired.content, item["content"])
                    self.assertEqual(acquired.sha256, item["sha256"])
                    self.assertEqual(acquired.start_byte, item["start_byte"])
                    self.assertEqual(acquired.end_byte, item["end_byte"])
                    self.assertNotIn("evidence_focus", acquired.to_dict())
                self.assertFalse("evidence_focus" in result.output, "internal focus leaked into model output")
                self.assertEqual(response, original)

    async def test_windows_preserve_matches_near_both_edges_and_middle(self):
        items, spans = [], {}
        marker = "精确🙂匹配"
        for number, offset in enumerate((0, 100, 2048, 3990, 4096 - len(marker)), 1):
            item = evidence(number, "字" * offset + marker + "字" * (4096 - offset - len(marker)))
            items.append(item)
            spans[item["evidence_id"]] = focus(item, offset, offset + len(marker))
        _, result = await self.execute({"evidence": items, "evidence_focus": spans})
        self.assertTrue(result.success, result.error)
        self.assertLessEqual(len(result.output), 14000)
        for item, entry in zip(items, json.loads(result.output)["evidence"]):
            self.assertTrue(entry["content_is_excerpt"])
            self.assertIn(marker, entry["content"])
            self.assert_excerpt(item, entry)

    async def test_missing_focus_preserves_prefix_and_exact_offsets(self):
        items = [evidence(number, "中🙂é" * 1365) for number in range(1, 6)]
        _, result = await self.execute({"evidence": items})
        self.assertTrue(result.success, result.error)
        for item, entry in zip(items, json.loads(result.output)["evidence"]):
            self.assertTrue(entry["content_is_excerpt"])
            self.assertTrue(item["content"].startswith(entry["content"]))
            self.assertEqual(entry["excerpt_start_byte"], item["start_byte"])
            self.assert_excerpt(item, entry)

    async def test_untruncated_parent_and_partial_focus_map(self):
        items = [evidence(1, "前🙂中é末"), evidence(2, "第二个父块")]
        _, result = await self.execute({"evidence": items, "evidence_focus": {
            items[0]["evidence_id"]: focus(items[0], 1, 3)}})
        self.assertTrue(result.success, result.error)
        for item, entry in zip(items, json.loads(result.output)["evidence"]):
            self.assertEqual(entry["content"], item["content"])
            self.assertFalse(entry["content_is_excerpt"])
            self.assert_excerpt(item, entry)

    async def test_focus_larger_than_window_remains_bounded(self):
        items = [evidence(number, "字" * 4096) for number in range(1, 6)]
        _, result = await self.execute({"evidence": items, "evidence_focus": {
            item["evidence_id"]: focus(item, 500, 4000) for item in items}})
        self.assertTrue(result.success, result.error)
        self.assertLessEqual(len(result.output), 14000)
        for item, entry in zip(items, json.loads(result.output)["evidence"]):
            self.assertTrue(entry["content_is_excerpt"])
            self.assert_excerpt(item, entry)

    async def test_invalid_focus_fails_before_registry_acquisition(self):
        item = evidence(1, "A中🙂Z")
        eid, start, end = item["evidence_id"], item["start_byte"], item["end_byte"]
        invalid = [None, [], "private-invalid-focus", {identifier(99): focus(item, 1, 2)}]
        for value in (
            None, [], {}, {"start_byte": start}, {"end_byte": end},
            {"start_byte": start, "end_byte": end, "content": "injected"},
            {"start_byte": True, "end_byte": end},
            {"start_byte": start, "end_byte": float(end)},
            {"start_byte": str(start), "end_byte": end},
            {"start_byte": start - 1, "end_byte": end},
            {"start_byte": start, "end_byte": end + 1},
            {"start_byte": start, "end_byte": start},
            {"start_byte": end, "end_byte": start},
            {"start_byte": start + 2, "end_byte": end},
            {"start_byte": start, "end_byte": start + 6},
        ):
            invalid.append({eid: value})
        for value in invalid:
            with self.subTest(focus=value):
                workspace, result = await self.execute({"evidence": [item], "evidence_focus": value})
                self.assertFalse(result.success)
                self.assertEqual(result.error, "invalid or untrusted tool data")
                with self.assertRaises(EvidenceError):
                    workspace.registry.require(self.ctx, eid)

    async def test_focus_cannot_extend_strict_evidence_dto(self):
        item = evidence(1, "正文")
        item["evidence_focus"] = focus(item, 0, 1)
        workspace, result = await self.execute({"evidence": [item]})
        self.assertFalse(result.success)
        self.assertEqual(result.error, "invalid or untrusted tool data")
        with self.assertRaises(EvidenceError):
            workspace.registry.require(self.ctx, item["evidence_id"])


class ParentRestoreTransportTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.raw = b'{"evidence":[]}'
        self.requests = []
        test = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                test.requests.append(self.rfile.read(int(self.headers["Content-Length"])))
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(test.raw)))
                self.end_headers()
                try:
                    self.wfile.write(test.raw)
                except (BrokenPipeError, ConnectionResetError):
                    pass

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.addCleanup(server.server_close)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addAsyncCleanup(asyncio.to_thread, server.shutdown)
        self.backend = TransportBackend({
            "url": f"http://127.0.0.1:{server.server_port}/tools", "token": "a" * 64})
        await self.backend.__aenter__()
        self.addAsyncCleanup(self.backend.__aexit__, None, None, None)
        self.ctx = ToolContext(
            caller=Caller(workspace_id=identifier(11), user_id=identifier(16), role="owner"),
            scope=ResourceScope(workspace_id=identifier(11), document_ids=frozenset({identifier(12)})),
            session_id=identifier(17), run_id=identifier(10), tool_call_id="restore-test",
        )

    async def test_restores_64_full_parents_with_utf8_and_json_escaped_content(self):
        for content in ("🙂" * 4096, "<>&" * 1365 + "<"):
            with self.subTest(content_kind=content[0]):
                items = [evidence(number, content, (number - 1) * len(content.encode("utf-8")))
                         for number in range(1, 65)]
                for item in items:
                    item["title"] = "<" * 255
                # Go's encoder escapes HTML characters but emits non-BMP UTF-8.
                encoded = json.dumps({"evidence": items}, ensure_ascii=False, separators=(",", ":"))
                self.raw = encoded.replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026").encode("utf-8")
                self.assertGreater(len(self.raw), 1 << 20)
                self.assertLessEqual(len(self.raw), MAX_RESPONSE)
                workspace = WorkspaceTools(self.backend)
                await workspace.restore(self.ctx)
                citations = workspace.registry.citations(
                    self.ctx, " ".join(f'[E:{item["evidence_id"]}]' for item in items))
                self.assertEqual(len(citations), 64)
                for item, restored in zip(items, citations):
                    self.assertEqual(restored.content, content)
                    self.assertEqual(restored.start_byte, item["start_byte"])
                    self.assertEqual(restored.end_byte, item["end_byte"])
                    self.assertEqual(restored.sha256, item["sha256"])
                self.assertEqual(json.loads(self.requests[-1]), {"tool": "restore_evidence", "arguments": {}})

    async def test_response_limit_accepts_exact_boundary_and_rejects_one_extra_byte(self):
        self.assertEqual(MAX_RESPONSE, 2 << 20)
        self.raw += b" " * (MAX_RESPONSE - len(self.raw))
        workspace = WorkspaceTools(self.backend)
        await workspace.restore(self.ctx)
        self.raw += b" "
        with self.assertRaisesRegex(RetrievalError, "^retrieval result exceeds limit$"):
            await workspace.restore(self.ctx)

    async def test_request_limit_stays_256_kib_and_rejects_before_network(self):
        self.assertEqual(MAX_REQUEST, 256 << 10)
        with self.assertRaisesRegex(RetrievalError, "^invalid retrieval request$"):
            await self.backend.call("restore_evidence", {"padding": "x" * MAX_REQUEST})
        self.assertEqual(self.requests, [])


if __name__ == "__main__":
    unittest.main()
