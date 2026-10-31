"""Deterministic query planning/routing contracts; no provider or network."""
import asyncio
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "bridges/magent"), str(Path(os.environ.get(
    "MAGENT_TEST_HARNESS_PATH", ROOT.parent / "Magent Harness")) / "src")]

from magent_harness.messages import Message, ToolCall
from magent_harness.model import ChatResponse, ModelError
from magent_harness.tools import ToolSpec
from magent_harness.context import ContextManager, ContextSettings, TokenEstimator
from query_understanding import (
    MAX_HISTORY_BYTES, PLAN_KIND, PROMPT_MARKER, Plan, QueryRoutingModel, QuerySummaryModel,
    plan_message, restore_plan, routing_prompt, search_was_started, understand,
)


def history(*pairs):
    return [message for user, assistant in pairs for message in (
        {"role": "user", "content": user}, {"role": "assistant", "content": assistant},
    )]


def plan(query="它有哪些约束？", rewrite="RAG 有哪些约束？", intent="kb_search"):
    return Plan(hashlib.sha256(query.encode()).hexdigest(), rewrite, intent)


class Model:
    def __init__(self, response=None, error=None):
        self.response = response or ChatResponse(content=json.dumps({
            "rewrite_query": "RAG 有哪些约束？", "intent": "kb_search",
        }))
        self.error = error
        self.calls = []

    async def complete(self, messages, tools, *, max_tokens=None):
        self.calls.append((messages, tools, max_tokens))
        if self.error is not None:
            raise self.error
        return self.response

    async def complete_stream(self, messages, tools, *, on_delta, max_tokens=None):
        response = await self.complete(messages, tools, max_tokens=max_tokens)
        if response.content:
            on_delta(response.content)
        return response


class UnderstandingTests(unittest.IsolatedAsyncioTestCase):
    async def test_complete_recent_rounds_and_model_contract(self):
        model = Model()
        source = history(*[(f"question {i}", f"answer {i}") for i in range(7)])
        result = await understand(model, "它有哪些约束？", source)
        self.assertEqual(result, plan())
        messages, tools, max_tokens = model.calls[0]
        self.assertEqual(tools, [])
        self.assertEqual(max_tokens, 512)
        self.assertTrue(messages[0].content.startswith(PROMPT_MARKER))
        self.assertEqual(json.loads(messages[1].content), {
            "query": "它有哪些约束？", "history": source[-10:],
        })
        model = Model()
        await understand(model, "它有哪些约束？", source, max_rounds=2)
        self.assertEqual(json.loads(model.calls[0][0][1].content)["history"], source[-4:])
        model = Model()
        await understand(model, "它有哪些约束？", source, max_rounds=50)
        self.assertEqual(len(json.loads(model.calls[0][0][1].content)["history"]), 10)

    async def test_history_budget_keeps_pairs_and_stops_at_oversized_pair(self):
        for middle in ("中" * 6000, "\\" * 9000):
            with self.subTest(text=middle[:1]):
                model = Model()
                source = history(("old small", "must not select"), ("large", middle),
                                 ("recent subject", "recent answer"))
                await understand(model, "more details", source)
                selected = json.loads(model.calls[0][0][1].content)["history"]
                self.assertEqual(selected, source[-2:])
                self.assertLessEqual(len(json.dumps(selected, ensure_ascii=False,
                                                    separators=(",", ":")).encode()), MAX_HISTORY_BYTES)
        model = Model()
        self.assertIsNone(await understand(model, "more details", history(
            ("old small", "not selected"), ("large", "中" * 6000))))
        self.assertEqual(model.calls, [])

    async def test_no_complete_contiguous_history_does_not_call_model(self):
        cases = [[], [{"role": "assistant", "content": "orphan"}],
                 history(("question", "answer")) + [{"role": "user", "content": "incomplete"}],
                 history(("", "answer")), history(("question", "  "))]
        for source in cases:
            with self.subTest(history=source):
                model = Model()
                self.assertIsNone(await understand(model, "current question", source))
                self.assertEqual(model.calls, [])
        source = history(("old", "answer")) + [{"role": "assistant", "content": "gap"}] + history(("recent", "answer"))
        model = Model()
        await understand(model, "current question", source)
        self.assertEqual(json.loads(model.calls[0][0][1].content)["history"], source[-2:])

    async def test_query_budget_uses_utf8_and_does_not_truncate(self):
        for query in ("x" * 4097, "中" * 1366, "", "\ud800"):
            model = Model()
            self.assertIsNone(await understand(model, query, history(("topic", "answer"))))
            self.assertEqual(model.calls, [])
        model = Model()
        query = "中" * 1365 + "a"
        result = await understand(model, query, history(("topic", "answer")))
        self.assertEqual(result.original_query_sha256, hashlib.sha256(query.encode()).hexdigest())
        self.assertEqual(json.loads(model.calls[0][0][1].content)["query"], query)

    async def test_old_citation_markers_are_redacted_without_mutating_history(self):
        marker = "a" * 32
        source = history((f"explain [E:{marker}]", f"old [historical evidence:{marker}]"))
        original = json.dumps(source)
        model = Model()
        await understand(model, "more details", source)
        content = model.calls[0][0][1].content
        self.assertNotIn(marker, content)
        self.assertEqual(content.count("[historical source; retrieve again]"), 2)
        self.assertEqual(json.dumps(source), original)

    async def test_strict_results_and_invalid_query_shapes_fall_back(self):
        invalid = ["not json", "```json\n{}\n```", "[]", "null",
                   '{"rewrite_query":"okay","intent":"kb_search","intent":"follow_up"}',
                   '{"rewrite_query":"okay","intent":"kb_search","extra":true}',
                   '{"rewrite_query":"okay"}',
                   json.dumps({"rewrite_query": "okay", "intent": "unknown"}),
                   json.dumps({"rewrite_query": "okay", "intent": []}),
                   json.dumps({"rewrite_query": "okay", "intent": "kb_search", "score": float("nan")})]
        for rewrite in (None, 1, "", " ", "x", "中", "x" * 513, "中" * 171,
                        "bad\nquery", "bad\tquery", "bad\x7fquery", "bad\u2028query", "\ud800"):
            invalid.append(json.dumps({"rewrite_query": rewrite, "intent": "kb_search"}))
        for raw in invalid:
            with self.subTest(raw=raw[:100]):
                model = Model(ChatResponse(content=raw))
                self.assertIsNone(await understand(model, "more details", history(("topic", "answer"))))
        for intent in ("kb_search", "follow_up", "other"):
            model = Model(ChatResponse(content=json.dumps({"rewrite_query": "中" * 170, "intent": intent})))
            self.assertEqual((await understand(model, "more details", history(("topic", "answer")))).intent, intent)

    async def test_nonstop_tool_and_oversized_responses_fall_back(self):
        valid = '{"rewrite_query":"more details","intent":"kb_search"}'
        cases = [ChatResponse(content=valid, finish_reason=value)
                 for value in ("length", "content_filter", "tool_calls", "unknown", None)]
        cases += [ChatResponse(content=valid, tool_calls=[ToolCall("bad", "bad", "{}")]),
                  ChatResponse(content=" " * 4096 + valid)]
        for response in cases:
            with self.subTest(response=response):
                self.assertIsNone(await understand(Model(response), "more details", history(("topic", "answer"))))

    async def test_excessively_nested_json_falls_back_on_supported_python_versions(self):
        # Python 3.11 and 3.13 have different decoder depth limits. Both must
        # treat a decoder recursion failure as an invalid bounded result.
        with patch("query_understanding.strict_json", side_effect=RecursionError):
            self.assertIsNone(await understand(Model(), "more details", history(("topic", "answer"))))

    async def test_model_error_timeout_and_cancellation(self):
        self.assertIsNone(await understand(Model(error=ModelError("private provider error")),
                                           "more details", history(("topic", "answer"))))
        cancelled = asyncio.Event()

        class WaitingModel:
            async def complete(self, *args, **kwargs):
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelled.set()

        self.assertIsNone(await understand(WaitingModel(), "more details", history(("topic", "answer")), timeout_s=0.01))
        self.assertTrue(cancelled.is_set())
        with self.assertRaises(asyncio.CancelledError):
            await understand(Model(error=asyncio.CancelledError()), "more details", history(("topic", "answer")))
        with self.assertRaises(RuntimeError):
            await understand(Model(error=RuntimeError("coding failure")), "more details", history(("topic", "answer")))


class PlanPersistenceTests(unittest.TestCase):
    def test_round_trip_binds_original_query_and_requires_private_kind(self):
        value = plan()
        message = plan_message(value)
        self.assertEqual(message.role, "user")
        self.assertEqual(message.kind, PLAN_KIND)
        self.assertEqual(restore_plan([message], "它有哪些约束？"), value)
        self.assertIsNone(restore_plan([replace(message, kind="")], "它有哪些约束？"))
        with self.assertRaises(ValueError):
            restore_plan([message], "a different query")
        with self.assertRaises(ValueError):
            restore_plan([message, message], "它有哪些约束？")

    def test_malformed_markers_are_not_silently_ignored(self):
        original = plan_message(plan())
        mutations = [replace(original, content="{"), replace(original, content="null"),
                     replace(original, content=1), replace(original, role="system"),
                     replace(original, tool_call_id="x"), replace(original, name="x"),
                     replace(original, reasoning_content=""),
                     replace(original, tool_calls=(ToolCall("x", "x", "{}"),)),
                     replace(original, content=" " * 4097),
                     replace(original, content=original.content.replace('"version":1', '"version":1,"version":1'))]
        source = json.loads(original.content)
        for field, value in (("version", True), ("version", 2), ("original_query_sha256", "invalid"),
                             ("rewrite_query", "a"), ("intent", "unknown"), ("extra", 1)):
            mutations.append(replace(original, content=json.dumps({**source, field: value})))
        for message in mutations:
            with self.subTest(message=message):
                with self.assertRaises(ValueError):
                    restore_plan([message], "它有哪些约束？")

    def test_recovery_distinguishes_created_pending_and_committed_search(self):
        self.assertFalse(search_was_started(None, "run"))
        state = SimpleNamespace(steps=[], pending=None)
        self.assertFalse(search_was_started(state, "run"))

        call = ToolCall("query-run", "search_knowledge", "{}")
        state.pending = SimpleNamespace(tool_calls=[call])
        self.assertTrue(search_was_started(state, "run"))
        state.pending = None
        state.steps = [SimpleNamespace(tool_calls=[call])]
        self.assertTrue(search_was_started(state, "run"))
        self.assertFalse(search_was_started(state, "other-run"))
        state.steps = [SimpleNamespace(tool_calls=[replace(call, name="read_document")])]
        self.assertFalse(search_was_started(state, "run"))

    def test_decoder_depth_failure_is_an_invalid_persisted_plan(self):
        with patch("query_understanding.strict_json", side_effect=RecursionError):
            with self.assertRaises(ValueError):
                restore_plan([plan_message(plan())], "它有哪些约束？")


class RoutingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        from workspace_tools import specifications
        self.tools = list(specifications())
        self.messages = [Message(role="system", content="base instructions"),
                         plan_message(plan()), Message(role="user", content="它有哪些约束？")]

    def search_response(self, args=None, *, identity="provider-search", finish="tool_calls"):
        return ChatResponse(tool_calls=[ToolCall(identity, "search_knowledge", json.dumps(
            args if args is not None else {"query": "它有哪些约束？"}, ensure_ascii=False,
        ))], finish_reason=finish, reasoning_content="private provider state")

    async def test_discovery_precedes_first_search_and_preserves_selected_arguments(self):
        discover = ChatResponse(tool_calls=[ToolCall("discover", "list_knowledge_bases", "{}")], finish_reason="tool_calls")
        model = Model(discover)
        router = QueryRoutingModel(model, plan(), "run")
        self.assertIs(await router.complete(self.messages, self.tools, max_tokens=100), discover)
        self.assertEqual(len(model.calls), 1)
        args = {"query": "它有哪些约束？", "knowledge_base_ids": ["a" * 32], "mode": "keyword", "limit": 2}
        original = self.search_response(args)
        model.response = original
        response = await router.complete(self.messages, self.tools, max_tokens=101)
        self.assertEqual(response.tool_calls[0].id, "provider-search")
        self.assertEqual(json.loads(response.tool_calls[0].arguments), {**args, "query": "RAG 有哪些约束？"})
        self.assertEqual(json.loads(original.tool_calls[0].arguments), args)
        self.assertEqual(response.reasoning_content, original.reasoning_content)
        self.assertIs(response.usage, original.usage)
        self.assertEqual(model.calls[-1][2], 101)
        model.response = self.search_response({"query": "additional evidence"}, identity="later")
        self.assertIs(await router.complete(self.messages, self.tools), model.response)

    async def test_streaming_waits_for_real_tools_and_preserves_empty_scope(self):
        model = Model(ChatResponse(content="I will inspect the selected libraries."))
        router = QueryRoutingModel(model, plan(), "run")
        deltas = []
        response = await router.complete_stream(self.messages, self.tools, on_delta=deltas.append)
        self.assertEqual(response.content, model.response.content)
        self.assertEqual(deltas, [model.response.content])
        model.response = self.search_response({"query": "follow-up", "knowledge_base_ids": [], "mode": "semantic", "limit": 10})
        response = await router.complete_stream(self.messages, self.tools, on_delta=deltas.append, max_tokens=33)
        self.assertEqual(len(deltas), 1)
        self.assertEqual(json.loads(response.tool_calls[0].arguments), {
            "query": "RAG 有哪些约束？", "knowledge_base_ids": [], "mode": "semantic", "limit": 10,
        })
        self.assertEqual(model.calls[-1][2], 33)

    async def test_follow_up_and_other_keep_tools_without_rewriting(self):
        for intent in ("follow_up", "other"):
            model = Model(self.search_response())
            value = plan(intent=intent)
            router = QueryRoutingModel(model, value, "run")
            response = await router.complete(self.messages, self.tools)
            self.assertIs(response, model.response)
            self.assertEqual(model.calls[0][1], self.tools)
            projection = model.calls[0][0]
            self.assertFalse(any(message.kind == PLAN_KIND for message in projection))
            self.assertEqual([message.content for message in projection if message.role == "user"], ["它有哪些约束？"])
            self.assertEqual(projection, [self.messages[0], self.messages[2]])
            self.assertIn('"intent":"' + intent + '"', routing_prompt(value))

    async def test_projection_preserves_input_and_plan_is_not_user_text(self):
        model = Model(ChatResponse(content="answer"))
        router = QueryRoutingModel(model, plan(), "run", search_started=True)
        original = list(self.messages)
        await router.complete(self.messages, self.tools)
        self.assertEqual(self.messages, original)
        self.assertEqual(self.messages[1].kind, PLAN_KIND)
        projected = model.calls[0][0]
        self.assertNotIn(self.messages[1], projected)
        self.assertEqual(projected, [self.messages[0], self.messages[2]])
        self.assertLess(TokenEstimator().messages(projected), TokenEstimator().messages(self.messages))

    async def test_routing_guidance_is_budgeted_by_caller_not_appended_at_provider(self):
        value = plan(intent="follow_up")
        guidance = routing_prompt(value)
        self.assertIn("untrusted", guidance.lower())
        self.assertIn("knowledge_base_ids", guidance)
        self.assertIn('"rewrite_query":"RAG 有哪些约束？"', guidance)
        self.assertIn('"intent":"follow_up"', guidance)
        self.assertNotIn(value.original_query_sha256, guidance)
        messages = [replace(self.messages[0], content="base instructions\n" + guidance), self.messages[2]]
        model = Model(ChatResponse(content="answer"))
        router = QueryRoutingModel(model, value, "run")
        await router.complete(messages, self.tools)
        self.assertEqual(model.calls[0][0], messages)
        self.assertEqual(TokenEstimator().messages(model.calls[0][0]), TokenEstimator().messages(messages))

    async def test_recovery_and_model_retry_do_not_rewrite_committed_calls(self):
        model = Model(error=ModelError("temporary"))
        router = QueryRoutingModel(model, plan(), "run")
        with self.assertRaises(ModelError):
            await router.complete(self.messages, self.tools)
        model.error = None
        model.response = self.search_response()
        rewritten = await router.complete(self.messages, self.tools)
        state = SimpleNamespace(steps=[], pending=SimpleNamespace(tool_calls=rewritten.tool_calls))
        self.assertTrue(search_was_started(state, "run", plan()))
        unchanged = SimpleNamespace(steps=[], pending=SimpleNamespace(tool_calls=self.search_response().tool_calls))
        self.assertFalse(search_was_started(unchanged, "run", plan()))
        model = Model(self.search_response({"query": "independent next query"}))
        resumed = QueryRoutingModel(model, plan(), "run", search_started=search_was_started(state, "run", plan()))
        self.assertIs(await resumed.complete(self.messages, self.tools), model.response)
        self.assertEqual(json.loads(rewritten.tool_calls[0].arguments)["query"], "RAG 有哪些约束？")
        self.assertEqual(len(model.calls), 1)

    async def test_missing_search_capability_only_projects_and_never_injects_tools(self):
        model = Model(self.search_response())
        router = QueryRoutingModel(model, plan(), "run")
        self.assertIs(await router.complete(self.messages, []), model.response)
        self.assertFalse(any(m.kind == PLAN_KIND for m in model.calls[0][0]))
        model.response = ChatResponse(content="answer")
        self.assertIs(await router.complete(self.messages, self.tools), model.response)
        self.assertEqual(len(model.calls), 2)

    async def test_invalid_partial_and_non_search_calls_do_not_consume_rewrite(self):
        invalid = ['{"query":"ok","query":"duplicate"}', '{"query":"ok","knowledge_base_ids":null}',
                   '{"query":"ok","knowledge_base_ids":["bad"]}', '{"query":"ok","limit":0}',
                   '{"query":"ok","mode":"sql"}', '{"query":"ok","user_id":"forged"}',
                   '{"query":"x"}', '{"query":', '[]', 'null']
        model = Model()
        router = QueryRoutingModel(model, plan(), "run")
        for raw in invalid:
            model.response = ChatResponse(tool_calls=[ToolCall("invalid", "search_knowledge", raw)], finish_reason="tool_calls")
            self.assertIs(await router.complete(self.messages, self.tools), model.response)
            self.assertFalse(search_was_started(SimpleNamespace(steps=[], pending=SimpleNamespace(tool_calls=model.response.tool_calls)), "run"))
        for finish in ("length", "content_filter", "stop"):
            model.response = self.search_response(finish=finish)
            self.assertIs(await router.complete(self.messages, self.tools), model.response)
        first = self.search_response({"query": "first real query", "knowledge_base_ids": ["b" * 32]}).tool_calls[0]
        second = self.search_response({"query": "keep second query"}, identity="second").tool_calls[0]
        model.response = ChatResponse(tool_calls=[first, second], finish_reason="tool_calls")
        response = await router.complete(self.messages, self.tools)
        self.assertEqual(json.loads(response.tool_calls[0].arguments)["query"], "RAG 有哪些约束？")
        self.assertIs(response.tool_calls[1], second)


class SummaryProjectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_harness_envelope_removes_plan_without_mutating_history(self):
        value = plan()
        source = [
            Message(role="user", content="原始用户问题 <data>"),
            Message(role="assistant", content="已有回答", tool_calls=(ToolCall("old", "read_document", '{"document_id":"one"}'),)),
            Message(role="tool", content="工具正文", name="read_document", tool_call_id="old"),
            plan_message(value),
            Message(role="user", content="它有哪些约束？"),
        ]
        original = list(source)
        settings = ContextSettings(max_context_tokens=10000, reserve_tokens=1000, max_summary_tokens=100)
        raw_model = Model(ChatResponse(content="checkpoint"))
        await ContextManager(settings, TokenEstimator(), raw_model)._summarize("previous checkpoint", source)
        model = Model(ChatResponse(content="checkpoint"))
        wrapper = QuerySummaryModel(model, value)
        result = await ContextManager(settings, TokenEstimator(), wrapper)._summarize("previous checkpoint", source)
        self.assertEqual(result, "checkpoint")
        self.assertEqual(source, original)
        received, tools, max_tokens = model.calls[0]
        expected = json.loads(raw_model.calls[0][0][1].content)
        expected["conversation"] = [record for record in expected["conversation"]
                                    if record["content"] != plan_message(value).content]
        self.assertEqual(len(received), 2)
        self.assertEqual(received[0], raw_model.calls[0][0][0])
        self.assertEqual(json.loads(received[1].content), expected)
        self.assertEqual(tools, [])
        self.assertEqual(max_tokens, raw_model.calls[0][2])
        self.assertNotIn(value.original_query_sha256, received[1].content)
        self.assertNotIn("original_query_sha256", received[1].content)
        self.assertNotIn("intent", received[1].content)
        self.assertNotIn("<data>", received[1].content)
        self.assertIn("\\u003cdata\\u003e", received[1].content)
        self.assertLess(TokenEstimator().messages(received), TokenEstimator().messages(raw_model.calls[0][0]))

    async def test_only_exact_user_plan_record_is_removed(self):
        value = plan()
        private = plan_message(value).content
        records = [{"role": "user", "content": private},
                   {"role": "assistant", "content": private},
                   {"role": "user", "content": private + " "},
                   {"role": "tool", "content": private, "name": "read_document", "tool_call_id": "old"},
                   {"role": "user", "content": "ordinary history", "content_omitted_chars": 10}]
        envelope = {"conversation": records, "previous_summary": "earlier", "omitted_messages": 3,
                    "previous_summary_omitted": True}
        messages = [Message(role="system", content="summary instruction"),
                    Message(role="user", content=json.dumps(envelope))]
        original = list(messages)
        model = Model(ChatResponse(content="checkpoint"))
        wrapper = QuerySummaryModel(model, value)
        deltas = []
        await wrapper.complete_stream(messages, [], on_delta=deltas.append, max_tokens=17)
        self.assertEqual(messages, original)
        self.assertEqual(deltas, ["checkpoint"])
        self.assertEqual(model.calls[0][2], 17)
        self.assertEqual(json.loads(model.calls[0][0][1].content), {**envelope, "conversation": records[1:]})

    async def test_non_envelope_requests_and_envelopes_without_plan_are_unchanged(self):
        value = plan()
        envelope = {"conversation": [{"role": "user", "content": "ordinary history"}],
                    "previous_summary": "", "omitted_messages": 0, "previous_summary_omitted": False}
        invalid = ["plain text", "{", json.dumps(envelope), json.dumps({**envelope, "extra": 1}),
                   json.dumps({**envelope, "omitted_messages": True}),
                   json.dumps({**envelope, "previous_summary_omitted": 0}),
                   json.dumps({**envelope, "conversation": ["not a record"]})]
        for content in invalid:
            with self.subTest(content=content):
                messages = [Message(role="system", content="summary"), Message(role="user", content=content)]
                model = Model(ChatResponse(content="checkpoint"))
                await QuerySummaryModel(model, value).complete(messages, [], max_tokens=10)
                self.assertEqual(model.calls[0], (messages, [], 10))
        model = Model(ChatResponse(content="checkpoint"))
        messages = [Message(role="user", content=json.dumps({**envelope,
                    "conversation": [{"role": "user", "content": plan_message(value).content}]}))]
        await QuerySummaryModel(model, value).complete(messages, [])
        self.assertEqual(model.calls[0][0], messages)


if __name__ == "__main__":
    unittest.main()
