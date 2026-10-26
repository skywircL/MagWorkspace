"""Filter native Harness SSE frames into the bridge's private NDJSON protocol."""

from __future__ import annotations

import json
import sys

MAX_FRAME_BYTES = 1 << 20
MAX_RECORD_BYTES = 2 << 20
MAX_STREAM_BYTES = 8 << 20
MAX_TEXT_BYTES = 64 << 10


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate event field")
        result[key] = value
    return result


def _invalid_constant(value):
    raise ValueError("invalid event number")


class StreamOutput:
    """Validate identity and response ordering before exposing provisional text.

    Harness rounds count tool iterations and may repeat after an empty response.
    Public rounds instead identify visible attempts; native response IDs remain
    bound to their original native round throughout each attempt.
    """

    def __init__(self, request: dict):
        self.run_id = request["run_id"]
        self.session_id = request["session_id"]
        self.round = 0
        self.total = 0
        self.native_total = 0
        self.round_bytes = 0
        self.native_round = None
        self.active_response = None
        self.response_round = None
        self.response_ids = set()
        self.started = False
        self.final_answer_seen = False
        self.terminal = None
        self.tools = "retrieval" in request or "memory" in request
        self.recovering = bool(request.get("recover"))
        self.resuming = "resume" in request or self.recovering

    def record(self, value: dict) -> None:
        encoded = json.dumps(
            value, ensure_ascii=False, allow_nan=False, separators=(",", ":")
        ).encode("utf-8")
        self.total += len(encoded) + 1
        if len(encoded) > MAX_RECORD_BYTES or self.total > MAX_STREAM_BYTES:
            raise ValueError("stream output exceeds limit")
        sys.stdout.buffer.write(encoded + b"\n")
        sys.stdout.buffer.flush()

    def _event(self, kind: str, delta: str = "") -> None:
        event = {
            "type": kind,
            "run_id": self.run_id,
            "session_id": self.session_id,
            "round": self.round,
        }
        if delta:
            event["delta"] = delta
        self.record({"event": event})

    def _valid_native_round(self, native_round) -> bool:
        if type(native_round) is not int or native_round < 0:
            return False
        if self.native_round is None:
            return native_round == 0 or self.resuming
        return native_round in (self.native_round, self.native_round + 1)

    def accept(self, frame: bytes) -> None:
        # This is the exact encode_sse_event contract, not a provider SSE parser.
        from magent_harness.events import EventType

        if not isinstance(frame, bytes) or len(frame) > MAX_FRAME_BYTES:
            raise ValueError("invalid native frame")
        self.native_total += len(frame)
        if self.native_total > MAX_STREAM_BYTES:
            raise ValueError("native stream exceeds limit")
        if frame == b": keep-alive\n\n":
            return
        lines = frame.decode("utf-8").split("\n")
        if (
            len(lines) != 4
            or not lines[0].startswith("event: ")
            or not lines[1].startswith("data: ")
            or lines[2:] != ["", ""]
        ):
            raise ValueError("invalid native frame encoding")
        kind = EventType(lines[0][7:]).value
        envelope = json.loads(
            lines[1][6:],
            object_pairs_hook=_unique_object,
            parse_constant=_invalid_constant,
        )
        if (
            not isinstance(envelope, dict)
            or set(envelope) != {"type", "run_id", "session_id", "data"}
            or envelope["type"] != kind
            or envelope["run_id"] != self.run_id
            or envelope["session_id"] != self.session_id
            or not isinstance(envelope["data"], dict)
            or self.terminal is not None
        ):
            raise ValueError("invalid native event identity or ordering")
        data = envelope["data"]
        if kind in ("run_started", "run_resumed"):
            if (kind == "run_resumed") != self.resuming:
                raise ValueError("wrong native start kind")
            if self.started:
                raise ValueError("duplicate native run start")
            self.started = True
            return
        if not self.started:
            raise ValueError("native event before run start")
        if kind == "round_started":
            native_round = data.get("round")
            if (
                not self._valid_native_round(native_round)
                or self.active_response is not None
                or self.final_answer_seen
            ):
                raise ValueError("invalid native round")
            self.native_round = native_round
        elif kind == "model_response_started":
            response_id = data.get("response_id")
            native_round = data.get("round")
            if (
                not isinstance(response_id, str)
                or not response_id.strip()
                or len(response_id.encode("utf-8")) > 256
                or response_id in self.response_ids
                or not self._valid_native_round(native_round)
                or self.active_response is not None
                or self.final_answer_seen
            ):
                raise ValueError("invalid native response start")
            # Final synthesis calls the model without round_started, including
            # recovery at the iteration limit. Apply the same monotonic bound;
            # response IDs still bind every delta/completion to this exact round.
            self.native_round = native_round
            self.round += 1
            if self.round > 32:
                raise ValueError("too many generation attempts")
            self.round_bytes = 0
            self.active_response = response_id
            self.response_round = native_round
            self.response_ids.add(response_id)
            self._event("message.start")
        elif kind in (
            "text_delta",
            "model_response_completed",
            "model_response_discarded",
        ):
            if (
                self.active_response is None
                or data.get("response_id") != self.active_response
                or type(data.get("round")) is not int
                or data["round"] != self.response_round
            ):
                raise ValueError("invalid native response identity")
            if kind == "text_delta":
                content = data.get("content")
                if not isinstance(content, str) or not content:
                    raise ValueError("invalid native text delta")
                self.round_bytes += len(content.encode("utf-8"))
                if self.round_bytes > MAX_TEXT_BYTES:
                    raise ValueError("generation text exceeds limit")
                self._event("message.delta", content)
                return
            if kind == "model_response_completed":
                if self.tools and data.get("kind") == "tool" and data.get("finish_reason") == "tool_calls":
                    # Any provisional words preceding a tool call were planning,
                    # not the final answer. Keep tools and their arguments private.
                    self._event("message.discard")
                elif data.get("kind") != "answer" or data.get("finish_reason") not in (
                    "stop", "length",
                ):
                    raise ValueError("unexpected native response completion")
            else:
                if data.get("reason") not in (
                    "empty",
                    "error",
                    "canceled",
                    "length",
                    "content_filter",
                ):
                    raise ValueError("invalid native discard")
                self._event("message.discard")
            self.active_response = None
            self.response_round = None
        elif kind == "final_answer":
            if self.active_response is not None or self.final_answer_seen:
                raise ValueError("invalid native final answer order")
            self.final_answer_seen = True
        elif kind in ("run_completed", "run_failed", "run_canceled", "run_suspended"):
            if kind == "run_completed" and (
                self.active_response is not None or not self.final_answer_seen
            ):
                raise ValueError("native run completed before final answer")
            self.terminal = kind
        # Other native events stay private, including prompts, query, thoughts,
        # context summaries, tools, and errors. Their envelope IDs are still checked.

    def finish(self, result: dict) -> None:
        if self.recovering and not self.started and self.terminal is None and self.round == 0 and result.get("status") in ("completed", "suspended"):
            # Recovering committed terminal/suspended facts need not call a
            # model or emit a fresh generation. The result remains authoritative.
            self.record({"result": result})
            return
        if result.get("status") == "suspended":
            if self.terminal != "run_suspended" or self.final_answer_seen or self.active_response is not None:
                raise ValueError("native stream did not suspend")
            self.record({"result": result})
            return
        if (
            self.terminal != "run_completed"
            or not self.final_answer_seen
            or self.round == 0
        ):
            raise ValueError("native stream did not complete")
        self.record({"result": result})
