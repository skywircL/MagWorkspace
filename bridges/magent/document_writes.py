"""Confirmed document mutations. Go independently checks durable approval."""
from __future__ import annotations

import json

NAMES = frozenset({"create_document", "update_document"})


def tools(backend):
    from magent_harness.tools import FunctionTool, ToolSpec
    from magent_harness.policy import Risk
    from magent_harness.messages import ToolResult
    from retrieval import RetrievalError
    from workspace_tools import WorkspaceTools

    string = {"type": "string"}
    definitions = (
        (
            "create_document",
            "Create a new TXT or Markdown document after the user approves its "
            "exact title, filename and full content. Content is limited to 32768 "
            "UTF-8 bytes. Only available with unrestricted workspace document scope.",
            {"title": string, "filename": string, "content": string},
        ),
        (
            "update_document",
            "Replace a TXT or Markdown document's full text after user approval, "
            "appending an immutable version. Read every read_document source=true "
            "page first and provide its current base_version_id. Preserve unrelated "
            "content; do not replace a document from a partial excerpt. Original "
            "and new content are limited to 32768 UTF-8 bytes. A version conflict "
            "requires a new proposal and approval.",
            {"document_id": string, "base_version_id": string, "content": string},
        ),
    )

    def handler(name):
        async def execute(ctx, args):
            if len(args["content"].encode("utf-8")) > 32768:
                return ToolResult.fail("document content exceeds 32768 UTF-8 bytes")
            try:
                result = await backend.call(name, args, tool_call_id=ctx.tool_call_id)
                if not isinstance(result, dict):
                    raise ValueError("invalid write receipt")
                # Match read tools' model-visible metadata: no caller IDs, object
                # paths or workspace authority fields in the model transcript.
                visible = WorkspaceTools._metadata(ctx, result)
                return ToolResult.ok(
                    json.dumps(visible, ensure_ascii=False),
                    data={"document_write": visible},
                )
            except RetrievalError as exc:
                code = str(exc)
                if code in {"write_conflict", "not_found", "forbidden", "invalid_arguments", "approval_required"}:
                    return ToolResult.fail(
                        code + "; write not confirmed; inspect the target before requesting fresh approval"
                    )
            except Exception:
                # Callback or receipt validation failures can follow a commit.
                pass
            # No valid receipt does not prove the transaction did not commit.
            # Native Harness must refuse automatic replay of this write.
            return ToolResult(
                success=False,
                error="document write outcome unknown; inspect the approval receipt",
                data={"outcome_unknown": True},
            )
        return execute

    return tuple(
        FunctionTool(
            ToolSpec(
                name,
                description,
                {"type": "object", "properties": props, "required": list(props), "additionalProperties": False},
                risk=Risk.WRITE,
                always_confirm=True,
            ),
            handler(name),
        )
        for name, description, props in definitions
    )
