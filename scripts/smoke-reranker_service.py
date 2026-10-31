#!/usr/bin/env python3
"""Verify a real cross-encoder without printing credentials or document text.

RERANKER_TEST_API_KEY is required. The service must already be running; this
script does not create or delete containers, volumes, or model downloads.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import time
import urllib.error
import urllib.parse
import urllib.request


def request(base: str, key: str, path: str, payload=None, *, auth=True, timeout=120):
    headers = {}
    data = None
    if auth:
        headers["Authorization"] = "Bearer " + key
    if payload is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(payload, ensure_ascii=False, allow_nan=False).encode("utf-8")
    req = urllib.request.Request(base + path, data=data, headers=headers)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    started = time.monotonic()
    try:
        with opener.open(req, timeout=timeout) as response:
            status = response.status
            body = response.read(2_000_001)
    except urllib.error.HTTPError as exc:
        status, body = exc.code, exc.read(2_000_001)
    if len(body) > 2_000_000:
        raise AssertionError("service response exceeds limit")
    try:
        decoded = json.loads(body) if body else None
    except json.JSONDecodeError:
        decoded = None
    return status, decoded, time.monotonic() - started


def checked_scores(rows, count):
    assert isinstance(rows, list) and len(rows) == count, "incomplete results"
    scores = {}
    for row in rows:
        assert set(row) == {"index", "score"}, "unexpected result fields"
        index, score = row["index"], row["score"]
        assert type(index) is int and 0 <= index < count and index not in scores
        assert type(score) in (int, float) and math.isfinite(score)
        scores[index] = score
    return scores


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:18091")
    parser.add_argument("--model", default="BAAI/bge-reranker-base")
    parser.add_argument("--device", choices=("cuda", "mps", "cpu"),
                        help="require the service to report this actual execution device")
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--benchmark", type=int, nargs="*", default=[])
    parser.add_argument("--output")
    args = parser.parse_args()
    url = urllib.parse.urlparse(args.url)
    if url.scheme != "http" or url.hostname not in {"127.0.0.1", "localhost", "::1"}:
        parser.error("use a loopback HTTP service URL")
    key = os.environ.get("RERANKER_TEST_API_KEY", "")
    if len(key) < 32:
        parser.error("RERANKER_TEST_API_KEY must contain at least 32 characters")
    base = args.url.rstrip("/")
    records = []

    def record(entry):
        records.append(entry)
        if args.output:
            with open(args.output, "w") as target:
                target.write(json.dumps({"model": args.model, "records": records}, indent=2) + "\n")
        print(json.dumps(entry), flush=True)

    status, _, _ = request(base, key, "/health", auth=False, timeout=10)
    assert status == 200, f"model health: HTTP {status}"
    record({"check": "model_loaded", "status": "passed"})
    status, info, _ = request(base, key, "/info", timeout=10)
    assert status == 200 and info["model_id"] == args.model, "wrong loaded model"
    if args.device:
        assert info.get("device") == args.device, "wrong execution device"
    assert "reranker" in info["model_type"], "model is not a cross-encoder reranker"
    assert info["auto_truncate"] is True
    assert info["max_input_length"] <= info["max_batch_tokens"], "token cap is not active"
    record({"check": "model_identity", "model": info["model_id"], "revision": info["model_sha"],
                    "max_input_tokens": info["max_input_length"],
                    "device": info.get("device"), "requested_device": info.get("requested_device"),
                    "status": "passed"})

    query = "完全分布式部署的虚拟机推荐内存是多少？"
    texts = [
        "完全分布式部署需要三台虚拟机，每台建议分配 1 核 CPU 和 4G 内存。",
        "伪分布式部署使用一台虚拟机，建议分配 2 核 CPU 和 6G 内存。",
        "周末的天气晴朗，公园里的花朵开了。",
        "文档的文件名可以在列表中修改，修改标题不会改变原始文件。",
    ]
    payload = {"query": query, "texts": texts, "truncate": True, "raw_scores": False, "return_text": False}
    status, rows, elapsed = request(base, key, "/rerank", payload, timeout=args.timeout)
    assert status == 200, f"rerank: HTTP {status}"
    scores = checked_scores(rows, len(texts))
    assert all(0 <= x <= 1 for x in scores.values()), "scores are not normalized"
    assert rows[0]["index"] == 0 and scores[0] > scores[2], "Chinese relevance ordering failed"
    record({"check": "chinese_relevance", "status": "passed", "scores": scores, "seconds": round(elapsed, 3)})

    status, raw, _ = request(base, key, "/rerank", dict(payload, raw_scores=True), timeout=args.timeout)
    assert status == 200
    raw_scores = checked_scores(raw, len(texts))
    assert all(abs(scores[i] - 1 / (1 + math.exp(-raw_scores[i]))) < 0.00001 for i in scores)
    record({"check": "sigmoid_contract", "status": "passed"})
    status, _, _ = request(base, key, "/rerank", payload, auth=False, timeout=10)
    assert status == 401, f"missing auth: HTTP {status}"
    status, _, _ = request(base, key + "incorrect", "/rerank", payload, timeout=10)
    assert status == 401, f"wrong auth: HTTP {status}"
    record({"check": "authentication", "status": "passed"})
    status, _, _ = request(base, key, "/rerank", dict(payload, texts=[]), timeout=10)
    assert status == 400, f"empty batch: HTTP {status}"
    status, _, _ = request(base, key, "/rerank", dict(payload, texts=["x"] * 201), timeout=10)
    assert status == 422, f"batch bound: HTTP {status}"
    status, _, _ = request(base, key, "/rerank", dict(payload, texts=["x" * 2_000_000]), timeout=10)
    assert status == 413, f"payload bound: HTTP {status}"
    record({"check": "request_bounds", "status": "passed"})
    long_text = "模型必须限制过长的输入并正确处理中文文本。" * 500
    status, _, _ = request(base, key, "/rerank", dict(payload, texts=[long_text], truncate=False), timeout=10)
    assert status == 422, f"token bound: HTTP {status}"
    status, rows, elapsed = request(base, key, "/rerank", dict(payload, texts=[long_text]), timeout=args.timeout)
    assert status == 200, f"automatic truncation: HTTP {status}"
    checked_scores(rows, 1)
    record({"check": "token_truncation", "status": "passed", "seconds": round(elapsed, 3)})

    for count in args.benchmark:
        assert 1 <= count <= 200, "benchmark must use 1..200 candidates"
        passage = ("完全分布式部署要求各个节点拥有足够的内存。"
                   "指导书建议每台虚拟机配置一核处理器和四GB内存。"
                   "如果内存不足，运行计算任务时数据节点或工作节点进程可能被终止。") * 12
        candidates = [passage[:800] for _ in range(count)]
        started = time.monotonic()
        try:
            status, rows, elapsed = request(base, key, "/rerank", dict(payload, texts=candidates), timeout=args.timeout)
        except (TimeoutError, urllib.error.URLError) as exc:
            if not isinstance(exc, TimeoutError) and not isinstance(exc.reason, TimeoutError):
                raise
            record({"check": "benchmark", "candidates": count, "runes_per_passage": len(candidates[0]),
                            "seconds": round(time.monotonic() - started, 3), "status": "timed_out"})
            # Existing inference may still be running. Do not add another
            # expensive request behind a benchmark that exceeded its budget.
            break
        assert status == 200, f"benchmark {count}: HTTP {status}"
        checked_scores(rows, count)
        record({"check": "benchmark", "candidates": count, "runes_per_passage": len(candidates[0]), "seconds": round(elapsed, 3), "status": "passed"})

    result = {"model": args.model, "records": records}
    encoded = json.dumps(result, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        with open(args.output, "w") as target:
            target.write(encoded)
    print(encoded, end="")


if __name__ == "__main__":
    main()
