#!/usr/bin/env python3
"""Measure a loaded loopback TEI service with fixed synthetic input distributions.

This is a latency and resource-sizing probe, not a retrieval quality evaluation.
The caller owns service lifecycle and must stop outstanding inference before
starting another probe after a timeout.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import math
import os
import pathlib
import time
import urllib.error
import urllib.parse


def load_smoke():
    path = pathlib.Path(__file__).with_name("smoke-reranker_service.py")
    spec = importlib.util.spec_from_file_location("reranker_service_smoke", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def synthetic_passages(distribution: str, count: int) -> list[str]:
    if distribution == "chinese_800":
        bases = [
            "完全分布式部署需要三台虚拟机，每台建议分配一核处理器和四GB内存。内存不足可能导致数据节点或工作节点进程终止。请检查各个节点的可用内存，再运行计算任务。",
            "伪分布式部署只需要一台虚拟机，建议分配双核处理器和六GB内存。安装环境准备完成后，检查服务端口、日志目录以及文件访问权限。",
            "课程包括分布式存储、数据仓库、批处理计算以及综合案例。实验报告应记录执行步骤、控制台结果、异常现象和对应处理方法。",
            "文档服务为每次更新建立不可变版本，旧版本保留原始文件和内容摘要。索引完成并校验后发布新版本，用户只能读取有权限访问的资料。",
        ]
        length = 800
    elif distribution == "mixed_800":
        bases = [
            "实验环境配置：worker.memory=4096MB; cpu.cores=1; node.count=3; Hadoop DataNode and Spark Worker require enough memory. 运行 start-dfs.sh 后使用 jps 检查进程，再检查日志里的 OutOfMemoryError。",
            "伪分布式：cpu.cores=2; memory=6144MB; nodes=1; ssh localhost; export JAVA_HOME=/usr/lib/jvm/java; hdfs dfs -mkdir /input; 本步骤记录本地实验的启动命令与文件目录。",
            "索引服务 API: POST /documents; GET /versions; object.version=immutable; checksum=SHA256; Workspace permissions are checked before reading each chunk. 检查版本发布状态后再发起检索。",
            "Spark SQL 实验包含 select count(*) from records group by category; Hive warehouse location is /user/hive/warehouse; 配置元数据服务后再启动查询，保留执行结果和错误日志。",
        ]
        length = 800
    else:
        bases = [
            "完全分布式部署建议三台虚拟机，每台一核处理器和四GB内存。",
            "伪分布式部署建议一台虚拟机，双核处理器和六GB内存。",
            "用户可以在文档列表中按标题搜索，并查看文档处理状态。",
            "课程实验报告需要记录每个阶段的执行步骤及结果。",
        ]
        length = 80
    return [((f"第{i + 1}节。" + bases[i % len(bases)]) * 30)[:length] for i in range(count)]


def latency_summary(samples: list[float], expected: int) -> dict:
    """Only complete runs receive percentiles; warmup/failure times are excluded."""
    if (type(expected) is not int or expected < 1 or len(samples) > expected
            or any(type(value) not in (int, float) or not math.isfinite(value) or value < 0 for value in samples)):
        raise ValueError("invalid latency samples or expected count")
    complete = len(samples) == expected
    ordered = sorted(samples)

    def percentile(fraction):
        position = (len(ordered) - 1) * fraction
        lower = int(position)
        upper = min(lower + 1, len(ordered) - 1)
        return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)

    return {"samples": len(samples), "requested_samples": expected, "complete": complete,
            "samples_seconds": samples,
            "p50_seconds": percentile(.5) if complete else None,
            "p95_seconds": percentile(.95) if complete else None}


def measure_candidate(smoke, base, key, count, distribution, timeout, repeats, warmup, identity):
    texts = synthetic_passages(distribution, count)
    payload = {"query": "完全分布式部署的虚拟机推荐内存是多少？", "texts": texts,
               "truncate": True, "raw_scores": False, "return_text": False}
    record = {"candidates": count, "runes_per_passage": len(texts[0]), **identity,
              "warmup_requested": warmup, "warmup_completed": 0, "status": "passed"}
    samples = []
    for number in range(warmup + repeats):
        phase = "warmup" if number < warmup else "measurement"
        started = time.monotonic()
        try:
            status, rows, elapsed = smoke.request(base, key, "/rerank", payload, timeout=timeout)
        except (TimeoutError, urllib.error.URLError) as exc:
            if not isinstance(exc, TimeoutError) and not isinstance(exc.reason, TimeoutError):
                raise
            record.update(status="timed_out", failed_phase=phase,
                          seconds=round(time.monotonic() - started, 3))
            break
        if status != 200:
            record.update(status="failed", failed_phase=phase, error="rerank_http_status", http_status=status)
            break
        try:
            scores = smoke.checked_scores(rows, count)
            if not all(0 <= score <= 1 for score in scores.values()):
                raise ValueError("unnormalized scores")
            if type(elapsed) not in (int, float) or not math.isfinite(elapsed) or elapsed < 0:
                raise ValueError("invalid request duration")
        except (AssertionError, KeyError, TypeError, ValueError):
            record.update(status="failed", failed_phase=phase, error="invalid_rerank_response")
            break
        if phase == "warmup":
            record["warmup_completed"] += 1
        else:
            samples.append(elapsed)
    record.update(latency_summary(samples, repeats))
    if record["status"] == "passed":
        # Preserve the existing single-request field with the default repeats=1.
        record["seconds"] = round(sum(samples), 3)
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:18091")
    parser.add_argument("--distribution", choices=["chinese_800", "mixed_800", "short_80"], default="chinese_800")
    parser.add_argument("--candidates", type=int, nargs="+", default=[8])
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--repeats", type=int, default=1, help="measured requests per candidate count (default: 1)")
    parser.add_argument("--warmup", type=int, default=0, help="unmeasured warmup requests per candidate count (default: 0)")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    url = urllib.parse.urlparse(args.url)
    if url.scheme != "http" or url.hostname not in {"127.0.0.1", "localhost", "::1"}:
        parser.error("use a loopback HTTP service URL")
    if any(count < 1 or count > 200 for count in args.candidates):
        parser.error("candidate counts must be between 1 and 200")
    if not 1 <= args.repeats <= 1000 or not 0 <= args.warmup <= 100:
        parser.error("repeats must be 1..1000 and warmup must be 0..100")
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("timeout must be a positive finite number")
    key = os.environ.get("RERANKER_TEST_API_KEY", "")
    if len(key) < 32:
        parser.error("RERANKER_TEST_API_KEY must contain at least 32 characters")
    smoke = load_smoke()
    base = args.url.rstrip("/")
    status, info, _ = smoke.request(base, key, "/info", timeout=10)
    assert status == 200 and "reranker" in info["model_type"]
    result = {
        "kind": "synthetic_latency_only",
        "model": info["model_id"],
        "revision": info["model_sha"],
        "device": info.get("device", "not_reported"),
        "requested_device": info.get("requested_device", "not_reported"),
        "max_input_tokens": info["max_input_length"],
        "max_batch_tokens": info["max_batch_tokens"],
        "distribution": args.distribution,
        "timeout_seconds": args.timeout,
        "repeats": args.repeats,
        "warmup": args.warmup,
        "latency_scope": "Synthetic serial reranker HTTP only; excludes retrieval, embedding and answer generation. Warmup excluded; no production SLO claim.",
        "records": [],
    }
    def save():
        pathlib.Path(args.output).write_text(json.dumps(result, indent=2) + "\n")
    save()
    for count in args.candidates:
        record = measure_candidate(smoke, base, key, count, args.distribution, args.timeout,
                                   args.repeats, args.warmup,
                                   {name: result[name] for name in ("model", "revision", "device", "requested_device")})
        result["records"].append(record)
        save()
        print(json.dumps(record), flush=True)
        if record["status"] != "passed":
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
