#!/usr/bin/env python3
"""Paired reranking check through an isolated application's real HTTP API.

Creates a temporary account and Workspace, uploads a fixed Chinese development
fixture, and deletes only that Workspace in finally. The account remains because
there is no account-deletion API. No chat endpoint or paid model is called.
"""
import argparse
from collections import Counter
import hashlib
import http.client
import json
import math
from pathlib import Path
import statistics
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
import uuid


FIXTURE = {
    "schema": 1,
    "name": "synthetic-zh-rerank-dev-v1",
    "documents": [
        {"id": "refund", "title": "售后服务：退款申请",
         "text": "消费者收到商品后，七天内可以申请无理由退款。退款申请需要提供订单号。超过七天的质量问题按保修政策处理。"},
        {"id": "refund-copy", "title": "客服常见问题：退款期限",
         "text": "消费者收到商品后，七天内可以申请无理由退款。退款申请需要提供订单号。超过七天的质量问题按保修政策处理。"},
        {"id": "dispatch", "title": "配送服务：发货时间",
         "text": "订单付款成功后三个工作日内发货。这里的三天指发货处理时间，不是退款申请期限。物流运输时间另计。"},
        {"id": "invoice", "title": "财务服务：开票时间",
         "text": "订单完成后三十天内可申请电子发票。开票需要填写税号与抬头，电子发票通过邮件发送。发票申请不办理退款。"},
        {"id": "pseudo", "title": "实验环境：伪分布式部署",
         "text": "伪分布式使用一台虚拟机，推荐这台机器配置两核 CPU、六 GB 内存。Hadoop 服务部署在同一台机器。"},
        {"id": "distributed", "title": "实验环境：完全分布式部署",
         "text": "完全分布式使用三台虚拟机，推荐每台机器配置一核 CPU、四 GB 内存。四 GB 是每台配置，不是三台机器的总内存。"},
        {"id": "memory-risk", "title": "实验排错：资源不足",
         "text": "虚拟机内存不足时，运行 Hive 或 Spark 可能导致 DataNode 或 Worker 相关进程被终止。指导材料未声明最先被终止的进程，也未声明 NodeManager 一定会被终止。"},
        {"id": "disk-risk", "title": "实验排错：磁盘不足",
         "text": "磁盘空间不足可能导致文件写入失败、日志无法追加或数据节点不可写。磁盘容量与虚拟机内存是不同资源，不能用扩容磁盘代替增加内存。"},
        {"id": "production", "title": "连接配置：生产数据库",
         "text": "生产环境的 MySQL 监听端口为 3306，仅允许后端服务通过 Docker 内部网络访问。不要使用测试环境的映射端口连接生产数据库。"},
        {"id": "testing", "title": "连接配置：测试数据库",
         "text": "测试环境把 MySQL 容器端口 3306 映射到宿主机端口 13306。宿主机测试客户端应连接 13306，该端口不适用于生产环境。"},
        {"id": "upload", "title": "文件限制：用户上传",
         "text": "知识库文档和聊天附件默认上传上限是五十 MB，可通过 MAX_FILE_SIZE_MB 配置。技能 ZIP 包有独立限制，沙箱生成文件也有独立限制。"},
        {"id": "sandbox", "title": "文件限制：沙箱写入",
         "text": "Agent 在沙箱中连续追加生成的单个文件累计上限为八 MB。该限制不控制用户上传知识库文件和聊天附件。"},
        {"id": "skills", "title": "文件限制：技能压缩包",
         "text": "技能 ZIP 包默认上传上限为二百五十六 MB，使用独立配置。此项配置不改变知识库文档或聊天附件的默认上限。"},
        {"id": "memory-command", "title": "Linux 检查：内存用量",
         "text": "使用 free -m 查看物理内存、已用内存和可用内存，单位为 MB。这个命令用于查看，不会自动增加虚拟机内存。"},
        {"id": "disk-command", "title": "Linux 检查：磁盘用量",
         "text": "使用 df -h 查看文件系统的磁盘容量、已用空间和剩余空间。这个命令检查磁盘，不显示进程消耗的内存。"},
        {"id": "modules", "title": "课程安排：实验模块",
         "text": "本课程包含四个实验模块：大数据开发技术 Hadoop、分布式数据仓库 Hive、高级大数据开发技术 Spark，以及大数据综合案例。部署方式有伪分布式和完全分布式。"},
    ],
    "queries": [
        {"id": "refund-window", "text": "收到商品后多久可以申请无理由退款？",
         "relevant": ["refund", "refund-copy"]},
        {"id": "distributed-per-machine", "text": "完全分布式需要几台虚拟机，每台推荐多少内存？",
         "relevant": ["distributed"]},
        {"id": "pseudo-memory", "text": "伪分布式单台虚拟机建议多少核、多少内存？",
         "relevant": ["pseudo"]},
        {"id": "killed-process", "text": "运行 Hive 或 Spark 时内存不足可能终止哪些进程？",
         "relevant": ["memory-risk"]},
        {"id": "test-db-port", "text": "从宿主机连接测试环境的 MySQL 应使用哪个端口？",
         "relevant": ["testing"]},
        {"id": "production-db-port", "text": "生产环境后端连接 MySQL 使用哪个端口和网络？",
         "relevant": ["production"]},
        {"id": "upload-limit", "text": "知识库文档和聊天附件默认最多能上传多大的文件？",
         "relevant": ["upload"]},
        {"id": "sandbox-limit", "text": "Agent 在沙箱连续追加生成的单个文件大小上限是多少？",
         "relevant": ["sandbox"]},
        {"id": "memory-command", "text": "用哪个 Linux 命令按 MB 查看可用内存？",
         "relevant": ["memory-command"]},
        {"id": "course-modules", "text": "课程包含哪四个实验模块？",
         "relevant": ["modules"]},
    ],
    "no_answer": [{"id": "astronomy", "text": "木卫二冰层下海洋的盐度是多少？"}],
    "protocol": {
        "top_k": 5,
        "mode": "hybrid",
        "order": "query order, baseline then rerank; no warmup exclusion",
        "judgments": "Binary document relevance fixed before any model call.",
        "limits": [
            "Synthetic development fixture, not SciFact or a business evaluation set.",
            "No parameter selection or guarantee of improvement; a small set can saturate Top 5.",
            "No-answer cases are reported separately and excluded from positive-query metrics.",
            "Jaccard uses cleaned body tokens, excludes document titles, and is a lexical proxy.",
            "Latency is descriptive; sequential order and cache/load are not controlled.",
        ],
    },
}

APPLIED_OUTCOMES = {"ok", "threshold_degraded", "fallback_top1", "all_below_threshold"}


class CheckError(Exception):
    """Fixed diagnostics only: never include a provider body or credentials."""


def require(condition, message):
    if not condition:
        raise CheckError(message)


def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                     separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def save(path, value):
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                    indent=2, allow_nan=False) + "\n", encoding="utf-8")
    temporary.replace(path)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class Client:
    def __init__(self, base, timeout=90):
        parsed = urllib.parse.urlsplit(base)
        require(parsed.scheme in ("http", "https") and parsed.netloc and not parsed.username
                and not parsed.password and not parsed.query and not parsed.fragment
                and parsed.path in ("", "/", "/api/v1", "/api/v1/"), "invalid API base URL")
        self.base = base.rstrip("/")
        if not self.base.endswith("/api/v1"):
            self.base += "/api/v1"
        self.token, self.timeout = "", timeout
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())

    def request(self, method, path, data=None, media="application/json", expected=200):
        headers = {"Content-Type": media}
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        if data is not None and not isinstance(data, bytes):
            data = json.dumps(data, ensure_ascii=False).encode()
        request = urllib.request.Request(self.base + path, data=data, headers=headers, method=method)
        try:
            try:
                response = self.opener.open(request, timeout=self.timeout)
            except urllib.error.HTTPError as error:
                response = error
            with response:
                require(response.status == expected, "API returned an unexpected HTTP status")
                raw = response.read((2 << 20) + 1)
                require(len(raw) <= 2 << 20, "API response exceeds the size limit")
                if expected == 204:
                    return None
                envelope = json.loads(raw)
                require(isinstance(envelope, dict) and envelope.get("success") is True
                        and "data" in envelope, "invalid API response envelope")
                return envelope["data"]
        except (urllib.error.URLError, TimeoutError, OSError, http.client.HTTPException):
            raise CheckError("API network or response-read failure") from None
        except (ValueError, UnicodeError):
            raise CheckError("API response is not valid JSON") from None

    def upload(self, workspace, document):
        boundary = uuid.uuid4().hex
        content = ("# " + document["title"] + "\n\n" + document["text"] + "\n").encode()
        body = (f'--{boundary}\r\nContent-Disposition: form-data; name="title"\r\n\r\n'
                f'{document["title"]}\r\n--{boundary}\r\n'
                'Content-Disposition: form-data; name="file"; filename="fixture.md"\r\n'
                'Content-Type: text/markdown\r\n\r\n').encode() + content
        body += f"\r\n--{boundary}--\r\n".encode()
        result = self.request("POST", workspace + "/documents", body,
                              f"multipart/form-data; boundary={boundary}", 201)
        return result, hashlib.sha256(content).hexdigest()


def body_terms(text):
    """NFKC words plus Han unigrams/bigrams; do not bridge whitespace."""
    terms, word, han = set(), [], []
    def flush():
        if word:
            terms.add("".join(word))
            word.clear()
        if han:
            terms.update(han)
            terms.update(han[i] + han[i + 1] for i in range(len(han) - 1))
            han.clear()
    for char in unicodedata.normalize("NFKC", text).casefold():
        if "\u3400" <= char <= "\u9fff" or "\U00020000" <= char <= "\U0003134f":
            if word:
                flush()
            han.append(char)
        elif char.isalnum():
            if han:
                flush()
            word.append(char)
        else:
            flush()
    flush()
    return terms


def redundancy(ranking, documents):
    sets = [body_terms(documents[doc]["text"]) for doc in ranking]
    pairs = [len(a & b) / len(a | b) if a | b else 0.0
             for i, a in enumerate(sets) for b in sets[i + 1:]]
    return {"pair_count": len(pairs),
            "mean_pairwise_jaccard": statistics.mean(pairs) if pairs else 0.0,
            "max_pairwise_jaccard": max(pairs, default=0.0),
            "pairs_at_least_0_8": sum(value >= 0.8 for value in pairs)}


def quality(ranking, relevant):
    relevant = set(relevant)
    require(bool(relevant), "positive-query metrics require a relevance judgment")
    require(len(ranking) == len(set(ranking)), "duplicate documents in metric input")
    selected = ranking[:5]
    found = relevant.intersection(selected)
    dcg = sum(1 / math.log2(i + 2) for i, doc in enumerate(selected) if doc in relevant)
    ideal = sum(1 / math.log2(i + 2) for i in range(min(5, len(relevant))))
    return {"Recall@5": len(found) / len(relevant), "Hit@5": float(bool(found)),
            "MRR@5": next((1 / (i + 1) for i, doc in enumerate(selected) if doc in relevant), 0.0),
            "nDCG@5": dcg / ideal}


def percentile(values, fraction):
    data = sorted(values)
    position = (len(data) - 1) * fraction
    low, high = int(position), min(int(position) + 1, len(data) - 1)
    return data[low] + (data[high] - data[low]) * (position - low)


def validate_response(response, variant, mapping, profile, policies):
    require(isinstance(response, dict) and response.get("profile_id") == profile
            and response.get("mode") == "hybrid", "search profile or mode changed")
    policy = response.get("retrieval_policy_id")
    require(isinstance(policy, str) and 0 < len(policy) <= 128, "search policy ID is missing")
    require(variant not in policies or policies[variant] == policy, "search policy changed during evaluation")
    policies[variant] = policy
    if len(policies) == 2:
        require(policies["baseline"] != policies["rerank"], "baseline and rerank policy IDs must differ")
    diagnostic = response.get("rerank")
    require(isinstance(diagnostic, dict), "rerank diagnostics are missing")
    if variant == "rerank":
        require(diagnostic.get("applied") is True and diagnostic.get("outcome") in APPLIED_OUTCOMES,
                "enabled reranking was skipped or fell back after a model error")
    else:
        require(diagnostic.get("applied") is False, "baseline unexpectedly applied reranking")
    hits = response.get("hits")
    require(isinstance(hits, list) and len(hits) <= 5, "invalid search result count")
    reverse = {entry["document_id"]: key for key, entry in mapping.items()}
    ranking, scored, chunks_seen = [], [], set()
    for hit in hits:
        require(isinstance(hit, dict) and isinstance(hit.get("chunk"), dict), "invalid search hit")
        chunk = hit["chunk"]
        key = reverse.get(chunk.get("document_id"))
        require(key is not None, "search returned a document outside the fixture")
        entry = mapping[key]
        require(all(chunk.get(field) == entry[field] for field in
                    ("version_id", "index_id", "source_sha256")), "search source identity changed")
        cid = chunk.get("id")
        require(isinstance(cid, str) and len(cid) == 32 and cid not in chunks_seen, "invalid or duplicate chunk ID")
        chunks_seen.add(cid)
        if key not in ranking:
            ranking.append(key)
        row = {"document": key}
        for field in ("score", "retrieval_score", "rerank_score", "normalized_retrieval_score"):
            if field in hit:
                value = hit[field]
                require(type(value) in (int, float) and math.isfinite(value), "invalid search score")
                row[field] = value
        scored.append(row)
    # Copy only bounded scalar diagnostics, never arbitrary provider payloads.
    safe_diagnostic = {"applied": diagnostic["applied"], "outcome": diagnostic.get("outcome", "unknown")}
    for field in ("candidate_count", "result_count", "threshold", "effective_threshold", "top_score"):
        if field in diagnostic:
            value = diagnostic[field]
            require(type(value) in (int, float) and math.isfinite(value), "invalid rerank diagnostic")
            safe_diagnostic[field] = value
    return {"ranking": ranking, "chunk_hits": len(hits), "scores": scored,
            "rerank": safe_diagnostic, "retrieval_policy_id": policy}


def verify_indexes(client, workspace, mapping, profile):
    for entry in mapping.values():
        path = workspace + "/documents/" + entry["document_id"]
        doc = client.request("GET", path)
        require(doc.get("current_version_id") == entry["version_id"], "fixture document head changed")
        job = client.request("GET", path + "/versions/" + entry["version_id"] + "/index")
        require(job.get("status") == "ready" and job.get("profile_id") == profile
                and job.get("id") == entry["index_id"] and job.get("generation") == entry["generation"]
                and job.get("source_sha256") == entry["source_sha256"], "fixture index changed")


def summarize(records):
    result = {}
    for variant in ("baseline", "rerank"):
        rows = [row[variant] for row in records]
        result[variant] = {
            "queries": len(rows),
            "metrics": {name: statistics.mean(row["metrics"][name] for row in rows)
                        for name in ("Recall@5", "Hit@5", "MRR@5", "nDCG@5")},
            "latency_ms": {"p50": percentile([row["latency_ms"] for row in rows], 0.5),
                           "p95": percentile([row["latency_ms"] for row in rows], 0.95)},
            "redundancy": {
                "mean_query_pairwise_jaccard": statistics.mean(row["redundancy"]["mean_pairwise_jaccard"] for row in rows),
                "pairs_at_least_0_8": sum(row["redundancy"]["pairs_at_least_0_8"] for row in rows),
                "pair_count": sum(row["redundancy"]["pair_count"] for row in rows),
            },
            "outcomes": dict(Counter(row["rerank"]["outcome"] for row in rows)),
            "model_error_fallbacks": sum(row["rerank"]["outcome"] == "model_error" for row in rows),
            "threshold_top1_fallbacks": sum(row["rerank"]["outcome"] == "fallback_top1" for row in rows),
        }
    result["delta_rerank_minus_baseline"] = {
        key: result["rerank"]["metrics"][key] - result["baseline"]["metrics"][key]
        for key in result["baseline"]["metrics"]}
    return result


def run(client, output, wait_timeout=900, poll_interval=1):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    require(not (output / "results.json").exists(), "results already exist; use a new output directory")
    frozen = {"fixture": FIXTURE, "fixture_sha256": digest(FIXTURE)}
    fixture_path = output / "fixture.json"
    if fixture_path.exists():
        require(json.loads(fixture_path.read_text()) == frozen, "existing fixture differs")
    else:
        save(fixture_path, frozen)
    report = {"schema": 1, "fixture_sha256": frozen["fixture_sha256"],
              "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              "status": "running", "records": [], "no_answer": [], "cleanup": "not_created",
              "limits": FIXTURE["protocol"]["limits"] + [
                  "Only the temporary Workspace is deleted; its temporary user account remains.",
                  "Model-error fallback or skipped reranking fails this check, even if retrieval succeeded."]}
    workspace = None
    try:
        account = client.request("POST", "/auth/register",
                                 {"name": "Rerank development check", "email": "rerank-" + uuid.uuid4().hex + "@example.test",
                                  "password": uuid.uuid4().hex}, expected=201)
        require(isinstance(account, dict) and isinstance(account.get("token"), str)
                and account["token"] and len(account.get("workspaces", [])) == 1, "registration response is invalid")
        client.token = account["token"]
        workspace = "/workspaces/" + account["workspaces"][0]["id"]
        mapping = {}
        for document in FIXTURE["documents"]:
            uploaded, source_sha = client.upload(workspace, document)
            mapping[document["id"]] = {"document_id": uploaded["document"]["id"],
                                       "version_id": uploaded["version"]["id"], "source_sha256": source_sha}
        deadline, profile = time.monotonic() + wait_timeout, None
        outstanding = dict(mapping)
        while outstanding:
            for key, entry in list(outstanding.items()):
                job = client.request("GET", workspace + "/documents/" + entry["document_id"]
                                     + "/versions/" + entry["version_id"] + "/index")
                require(job.get("status") != "failed", "fixture indexing failed")
                if job.get("status") == "ready":
                    require(profile is None or profile == job.get("profile_id"), "mixed fixture index profiles")
                    profile = job["profile_id"]
                    entry.update(index_id=job["id"], generation=job["generation"])
                    del outstanding[key]
            if outstanding:
                require(time.monotonic() < deadline, "fixture indexing timed out")
                time.sleep(poll_interval)
        verify_indexes(client, workspace, mapping, profile)
        report.update(profile_id=profile, corpus_verification_before="passed")
        policies, documents = {}, {row["id"]: row for row in FIXTURE["documents"]}
        for case in FIXTURE["queries"] + FIXTURE["no_answer"]:
            record = {"query_id": case["id"]}
            for variant in ("baseline", "rerank"):
                started = time.perf_counter()
                response = client.request("POST", workspace + "/search",
                                          {"query": case["text"], "mode": "hybrid", "limit": 5, "rerank": variant == "rerank"})
                elapsed = (time.perf_counter() - started) * 1000
                value = validate_response(response, variant, mapping, profile, policies)
                value["latency_ms"] = elapsed
                value["redundancy"] = redundancy(value["ranking"], documents)
                if "relevant" in case:
                    value["metrics"] = quality(value["ranking"], case["relevant"])
                else:
                    value["returned_any"] = bool(value["ranking"])
                record[variant] = value
            report["records" if "relevant" in case else "no_answer"].append(record)
            save(output / "results.json", report)
        verify_indexes(client, workspace, mapping, profile)
        require(digest(FIXTURE) == frozen["fixture_sha256"], "fixture changed during evaluation")
        require(hashlib.sha256(Path(__file__).read_bytes()).hexdigest() == report["script_sha256"],
                "evaluation script changed during execution")
        report.update(status="passed", corpus_verification_after="passed", retrieval_policy_ids=policies,
                      summary=summarize(report["records"]))
    except CheckError as error:
        report.update(status="failed", error=str(error))
    except (KeyError, TypeError, ValueError):
        report.update(status="failed", error="unexpected API response structure")
    finally:
        if workspace:
            try:
                client.request("DELETE", workspace, expected=204)
                report["cleanup"] = "temporary_workspace_deleted"
            except Exception:
                report.update(status="failed", cleanup="failed",
                              cleanup_error="could not delete the temporary Workspace")
        save(output / "results.json", report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True, help="isolated app HTTP origin")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--wait-timeout", type=int, default=900)
    args = parser.parse_args()
    try:
        require(1 <= args.wait_timeout <= 7200, "wait timeout must be 1..7200 seconds")
        report = run(Client(args.base_url), args.output, args.wait_timeout)
    except (CheckError, OSError, ValueError) as error:
        print("FAIL: " + (str(error) if isinstance(error, CheckError) else "local artifact access failed"))
        return 1
    print(("PASS" if report["status"] == "passed" else "FAIL") + ": paired Chinese development fixture; "
          + report["cleanup"])
    if report["status"] == "passed":
        print(json.dumps(report["summary"], ensure_ascii=False, sort_keys=True))
    else:
        print(report.get("error", report.get("cleanup_error", "check failed")))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
