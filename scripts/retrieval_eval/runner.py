"""Resumable ingestion, measured API queries, immutable evaluation manifests."""
import concurrent.futures
from collections import Counter
import datetime
import hashlib
import json
import math
import platform
import time
import urllib.parse
from pathlib import Path

from .api import atomic_json
from .data import EvaluationError, corpus_content, dataset_info, query_for_api, sha256_file
from .metrics import deduplicate_documents, paired_compare, score_records


def utc_now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def digest_json(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()


POLICY_CONTRACT = "retrieval-policy-v1"
RERANK_OUTCOMES = {"disabled", "no_candidates", "ok", "threshold_degraded",
                   "fallback_top1", "all_below_threshold", "model_error"}
APPLIED_RERANK_OUTCOMES = {"ok", "threshold_degraded", "fallback_top1", "all_below_threshold"}


class RetrievalPolicyError(EvaluationError):
    """A changed policy aborts the run instead of becoming a scored failure."""


def valid_policy_id(value):
    return isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def load_policy_freeze(path, manifest, required=False):
    if not path.exists() and not required:
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        raise RetrievalPolicyError("retrieval policy freeze is missing or invalid") from None
    if (not isinstance(value, dict) or set(value) != {"schema", "manifest_sha256", "retrieval_policy_id"}
            or type(value["schema"]) is not int or value["schema"] != 1
            or value["manifest_sha256"] != digest_json(manifest)
            or not valid_policy_id(value["retrieval_policy_id"])):
        raise RetrievalPolicyError("retrieval policy freeze differs from the evaluation manifest")
    return value


def freeze_response_policy(response, path, manifest, frozen):
    policy_id = response.get("retrieval_policy_id") if isinstance(response, dict) else None
    if not valid_policy_id(policy_id):
        raise RetrievalPolicyError("search response lacks a valid retrieval policy ID; use the current API")
    if frozen is None:
        frozen = {"schema": 1, "manifest_sha256": digest_json(manifest), "retrieval_policy_id": policy_id}
        # Persist before interpreting model diagnostics or appending any ranking:
        # even a failed reranker response establishes the run's observed policy.
        atomic_json(path, frozen)
    elif frozen["retrieval_policy_id"] != policy_id:
        raise RetrievalPolicyError("retrieval policy changed; use a new evaluation output directory/label")
    return frozen


def safe_rerank_diagnostics(value):
    if (not isinstance(value, dict) or type(value.get("applied")) is not bool
            or not isinstance(value.get("outcome"), str)
            or value.get("outcome") not in RERANK_OUTCOMES
            or value["applied"] != (value["outcome"] in APPLIED_RERANK_OUTCOMES)):
        raise EvaluationError("search response has invalid rerank diagnostics")
    result = {"applied": value["applied"], "outcome": value["outcome"]}
    for field in ("candidate_count", "result_count"):
        if field in value:
            count = value[field]
            if type(count) is not int or not 0 <= count <= 1000:
                raise EvaluationError("search response has invalid rerank counts")
            result[field] = count
    for field in ("threshold", "effective_threshold", "top_score"):
        if field in value:
            number = value[field]
            if type(number) not in (int, float) or not math.isfinite(number) or not 0 <= number <= 1:
                raise EvaluationError("search response has invalid rerank score diagnostics")
            result[field] = number
    if "fallback_used" in value:
        if type(value["fallback_used"]) is not bool:
            raise EvaluationError("search response has invalid rerank fallback diagnostic")
        result["fallback_used"] = value["fallback_used"]
    code = value.get("error_code")
    if isinstance(code, str) and code in {"rerank_invalid", "rerank_invalid_response", "rerank_unavailable", "rerank_timeout"}:
        result["error_code"] = code
    return result


def expected_rerank_mode(frozen_environment):
    if frozen_environment is None:
        return None
    measured = frozen_environment["value"]["measured_environment"]
    if "reranker" not in measured:
        return None  # Older completed environments remain readable.
    reranker = measured["reranker"]
    if not isinstance(reranker, dict) or type(reranker.get("enabled")) is not bool:
        raise RetrievalPolicyError("frozen environment has invalid reranker enablement")
    return reranker["enabled"]


def verify_rerank_mode(diagnostic, expected_enabled):
    if expected_enabled is not None and (diagnostic["outcome"] == "disabled") != (not expected_enabled):
        raise RetrievalPolicyError("rerank enablement differs from frozen environment")


def verify_record_policy(records, frozen, expected_enabled=None):
    for record in records:
        # New network failures explicitly carry null. Older rows without this
        # key must never be silently resumed under a new retrieval policy.
        if "retrieval_policy_id" not in record:
            raise RetrievalPolicyError("existing rankings lack retrieval policy evidence; use a new output label")
        observed = record["retrieval_policy_id"]
        if observed is None:
            if record.get("status") != "error" or "rerank" in record:
                raise RetrievalPolicyError("ranking is missing retrieval policy evidence")
        elif (frozen is None or not valid_policy_id(observed)
              or observed != frozen["retrieval_policy_id"]):
            raise RetrievalPolicyError("rankings contain mixed or unfrozen retrieval policies")
        if "rerank" in record:
            diagnostic = safe_rerank_diagnostics(record["rerank"])
            verify_rerank_mode(diagnostic, expected_enabled)
            if diagnostic["outcome"] == "model_error" and record.get("status") != "error":
                raise RetrievalPolicyError("rerank model fallback was recorded as a successful query")
        elif record.get("status") == "ok":
            raise RetrievalPolicyError("successful ranking lacks rerank diagnostics")


def evaluator_source_sha256(directory=None):
    root = Path(directory) if directory else Path(__file__).parent
    sources = {path.name: sha256_file(path) for path in sorted(root.glob("*.py"))
               if not path.name.startswith("test") and not path.name.endswith("_test.py")}
    if not sources:
        raise EvaluationError("evaluator production source files are missing")
    return digest_json(sources)


def validate_environment(value):
    def hex_hash(candidate):
        return isinstance(candidate, str) and len(candidate) == 64 and all(c in "0123456789abcdef" for c in candidate)
    if (not isinstance(value, dict) or set(value) != {"schema", "source_sha256", "measured_environment"} or
            type(value.get("schema")) is not int or value["schema"] != 1 or
            not hex_hash(value.get("source_sha256")) or not isinstance(value.get("measured_environment"), dict) or
            not value["measured_environment"]):
        raise EvaluationError("environment freeze requires schema=1, source_sha256, and nonempty measured_environment")
    sensitive = {"password", "token", "accesstoken", "refreshtoken", "secret", "apikey", "privatekey", "credential", "credentials", "authorization"}
    def check(item, depth=0):
        if depth > 20:
            raise EvaluationError("environment freeze nesting is too deep")
        if isinstance(item, dict):
            for key, child in item.items():
                normalized = key.lower().replace("_", "").replace("-", "")
                if normalized in sensitive or normalized.endswith(("password", "secret", "apikey", "accesstoken", "refreshtoken", "privatekey")):
                    raise EvaluationError("environment freeze must not include credential fields")
                check(child, depth + 1)
        elif isinstance(item, list):
            for child in item:
                check(child, depth + 1)
        elif isinstance(item, float) and not math.isfinite(item):
            raise EvaluationError("environment freeze contains a non-finite number")
        elif isinstance(item, str) and item.startswith(("http://", "https://")):
            parsed = urllib.parse.urlsplit(item)
            if parsed.username or parsed.password:
                raise EvaluationError("environment freeze URL must not contain credentials")
    check(value)


def environment_freeze(path):
    if path is None:
        return None
    try:
        raw = Path(path).read_bytes()
        if len(raw) > 1 << 20:
            raise EvaluationError("environment freeze exceeds 1 MiB")
        def unique_object(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    raise EvaluationError("environment freeze contains duplicate JSON keys")
                result[key] = value
            return result
        value = json.loads(raw, object_pairs_hook=unique_object)
    except (OSError, ValueError):
        raise EvaluationError("environment freeze file is unreadable or invalid JSON") from None
    validate_environment(value)
    return {"file_sha256": hashlib.sha256(raw).hexdigest(), "content_sha256": digest_json(value), "value": value}


def load_state(path, dataset):
    try:
        with open(path, encoding="utf-8") as stream:
            state = json.load(stream)
    except (OSError, ValueError):
        raise EvaluationError("state file is unreadable or invalid JSON") from None
    if (not isinstance(state, dict) or type(state.get("schema")) is not int or state["schema"] != 1 or
            state.get("corpus_sha256") != dataset["hashes"]["corpus"]):
        raise EvaluationError("state belongs to a different corpus or unsupported schema")
    if not isinstance(state.get("documents"), dict) or not isinstance(state.get("workspace_id"), str):
        raise EvaluationError("state mapping is invalid")
    corpus_by_id = {row["_id"]: row for row in dataset["corpus"]}
    if not set(state["documents"]).issubset(corpus_by_id):
        raise EvaluationError("state contains unknown corpus IDs")
    for corpus_id, entry in state["documents"].items():
        expected_sha = hashlib.sha256(corpus_content(corpus_by_id[corpus_id])).hexdigest()
        if not isinstance(entry, dict) or entry.get("source_sha256") != expected_sha:
            raise EvaluationError("state source digest differs from the fixed corpus content policy")
    return state


def remote_title(corpus_id):
    return "beir:" + hashlib.sha256(corpus_id.encode()).hexdigest()


def verify_remote_heads(client, state):
    expected = {entry["document_id"]: (entry["version_id"], remote_title(corpus_id))
                for corpus_id, entry in state["documents"].items()}
    seen, page = set(), 1
    while True:
        listing = client.request("GET", f'/workspaces/{state["workspace_id"]}/documents?page={page}&page_size=100', envelope=True)
        for document in listing["data"]:
            identifier = document["id"]
            if (identifier in seen or identifier not in expected or document["current_version_id"] != expected[identifier][0] or
                    document.get("title") != expected[identifier][1]):
                raise EvaluationError("remote evaluation corpus contains duplicate, foreign, or changed document heads/titles")
            seen.add(identifier)
        if page * 100 >= listing["total"]:
            break
        page += 1
    if seen != set(expected):
        raise EvaluationError("remote evaluation corpus is incomplete")


def verify_remote_corpus(client, state, verify_indexes=True, phase="before", progress=print):
    entries = list(state["documents"].values())
    if verify_indexes:
        def check_index(entry):
            path = f'/workspaces/{state["workspace_id"]}/documents/{entry["document_id"]}/versions/{entry["version_id"]}/index'
            job = client.request("GET", path)
            if (job.get("status") != "ready" or job.get("id") != entry["index_id"] or
                    job.get("generation") != entry["generation"] or job.get("profile_id") != entry["profile_id"] or
                    job.get("source_sha256") != entry["source_sha256"]):
                raise EvaluationError("remote evaluation index changed or is no longer ready")
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(check_index, entry) for entry in entries]
            try:
                for count, future in enumerate(concurrent.futures.as_completed(futures), 1):
                    future.result()
                    if count % 500 == 0 or count == len(entries):
                        progress(f"verify {phase}: indexes={count}/{len(entries)}")
            except Exception:
                for future in futures:
                    future.cancel()
                raise
    # Check heads after remote index I/O so a replacement/deletion during it is visible.
    verify_remote_heads(client, state)
    return {"status": "passed", "documents": len(entries), "indexes": len(entries) if verify_indexes else 0,
            "checked_at": utc_now()}


def ingest(client, dataset, state_path, workers=2, wait_timeout=21600, poll_interval=2, progress=print):
    if workers < 1 or workers > 8 or wait_timeout <= 0 or poll_interval <= 0:
        raise EvaluationError("workers must be 1..8 and timeouts/intervals positive")
    state_path = Path(state_path)
    if state_path.exists():
        state = load_state(state_path, dataset)
        client.request("GET", "/workspaces/" + state["workspace_id"])
    else:
        workspace = client.request("POST", "/workspaces", {
            "name": "Retrieval evaluation " + dataset["hashes"]["corpus"][:12],
            "description": "Dedicated BEIR corpus; do not modify during evaluation."})
        state = {"schema": 1, "corpus_sha256": dataset["hashes"]["corpus"],
                 "workspace_id": workspace["id"], "created_at": utc_now(), "documents": {}}
        atomic_json(state_path, state)
    workspace = state["workspace_id"]
    # Recover uploads whose server commit succeeded but client/state write failed.
    remote, page = {}, 1
    while True:
        listing = client.request("GET", f"/workspaces/{workspace}/documents?page={page}&page_size=100", envelope=True)
        for document in listing["data"]:
            title = document["title"]
            if title in remote:
                raise EvaluationError("duplicate remote corpus title; manual reconciliation required")
            remote[title] = document
        if page * 100 >= listing["total"]:
            break
        page += 1
    expected_titles = {remote_title(row["_id"]) for row in dataset["corpus"]}
    if not set(remote).issubset(expected_titles):
        raise EvaluationError("evaluation workspace contains foreign documents")

    def acquire(row):
        corpus_id = row["_id"]
        content = corpus_content(row)
        expected_sha = hashlib.sha256(content).hexdigest()
        document = remote.get(remote_title(corpus_id))
        if document is None:
            result = client.upload(workspace, remote_title(corpus_id), content)
            document, version = result["document"], result["version"]
        else:
            version = client.request("GET", f'/workspaces/{workspace}/documents/{document["id"]}/versions/{document["current_version_id"]}')
        if version["sha256"] != expected_sha:
            raise EvaluationError("remote corpus content digest differs from input")
        prior = state["documents"].get(corpus_id)
        if prior and (prior["document_id"] != document["id"] or prior["version_id"] != version["id"]):
            raise EvaluationError("remote corpus document/head changed since ingestion")
        return corpus_id, {"document_id": document["id"], "version_id": version["id"], "source_sha256": expected_sha,
                           "status": "uploaded"}

    failures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        pending = {executor.submit(acquire, row): row["_id"] for row in dataset["corpus"]}
        for completed, future in enumerate(concurrent.futures.as_completed(pending), 1):
            try:
                corpus_id, entry = future.result()
                state["documents"][corpus_id] = entry
                atomic_json(state_path, state)
            except EvaluationError as error:
                failures.append(str(error))
            if completed % 100 == 0 or completed == len(pending):
                progress(f"ingest: {completed}/{len(pending)} checked; failed={len(failures)}")
    if failures:
        state["ingestion_failures"] = len(failures)
        atomic_json(state_path, state)
        raise EvaluationError(f"{len(failures)} corpus uploads failed; resume same state ({failures[0]})")
    deadline, profiles = time.monotonic() + wait_timeout, set()
    outstanding = dict(state["documents"])
    while outstanding:
        for corpus_id, entry in list(outstanding.items()):
            path = f'/workspaces/{workspace}/documents/{entry["document_id"]}/versions/{entry["version_id"]}/index'
            job = client.request("GET", path)
            if job["source_sha256"] != entry["source_sha256"]:
                raise EvaluationError("index source digest differs from imported corpus")
            if job["status"] == "failed":
                state["documents"][corpus_id].update(status="failed", error_code=job.get("error_code", "unknown"))
                atomic_json(state_path, state)
                raise EvaluationError("index job failed; inspect safe index error_code and explicitly retry before resuming")
            if job["status"] == "ready":
                entry.update(status="ready", index_id=job["id"], generation=job["generation"], profile_id=job["profile_id"],
                             chunk_count=job["chunk_count"])
                profiles.add(job["profile_id"])
                del outstanding[corpus_id]
        atomic_json(state_path, state)
        progress(f'index: ready={len(state["documents"]) - len(outstanding)}/{len(dataset["corpus"])}')
        if len(profiles) > 1:
            raise EvaluationError("corpus contains mixed index profiles")
        if outstanding:
            if time.monotonic() >= deadline:
                raise EvaluationError(f"index wait timed out with {len(outstanding)} pending documents; resume same state")
            time.sleep(min(poll_interval, max(.01, deadline - time.monotonic())))
    state["profile_id"] = next(iter(profiles))
    state["ready_at"] = utc_now()
    state.pop("ingestion_failures", None)
    atomic_json(state_path, state)
    return {"workspace_id": workspace, "documents": len(state["documents"]), "profile_id": state["profile_id"], "complete": True}


def load_records(path):
    records = []
    with open(path, encoding="utf-8") as stream:
        for number, line in enumerate(stream, 1):
            try:
                value = json.loads(line)
            except ValueError:
                raise EvaluationError(f"rankings line {number} is not complete JSON; retain evidence and repair explicitly") from None
            if not isinstance(value, dict):
                raise EvaluationError("ranking records must be objects")
            records.append(value)
    return records


def append_record(path, value):
    with open(path, "a", encoding="utf-8") as stream:
        stream.write(json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False) + "\n")
        stream.flush()


def rewrite_records(path, records):
    path = Path(path)
    temporary = path.with_name(path.name + ".rewrite.tmp")
    with open(temporary, "w", encoding="utf-8") as stream:
        for value in records:
            stream.write(json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False) + "\n")
        stream.flush()
    temporary.replace(path)


def summary_markdown(summary):
    lines = ["# Retrieval evaluation", "", f'Label: {summary.get("label", "offline")} · Mode: {summary.get("mode", "unknown")}', "",
             "| Metric | Value |", "|---|---:|"]
    lines += [f"| {name} | {value:.6f} |" for name, value in summary["metrics"].items()]
    coverage = summary["coverage"]
    lines += ["", f'Coverage: {coverage["successful"]}/{coverage["expected"]} successful; {coverage["failed"]} failed.',
              "", f'Latency ms: {json.dumps(summary["latency_ms"], sort_keys=True)}', "",
              "Unjudged documents are nonrelevant; failed queries remain in the metric denominator.",
              "Chunk hits are deduplicated by corpus document, preserving first occurrence. API chunk depth limits document recall.",
              "Test judgments are held out from optimization. Business relevance requires a separate, labeled business dataset.", ""]
    if "corpus_verification" in summary:
        verification = summary["corpus_verification"]
        lines += [f'Observed server execution: {summary.get("observed_execution")}.', "",
                  f'Corpus verification: {verification["mode"]}; before={verification["before"]["status"]}, '
                  f'after={verification["after"]["status"]}.', ""]
        if verification["mode"] == "heads":
            lines += ["Limited verification: document heads only; ready index generations were not checked.", ""]
    if summary.get("recorded_failed_attempts_before_retry"):
        lines += [f'Failed attempts retained before explicit retry: {summary["recorded_failed_attempts_before_retry"]}.', ""]
    if "observed_retrieval_policy_id" in summary:
        lines += [f'Retrieval policy: {summary["observed_retrieval_policy_id"]}.',
                  "Rerank outcomes: " + json.dumps(summary["rerank_outcomes"], sort_keys=True) + ".",
                  "Rerank model failures score zero and remain in the query denominator.", ""]
    return "\n".join(lines)


def evaluate(client, dataset, state_path, output, modes, label, chunk_limit=50, policy="fail", progress=print,
             retry_errors=False, verify_indexes=True, expected_execution=None, environment_file=None):
    if chunk_limit < 20 or chunk_limit > 50:
        raise EvaluationError("chunk limit must be 20..50 for Recall@20")
    if not modes or len(set(modes)) != len(modes) or not set(modes).issubset({"lexical", "vector", "hybrid"}):
        raise EvaluationError("modes must be unique lexical/vector/hybrid names")
    if not label or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for c in label):
        raise EvaluationError("label must contain only letters, numbers, underscore, or hyphen")
    if expected_execution not in (None, "baseline", "optimized", "qdrant_native"):
        raise EvaluationError("expected execution must be baseline, optimized or qdrant_native")
    evaluator_sha = evaluator_source_sha256()
    frozen_environment = environment_freeze(environment_file)
    expected_rerank = expected_rerank_mode(frozen_environment)
    # Preflight all queries; invalid/oversized inputs never disappear from a run.
    prepared = {row["_id"]: query_for_api(row["text"], policy) for row in dataset["queries"]}
    state = load_state(state_path, dataset)
    if len(state["documents"]) != len(dataset["corpus"]) or any(e.get("status") != "ready" for e in state["documents"].values()):
        raise EvaluationError("full corpus must be imported and ready before evaluation")
    reverse = {entry["document_id"]: cid for cid, entry in state["documents"].items()}
    if len(reverse) != len(state["documents"]):
        raise EvaluationError("state has duplicate remote document IDs")
    workspace = state["workspace_id"]
    client.request("GET", "/workspaces/" + workspace)
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    manifest = {"schema": 1, "dataset": dataset_info(dataset), "label": label, "modes": modes,
                "retrieval_policy_contract": POLICY_CONTRACT,
                "evaluator_source_sha256": evaluator_sha, "environment_freeze": frozen_environment,
                "chunk_limit": chunk_limit, "query_policy": policy, "truncated_queries": sum(v[1] for v in prepared.values()),
                "workspace_id": workspace, "profile_id": state["profile_id"], "mapping_sha256": digest_json(state["documents"]),
                "corpus_verification": "indexes" if verify_indexes else "heads", "expected_execution": expected_execution,
                "api_base": client.base, "python": platform.python_version(), "query_order": "dataset order, modes interleaved per query",
                "latency_note": "HTTP wall clock; cache/load/order are not controlled; no warmup excluded.",
                "quality_protocol": "Full corpus, split qrels only, no test-guided parameter selection."}
    manifest_path = output / (label + ".manifest.json")
    if manifest_path.exists():
        with open(manifest_path, encoding="utf-8") as stream:
            if json.load(stream) != manifest:
                raise EvaluationError("output manifest changed; use a new output directory/label")
    else:
        atomic_json(manifest_path, manifest)
    policy_path = output / (label + ".policy.json")
    frozen_policy = load_policy_freeze(policy_path, manifest)
    verification_before = verify_remote_corpus(client, state, verify_indexes, "before", progress)
    records, paths = {}, {}
    for mode in modes:
        paths[mode] = output / f"{label}.{mode}.rankings.jsonl"
        records[mode] = load_records(paths[mode]) if paths[mode].exists() else []
        ids = [r.get("query_id") for r in records[mode]]
        if len(ids) != len(set(ids)) or not set(ids).issubset(dataset["qrels"]):
            raise EvaluationError("existing rankings contain unknown or duplicate queries")
        verify_record_policy(records[mode], frozen_policy, expected_rerank)
        attempts_path = output / f"{label}.{mode}.failed-attempts.jsonl"
        if attempts_path.exists():
            verify_record_policy(load_records(attempts_path), frozen_policy, expected_rerank)
        if retry_errors:
            failures = [record for record in records[mode] if record.get("status") == "error"]
            for failure in failures:
                append_record(output / f"{label}.{mode}.failed-attempts.jsonl", failure)
            if failures:
                records[mode] = [record for record in records[mode] if record.get("status") != "error"]
                rewrite_records(paths[mode], records[mode])
    completed = {mode: {r["query_id"] for r in records[mode]} for mode in modes}
    previous_execution = {record.get("execution") for mode in modes for record in records[mode] if record.get("status") == "ok"}
    if (not previous_execution.issubset({"baseline", "optimized", "qdrant_native"}) or len(previous_execution) > 1 or
            (previous_execution and expected_execution and previous_execution != {expected_execution})):
        raise EvaluationError("existing rankings have missing, mixed, or unexpected execution strategy")
    observed_execution = next(iter(previous_execution)) if previous_execution else None
    for number, row in enumerate(dataset["queries"], 1):
        qid = row["_id"]
        query, truncated = prepared[qid]
        for mode in modes:
            if qid in completed[mode]:
                continue
            record = {"query_id": qid, "status": "ok", "ranking": [], "query_truncated": truncated,
                      "retrieval_policy_id": None,
                      "submitted_query_sha256": hashlib.sha256(query.encode()).hexdigest(), "attempted_at": utc_now()}
            started = time.perf_counter()
            try:
                response = client.request("POST", f"/workspaces/{workspace}/search", {"query": query, "mode": mode, "limit": chunk_limit})
                frozen_policy = freeze_response_policy(response, policy_path, manifest, frozen_policy)
                record["retrieval_policy_id"] = frozen_policy["retrieval_policy_id"]
                record["rerank"] = safe_rerank_diagnostics(response.get("rerank"))
                verify_rerank_mode(record["rerank"], expected_rerank)
                if response["profile_id"] != state["profile_id"] or response["mode"] != mode:
                    raise EvaluationError("search profile/mode differs from frozen evaluation manifest")
                execution = response.get("execution")
                record["execution"] = execution
                if execution not in ("baseline", "optimized", "qdrant_native"):
                    raise EvaluationError("search response lacks a valid execution strategy")
                if ((expected_execution and execution != expected_execution) or
                        (observed_execution and execution != observed_execution)):
                    raise EvaluationError("search execution strategy changed or differs from expected execution")
                observed_execution = execution
                if record["rerank"]["outcome"] == "model_error":
                    raise EvaluationError("rerank model failed; retrieval fallback is not a successful rerank evaluation")
                hits = response["hits"]
                for hit in hits:
                    corpus_id = reverse.get(hit["chunk"]["document_id"])
                    if corpus_id is None:
                        raise EvaluationError("search returned foreign corpus document")
                    entry = state["documents"][corpus_id]
                    if any(hit["chunk"][field] != entry[field] for field in ("version_id", "index_id", "source_sha256")):
                        raise EvaluationError("search returned changed corpus version, index, or source digest")
                record["ranking"] = deduplicate_documents(hits, reverse)
                record["chunk_hits"], record["document_hits"] = len(hits), len(record["ranking"])
                record["chunk_ranking"] = [{"corpus_id": reverse[hit["chunk"]["document_id"]],
                                            "chunk_id": hit["chunk"].get("id"), "score": hit.get("score")} for hit in hits]
            except RetrievalPolicyError:
                raise
            except EvaluationError as error:
                record.update(status="error", error=str(error))
            except (KeyError, TypeError, ValueError):
                record.update(status="error", error="invalid search response structure")
            record["request_id"] = getattr(client, "last_request_id", None)
            record["latency_ms"] = (time.perf_counter() - started) * 1000
            records[mode].append(record)
            append_record(paths[mode], record)
        if number % 25 == 0 or number == len(dataset["queries"]):
            progress(f'evaluate {label}: {number}/{len(dataset["queries"])} queries across {len(modes)} modes')
    verification_after = verify_remote_corpus(client, state, verify_indexes, "after", progress)
    if evaluator_source_sha256() != evaluator_sha or environment_freeze(environment_file) != frozen_environment:
        raise EvaluationError("evaluator source or external environment freeze changed during evaluation")
    if load_policy_freeze(policy_path, manifest) != frozen_policy:
        raise RetrievalPolicyError("retrieval policy freeze changed during evaluation")
    summaries = {}
    for mode in modes:
        summary = score_records(records[mode], dataset["qrels"])
        summary.update(label=label, mode=mode, manifest_sha256=digest_json(manifest), created_at=utc_now(), dataset=dataset_info(dataset))
        summary["observed_execution"] = observed_execution
        summary["observed_retrieval_policy_id"] = frozen_policy["retrieval_policy_id"] if frozen_policy else None
        summary["retrieval_policy_freeze_sha256"] = digest_json(frozen_policy) if frozen_policy else None
        summary["rerank_outcomes"] = dict(Counter(record.get("rerank", {}).get("outcome", "unavailable")
                                                  for record in records[mode]))
        summary["rerank_model_error_queries"] = summary["rerank_outcomes"].get("model_error", 0)
        summary["evaluator_source_sha256"] = evaluator_sha
        summary["environment_frozen"] = frozen_environment is not None
        summary["ranking_sha256"] = sha256_file(paths[mode])
        summary["corpus_verification"] = {"mode": manifest["corpus_verification"],
                                          "before": verification_before, "after": verification_after}
        attempts_path = output / f"{label}.{mode}.failed-attempts.jsonl"
        summary["recorded_failed_attempts_before_retry"] = len(load_records(attempts_path)) if attempts_path.exists() else 0
        atomic_json(output / f"{label}.{mode}.summary.json", summary)
        (output / f"{label}.{mode}.summary.md").write_text(summary_markdown(summary), encoding="utf-8")
        summaries[mode] = summary
    return summaries


def comparison_manifest(path, dataset):
    path = Path(path)
    suffix = ".rankings.jsonl"
    prefix = path.name[:-len(suffix)] if path.name.endswith(suffix) else ""
    if "." not in prefix:
        raise EvaluationError("comparison requires LABEL.MODE.rankings.jsonl and companion manifest")
    label, mode = prefix.rsplit(".", 1)
    manifest_path = path.parent / (label + ".manifest.json")
    if not manifest_path.is_file():
        raise EvaluationError("comparison requires both companion manifests; offline score remains independent")
    try:
        with open(manifest_path, encoding="utf-8") as stream:
            manifest = json.load(stream)
    except (OSError, ValueError):
        raise EvaluationError("comparison manifest is unreadable or invalid JSON") from None

    def hex_string(value, size):
        return isinstance(value, str) and len(value) == size and all(c in "0123456789abcdef" for c in value)

    if (not isinstance(manifest, dict) or type(manifest.get("schema")) is not int or manifest["schema"] != 1 or
            manifest.get("label") != label or not label or
            any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for c in label)):
        raise EvaluationError("comparison manifest schema or label is invalid")
    modes = manifest.get("modes")
    if (not isinstance(modes, list) or any(not isinstance(value, str) for value in modes) or
            not modes or len(modes) != len(set(modes)) or
            not set(modes).issubset({"lexical", "vector", "hybrid"}) or mode not in modes):
        raise EvaluationError("comparison manifest modes do not match ranking filename")
    if manifest.get("dataset") != dataset_info(dataset):
        raise EvaluationError("comparison ranking manifest belongs to different dataset/split or invalid input hashes/counts")
    if (not hex_string(manifest.get("profile_id"), 64) or not hex_string(manifest.get("mapping_sha256"), 64) or
            not hex_string(manifest.get("workspace_id"), 32) or not hex_string(manifest.get("evaluator_source_sha256"), 64)):
        raise EvaluationError("comparison manifest requires valid profile, mapping hash, and workspace identity")
    if "environment_freeze" not in manifest:
        raise EvaluationError("comparison manifest is missing environment freeze policy")
    freeze = manifest["environment_freeze"]
    if freeze is not None:
        if (not isinstance(freeze, dict) or set(freeze) != {"file_sha256", "content_sha256", "value"} or
                not hex_string(freeze.get("file_sha256"), 64) or not hex_string(freeze.get("content_sha256"), 64)):
            raise EvaluationError("comparison environment freeze hashes/schema are invalid")
        validate_environment(freeze["value"])
        if freeze["content_sha256"] != digest_json(freeze["value"]):
            raise EvaluationError("comparison environment freeze content digest is invalid")
    depth, policy, truncated = manifest.get("chunk_limit"), manifest.get("query_policy"), manifest.get("truncated_queries")
    if (type(depth) is not int or not 20 <= depth <= 50 or policy not in ("fail", "truncate_utf8") or
            type(truncated) is not int or not 0 <= truncated <= len(dataset["queries"])):
        raise EvaluationError("comparison manifest depth or query policy is invalid")
    expected_truncated = sum(query_for_api(row["text"], policy)[1] for row in dataset["queries"])
    if truncated != expected_truncated:
        raise EvaluationError("comparison manifest truncation count differs from frozen query policy")
    if manifest.get("corpus_verification") not in ("indexes", "heads") or "expected_execution" not in manifest or manifest["expected_execution"] not in (None, "baseline", "optimized", "qdrant_native"):
        raise EvaluationError("comparison manifest verification/execution policy is missing or invalid")
    api_base = manifest.get("api_base")
    if not isinstance(api_base, str):
        raise EvaluationError("comparison manifest API base is missing")
    parsed = urllib.parse.urlsplit(api_base)
    if parsed.scheme not in ("http", "https") or not parsed.netloc or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise EvaluationError("comparison manifest API base is invalid")
    for key in ("python", "query_order", "latency_note", "quality_protocol"):
        if not isinstance(manifest.get(key), str) or not manifest[key]:
            raise EvaluationError("comparison manifest is incomplete")
    summary_path = path.parent / (prefix + ".summary.json")
    if not summary_path.is_file():
        raise EvaluationError("comparison requires completed summaries with final corpus verification")
    try:
        with open(summary_path, encoding="utf-8") as stream:
            summary = json.load(stream)
    except (OSError, ValueError):
        raise EvaluationError("comparison summary is unreadable or invalid JSON") from None
    if (not isinstance(summary, dict) or summary.get("manifest_sha256") != digest_json(manifest) or
            summary.get("ranking_sha256") != sha256_file(path)):
        raise EvaluationError("comparison manifest/ranking digest differs from recorded summary")
    policy_contract = manifest.get("retrieval_policy_contract")
    if policy_contract is not None:
        if policy_contract != POLICY_CONTRACT:
            raise EvaluationError("comparison retrieval policy contract is unsupported")
        frozen_policy = load_policy_freeze(path.parent / (label + ".policy.json"), manifest, required=True)
        if (summary.get("observed_retrieval_policy_id") != frozen_policy["retrieval_policy_id"]
                or summary.get("retrieval_policy_freeze_sha256") != digest_json(frozen_policy)):
            raise EvaluationError("comparison summary differs from the frozen retrieval policy")
        verify_record_policy(load_records(path), frozen_policy, expected_rerank_mode(manifest.get("environment_freeze")))
    execution = summary.get("observed_execution")
    if execution not in ("baseline", "optimized", "qdrant_native") or (manifest["expected_execution"] and execution != manifest["expected_execution"]):
        raise EvaluationError("comparison summary has invalid or unexpected observed execution")
    verification = summary.get("corpus_verification")
    if not isinstance(verification, dict) or verification.get("mode") != manifest["corpus_verification"]:
        raise EvaluationError("comparison summary corpus verification mode is invalid")
    for phase in ("before", "after"):
        evidence = verification.get(phase)
        if (not isinstance(evidence, dict) or evidence.get("status") != "passed" or
                evidence.get("documents") != len(dataset["corpus"]) or
                evidence.get("indexes") != (len(dataset["corpus"]) if manifest["corpus_verification"] == "indexes" else 0) or
                not isinstance(evidence.get("checked_at"), str) or not evidence["checked_at"]):
            raise EvaluationError("comparison lacks complete before/after corpus verification evidence")
    return manifest, summary


def comparable_environments(baseline, candidate):
    if baseline == candidate:
        return True
    if baseline is None or candidate is None:
        return False
    invariants = []
    for freeze in (baseline, candidate):
        value = dict(freeze["value"])
        measured = dict(value["measured_environment"])
        # Allow only the explicitly recorded reranking treatment to vary.
        # Source, binary, embedding, physical corpus and chunking remain frozen.
        if expected_rerank_mode(freeze) is None:
            return False
        reranker = dict(measured["reranker"])
        for field in ("enabled", "configuration", "loaded_model"):
            reranker.pop(field, None)
        measured["reranker"] = reranker
        value["measured_environment"] = measured
        invariants.append(value)
    return invariants[0] == invariants[1]


def compare_runs(baseline_path, candidate_path, dataset, samples, seed):
    manifests, summaries = {}, {}
    for name, path in (("baseline", baseline_path), ("candidate", candidate_path)):
        manifests[name], summaries[name] = comparison_manifest(path, dataset)
    if any(manifests["baseline"][key] != manifests["candidate"][key]
           for key in ("profile_id", "mapping_sha256", "workspace_id", "query_policy", "chunk_limit", "corpus_verification",
                       "evaluator_source_sha256")):
        raise EvaluationError("paired comparison requires same frozen corpus/profile/query policy/chunk depth")
    if not comparable_environments(manifests["baseline"]["environment_freeze"], manifests["candidate"]["environment_freeze"]):
        raise EvaluationError("paired comparison requires the same frozen environment outside reranking treatment")
    if manifests["baseline"].get("retrieval_policy_contract") != manifests["candidate"].get("retrieval_policy_contract"):
        raise EvaluationError("paired comparison requires the same retrieval policy evidence contract")
    baseline, candidate = load_records(baseline_path), load_records(candidate_path)
    for name, records in (("baseline", baseline), ("candidate", candidate)):
        prepared = {row["_id"]: query_for_api(row["text"], manifests[name]["query_policy"]) for row in dataset["queries"]}
        for record in records:
            if record.get("execution") != summaries[name]["observed_execution"]:
                raise EvaluationError("ranking execution evidence differs from summary")
            query, truncated = prepared.get(record.get("query_id"), (None, None))
            if (query is None or record.get("query_truncated") != truncated or
                    record.get("submitted_query_sha256") != hashlib.sha256(query.encode()).hexdigest()):
                raise EvaluationError("ranking query digest differs from frozen input/policy")
    result = paired_compare(baseline, candidate, dataset["qrels"], samples, seed)
    result["ranking_parity"] = all(a["ranking"] == b["ranking"] for a, b in zip(
        sorted(baseline, key=lambda r: r["query_id"]), sorted(candidate, key=lambda r: r["query_id"])))
    result["chunk_ranking_parity"] = all(a.get("chunk_ranking") == b.get("chunk_ranking") for a, b in zip(
        sorted(baseline, key=lambda r: r["query_id"]), sorted(candidate, key=lambda r: r["query_id"])))
    result["dataset"] = dataset_info(dataset)
    result["ranking_sha256"] = {"baseline": sha256_file(baseline_path), "candidate": sha256_file(candidate_path)}
    result["manifests"] = manifests
    result["execution"] = {name: summary["observed_execution"] for name, summary in summaries.items()}
    result["retrieval_policy_ids"] = {name: summary.get("observed_retrieval_policy_id") for name, summary in summaries.items()}
    result["retrieval_policy_frozen"] = all(manifest.get("retrieval_policy_contract") == POLICY_CONTRACT
                                           for manifest in manifests.values())
    result["corpus_verification"] = {name: summary["corpus_verification"] for name, summary in summaries.items()}
    result["environment_frozen"] = manifests["baseline"]["environment_freeze"] is not None
    return result


def comparison_markdown(comparison):
    lines = ["# Paired retrieval comparison", "", f'Paired queries: {comparison["queries"]}; bootstrap samples: '
             f'{comparison["bootstrap_samples"]}; seed: {comparison["seed"]}.', "",
             "| Metric | Candidate − baseline | Bootstrap 95% CI |", "|---|---:|---|" ]
    for metric, item in comparison["delta_candidate_minus_baseline"].items():
        lines.append(f'| {metric} | {item["mean"]:.6f} | [{item["ci95"][0]:.6f}, {item["ci95"][1]:.6f}] |')
    lines += ["", f'Document ranking parity: {comparison["ranking_parity"]}; chunk ranking parity: {comparison["chunk_ranking_parity"]}.',
              "", "Observed execution: " + json.dumps(comparison["execution"], sort_keys=True),
              "", "Retrieval policies: " + json.dumps(comparison["retrieval_policy_ids"], sort_keys=True),
              "", "Corpus verification: " + json.dumps({name: item["mode"] for name, item in comparison["corpus_verification"].items()}, sort_keys=True),
              "", "Latency (ms): " + json.dumps(comparison["latency_ms"], sort_keys=True), "", comparison["latency_caveat"], ""]
    return "\n".join(lines)
