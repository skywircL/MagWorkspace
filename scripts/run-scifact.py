#!/usr/bin/env python3
"""Run the full frozen SciFact benchmark, reusing durable ingestion state."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
RUNTIME = ROOT / ".cache/retrieval-evaluation-qdrant"


def run(*arguments):
    subprocess.run([sys.executable, *map(str, arguments)], cwd=ROOT, check=True)


def base_url():
    return json.loads((RUNTIME / "endpoints.json").read_text())["backend:8080"]


def source_fingerprint():
    production = sorted(p for folder in (ROOT / "cmd", ROOT / "internal") for p in folder.rglob("*.go") if not p.name.endswith("_test.go"))
    evaluator = sorted(p for p in (ROOT / "scripts/retrieval_eval").glob("*.py") if not p.name.startswith("test"))
    sql = sorted((ROOT / "internal").rglob("*.sql"))
    packaging = sorted(p for folder in (ROOT / "docker", ROOT / "bridges", ROOT / "observability") if folder.exists() for p in folder.rglob("*") if p.is_file() and not any(part in (".deps", "__pycache__") for part in p.parts) and (p.suffix in (".py", ".yaml", ".yml", ".txt", ".xml") or p.name == "Dockerfile"))
    schedulers = [ROOT / "scripts" / name for name in ("run-scifact.py", "evaluation-stack.py", "download-scifact.py")]
    files = [ROOT / "go.mod", ROOT / "go.sum", ROOT / "Dockerfile", ROOT / ".dockerignore", ROOT / "compose.yaml", ROOT / "compose.observability.yaml", *production, *evaluator, *sql, *packaging, *schedulers]
    return {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in files}


def ingestion_complete(state, corpus_ids):
    entries = state.get("documents", {})
    profile = state.get("profile_id")
    return bool(state.get("ready_at") and not state.get("ingestion_failures") and profile and
                set(entries) == corpus_ids and all(row.get("status") == "ready" and row.get("profile_id") == profile and
                row.get("index_id") and row.get("generation") for row in entries.values()))


def persist_freeze(path, value):
    if path.exists():
        if json.loads(path.read_text()) != value:
            raise ValueError("frozen benchmark source/model/environment changed; use a new output directory")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + "." + str(os.getpid()) + ".tmp")
    with temporary.open("x") as stream:
        json.dump(value, stream, sort_keys=True, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def index_request(state, suffix, method="GET", timeout=30):
    profile = state["profile_id"]
    if len(profile) != 64 or any(c not in "0123456789abcdef" for c in profile):
        raise ValueError("invalid physical index profile")
    endpoint = json.loads((RUNTIME / "endpoints.json").read_text())["elasticsearch:9200"]
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    request = urllib.request.Request(endpoint + "/mw_" + profile + suffix,
                                     data=b"" if method == "POST" else None, method=method)
    with opener.open(request, timeout=timeout) as response:
        return json.load(response)


def index_shape(state):
    name = "mw_" + state["profile_id"]
    response = index_request(state, "/_segments")
    shards = response["indices"][name]["shards"]
    shape = {}
    for shard, copies in sorted(shards.items()):
        primaries = [copy for copy in copies if copy["routing"]["primary"]]
        if len(primaries) != 1 or primaries[0]["num_search_segments"] != 1:
            raise ValueError("benchmark physical index is not a single searchable segment per primary")
        segments = primaries[0]["segments"]
        if len(segments) != 1 or not all(segment["search"] for segment in segments.values()):
            raise ValueError("benchmark segment visibility changed")
        shape[shard] = {segment: {key: info[key] for key in ("generation", "num_docs", "deleted_docs")}
                        for segment, info in sorted(segments.items())}
    if not shape:
        raise ValueError("benchmark physical index has no primary shards")
    statistics = index_request(state, "/_stats")
    if statistics["indices"][name]["primaries"]["merges"]["current"] != 0:
        raise ValueError("benchmark physical index is still merging")
    return shape


def prepare_index(state, output):
    if (output / "environment.freeze.json").exists():
        return  # A resumed experiment must verify its old graph, never repair it.
    if list(output.glob("*.rankings.jsonl")) or list(output.glob("*.manifest.json")):
        raise ValueError("existing results lack a physical index freeze; use a new output directory")
    started = time.monotonic()
    result = index_request(state, "/_forcemerge?max_num_segments=1&wait_for_completion=true", "POST", 900)
    if result["_shards"]["failed"] != 0:
        raise ValueError("benchmark force merge failed")
    result = index_request(state, "/_refresh", "POST")
    if result["_shards"]["failed"] != 0:
        raise ValueError("benchmark refresh failed")
    shape = index_shape(state)
    output.mkdir(parents=True, exist_ok=True)
    (output / "index-preparation.json").write_text(json.dumps({
        "method": "force_merge_single_segment_before_queries", "duration_seconds": time.monotonic() - started,
        "primary_segments": shape, "completed_at": datetime.datetime.now(datetime.timezone.utc).isoformat()}, indent=2) + "\n")


def measured_environment(state):
    endpoints = json.loads((RUNTIME / "endpoints.json").read_text())
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(endpoints["ollama:11434"] + "/api/tags", timeout=15) as response:
        models = json.load(response)["models"]
    selected = [m for m in models if m["name"] == "bge-m3:567m"]
    if len(selected) != 1:
        raise ValueError("expected embedding model is unavailable")
    settings = next(iter(index_request(state, "/_settings").values()))["settings"]["index"]
    return {"embedding_model": {"name": selected[0]["name"], "digest": selected[0]["digest"], "details": selected[0].get("details", {})},
            "index_settings": {key: settings.get(key, "default") for key in ("refresh_interval", "number_of_shards", "number_of_replicas")},
            "physical_index": {"uuid": settings["uuid"], "preparation": "single_segment_before_queries", "primary_segments": index_shape(state)}}


def qdrant_request(state, suffix="", body=None):
    profile = state["profile_id"]
    if len(profile) != 64 or any(c not in "0123456789abcdef" for c in profile):
        raise ValueError("invalid physical index profile")
    endpoint = json.loads((RUNTIME / "endpoints.json").read_text())["qdrant:6333"]
    credentials = dict(line.split("=", 1) for line in (RUNTIME / "stack.env").read_text().splitlines() if line and not line.startswith("#"))
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    request = urllib.request.Request(endpoint + "/collections/mw_" + profile + suffix,
        data=None if body is None else json.dumps(body).encode(), headers={"Content-Type": "application/json", "api-key": credentials["QDRANT_API_KEY"]})
    with opener.open(request, timeout=60) as response:
        value = json.load(response)
    if value.get("status") != "ok":
        raise ValueError("Qdrant evaluation request failed")
    return value["result"]


def qdrant_environment(state):
    info = qdrant_request(state)
    if info.get("status") != "green" or info.get("optimizer_status") != "ok":
        raise ValueError("Qdrant collection is not ready for a frozen evaluation")
    digest = hashlib.sha256()
    offset, count = None, 0
    while True:
        body = {"limit": 64, "with_payload": True, "with_vector": True}
        if offset is not None:
            body["offset"] = offset
        page = qdrant_request(state, "/points/scroll", body)
        for point in page["points"]:
            digest.update(json.dumps(point, sort_keys=True, separators=(",", ":")).encode())
            count += 1
        offset = page.get("next_page_offset")
        if offset is None:
            break
    if count != info.get("points_count"):
        raise ValueError("Qdrant point count changed during evaluation freeze")
    endpoint = json.loads((RUNTIME / "endpoints.json").read_text())["ollama:11434"]
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(endpoint + "/api/tags", timeout=15) as response:
        models = json.load(response)["models"]
    selected = [m for m in models if m["name"] == "bge-m3:567m"]
    if len(selected) != 1:
        raise ValueError("expected embedding model unavailable")
    return {"engine": "qdrant_native", "embedding_model": {"name":selected[0]["name"], "digest":selected[0]["digest"]},
        "config": info["config"], "points_count":count, "points_sha256":digest.hexdigest(),
        "segments_count":info["segments_count"], "indexed_vectors_count":info["indexed_vectors_count"],
        "scope":"logical points/config and aggregate optimizer state; not a byte-identical HNSW snapshot"}


def application_sha256():
    values = dict(line.split("=", 1) for line in (RUNTIME / "stack.env").read_text().splitlines() if line and not line.startswith("#"))
    container = values["COMPOSE_PROJECT_NAME"] + "-backend-1"
    result = subprocess.run(["docker", "exec", container, "sha256sum", "/app/magworkspace"],
                            check=True, capture_output=True, text=True, timeout=30)
    fields = result.stdout.strip().split()
    if len(fields) != 2 or fields[1] != "/app/magworkspace":
        raise ValueError("could not verify application binary")
    digest = fields[0]
    if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
        raise ValueError("could not verify application binary")
    return digest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wait-for-existing-ingest", action="store_true", help="wait for a separately running ingest instead of starting a second process")
    parser.add_argument("--output", type=Path, default=ROOT / "artifacts/evaluations/scifact-qdrant")
    args = parser.parse_args()
    args.output = args.output.resolve()
    dataset, state, auth = ROOT / ".cache/datasets/scifact", RUNTIME / "scifact-state.json", RUNTIME / "scifact-auth.json"
    run("scripts/download-scifact.py")
    corpus_ids = {json.loads(line)["_id"] for line in (dataset / "corpus.jsonl").read_text().splitlines()}
    if args.wait_for_existing_ingest:
        deadline = time.monotonic() + 24 * 3600
        while True:
            current = json.loads(state.read_text())
            ready = sum(row.get("status") == "ready" for row in current["documents"].values())
            print(f"waiting for full ingestion: {ready}/{len(current['documents'])}", flush=True)
            if ingestion_complete(current, corpus_ids):
                break
            if any(row.get("status") == "failed" for row in current["documents"].values()):
                raise ValueError("index failure requires explicit inspection/retry")
            if time.monotonic() >= deadline:
                raise ValueError("ingestion wait expired; state retained")
            time.sleep(60)
    else:
        run("scripts/evaluation-stack.py", "up", "--observability", "--execution", "qdrant_native")
        run("scripts/evaluation-stack.py", "register")
        run("-u", "-m", "scripts.retrieval_eval", "ingest", "--dataset", dataset, "--split", "test",
            "--state", state, "--auth-file", auth, "--base-url", base_url(), "--workers", "2", "--wait-timeout", "86400", "--poll-interval", "10")
    # Rebuild after ingestion so a long indexing run cannot evaluate stale code.
    frozen_sources = source_fingerprint()
    ready_state = json.loads(state.read_text())
    if not ingestion_complete(ready_state, corpus_ids):
        raise ValueError("full ingestion state is not complete")
    frozen_environment = qdrant_environment(ready_state)
    tree_hash = hashlib.sha256(json.dumps(frozen_sources, sort_keys=True).encode()).hexdigest()
    run("scripts/evaluation-stack.py", "up", "--observability", "--execution", "qdrant_native")
    if source_fingerprint() != frozen_sources:
        raise ValueError("source changed while building the frozen benchmark")
    frozen_binary = application_sha256()
    freeze = {"schema": 1, "source_sha256": tree_hash, "measured_environment": {
        **frozen_environment, "application_sha256": frozen_binary, "source_files": frozen_sources,
        "corpus_sha256": ready_state["corpus_sha256"], "profile_id": ready_state["profile_id"]}}
    freeze_path = args.output / "environment.freeze.json"
    persist_freeze(freeze_path, freeze)
    common = ("--dataset", dataset, "--split", "test", "--state", state, "--auth-file", auth, "--output", args.output, "--environment-file", freeze_path)
    run("-u", "-m", "scripts.retrieval_eval", "run", *common, "--base-url", base_url(),
        "--label", "qdrant", "--expected-execution", "qdrant_native", "--modes", "lexical,vector,hybrid")
    for name, before, after in (("hybrid-vs-lexical", "qdrant.lexical", "qdrant.hybrid"),
                                ("hybrid-vs-vector", "qdrant.vector", "qdrant.hybrid")):
        run("-m", "scripts.retrieval_eval", "compare", "--dataset", dataset, "--split", "test",
            "--baseline", args.output / (before + ".rankings.jsonl"),
            "--candidate", args.output / (after + ".rankings.jsonl"), "--output", args.output / (name + ".json"))
    if source_fingerprint() != frozen_sources or application_sha256() != frozen_binary or qdrant_environment(json.loads(state.read_text())) != frozen_environment:
        raise ValueError("source/model/index environment changed during benchmark; do not accept comparison")
    report = {"completed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(), "source_sha256": tree_hash,
              "source_files": frozen_sources, "application_sha256": frozen_binary,
              "evaluation_stack": json.loads((RUNTIME / "settings.json").read_text()),
              "measured_environment": frozen_environment,
              "latency_limit": "Sequential runs; quality paired bootstrap is valid, latency remains descriptive under changing cache/load/order."}
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "environment.json").write_text(json.dumps(report, indent=2) + "\n")
    print("Full SciFact benchmark completed; reports: " + str(args.output), flush=True)


if __name__ == "__main__":
    try:
        main()
    except (subprocess.CalledProcessError, OSError, ValueError, KeyError):
        print("SciFact benchmark stopped before completion; retain state/results, inspect the preceding safe error, then resume.", file=sys.stderr)
        sys.exit(1)
