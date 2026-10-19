"""Strict BEIR readers; dataset validation happens before remote writes."""
import csv
import hashlib
import json
import unicodedata
from pathlib import Path


class EvaluationError(Exception):
    """A safe, actionable evaluation failure without remote payloads/secrets."""


CONTENT_POLICY = "beir-title-text-cc-space-v1"
ALLOWED_CONTENT_CONTROLS = "\n\r\t\f"


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def read_jsonl(path):
    rows = []
    seen = set()
    with open(path, encoding="utf-8") as stream:
        for number, line in enumerate(stream, 1):
            try:
                row = json.loads(line)
            except (ValueError, TypeError):
                raise EvaluationError(f"{path.name}:{number}: invalid JSON") from None
            if not isinstance(row, dict) or not isinstance(row.get("_id"), str) or not row["_id"]:
                raise EvaluationError(f"{path.name}:{number}: nonempty string _id required")
            if row["_id"] in seen:
                raise EvaluationError(f"{path.name}:{number}: duplicate _id")
            seen.add(row["_id"])
            rows.append(row)
    if not rows:
        raise EvaluationError(f"{path.name}: empty dataset")
    return rows


def corpus_text(row):
    return (row.get("title") or "") + "\n\n" + row["text"]


def disallowed_content_control(char):
    return unicodedata.category(char) == "Cc" and char not in ALLOWED_CONTENT_CONTROLS


def corpus_content(row):
    # Keep official files/IDs untouched. The production text parser accepts the
    # same four controls; a space preserves word boundaries at rejected controls.
    text = "".join(" " if disallowed_content_control(char) else char for char in corpus_text(row))
    return text.strip().encode("utf-8")


def load_dataset(directory, split):
    root = Path(directory)
    if not split or Path(split).name != split:
        raise EvaluationError("split must be a filename component")
    corpus_path, query_path = root / "corpus.jsonl", root / "queries.jsonl"
    qrels_path = root / "qrels" / (split + ".tsv")
    corpus = read_jsonl(corpus_path)
    queries = read_jsonl(query_path)
    content_hashes, changed_documents, changed_characters = {}, 0, 0
    for row in corpus:
        if not isinstance(row.get("text"), str) or not isinstance(row.get("title", ""), str):
            raise EvaluationError("corpus title/text must be strings")
        content = corpus_content(row)
        if not content or len(content) > (1 << 20):
            raise EvaluationError("corpus contains empty or over-1MiB extracted document")
        content_hashes[row["_id"]] = hashlib.sha256(content).hexdigest()
        replacements = sum(disallowed_content_control(char) for char in corpus_text(row))
        changed_documents += int(replacements > 0)
        changed_characters += replacements
    for row in queries:
        if not isinstance(row.get("text"), str) or not row["text"].strip():
            raise EvaluationError("query text must be a nonempty string")
    corpus_ids, query_ids = {r["_id"] for r in corpus}, {r["_id"] for r in queries}
    qrels = {}
    with open(qrels_path, encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream, delimiter="\t")
        if reader.fieldnames != ["query-id", "corpus-id", "score"]:
            raise EvaluationError("qrels header must be query-id, corpus-id, score (TSV)")
        for row in reader:
            qid, did = row.get("query-id"), row.get("corpus-id")
            if qid not in query_ids or did not in corpus_ids:
                raise EvaluationError("qrels references unknown query/corpus ID")
            try:
                grade = int(row["score"])
            except (TypeError, ValueError):
                raise EvaluationError("qrels score must be an integer") from None
            if grade < 0 or did in qrels.get(qid, {}):
                raise EvaluationError("qrels has negative grade or duplicate query/document pair")
            qrels.setdefault(qid, {})[did] = grade
    if not qrels or any(not any(v > 0 for v in rel.values()) for rel in qrels.values()):
        raise EvaluationError("every evaluated query needs at least one positive judgment")
    selected = [r for r in queries if r["_id"] in qrels]
    return {
        "corpus": corpus, "queries": selected, "qrels": qrels, "split": split,
        "hashes": {"corpus": sha256_file(corpus_path), "queries": sha256_file(query_path),
                   "qrels": sha256_file(qrels_path)},
        "counts": {"corpus": len(corpus), "queries_file": len(queries), "evaluated_queries": len(selected),
                   "judgments": sum(len(v) for v in qrels.values())},
        "content_policy": CONTENT_POLICY,
        "normalized_content_sha256": hashlib.sha256(json.dumps(content_hashes, sort_keys=True, ensure_ascii=False,
                                                                 separators=(",", ":")).encode("utf-8")).hexdigest(),
        "control_normalization": {"documents": changed_documents, "characters": changed_characters},
    }


def query_for_api(text, policy, max_bytes=512):
    value = text.strip()
    if any(ord(c) < 32 or 127 <= ord(c) <= 159 for c in value):
        raise EvaluationError("query contains API-disallowed control characters")
    changed = len(value.encode("utf-8")) > max_bytes
    if changed:
        if policy != "truncate_utf8":
            raise EvaluationError(f"query exceeds API {max_bytes}-byte limit; select explicit truncate_utf8 policy")
        value = value.encode("utf-8")[:max_bytes].decode("utf-8", errors="ignore").rstrip()
    if len(value) < 2:
        raise EvaluationError("query is shorter than API minimum of two characters")
    return value, changed


def dataset_info(dataset):
    lengths = [len(r["text"].strip().encode("utf-8")) for r in dataset["queries"]]
    return {"split": dataset["split"], "input_sha256": dataset["hashes"], "counts": dataset["counts"],
            "content_policy": dataset["content_policy"], "normalized_content_sha256": dataset["normalized_content_sha256"],
            "control_normalization": dataset["control_normalization"],
            "query_max_utf8_bytes": max(lengths), "queries_over_512_bytes": sum(n > 512 for n in lengths)}
