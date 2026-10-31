#!/usr/bin/env python3
"""Resume the pinned public reranker into a Docker-mounted Hugging Face cache.

Runs with Python's standard library inside a disposable container. It downloads
only inference files and validates published LFS SHA256 or Git blob SHA1 before
publishing the snapshot. Interrupted .part files are retained for a later run.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import re
import time
import urllib.error
import urllib.request

MODEL = "BAAI/bge-reranker-base"
REVISION = "2cfc18c9415c912f9d8155881c133215df768a70"
FILES = (
    "config.json",
    "tokenizer_config.json",
    "special_tokens_map.json",
    "tokenizer.json",
    "sentencepiece.bpe.model",
    "model.safetensors",
)


def valid_file(path, metadata):
    if not path.is_file() or path.stat().st_size != metadata["size"]:
        return False
    lfs = metadata.get("lfs")
    digest = hashlib.sha256() if lfs else hashlib.sha1()
    if not lfs:
        digest.update(f"blob {metadata['size']}\0".encode())
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(4 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest() == (lfs["sha256"] if lfs else metadata["blobId"])


def download(target, metadata, opener, *, model=MODEL, revision=REVISION):
    if valid_file(target, metadata):
        print(json.dumps({"file": target.name, "status": "cached"}), flush=True)
        return
    part = target.with_suffix(target.suffix + ".part")
    if part.exists() and part.stat().st_size > metadata["size"]:
        raise RuntimeError("partial file exceeds published file size")
    for attempt in range(1, 21):
        offset = part.stat().st_size if part.exists() else 0
        if offset == metadata["size"] and valid_file(part, metadata):
            part.replace(target)
            return
        # Resolve a fresh signed download URL each attempt; expired CDN URLs are
        # never persisted, printed, or used as the durable download identity.
        request = urllib.request.Request(
            f"https://huggingface.co/{model}/resolve/{revision}/{target.name}",
            headers={"Range": f"bytes={offset}-"} if offset else {},
        )
        try:
            with opener.open(request, timeout=60) as response:
                if offset and response.status == 206:
                    if not response.headers.get("Content-Range", "").startswith(f"bytes {offset}-"):
                        raise RuntimeError("unexpected partial content range")
                    mode = "ab"
                elif response.status == 200:
                    mode, offset = "wb", 0
                else:
                    raise RuntimeError("unexpected download status")
                reported = time.monotonic()
                with part.open(mode) as stream:
                    while block := response.read(1024 * 1024):
                        offset += len(block)
                        if offset > metadata["size"]:
                            raise RuntimeError("download exceeds published file size")
                        stream.write(block)
                        if time.monotonic() - reported >= 30:
                            stream.flush()
                            print(json.dumps({"file": target.name, "downloaded": offset, "total": metadata["size"]}), flush=True)
                            reported = time.monotonic()
            if part.stat().st_size < metadata["size"]:
                # Some HTTP transports end a ranged stream without surfacing
                # IncompleteRead. Preserve those bytes and request the remainder.
                raise OSError("incomplete download")
            if not valid_file(part, metadata):
                raise RuntimeError("download checksum mismatch")
            part.replace(target)
            print(json.dumps({"file": target.name, "status": "verified", "bytes": metadata["size"]}), flush=True)
            return
        except (OSError, urllib.error.URLError) as exc:
            # Error codes identify connectivity failures without signed URL or
            # proxy credential leakage from urllib's exception representation.
            print(json.dumps({"file": target.name, "attempt": attempt, "status": "retry", "error_type": type(exc).__name__}), flush=True)
            if attempt == 20:
                raise RuntimeError("download retries exhausted") from None
            time.sleep(min(3 * attempt, 30))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache", default="/data")
    parser.add_argument("--model", default=MODEL)
    parser.add_argument("--revision", default=REVISION)
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_-]+/[A-Za-z0-9_.-]+", args.model):
        parser.error("model must be a public owner/name repository")
    if not re.fullmatch(r"[a-f0-9]{40}", args.revision):
        parser.error("revision must be a pinned 40-character commit")
    os.umask(0o022)
    cache = pathlib.Path(args.cache) / ("models--" + args.model.replace("/", "--"))
    snapshot = cache / "snapshots" / args.revision
    snapshot.mkdir(parents=True, exist_ok=True)
    opener = urllib.request.build_opener()
    with opener.open(f"https://huggingface.co/api/models/{args.model}/revision/{args.revision}?blobs=true", timeout=60) as response:
        metadata = json.load(response)
    if metadata["sha"] != args.revision:
        raise RuntimeError("model revision does not match")
    files = {item["rfilename"]: item for item in metadata["siblings"]}
    for filename in FILES:
        download(snapshot / filename, files[filename], opener, model=args.model, revision=args.revision)
    refs = cache / "refs"
    refs.mkdir(exist_ok=True)
    (refs / args.revision).write_text(args.revision)
    print(json.dumps({"model": args.model, "revision": args.revision, "status": "complete"}), flush=True)


if __name__ == "__main__":
    main()
