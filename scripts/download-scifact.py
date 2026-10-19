#!/usr/bin/env python3
"""Download the official BEIR SciFact archive with a fixed content checksum."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import sys
import urllib.request
import zipfile

URL = "https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/scifact.zip"
MD5 = "5f7d1de60b170fc8027bb7898e2efca1"  # Published by beir-cellar/beir.
SHA256 = {
    "corpus.jsonl": "dec31c8182f3d744c7d2c09423756fd1d17cbef75808db13ba01cc0aab4d1ac6",
    "queries.jsonl": "8ff84a7c903f722981cd8d595c022660140c51867b27608a6d4910db86080313",
    "qrels/train.tsv": "a53f2114831916c096b6c37d9e54da68cef4efdcdbd5ed46533601af972acf1d",
    "qrels/test.tsv": "0864bb985e0ca2367ba217977e72004d549054b2b06666ed9d4825ac7c21284c",
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path(".cache/datasets"))
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    archive = args.output / "scifact.zip"
    if not archive.exists():
        temporary = archive.with_suffix(".zip.partial")
        with urllib.request.urlopen(URL, timeout=120) as source, temporary.open("wb") as target:
            copied = 0
            while chunk := source.read(1 << 20):
                copied += len(chunk)
                if copied > 16 << 20:
                    raise ValueError("archive exceeds download limit")
                target.write(chunk)
        if hashlib.md5(temporary.read_bytes()).hexdigest() != MD5:
            raise ValueError("archive checksum differs from official BEIR checksum")
        temporary.replace(archive)
    if hashlib.md5(archive.read_bytes()).hexdigest() != MD5:
        raise ValueError("cached archive checksum differs from official BEIR checksum")
    with zipfile.ZipFile(archive) as source:
        for name, expected in SHA256.items():
            entry = "scifact/" + name
            data = source.read(entry)
            if hashlib.sha256(data).hexdigest() != expected:
                raise ValueError("dataset entry checksum mismatch")
            destination = args.output / entry
            if destination.exists():
                if hashlib.sha256(destination.read_bytes()).hexdigest() != expected:
                    raise ValueError("existing dataset was modified; refusing overwrite")
                continue
            destination.parent.mkdir(parents=True, exist_ok=True)
            with source.open(entry) as member, destination.open("xb") as target:
                shutil.copyfileobj(member, target)
    print(json.dumps({"source": URL, "archive_md5": MD5, "sha256": SHA256,
                      "dataset": str(args.output / "scifact")}, indent=2))


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError, zipfile.BadZipFile):
        print("SciFact download/verification failed; cached files retained for inspection.", file=sys.stderr)
        sys.exit(1)
