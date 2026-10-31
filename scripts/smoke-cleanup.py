"""Verify physical resource cleanup in an explicit disposable indexing stack.

prepare creates one synthetic ready document and saves private fixture state.
After restarting backend with DOCUMENT_INDEXING_ENABLED=false, verify deletes
its Workspace and checks both initial deletion and late-write retirement scans.
Only Python's standard library and the stack's existing mysql CLI are required.
"""

import argparse
import base64
import datetime
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


def check(condition, message):
    if not condition:
        raise RuntimeError(message)


def literal(value):
    """A SQL string encoded as bytes, without shell or SQL interpolation rules."""
    return "0x" + value.encode("utf-8").hex()


class Fixture:
    def __init__(self, args):
        self.args = args
        check(re.fullmatch(r"mw-index-test-[a-z0-9-]+", args.project),
              "cleanup smoke requires a disposable mw-index-test-* project")
        self.root = Path(__file__).resolve().parent.parent
        self.compose = ["docker", "compose", "--project-name", args.project,
                        "--env-file", str(Path(args.env_file).resolve()), "--profile", "app"]
        for filename in args.compose_file:
            self.compose.extend(["-f", str(Path(filename).resolve())])
        self.config = {}
        for line in Path(args.env_file).read_text().splitlines():
            key, separator, value = line.partition("=")
            if separator and not key.startswith("#"):
                self.config[key] = value
        self.client = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        self.base = args.base_url.rstrip("/")
        check(self.base == self.service_url("backend", 8080),
              "base URL is not this disposable project's published backend")
        self.minio = self.service_url("minio", 9000)
        self.qdrant = self.service_url("qdrant", 6333)
        self.token = ""
        for key in ("MINIO_ACCESS_KEY", "MINIO_SECRET_KEY", "MINIO_BUCKET", "QDRANT_API_KEY"):
            check(self.config.get(key), "isolated environment is missing a required storage setting")

    def command(self, arguments, data=None):
        result = subprocess.run(self.compose + arguments, input=data, cwd=self.root,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=45)
        check(result.returncode == 0, "disposable stack command failed")
        return result.stdout.decode("utf-8").strip()

    def service_url(self, service, port):
        endpoint = self.command(["port", service, str(port)])
        check(re.fullmatch(r"127\.0\.0\.1:[0-9]+", endpoint),
              "test service must expose exactly one loopback endpoint")
        return "http://" + endpoint

    def indexing_is(self, value):
        actual = self.command(["exec", "-T", "backend", "sh", "-ec",
                               'printf "%s" "$DOCUMENT_INDEXING_ENABLED"'])
        check(actual == value, "backend indexing mode differs from the requested smoke phase")

    def sql(self, statement):
        # Credentials remain in the container environment, never command arguments.
        return self.command(["exec", "-T", "mysql", "sh", "-ec",
                             'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql --protocol=TCP '
                             '-h127.0.0.1 -uroot -Dmagworkspace --batch --raw --skip-column-names'],
                            (statement + ";\n").encode())

    def http(self, method, url, body=None, headers=None, expected=(200,)):
        request = urllib.request.Request(url, data=body, headers=headers or {}, method=method)
        try:
            response = self.client.open(request, timeout=40)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            data = response.read()
            check(response.status in expected, f"HTTP {method} returned unexpected status {response.status}")
            return response.status, data

    def api(self, method, path, body=None, media="application/json", expected=200):
        headers = {"Content-Type": media}
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        if body is not None and not isinstance(body, bytes):
            body = json.dumps(body).encode()
        _, data = self.http(method, self.base + "/api/v1" + path, body, headers, (expected,))
        return json.loads(data)["data"] if data else None

    def s3(self, method, key, body=b"", expected=(200,)):
        path = "/" + self.config["MINIO_BUCKET"] + "/" + urllib.parse.quote(key, safe="/-_.~")
        stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        day, payload_hash = stamp[:8], hashlib.sha256(body).hexdigest()
        host = urllib.parse.urlsplit(self.minio).netloc
        canonical_headers = ("host:" + host + "\nx-amz-content-sha256:" + payload_hash
                             + "\nx-amz-date:" + stamp + "\n")
        signed_headers = "host;x-amz-content-sha256;x-amz-date"
        canonical = "\n".join([method, path, "", canonical_headers, signed_headers, payload_hash])
        scope = day + "/us-east-1/s3/aws4_request"
        to_sign = "AWS4-HMAC-SHA256\n" + stamp + "\n" + scope + "\n" + hashlib.sha256(canonical.encode()).hexdigest()
        signing_key = ("AWS4" + self.config["MINIO_SECRET_KEY"]).encode()
        for part in (day, "us-east-1", "s3", "aws4_request"):
            signing_key = hmac.new(signing_key, part.encode(), hashlib.sha256).digest()
        signature = hmac.new(signing_key, to_sign.encode(), hashlib.sha256).hexdigest()
        headers = {"x-amz-date": stamp, "x-amz-content-sha256": payload_hash,
                   "Authorization": "AWS4-HMAC-SHA256 Credential=" + self.config["MINIO_ACCESS_KEY"]
                   + "/" + scope + ", SignedHeaders=" + signed_headers + ", Signature=" + signature}
        return self.http(method, self.minio + path, body if method == "PUT" else None, headers, expected)

    def vector(self, method, profile, suffix, body):
        _, raw = self.http(method, self.qdrant + "/collections/mw_" + profile + suffix,
                           json.dumps(body, allow_nan=False).encode(),
                           {"Content-Type": "application/json", "api-key": self.config["QDRANT_API_KEY"]})
        response = json.loads(raw)
        check(response.get("status") == "ok", "vector operation did not succeed")
        return response["result"]

    @staticmethod
    def generation_filter(state):
        return {"must": [{"key": "generation_id", "match": {"value": state["generation"]}}]}

    def counts(self, state):
        obj = "storage_id=" + literal(state["object_storage_id"]) + " AND object_key=" + literal(state["object_key"])
        index = ("storage_id=" + literal(state["index_storage_id"]) + " AND profile_id="
                 + literal(state["profile_id"]) + " AND generation=" + literal(state["generation"]))
        retired = ("(kind='object' AND storage_id=" + literal(state["object_storage_id"])
                   + " AND resource_key=" + literal(state["object_key"]) + ") OR (kind='index' AND storage_id="
                   + literal(state["index_storage_id"]) + " AND profile_id=" + literal(state["profile_id"])
                   + " AND resource_key=" + literal(state["generation"]) + ")")
        return json.loads(self.sql("SELECT JSON_OBJECT('objects',(SELECT COUNT(*) FROM object_cleanup WHERE "
                                   + obj + "),'indexes',(SELECT COUNT(*) FROM index_cleanup WHERE " + index
                                   + "),'retired',(SELECT COUNT(*) FROM cleanup_retirements WHERE " + retired + "))"))

    def wait_deleted(self, state):
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            object_status, _ = self.s3("HEAD", state["object_key"], expected=(200, 404))
            vectors = self.vector("POST", state["profile_id"], "/points/count",
                                  {"filter": self.generation_filter(state), "exact": True})["count"]
            counts = self.counts(state)
            if object_status == 404 and vectors == 0 and counts == {"objects": 0, "indexes": 0, "retired": 2}:
                return
            time.sleep(2)
        raise RuntimeError("workers did not physically delete resources and retain both retirement records")

    def prepare(self):
        self.indexing_is("true")
        account = self.api("POST", "/auth/register", {"name": "Cleanup smoke", "email": "cleanup-"
                           + uuid.uuid4().hex + "@example.test", "password": uuid.uuid4().hex}, expected=201)
        self.token = account["token"]
        workspace = account["workspaces"][0]["id"]
        check(re.fullmatch(r"[0-9a-f]{32}", workspace), "invalid synthetic workspace identifier")
        source = "# 清理验收\n删除知识库资源后必须回收原文和向量，迟到写入也必须再次清理。\n".encode()
        boundary = uuid.uuid4().hex
        body = ("--" + boundary + '\r\nContent-Disposition: form-data; name="file"; filename="cleanup.md"'
                + "\r\nContent-Type: text/markdown\r\n\r\n").encode() + source + ("\r\n--" + boundary + "--\r\n").encode()
        try:
            write = self.api("POST", "/workspaces/" + workspace + "/documents", body,
                             "multipart/form-data; boundary=" + boundary, 201)
            document, version = write["document"]["id"], write["version"]["id"]
            path = "/workspaces/" + workspace + "/documents/" + document + "/versions/" + version
            deadline = time.monotonic() + 330
            while True:
                job = self.api("GET", path + "/index")
                if job["status"] == "ready":
                    break
                check(job["status"] != "failed" and time.monotonic() < deadline,
                      "synthetic document did not become ready")
                time.sleep(1)
            state = json.loads(self.sql("SELECT JSON_OBJECT('object_key',v.object_key,'object_storage_id',v.storage_id,"
                                        "'index_storage_id',i.storage_id,'profile_id',i.profile_id,'generation',i.generation) "
                                        "FROM document_versions v JOIN document_indexes i ON i.version_id=v.id "
                                        "WHERE v.id=" + literal(version) + " AND i.id=" + literal(job["id"])))
            for key, length in (("object_storage_id", 64), ("index_storage_id", 64), ("profile_id", 64), ("generation", 32)):
                check(re.fullmatch("[0-9a-f]{" + str(length) + "}", state[key]), "resource binding was not persisted")
            check(state["object_key"] == "documents/" + document + "/" + version + "/original", "unexpected object namespace")
            _, actual = self.s3("GET", state["object_key"])
            check(actual == source, "physical source differs from uploaded bytes")
            points = self.vector("POST", state["profile_id"], "/points/scroll",
                                 {"filter": self.generation_filter(state), "limit": 256,
                                  "with_payload": True, "with_vector": True})
            check(points["points"] and points.get("next_page_offset") is None, "synthetic generation was not completely captured")
            restored_points = [{key: point[key] for key in ("id", "payload", "vector")}
                               for point in points["points"]]
            state.update(project=self.args.project, workspace=workspace, token=self.token,
                         content=base64.b64encode(source).decode(), points=restored_points)
            descriptor = os.open(self.args.state, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(descriptor, "w") as output:
                os.fchmod(output.fileno(), 0o600)
                json.dump(state, output, allow_nan=False)
            print("PASS: ready document has persisted object/vector bindings and physical artifacts", flush=True)
        except BaseException:
            try:
                self.api("DELETE", "/workspaces/" + workspace, expected=204)
            except Exception:
                pass
            raise

    def verify(self):
        self.indexing_is(self.args.expect_indexing)
        state = json.loads(Path(self.args.state).read_text())
        check(state["project"] == self.args.project, "fixture belongs to another disposable project")
        self.token = state["token"]
        self.api("DELETE", "/workspaces/" + state["workspace"], expected=204)
        self.sql("UPDATE object_cleanup SET available_at=UTC_TIMESTAMP(6) WHERE storage_id="
                 + literal(state["object_storage_id"]) + " AND object_key=" + literal(state["object_key"]))
        self.sql("UPDATE index_cleanup SET not_before=UTC_TIMESTAMP(6) WHERE storage_id="
                 + literal(state["index_storage_id"]) + " AND profile_id=" + literal(state["profile_id"])
                 + " AND generation=" + literal(state["generation"]))
        self.wait_deleted(state)
        print("PASS: real backend reclaimed MinIO bytes and Qdrant points with indexing="
              + self.args.expect_indexing + "; permanent retirement records remain", flush=True)
        self.s3("PUT", state["object_key"], base64.b64decode(state["content"]))
        result = self.vector("PUT", state["profile_id"], "/points?wait=true", {"points": state["points"]})
        check(result.get("status") == "completed", "late vector write did not finish")
        self.s3("HEAD", state["object_key"])
        count = self.vector("POST", state["profile_id"], "/points/count",
                            {"filter": self.generation_filter(state), "exact": True})["count"]
        check(count == len(state["points"]), "late vector generation was not restored")
        self.sql("UPDATE cleanup_retirements SET next_check_at=UTC_TIMESTAMP(6) WHERE "
                 "(kind='object' AND storage_id=" + literal(state["object_storage_id"]) + " AND resource_key="
                 + literal(state["object_key"]) + ") OR (kind='index' AND storage_id=" + literal(state["index_storage_id"])
                 + " AND profile_id=" + literal(state["profile_id"]) + " AND resource_key=" + literal(state["generation"]) + ")")
        self.wait_deleted(state)
        print("PASS: real late object/vector writes were reclaimed again through retirement scans", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("prepare", "verify"))
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--env-file", required=True)
    parser.add_argument("--compose-file", action="append", required=True)
    parser.add_argument("--state", required=True)
    parser.add_argument("--expect-indexing", choices=("true", "false"), default="false")
    args = parser.parse_args()
    fixture = Fixture(args)
    if args.phase == "prepare":
        fixture.prepare()
    else:
        fixture.verify()


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        # HTTP/provider output, command errors and private state stay off logs.
        detail = str(error) if type(error) is RuntimeError else "isolated cleanup smoke did not satisfy its checks"
        print("FAIL: " + detail, file=sys.stderr)
        raise SystemExit(1)
