"""Real document pipeline smoke against an isolated running application.

Usage: python3 scripts/smoke-indexing.py http://127.0.0.1:PORT
Creates only synthetic content; use a disposable Compose project/database.
"""
import hashlib
import json
import sys
import time
import urllib.error
import urllib.request
import uuid

base = sys.argv[1].rstrip("/") + "/api/v1"
client = urllib.request.build_opener(urllib.request.ProxyHandler({}))
token = ""


def check(condition, message):
    if not condition:
        raise RuntimeError(message)


def request(method, path, data=None, media="application/json", expected=200):
    headers = {"Content-Type": media}
    if token:
        headers["Authorization"] = "Bearer " + token
    if data is not None and not isinstance(data, bytes):
        data = json.dumps(data).encode()
    try:
        response = client.open(urllib.request.Request(base + path, data=data, headers=headers, method=method), timeout=40)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        payload = response.read()
        if response.status != expected:
            raise RuntimeError(f"{method} {path}: status {response.status}, expected {expected}")
        if response.headers.get_content_type() == "application/json":
            envelope = json.loads(payload)
            return envelope["error"] if expected >= 400 else envelope["data"]
        return payload


def upload(path, text, previous=None):
    boundary = uuid.uuid4().hex
    data = bytearray()
    if previous:
        data.extend(f'--{boundary}\r\nContent-Disposition: form-data; name="base_version_id"\r\n\r\n{previous}\r\n'.encode())
    data.extend(f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="notes.md"\r\nContent-Type: text/markdown\r\n\r\n'.encode())
    data.extend(text)
    data.extend(f'\r\n--{boundary}--\r\n'.encode())
    return request("POST", path, bytes(data), f"multipart/form-data; boundary={boundary}", 201)


def ready(path):
    deadline = time.monotonic() + 330
    while time.monotonic() < deadline:
        job = request("GET", path + "/index")
        if job["status"] == "ready":
            return job
        if job["status"] == "failed":
            raise RuntimeError("index failed: " + job.get("error_code", "unknown"))
        time.sleep(1)
    raise RuntimeError("index did not become ready")


account = request("POST", "/auth/register", {"name": "Index smoke", "email": f"index-{uuid.uuid4().hex}@example.test", "password": uuid.uuid4().hex}, expected=201)
token = account["token"]
workspace = "/workspaces/" + account["workspaces"][0]["id"]
table = "| 部署方式 | 虚拟机 | 每台配置 |\n| --- | --- | --- |\n| 伪分布式 | 一台 | 两核六GB |\n| 完全分布式 | 三台 | 一核四GB |\n"
code = "```sh\necho '检查虚拟机内存'\nfree -m\n```\n"
source = ("# 文档知识库\n" + "\n" * 1600 +
          "工作台通过不可变版本管理个人知识，全文检索和向量检索可以帮助查找资料。\n\n" +
          "这是完整的说明句子，文档中的配置需要保留上下文。" * 22 +
          "\n\n## 推荐配置\n\n" + table + "\n" + code + "\n" +
          "检查完成后记录结果，并在知识库保存实验说明。" * 26 + "\n").encode()
try:
    first = upload(workspace + "/documents", source)
    doc = workspace + "/documents/" + first["document"]["id"]
    version = doc + "/versions/" + first["version"]["id"]
    job = ready(version)
    check(request("GET", version + "/content") == source, "original bytes changed")
    chunks = request("GET", version + "/indexes/" + job["id"] + "/chunks?page_size=100")
    check(len(chunks) == job["chunk_count"] and chunks[0]["start_byte"] == 0 and
          chunks[-1]["end_byte"] == len(source), "canonical chunk coverage endpoints changed")
    for i, item in enumerate(chunks):
        check(source[item["start_byte"]:item["end_byte"]] == item["content"].encode() and
              len(item["content"]) <= 800, "chunk is not a bounded exact source slice")
        if i:
            check(chunks[i-1]["start_byte"] < item["start_byte"] <= chunks[i-1]["end_byte"] < item["end_byte"],
                  "chunk coverage has a gap or fails to advance")
    check(any(table in item["content"] for item in chunks), "fitting Markdown table was fragmented")
    check(any(code in item["content"] for item in chunks), "fitting fenced code was fragmented")
    print("PASS: structure_v2 exact byte coverage, bounded chunks, intact Markdown table/code; chunks=" + str(len(chunks)), flush=True)
    chunk = None
    for mode in ("lexical", "vector", "hybrid"):
        result = request("POST", workspace + "/search", {"query": "知识库", "mode": mode, "limit": 5})
        check(result["hits"], mode + " returned no hit")
        check(all(hit["chunk"]["content"].strip() for hit in result["hits"]), mode + " returned a whitespace-only hit")
        chunk = result["hits"][0]["chunk"]
        check(chunk["version_id"] == first["version"]["id"], mode + " returned wrong version")
        check(chunk["source_sha256"] == hashlib.sha256(source).hexdigest(), "source hash mismatch")
        check(chunk["sha256"] == hashlib.sha256(chunk["content"].encode()).hexdigest(), "chunk hash mismatch")
        configuration = request("POST", workspace + "/search", {"query": "完全分布式 推荐配置", "mode": mode, "limit": 10})
        check(any(table in hit["chunk"]["content"] for hit in configuration["hits"]), mode + " failed to retrieve the complete configuration table")
    citation = version + "/indexes/" + job["id"] + "/chunks/" + chunk["id"]
    check(request("GET", citation)["content"] == chunk["content"], "citation content mismatch")
    title = "更新后的个人知识库"
    renamed = request("PATCH", doc, {"title": title})
    check(renamed["title"] == title and renamed["current_version_id"] == first["version"]["id"], "rename changed document head")
    check(request("GET", version + "/index") == job, "rename changed the published index")
    second_source = "# 新版\n当前资料介绍天文学和望远镜。\n".encode()
    second = upload(doc + "/versions", second_source, first["version"]["id"])
    ready(doc + "/versions/" + second["version"]["id"])
    for mode in ("lexical", "vector", "hybrid"):
        result = request("POST", workspace + "/search", {"query": "知识库", "mode": mode})
        check(all(hit["chunk"]["version_id"] == second["version"]["id"] for hit in result["hits"]), mode + " exposed an old version")
        result = request("POST", workspace + "/search", {"query": "天文学", "mode": mode})
        check(result["hits"], mode + " returned no current-version hit")
        check(all(hit["chunk"]["version_id"] == second["version"]["id"] and
                  hit["chunk"]["source_sha256"] == hashlib.sha256(second_source).hexdigest()
                  for hit in result["hits"]), mode + " returned wrong current-version source")
    check(request("GET", version + "/content") == source, "historical original changed")
    check(request("GET", citation)["content"] == chunk["content"], "historical citation changed")
    edited_source = "# 在线编辑\r\n银河系观测记录🙂\t望远镜的新发现。\n"
    third = request("PUT", doc + "/content", {"base_version_id": second["version"]["id"], "content": edited_source})
    third_version = doc + "/versions/" + third["version"]["id"]
    check(third["version"]["number"] == 3 and third["version"]["filename"] == first["version"]["filename"], "inline edit did not append the expected version")
    ready(third_version)
    check(request("GET", third_version + "/content") == edited_source.encode(), "inline edit changed UTF-8 bytes")
    check(request("GET", doc + "/versions/" + second["version"]["id"] + "/content") == second_source, "previous file version changed")
    check(request("GET", citation)["content"] == chunk["content"], "inline edit changed the historical citation")
    for mode in ("lexical", "vector", "hybrid"):
        hits = request("POST", workspace + "/search", {"query": "银河系", "mode": mode})["hits"]
        check(hits, mode + " returned no inline-edit hit")
        check(all(hit["title"] == title and hit["chunk"]["version_id"] == third["version"]["id"] and
                  hit["chunk"]["source_sha256"] == hashlib.sha256(edited_source.encode()).hexdigest()
                  for hit in hits), mode + " returned stale inline-edit metadata")
    request("PUT", doc + "/content", {"base_version_id": second["version"]["id"], "content": "过期编辑"}, expected=409)
    check(request("GET", doc)["current_version_id"] == third["version"]["id"], "stale inline edit changed the head")
    request("DELETE", doc, expected=204)
    check(request("POST", workspace + "/search", {"query": "银河系"})["hits"] == [], "deleted document remains visible")
    print("PASS: original bytes, async ready, Chinese lexical/vector/hybrid, rename without reindex, inline-edit CAS, immutable citation, current-version filtering, soft-delete visibility")
finally:
    request("DELETE", workspace, expected=204)
