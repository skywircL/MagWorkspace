"""50 MB upload regression against a disposable stack (no model API calls).
Usage: python3 scripts/smoke-upload-limits.py http://127.0.0.1:PORT
"""
import hashlib
import io
import json
import sys
import time
import urllib.error
import urllib.request
import uuid
import zipfile

base = sys.argv[1].rstrip('/') + '/api/v1'
client = urllib.request.build_opener(urllib.request.ProxyHandler({}))
token = ''
checks = []

def check(value, label):
    if not value:
        raise RuntimeError(label)
    checks.append(label)

def request(method, path, data=None, media='application/json', expected=200):
    headers = {'Content-Type': media}
    if token:
        headers['Authorization'] = 'Bearer ' + token
    if data is not None and not isinstance(data, bytes):
        data = json.dumps(data).encode()
    try:
        response = client.open(urllib.request.Request(base + path, data=data, headers=headers, method=method), timeout=120)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        payload = response.read()
        if response.status != expected:
            raise RuntimeError(f'{method} {path}: {response.status}, expected {expected}')
        if response.headers.get_content_type() == 'application/json':
            envelope = json.loads(payload)
            return envelope['error'] if expected >= 400 else envelope['data']
        return payload

def upload(path, content, filename='large.docx', previous=None, expected=201):
    boundary = uuid.uuid4().hex
    prefix = b''
    if previous:
        prefix = f'--{boundary}\r\nContent-Disposition: form-data; name="base_version_id"\r\n\r\n{previous}\r\n'.encode()
    prefix += f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="{filename}"\r\n\r\n'.encode()
    return request('POST', path, prefix + content + f'\r\n--{boundary}--\r\n'.encode(), f'multipart/form-data; boundary={boundary}', expected)

def docx(size, body):
    files = {
        '[Content_Types].xml': '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Default Extension="bin" ContentType="application/octet-stream"/><Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/></Types>',
        '_rels/.rels': '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/></Relationships>',
        'word/document.xml': f'<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:body><w:p><w:r><w:t>{body}</w:t></w:r></w:p></w:body></w:document>',
        'padding.bin': b'',
    }
    def build():
        stream = io.BytesIO()
        with zipfile.ZipFile(stream, 'w', compression=zipfile.ZIP_STORED) as archive:
            for name, data in files.items():
                archive.writestr(name, data)
        return stream.getvalue()
    files['padding.bin'] = b'\0' * (size - len(build()))
    result = build()
    assert len(result) == size
    return result

def index(path, status='ready'):
    deadline = time.monotonic() + 330
    while time.monotonic() < deadline:
        result = request('GET', path + '/index')
        if result['status'] in ('ready', 'failed'):
            check(result['status'] == status, 'index ' + status + ': ' + result.get('error_code', ''))
            return result
        time.sleep(1)
    raise RuntimeError('index timed out')

check(request('GET', '/config') == {'max_file_size_mb': 50, 'max_file_size_bytes': 50 << 20}, 'public config default 50 MB')
account = request('POST', '/auth/register', {'name': 'Upload regression', 'email': f'upload-{uuid.uuid4().hex}@example.test', 'password': uuid.uuid4().hex}, expected=201)
token = account['token']
workspace = '/workspaces/' + account['workspaces'][0]['id']
try:
    source = docx(50 << 20, '大文件知识库验收：差旅住宿报销标准为每天六百元。')
    first = upload(workspace + '/documents', source)
    doc = workspace + '/documents/' + first['document']['id']
    version = doc + '/versions/' + first['version']['id']
    check(first['version']['size_bytes'] == len(source), 'exact 50 MB upload accepted')
    check(hashlib.sha256(request('GET', version + '/content')).hexdigest() == hashlib.sha256(source).hexdigest(), '50 MB MinIO download matches SHA256')
    index(version)
    for mode in ('lexical', 'vector', 'hybrid'):
        result = request('POST', workspace + '/search', {'query': '差旅住宿报销标准', 'mode': mode, 'limit': 5})
        check(any(hit['chunk']['version_id'] == first['version']['id'] for hit in result['hits']), 'large DOCX ' + mode + ' searchable')
    upload(workspace + '/documents', source + b'x', expected=413)
    check(len(request('GET', workspace + '/documents')) == 1, 'oversize create leaves no document')
    upload(doc + '/versions', source + b'x', previous=first['version']['id'], expected=413)
    check(request('GET', doc)['current_version_id'] == first['version']['id'], 'oversize replace preserves head')
    replacement = docx(9 << 20, '新版本：差旅住宿报销标准已更新为每天七百元。')
    second = upload(doc + '/versions', replacement, previous=first['version']['id'])
    next_version = doc + '/versions/' + second['version']['id']
    index(next_version)
    check(request('GET', next_version + '/content') == replacement, 'replacement above 8 MB preserved')
    check(request('GET', version + '/content') == source, 'original 50 MB version immutable after replacement')
    text = upload(workspace + '/documents', b'a' * ((1 << 20) + 1), filename='long.txt')
    failure = index(workspace + '/documents/' + text['document']['id'] + '/versions/' + text['version']['id'], 'failed')
    check(failure['error_code'] == 'extraction_limit', 'extraction budget remains separate from upload cap')
finally:
    request('DELETE', workspace, expected=204)
print(json.dumps({'status': 'passed', 'checks': checks, 'paid_model_calls': 0}, ensure_ascii=False, indent=2))
