"""Redis SSE smoke for a disposable Compose backend in MAGENT_MODE=demo.

Never point this at a live-model backend: the script sends two Agent turns.
"""
import json
import sys
import urllib.request
import uuid

base = sys.argv[1].rstrip('/')
client = urllib.request.build_opener(urllib.request.ProxyHandler({}))
token = ''


def call(method, path, body=None, headers=None):
    request_headers = {'Content-Type': 'application/json'}
    if token:
        request_headers['Authorization'] = 'Bearer ' + token
    request_headers.update(headers or {})
    data = json.dumps(body).encode() if body is not None else None
    with client.open(urllib.request.Request(base + path, data=data, headers=request_headers, method=method), timeout=60) as response:
        raw = response.read()
        return raw if response.headers.get_content_type() == 'text/event-stream' else json.loads(raw)['data'] if raw else None


def frames(raw):
    result = []
    for frame in raw.decode().split('\n\n'):
        fields = dict(line.split(': ', 1) for line in frame.splitlines() if ': ' in line)
        if 'event' in fields:
            result.append(fields)
    return result


account = call('POST', '/api/v1/auth/register', {'name': 'Redis smoke', 'email': uuid.uuid4().hex + '@example.test', 'password': uuid.uuid4().hex})
token = account['token']
workspace = '/api/v1/workspaces/' + account['workspaces'][0]['id']
try:
    session = call('POST', workspace + '/sessions', {'title': 'Redis replay demo'})
    path = workspace + '/sessions/' + session['id']
    for turn in range(2):
        original = frames(call('POST', path + '/messages', {'content': 'Redis verification', 'document_ids': []}, {'Accept': 'text/event-stream'}))
        assert original[0]['event'] == 'run.started' and original[-1]['event'] == 'run.completed'
        assert all('id' in event for event in original), 'Redis event persistence unavailable'
        run_id = json.loads(original[0]['data'])['run_id']
        replayed = frames(call('GET', path + '/runs/' + run_id + '/events', headers={'Last-Event-ID': original[0]['id']}))
        assert replayed == original[1:], 'replay differs from original public events'
        run = call('GET', path + '/runs/' + run_id)
        assert run['status'] == 'completed'
    messages = call('GET', path + '/messages')
    assert len(messages) == 4, 'replay duplicated conversation messages'
    print('PASS: real Go/Harness demo, two consecutive SSE turns, Redis cursors, exact replay, durable results, no duplicate messages')
finally:
    call('DELETE', workspace)
