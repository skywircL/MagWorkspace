"""Real cmd/server + disposable Compose MySQL/Redis/MinIO; controlled Harness HTTP.
No application .env, paid model, existing container or application data is used.
Usage: python3 scripts/smoke-stream-manager.py --binary /absolute/server
"""
import argparse
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument('--binary', type=Path, required=True)
args = parser.parse_args()
binary = args.binary.resolve(strict=True)
client = urllib.request.build_opener(urllib.request.ProxyHandler({}))
releases = {}
started = threading.Condition()
calls = []
checks = []


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


class Harness(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_POST(self):
        payload = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        if self.path == '/v1/memories/extract':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('X-Magent-Protocol', '1')
            self.end_headers()
            self.wfile.write(b'{"items":[]}')
            return
        assert self.path == '/v1/runs'
        assert self.headers['Accept'] == 'text/event-stream'
        self.send_response(200)
        self.send_header('Content-Type', 'text/event-stream')
        self.send_header('X-Magent-Protocol', '1')
        self.end_headers()
        rid, sid = payload['run_id'], payload['session_id']
        gate = threading.Event()
        with started:
            releases[rid] = gate
            calls.append(rid)
            started.notify_all()

        def send(kind, data):
            self.wfile.write(('event: ' + kind + '\ndata: ' + json.dumps(data) + '\n\n').encode())
            self.wfile.flush()

        try:
            send('message.start', dict(type='message.start', run_id=rid, session_id=sid, round=1))
            send('message.delta', dict(type='message.delta', run_id=rid, session_id=sid, round=1, delta='draft'))
            assert gate.wait(20), 'test did not release controlled Harness'
            send('result', dict(run_id=rid, session_id=sid, mode='demo', status='completed',
                                answer='durable answer', messages=[dict(role='user', content=payload['query']),
                                                                  dict(role='assistant', content='durable answer')]))
        except (BrokenPipeError, ConnectionResetError):
            pass


def request(base, path, token='', data=None, stream=False):
    headers = {'Content-Type': 'application/json'}
    if token:
        headers['Authorization'] = 'Bearer ' + token
    if stream:
        headers['Accept'] = 'text/event-stream'
    raw = None if data is None else json.dumps(data).encode()
    response = client.open(urllib.request.Request(base + path, data=raw, headers=headers), timeout=30)
    if stream:
        return response
    with response:
        return json.load(response)['data']


with tempfile.TemporaryDirectory(prefix='mw-stream-smoke-') as directory:
    private = Path(directory)
    os.chmod(private, 0o700)
    project = 'mw-stream-' + secrets.token_hex(5)
    settings = {key: secrets.token_hex(24) for key in ('MYSQL_PASSWORD', 'MYSQL_ROOT_PASSWORD',
                'MINIO_ACCESS_KEY', 'MINIO_SECRET_KEY', 'REDIS_PASSWORD', 'QDRANT_API_KEY', 'MAGENT_SERVICE_API_KEY')}
    settings.update({key: '0' for key in ('MYSQL_PORT', 'MINIO_PORT', 'MINIO_CONSOLE_PORT', 'REDIS_PORT',
                                        'HTTP_PORT', 'MAGENT_SERVICE_PORT', 'TIKA_PORT', 'QDRANT_PORT', 'OLLAMA_PORT')})
    settings.update(DOCUMENT_INDEXING_ENABLED='false', MAGENT_MODE='demo', RERANK_ENABLED='false')
    env_file = private / 'test.env'
    env_file.write_text(''.join(f'{key}={value}\n' for key, value in settings.items()))
    os.chmod(env_file, 0o600)
    compose = ['docker', 'compose', '--env-file', str(env_file), '--project-name', project,
               '--profile', 'app', '-f', str(ROOT / 'compose.yaml')]
    processes = []
    harness = ThreadingHTTPServer(('127.0.0.1', 0), Harness)
    threading.Thread(target=harness.serve_forever, daemon=True).start()
    log = (private / 'private.log').open('wb')
    try:
        subprocess.run(compose + ['config', '--quiet'], cwd=ROOT, check=True, stdout=log, stderr=log)
        subprocess.run(compose + ['up', '-d', '--no-build', '--pull', 'never', '--wait', 'mysql', 'redis', 'minio'],
                       cwd=ROOT, check=True, stdout=log, stderr=log, timeout=150)

        def port(service, container_port):
            out = subprocess.check_output(compose + ['port', service, str(container_port)], cwd=ROOT, text=True)
            return out.strip().rsplit(':', 1)[1]

        shared = dict(DATABASE_URL=f"magworkspace:{settings['MYSQL_PASSWORD']}@tcp(127.0.0.1:{port('mysql', 3306)})/magworkspace?parseTime=true",
                      REDIS_ADDR='127.0.0.1:' + port('redis', 6379), REDIS_PASSWORD=settings['REDIS_PASSWORD'],
                      MINIO_ENDPOINT='http://127.0.0.1:' + port('minio', 9000), MINIO_BUCKET='stream-smoke',
                      MINIO_ACCESS_KEY=settings['MINIO_ACCESS_KEY'], MINIO_SECRET_KEY=settings['MINIO_SECRET_KEY'],
                      DOCUMENT_INDEXING_ENABLED='false', RERANK_ENABLED='false', ALLOW_REGISTRATION='true',
                      MAGENT_MODE='demo', MAGENT_SERVICE_URL=f'http://127.0.0.1:{harness.server_port}',
                      MAGENT_SERVICE_API_KEY=settings['MAGENT_SERVICE_API_KEY'], MAGENT_TIMEOUT='30s')

        def launch(kind):
            http_port, callback_port, metrics_port = free_port(), free_port(), free_port()
            env = {key: value for key, value in os.environ.items() if key in ('PATH', 'TMPDIR', 'HOME')}
            env.update(shared, STREAM_MANAGER_TYPE=kind, HTTP_ADDR=f'127.0.0.1:{http_port}',
                       MAGENT_CALLBACK_ADDR=f'127.0.0.1:{callback_port}',
                       MAGENT_CALLBACK_BASE_URL=f'http://127.0.0.1:{callback_port}', METRICS_ADDR=f'127.0.0.1:{metrics_port}')
            process = subprocess.Popen([str(binary)], cwd=ROOT, env=env, stdout=log, stderr=log)
            processes.append(process)
            base = f'http://127.0.0.1:{http_port}'
            for _ in range(100):
                if process.poll() is not None:
                    raise RuntimeError('backend exited during startup (private log retained only during test)')
                try:
                    with client.open(base + '/readyz', timeout=1) as response:
                        if response.status == 200:
                            return base
                except OSError:
                    time.sleep(.1)
            raise RuntimeError('backend readiness timed out')

        for kind in ('memory', 'redis'):
            first = launch(kind)
            second = launch(kind) if kind == 'redis' else first
            owner = request(first, '/api/v1/auth/register', data=dict(name='Stream smoke',
                            email=secrets.token_hex(8)+'@example.test', password=secrets.token_hex(20)))
            token = owner['token']
            wid = owner['workspaces'][0]['id']
            session = request(first, f'/api/v1/workspaces/{wid}/sessions', token, dict(title=kind))
            path = f"/api/v1/workspaces/{wid}/sessions/{session['id']}"
            response = request(first, path+'/messages', token, dict(content='stream smoke'), stream=True)
            rid = response.headers['X-Run-ID']
            while True:
                line = response.readline()
                assert line, 'stream ended before provisional text'
                if line.strip() == b'event: message.delta':
                    break
            response.close()
            time.sleep(.2)
            assert request(second, path+'/runs/'+rid, token)['status'] == 'running'
            replay = request(second, path+'/runs/'+rid+'/events', token, stream=True)
            with started:
                assert started.wait_for(lambda: rid in releases, timeout=5)
                releases[rid].set()
            body = replay.read().decode(); replay.close()
            assert 'run.started' in body and 'message.delta' in body and 'run.completed' in body, body
            assert calls.count(rid) == 1
            assert request(second, path+'/runs/'+rid, token)['status'] == 'completed'
            assert request(second, path+'/messages', token)[-1]['content'] == 'durable answer'
            checks.append(kind + ': compiled entry, disconnect continues, replay, SQL commit, single execution' +
                          (' across two processes' if kind == 'redis' else ''))
    finally:
        for gate in releases.values():
            gate.set()
        for process in processes:
            process.terminate()
        for process in processes:
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill(); process.wait()
                raise RuntimeError('backend failed to exit cleanly')
        harness.shutdown(); harness.server_close()
        subprocess.run(compose + ['down', '--volumes', '--remove-orphans'], cwd=ROOT,
                       check=True, stdout=log, stderr=log, timeout=60)
        log.close()
print(json.dumps(dict(status='passed', checks=checks, harness='controlled HTTP boundary; real Harness covered by Go integration tests'), ensure_ascii=False, indent=2))
