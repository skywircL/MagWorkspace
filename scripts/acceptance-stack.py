"""Disposable real stack; credentials never written to public results.

Usage: MAGWORKSPACE_ACCEPTANCE_DIR=/private/tmp/... acceptance-stack.py start|rebuild|stop
Rebuild replaces only this project's backend and refreshes its published URL.
"""
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
COMMAND = sys.argv[1] if len(sys.argv) == 2 else ''
if COMMAND not in ('start', 'rebuild', 'stop'):
    raise SystemExit('usage: acceptance-stack.py start|rebuild|stop')
OUT = Path(os.environ['MAGWORKSPACE_ACCEPTANCE_DIR']).resolve()
OUT.mkdir(mode=0o700, parents=True, exist_ok=True)
STATE = OUT / 'private-state.json'

def run(args, env=None):
    with (OUT / 'private-commands.log').open('ab') as log:
        return subprocess.run(args, cwd=ROOT, env=env, stdout=log, stderr=log, check=True)

if COMMAND == 'start':
    if STATE.exists():
        raise RuntimeError('state already exists; inspect or stop existing acceptance stack first')
    values = {}
    for line in (ROOT / '.env').read_text().splitlines():
        if line.strip() and not line.lstrip().startswith('#') and '=' in line:
            key, value = line.split('=', 1)
            values[key.strip()] = value.strip().strip('\"\'')
    for key in ('MAGENT_BASE_URL', 'MAGENT_API_KEY', 'MAGENT_MODEL'):
        if not values.get(key):
            raise RuntimeError('missing live model configuration: ' + key)
    configuration = {key: values[key] for key in ('MAGENT_BASE_URL', 'MAGENT_API_KEY', 'MAGENT_MODEL')}
    configuration.update(MYSQL_PASSWORD=secrets.token_hex(24), MYSQL_ROOT_PASSWORD=secrets.token_hex(24),
        MINIO_ACCESS_KEY=secrets.token_hex(12), MINIO_SECRET_KEY=secrets.token_hex(24), QDRANT_API_KEY=secrets.token_hex(24), REDIS_PASSWORD=secrets.token_hex(24),
        MAGENT_SERVICE_API_KEY=secrets.token_hex(32), MINIO_BUCKET='magworkspace', MAGENT_MODE='live', MAGENT_TIMEOUT='240s',
        MAGENT_HARNESS_PATH=str(ROOT.parent / 'Magent Harness'), ALLOW_REGISTRATION='true',
        DOCUMENT_INDEXING_ENABLED='true', EMBEDDING_MODEL='bge-m3:567m', EMBEDDING_DIMENSIONS='1024',
        MAGWORKSPACE_MODEL_CACHE_VOLUME='magworkspace_ollama-models')
    for key in ('MAGENT_SERVICE_PORT','REDIS_PORT','MYSQL_PORT','MINIO_PORT','MINIO_CONSOLE_PORT','HTTP_PORT','SEARCH_PORT','QDRANT_PORT','OLLAMA_PORT','TIKA_PORT'):
        configuration[key] = '0'
    envfile = OUT / 'private.env'
    envfile.touch(mode=0o600)
    envfile.write_text(''.join(k + "='" + v.replace("'", "\\'") + "'\n" for k,v in configuration.items()))
    override = OUT / 'override.yaml'
    override.write_text('volumes:\n  ollama-models:\n    external: true\n    name: ${MAGWORKSPACE_MODEL_CACHE_VOLUME}\n')
    state = {'project': 'mw-web-' + secrets.token_hex(5)}
    STATE.touch(mode=0o600)
    STATE.write_text(json.dumps(state))
else:
    state = json.loads(STATE.read_text())
    configuration = {}

# Keep rebuild/stop usable for a disposable stack created before Redis existed.
private_env = OUT / 'private.env'
if not any(line.startswith('REDIS_PASSWORD=') for line in private_env.read_text().splitlines()):
    with private_env.open('a') as output:
        output.write('\nREDIS_PORT=0\nREDIS_PASSWORD=' + secrets.token_hex(24) + '\n')
    os.chmod(private_env, 0o600)

if not any(line.startswith('MAGENT_SERVICE_API_KEY=') for line in private_env.read_text().splitlines()):
    with private_env.open('a') as output:
        output.write('\nMAGENT_SERVICE_PORT=0\nMAGENT_SERVICE_API_KEY=' + secrets.token_hex(32) + '\n')
    os.chmod(private_env, 0o600)

compose = ['docker','compose','--env-file',str(OUT/'private.env'),'--project-name',state['project'],
    '--profile','app','-f','compose.yaml','-f',str(OUT/'override.yaml')]
def port(service, internal):
    result = subprocess.check_output(compose + ['port',service,str(internal)], cwd=ROOT, text=True)
    return result.strip().rsplit(':',1)[1]

def wait_ready():
    state.update(base_url='http://127.0.0.1:' + port('backend',8080),
        minio_url='http://127.0.0.1:' + port('minio',9000),
        tika_url='http://127.0.0.1:' + port('tika',9998))
    STATE.write_text(json.dumps(state))
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    for attempt in range(90):
        try:
            with opener.open(state['base_url'] + '/readyz', timeout=2) as response:
                if response.status == 200:
                    break
        except OSError:
            pass
        time.sleep(1)
    else:
        raise RuntimeError('backend not ready')
    print('READY ' + state['base_url'], flush=True)

if COMMAND == 'start':
    run(compose + ['config','--quiet'])
    run(['docker','volume','create','magworkspace_ollama-models'])
    print('Starting isolated MySQL / MinIO / Qdrant / Tika / Ollama', flush=True)
    run(compose + ['up','-d','--build','--wait','mysql','minio','qdrant','tika','ollama'])
    print('Infrastructure healthy; checking real bge-m3 model cache', flush=True)
    run(compose + ['run','--rm','ollama-model'])
    print('Building current backend source and starting live bridge', flush=True)
    run(compose + ['up','-d','--build','backend'])
    wait_ready()
    testenv = os.environ.copy()
    testenv.update(MAGWORKSPACE_TEST_MINIO_URL=state['minio_url'],
        MAGWORKSPACE_TEST_MINIO_ACCESS_KEY=configuration['MINIO_ACCESS_KEY'],
        MAGWORKSPACE_TEST_MINIO_SECRET_KEY=configuration['MINIO_SECRET_KEY'],
        MAGWORKSPACE_TEST_TIKA_URL=state['tika_url'],
        GOMODCACHE=str(ROOT/'.cache/gomod'), GOCACHE=str(ROOT/'.cache/gobuild'))
    run(['go','test','./internal/blob','./internal/indexing','-run','^(TestMinIOLive|TestParserRealTika)$','-count=1','-v'], testenv)
    print('PASS real MinIO and Tika adapters', flush=True)
    run([sys.executable,'scripts/smoke-indexing.py',state['base_url']])
    print('PASS real document indexing / lexical / vector / hybrid / immutable version smoke', flush=True)
elif COMMAND == 'rebuild':
    print('Rebuilding isolated backend with current source; existing synthetic data retained', flush=True)
    run(compose + ['up','-d','--no-deps','--build','--wait','harness'])
    run(compose + ['up','-d','--no-deps','--build','backend'])
    wait_ready()
elif COMMAND == 'stop':
    run(compose + ['down','--volumes','--remove-orphans'])
    (OUT/'private.env').unlink(missing_ok=True)
    (OUT/'private-browser.json').unlink(missing_ok=True)
    STATE.unlink(missing_ok=True)
    print('Removed isolated stack and synthetic data volumes; model cache preserved', flush=True)
