"""Opt-in live-model acceptance, only against the disposable acceptance stack.
Usage: MAGWORKSPACE_ACCEPTANCE_DIR=/private/tmp/... python3 scripts/smoke-workbench.py
Sends synthetic documents to the configured provider. Never retries mutations.
"""
import hashlib
import json
import os
from pathlib import Path
import secrets
import time
import urllib.error
import urllib.request

out=Path(os.environ['MAGWORKSPACE_ACCEPTANCE_DIR'])
base=json.loads((out/'private-state.json').read_text())['base_url']
client=urllib.request.build_opener(urllib.request.ProxyHandler({}))
token=''
report={'checks':[], 'message_requests':0,'resume_requests':0}
def check(value,label):
    if not value: raise RuntimeError(label)
    report['checks'].append(label)
    (out/'live-results.json').write_text(json.dumps(report,ensure_ascii=False,indent=2))
    print('PASS '+label,flush=True)
def request(method,path,data=None,expected=200,stream=False):
    headers={}
    if token:headers['Authorization']='Bearer '+token
    if data is not None:
        headers['Content-Type']='application/json';data=json.dumps(data).encode()
    if stream:headers['Accept']='text/event-stream'
    req=urllib.request.Request(base+path,method=method,data=data,headers=headers)
    try:r=client.open(req,timeout=270)
    except urllib.error.HTTPError as e:r=e
    with r:
        if r.status!=expected:raise RuntimeError(f'{method} status={r.status} expected={expected}')
        if stream:
            events=[];deltas=0
            while True:
                line=r.readline()
                if not line:break
                if line.startswith(b'event: '):
                    name=line[7:].decode().strip()
                    data_line=r.readline()
                    value=json.loads(data_line[6:]);events.append((name,value))
                    if name=='message.delta':deltas+=1
            check(bool(events) and events[0][0]=='run.started','stream starts with Run identity')
            name,value=events[-1]
            check(name in ('run.completed','run.suspended'), 'stream durable terminal '+name)
            report.setdefault('streams',[]).append({'terminal':name,'deltas':deltas,'run_id':value['run_id']})
            return value
        raw=r.read()
        if expected>=400:return json.loads(raw)
        if r.headers.get_content_type()=='application/json':return json.loads(raw)['data']
        return raw

def turn(session,content):
    report['message_requests']+=1
    return request('POST',session+'/messages',{'content':content},stream=True)
def resume(session,run,prompt,kind,data):
    report['resume_requests']+=1
    return request('POST',session+'/runs/'+run+'/prompts/'+prompt+'/'+kind,data,stream=True)
def ready(path):
    until=time.monotonic()+360
    while time.monotonic()<until:
        job=request('GET',path+'/index')
        if job['status']=='ready':return job
        if job['status']=='failed':raise RuntimeError('index failed '+job.get('error_code',''))
        time.sleep(1)
    raise RuntimeError('index deadline')
def session(title):return wp+'/sessions/'+request('POST',wp+'/sessions',{'title':title},201)['id']

email='workbench-'+secrets.token_hex(5)+'@example.test';password=secrets.token_hex(16)
account=request('POST','/api/v1/auth/register',{'name':'工作台验收','email':email,'password':password},201)
token=account['token'];wid=account['workspaces'][0]['id'];wp='/api/v1/workspaces/'+wid
private=out/'private-browser.json';private.touch(mode=0o600);private.write_text(json.dumps({'base_url':base,'email':email,'password':password,'workspace_id':wid}))
marker=secrets.token_hex(6)
original=f'# 星河项目差旅制度\n核验编号：{marker}\n住宿报销上限为每晚 417 元。\n申请须在出差结束后 13 天内提交。\n'
# Multipart through the real upload endpoint.
boundary=secrets.token_hex(12)
body=(f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="travel.md"\r\nContent-Type: text/markdown\r\n\r\n'+original+f'\r\n--{boundary}--\r\n').encode()
req=urllib.request.Request(base+wp+'/documents',data=body,headers={'Authorization':'Bearer '+token,'Content-Type':'multipart/form-data; boundary='+boundary})
with client.open(req,timeout=40) as r:doc=json.load(r)['data']
did=doc['document']['id'];v1=doc['version']['id'];dp=wp+'/documents/'+did
ready(dp+'/versions/'+v1);check(request('GET',dp+'/versions/'+v1+'/content')==original.encode(),'upload original bytes and ready index')
for mode in ['lexical','vector','hybrid']:
    result=request('POST',wp+'/search',{'query':'星河项目住宿报销','mode':mode,'limit':5})
    check(any(x['chunk']['version_id']==v1 for x in result['hits']),mode+' retrieves v1')
s1=session('核对住宿标准')
a=turn(s1,'请检索知识库中的星河项目差旅制度，告诉我住宿上限和核验编号。必须引用检索证据，不要猜测。')
check(a['run']['mode']=='live' and '417' in a.get('answer','') and marker in a.get('answer',''),'real model retrieves random fact and amount')
check(len(a.get('citations',[]))>0,'answer has verified citations')
e=a['citations'][0]['evidence'];check(e['version_id']==v1,'citation binds v1')
fixed=s1+'/runs/'+a['run_id']+'/evidence/'+e['evidence_id']
check(original.strip() in request('GET',fixed)['evidence']['content'],'fixed evidence readable')
s2=session('审批更新差旅制度')
b=turn(s2,'请使用 read_document 的 source 模式读取星河项目差旅制度的完整原文，再用 update_document 把住宿报销上限从每晚 417 元改为每晚 563 元，其他内容包括核验编号、提交天数、换行都保留。请实际提出文档更新工具审批，不要仅提供修改建议，不要调用 ask_user。')
p=b.get('prompt',{});check(p.get('kind')=='tool_approval','model proposes native write approval')
check(p['approval']['tool']=='update_document' and p['approval']['document_id']==did,'approval targets expected document')
newtext=p['approval']['arguments']['content'];check(newtext==original.replace('417','563'),'proposal changes only requested amount preserving exact other bytes')
check(request('GET',dp)['current_version_id']==v1,'pending approval has no write side effect')
check(request('GET',s2+'/runs/'+b['run_id']+'/prompt')['id']==p['id'],'pending approval reload persists')
# Another account cannot approve this private run.
owner_token=token
other=request('POST','/api/v1/auth/register',{'name':'隔离用户','email':'other-'+secrets.token_hex(5)+'@example.test','password':secrets.token_hex(16)},201)
token=other['token'];request('POST',s2+'/runs/'+b['run_id']+'/prompts/'+p['id']+'/approve',{'approved':True},404)
token=owner_token;check(True,'cross-user approval rejected 404')
c=resume(s2,b['run_id'],p['id'],'approve',{'approved':True})
receipt=request('GET',s2+'/runs/'+b['run_id']+'/prompts/'+p['id'])
check(bool(receipt['approval'].get('result')),'write committed receipt present')
v2=request('GET',dp)['current_version_id'];check(v2!=v1,'approval advances version head')
check(request('GET',dp+'/versions/'+v2+'/content')==newtext.encode(),'new version exact proposed bytes')
request('POST',s2+'/runs/'+b['run_id']+'/prompts/'+p['id']+'/approve',{'approved':True},409)
check(request('GET',dp)['current_version_id']==v2,'duplicate approval rejected without second version')
request('PUT',dp+'/content',{'base_version_id':v1,'content':'stale'},409);check(True,'stale human edit rejected')
ready(dp+'/versions/'+v2)
for mode in ['lexical','vector','hybrid']:
    result=request('POST',wp+'/search',{'query':'星河项目住宿报销','mode':mode,'limit':5})
    check(bool(result['hits']) and all(x['chunk']['version_id']==v2 for x in result['hits']),mode+' only retrieves new head v2')
check(request('GET',dp+'/versions/'+v1+'/content')==original.encode(),'immutable v1 unchanged')
check('417' in request('GET',fixed)['evidence']['content'],'old citation remains bound to v1')
s3=session('更新后的制度核对')
d=turn(s3,'请重新检索星河项目差旅制度，现在住宿报销上限是多少？附有效引用。')
check('563' in d.get('answer','') and any(x['evidence']['version_id']==v2 for x in d.get('citations',[])),'fresh live answer cites v2 and new amount')
s4=session('拒绝修改验收')
f=turn(s4,'请读取星河项目差旅制度原文，把住宿金额改为每晚 999 元，保留其他内容，用 update_document 提出审批，不要仅回复文字，也不要 ask_user。')
pf=f.get('prompt',{});check(pf.get('kind')=='tool_approval','rejection scenario reaches approval')
resume(s4,f['run_id'],pf['id'],'approve',{'approved':False})
check(request('GET',dp)['current_version_id']==v2,'rejecting proposal leaves document head unchanged')
pr=request('GET',s4+'/runs/'+f['run_id']+'/prompts/'+pf['id']);check(not pr['approval'].get('result'),'rejected proposal has no committed receipt')
report['status']='passed';report['document_id']=did;report['version_ids']=[v1,v2]
(out/'live-results.json').write_text(json.dumps(report,ensure_ascii=False,indent=2))
print('PASS full live acceptance; synthetic workspace retained for browser checks until stack cleanup',flush=True)
