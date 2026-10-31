// Browser fixture for private memory management; backend authority tested separately.
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
const root = fileURLToPath(new URL('../', import.meta.url));
const output = process.env.MAGWORKSPACE_MEMORY_TEST_OUTPUT || path.join(root, 'artifacts/acceptance/030');
const { chromium } = await import(process.env.MAGWORKSPACE_PLAYWRIGHT_PATH || 'playwright');
const assets = { '/': ['index.html','text/html'], '/app.js':['app.js','text/javascript'], '/core.js':['core.js','text/javascript'], '/style.css':['style.css','text/css'] };
const scopes = new Map(['one','two'].map(id => [id,{enabled:true,items:[]}]));
const checks=[], unexpected=[];
let counter=0, delayed=null, delayedStatus=null, statusReads=0;
const malicious='<img src=x onerror="window.__memoryXSS=1"> 请用简洁中文回答';
const sample = (content, origin='automatic') => ({id:String(++counter).padStart(32,'0'),kind:'preference',content,topic:'回答风格',importance:3,status:'active',created_at:Date.now()/1000,use_count:0,source_session_id:'conversation',source_run_id:'current',origin});
scopes.get('one').items.push(sample(malicious),sample('项目数据库使用 MySQL。','explicit'));
let memoryStatus={status:'queued',saved_count:0};
const json = (res,data,status=200,extra={}) => {res.writeHead(status,{'Content-Type':'application/json'}); res.end(JSON.stringify({success:true,data,...extra}));};
const fail = (res,status) => {res.writeHead(status,{'Content-Type':'application/json'});res.end(JSON.stringify({success:false,error:{code:'conflict'}}));};
async function handler(req,res) {
 const url=new URL(req.url,'http://localhost');
 if(url.pathname==='/favicon.ico'){res.writeHead(204);res.end();return;}
 if(assets[url.pathname]) {const [file,type]=assets[url.pathname];res.writeHead(200,{'Content-Type':type,'Content-Security-Policy':"default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' blob:; base-uri 'none'; form-action 'self'"});res.end(await readFile(path.join(root,'internal/webui/assets',file)));return;}
 let raw='';for await(const chunk of req) raw+=chunk;const body=raw?JSON.parse(raw):null;
 if(url.pathname==='/api/v1/auth/browser/refresh') return json(res,{token:'fixture',expires_at:'2099-01-01T00:00:00Z'});
 if(url.pathname==='/api/v1/auth/browser/logout') {res.writeHead(204);res.end();return;}
 if(url.pathname==='/api/v1/config') return json(res,{max_file_size_mb:50,max_file_size_bytes:50*1024*1024});
 if(url.pathname==='/api/v1/auth/me') return json(res,{user:{id:'me',name:'演示用户',email:'memory@example.test'},workspaces:[{id:'one',name:'个人工作空间',role:'owner'},{id:'two',name:'只读协作空间',role:'viewer'}]});
 const m=url.pathname.match(/^\/api\/v1\/workspaces\/(one|two)\/(.*)$/);
 if(!m) {unexpected.push(req.url);return fail(res,404);}
 const scope=scopes.get(m[1]), resource=m[2];
 if(resource==='knowledge-bases') return json(res,[{id:'default',name:'默认知识库',workspace_id:m[1],is_default:true}]);
 if(resource==='knowledge-bases/default/documents') return json(res,[],200,{total:0,page:1,page_size:10});
 if(resource==='sessions') return json(res,m[1]==='one'?[{id:'conversation',title:'记忆测试会话'}]:[],200,{total:m[1]==='one'?1:0,page:1,page_size:10});
 if(resource==='sessions/conversation') return json(res,{id:'conversation',title:'记忆测试会话'});
 if(resource==='sessions/conversation/messages') return json(res,[
  {id:'old-user',role:'user',content:'之前的一个问题',run_id:'old',status:'completed'},
  {id:'old-assistant',role:'assistant',content:'历史回答。',run_id:'old',status:'completed'},
  {id:'new-user',role:'user',content:'我偏好简洁的中文回答。',run_id:'current',status:'completed'},
  {id:'new-assistant',role:'assistant',content:'好的，下面是回答。',run_id:'current',status:'completed'},
 ],200,{total:4,page:1,page_size:20});
 if(resource==='sessions/conversation/runs/current') return json(res,{id:'current',status:'completed'});
 if(resource==='sessions/conversation/runs/current/prompt') return fail(res,404);
 if(resource==='sessions/conversation/runs/old/memory') return json(res,{status:'not_scheduled',saved_count:0});
 if(resource==='sessions/conversation/runs/current/memory') {
  statusReads++;
  const value={...memoryStatus};
  if(delayedStatus) {const gate=delayedStatus;delayedStatus=null;res.writeHead(200,{'Content-Type':'application/json'});res.flushHeaders();await gate;res.end(JSON.stringify({success:true,data:value}));return;}
  return json(res,value);
 }
 if(resource==='memory-settings') {if(body)scope.enabled=body.enabled;return json(res,{enabled:scope.enabled});}
 if(resource==='memories/export') {res.setHeader('Content-Disposition','attachment; filename="personal-memories.json"');return json(res,scope.items);}
 if(resource==='memories' && req.method==='GET') {
  const status=url.searchParams.get('status')||'active';
  const items=scope.items.filter(i=>status==='all'||i.status===status);
  if(delayed) {const gate=delayed;delayed=null;res.writeHead(200,{'Content-Type':'application/json'});res.flushHeaders();await gate;res.end(JSON.stringify({success:true,data:items}));return;}
  return json(res,items);
 }
 if(resource==='memories' && req.method==='DELETE') {assert.equal(body.confirm,true);scope.items.forEach(i=>i.status='archived');res.writeHead(204);res.end();return;}
 if(resource.startsWith('memories/')) {
  const item=scope.items.find(i=>i.id===resource.split('/')[1]);if(!item)return fail(res,404);
  if(req.method==='DELETE') {item.status='archived';res.writeHead(204);res.end();return;}
  if(req.method==='PUT') {if(item.status!=='active')return fail(res,409);item.status='superseded';const next={...item,...body,id:String(++counter).padStart(32,'0'),status:'active',origin:'manual'};scope.items.push(next);return json(res,next);}
 }
 unexpected.push(req.method+' '+req.url);fail(res,404);
}
const server=createServer((q,r)=>handler(q,r).catch(e=>{unexpected.push(String(e));if(!r.headersSent)fail(r,500);else r.end();}));
let browser,page;
try {
 await mkdir(output,{recursive:true});
 await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
 browser=await chromium.launch({headless:true,...(process.env.MAGWORKSPACE_BROWSER_CHANNEL?{channel:process.env.MAGWORKSPACE_BROWSER_CHANNEL}:{})});
 const context=await browser.newContext({viewport:{width:1440,height:1000},locale:'zh-CN',timezoneId:'Asia/Shanghai',acceptDownloads:true});
 page=await context.newPage();page.setDefaultTimeout(8000);const errors=[];page.on('pageerror',e=>errors.push(String(e)));
 await page.goto('http://127.0.0.1:'+server.address().port);
 await page.locator('#nav-memory').click();await page.locator('#memory-toggle').filter({hasText:'记忆已开启'}).waitFor();
 assert.equal(await page.locator('#memory-add').count(),0);assert.equal(await page.getByRole('button',{name:'添加记忆',exact:false}).count(),0);
 await page.getByText('从对话中积累个人记忆',{exact:true}).waitFor();assert.match(await page.locator('#memory-view').textContent(),/额外的模型调用/);checks.push('automatic conversation capture replaces manual creation');
 await page.getByText(malicious,{exact:true}).waitFor();assert.equal(await page.locator('#memory-list img').count(),0);assert.equal(await page.evaluate(()=>window.__memoryXSS),undefined);
 await page.getByText('对话自动提取',{exact:true}).waitFor();await page.getByText('聊天明确保存',{exact:true}).waitFor();checks.push('automatic and explicit origin labels with text-only rendering');
 await page.locator('.memory-row').filter({hasText:malicious}).getByRole('button',{name:'纠正',exact:true}).click();await page.locator('#detail textarea').fill('请使用简洁中文回答，配置示例使用 MySQL。');await page.locator('#detail button[type=submit]').click();await page.getByText('请使用简洁中文回答，配置示例使用 MySQL。',{exact:true}).waitFor();await page.getByText('手动纠正或历史保存',{exact:true}).waitFor();
 await page.locator('#memory-status').selectOption('superseded');await page.getByText(malicious,{exact:true}).waitFor();assert.equal(await page.getByRole('button',{name:'纠正',exact:true}).count(),0);checks.push('corrections retain read-only replacement history');
 await page.locator('#memory-status').selectOption('all');await page.getByText(malicious,{exact:true}).waitFor();assert.equal(await page.locator('#memory-list .memory-row').count(),3);
 await page.locator('#memory-status').selectOption('active');await page.getByRole('button',{name:'纠正',exact:true}).first().waitFor();
 await page.locator('#nav-chat').click();await page.locator('#sessions button').filter({hasText:'记忆测试会话'}).click();await page.getByText('记忆已排队',{exact:true}).waitFor();assert.equal(await page.locator('.message-memory:visible').count(),1);
 memoryStatus={status:'running',saved_count:0};await page.getByRole('button',{name:'刷新记忆状态',exact:true}).click();await page.getByText('正在整理本轮记忆',{exact:true}).waitFor();
 memoryStatus={status:'done',saved_count:2};await page.getByRole('button',{name:'刷新记忆状态',exact:true}).click();await page.getByText('已保存 2 条记忆',{exact:true}).waitFor();await page.getByRole('button',{name:'查看个人记忆',exact:true}).click();await page.getByText('从对话中积累个人记忆',{exact:true}).waitFor();checks.push('completed answer shows queued running and persisted memory result; older runs remain compatible');
 const readCount=statusReads;await page.waitForTimeout(300);assert.equal(statusReads,readCount);checks.push('memory status uses explicit refresh without background polling');
 await page.locator('#memory-toggle').click();await page.locator('#memory-toggle').filter({hasText:'记忆已关闭'}).waitFor();await page.reload();await page.locator('#memory-toggle').filter({hasText:'记忆已关闭'}).waitFor();checks.push('private switch and navigation survive reload');
 assert.match(await page.locator('#memory-setting-note').textContent(),/停止采集和召回/);
 await page.locator('.memory-row').filter({hasText:'请使用简洁中文回答，配置示例使用 MySQL。'}).getByRole('button',{name:'纠正',exact:true}).click();await page.locator('#detail textarea').fill('偏好先给结论，再给步骤。');await page.locator('#detail button[type=submit]').click();await page.getByText('偏好先给结论，再给步骤。',{exact:true}).waitFor();
 page.once('dialog',d=>d.accept());await page.locator('.memory-row').filter({hasText:'偏好先给结论，再给步骤。'}).getByRole('button',{name:'遗忘',exact:true}).click();assert.equal(scopes.get('one').items.filter(i=>i.status==='active').length,1);checks.push('disabled capture and recall still allow corrections and forgetting');
 const downloading=page.waitForEvent('download');await page.locator('#memory-export').click();const dl=await downloading;const exported=JSON.parse(await readFile(await dl.path(),'utf8'));assert.equal(exported.data.length,4);checks.push('export includes automatic explicit and correction history');
 for (const [seconds, localValue] of [
  [253402300799, '10000-01-01T07:59:59'],
  [Date.parse('2030-06-01T04:05:06.125Z')/1000, '2030-06-01T12:05:06.125'],
 ]) {
  const current=scopes.get('one').items.find(i=>i.status==='active');current.expires_at=seconds;
  await page.locator('#memory-refresh').click();
  await page.locator('.memory-row').filter({hasText:current.content}).getByRole('button',{name:'纠正',exact:true}).click();
  const expiry=page.locator('#detail input[name=expiry]');
  assert.equal(await expiry.inputValue(),localValue);
  assert.equal(await expiry.evaluate(input=>input.validity.valid),true);
  const corrected='保留到期时间 '+localValue;
  await page.locator('#detail textarea').fill(corrected);
  const saving=page.waitForResponse(r=>r.request().method()==='PUT' && r.url().endsWith('/memories/'+current.id));
  await page.locator('#detail button[type=submit]').click();
  const saved=await (await saving).json();assert.equal(saved.data.expires_at,seconds);
  await page.getByText(corrected,{exact:true}).waitFor();
  await page.locator('.memory-row').filter({hasText:corrected}).getByRole('button',{name:'纠正',exact:true}).click();
  assert.equal(await expiry.inputValue(),localValue);
  await page.locator('#close-detail').click();
 }
 checks.push('Shanghai datetime-local preserves UTC upper boundary, five-digit year and fractional seconds through save and reopen');
 await page.locator('#workspace').selectOption('two');await page.locator('#memory-toggle').filter({hasText:'记忆已开启'}).waitFor();assert.equal(await page.locator('#memory-list .memory-row').count(),0);assert.equal(await page.locator('#memory-toggle').isEnabled(),true);checks.push('workspace isolation and viewer owns personal memory');
 await page.locator('#workspace').selectOption('one');await page.getByRole('button',{name:'纠正',exact:true}).waitFor();
 await page.screenshot({path:path.join(output,'memory-desktop.png'),fullPage:true});
 await page.setViewportSize({width:390,height:844});await page.screenshot({path:path.join(output,'memory-mobile.png'),fullPage:true});assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);checks.push('desktop and mobile management layouts');
 await page.locator('#nav-chat').click();await page.locator('#sessions button').filter({hasText:'记忆测试会话'}).click();
 memoryStatus={status:'failed',saved_count:0,error_code:'invalid_model_output'};await page.locator('#refresh-chat').click();await page.getByText('记忆处理失败，回答已保留',{exact:true}).waitFor();await page.getByText('好的，下面是回答。',{exact:true}).waitFor();
 memoryStatus={status:'cancelled',saved_count:0};await page.locator('#refresh-chat').click();await page.getByText('本轮记忆采集已取消',{exact:true}).waitFor();
 memoryStatus={status:'done',saved_count:0};await page.locator('#refresh-chat').click();await page.getByText('本轮未新增长期记忆',{exact:true}).waitFor();checks.push('failed cancelled and empty extraction do not replace successful answers');
 let releaseStatus;memoryStatus={status:'done',saved_count:999};delayedStatus=new Promise(r=>releaseStatus=r);await page.locator('#refresh-chat').click();await page.waitForTimeout(100);await page.locator('#workspace').selectOption('two');releaseStatus();await page.waitForTimeout(100);assert.doesNotMatch(await page.locator('#messages').textContent(),/999/);checks.push('late status response cannot cross workspace navigation');
 await page.locator('#workspace').selectOption('one');await page.locator('#nav-memory').click();await page.getByRole('button',{name:'纠正',exact:true}).waitFor();
 page.once('dialog',d=>d.dismiss());await page.locator('#memory-clear').click();assert.equal(scopes.get('one').items.filter(i=>i.status==='active').length,1);
 page.once('dialog',d=>d.accept());await page.locator('#memory-clear').click();await page.locator('#memory-count').filter({hasText:'0'}).waitFor();assert.equal(scopes.get('one').items.every(i=>i.status==='archived'),true);checks.push('clear requires explicit confirmation');
 await page.locator('#memory-status').selectOption('archived');await page.getByText(malicious,{exact:true}).waitFor();
 let release;delayed=new Promise(r=>release=r);await page.locator('#memory-refresh').click();await page.waitForTimeout(120);
 await page.evaluate(()=>{const c=new BroadcastChannel('magworkspace-auth');c.postMessage({kind:'logout'});c.close();});await page.locator('#auth').waitFor();release();await page.waitForTimeout(150);assert.equal(await page.locator('#memory-list').textContent(),'');assert.equal(await page.locator('#detail-body').textContent(),'');checks.push('late memory response cannot render after logout');
 assert.deepEqual(errors,[]);assert.deepEqual(unexpected,[]);
 await writeFile(path.join(output,'memory-browser.json'),JSON.stringify({fixture:true,checks},null,2));console.log('PASS: '+checks.length+' memory browser checks');
} catch(error) {
 console.error({checks,unexpected});throw error;
} finally {await browser?.close();await new Promise(resolve=>server.close(resolve));}
