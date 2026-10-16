const {test} = require('node:test');
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const {resolve} = require('node:path');
const {JSDOM, VirtualConsole} = require('jsdom');

const root = resolve(__dirname, '../..');
const html = readFileSync(resolve(root, 'static/explorer/index.html'), 'utf8');
const script = readFileSync(resolve(root, 'static/explorer/explorer.js'), 'utf8');
const subject = {subject_id:'fam_1', subject_name:'Fed decisions', theme_id:'rates', event_count:2};
const event = {canonical_event_id:'evt_1', canonical_name:'Fed <img src=x onerror=alert(1)>?', subject_name:'Fed decisions', theme_id:'rates', sources:'kalshi', contract_count:1};
const summary = {as_of:'2026-10-16T12:00:00Z', basis:'Current taxonomy.', coverage:{events:3,subjects:2,themes:1,cross_source_events:0,uncategorized_events:0,formal_subject_events:0}, themes:[{theme_id:'rates',event_count:3,subject_count:2}], sources:[{source_id:'kalshi',source_name:'Kalshi'}], reference:{instruments:7192,instruments_with_issuer:0,entities:12}};
const mapping = {event:{...event,event_class:'rates'}, subject:{event_family_id:'fam_1',canonical_name:'Fed decisions'},theme:{theme_id:'rates'},insights:{source_ids:['kalshi'],current_contract_count:1,probability_range:null},source_contracts:[{source_id:'kalshi',current_title:event.canonical_name,source_url:'javascript:alert(1)',is_current:true,mapping_type:'source_native',review_status:'source_native',displayed_probability:0.4}],entity_mentions:[],include_expired:false,basis:'Current taxonomy.'};

function setup(override) {
  const requests = [], errors = [];
  const virtualConsole = new VirtualConsole();
  virtualConsole.on('jsdomError', error => errors.push(error));
  const dom = new JSDOM(html, {url:'https://eventrisk.test/explorer',runScripts:'outside-only',virtualConsole});
  dom.window.fetch = async (path, options) => {
    requests.push({path,options});
    let body, status = 200;
    const special = override && await override(path, options);
    if (special) {body=special.body; status=special.status ?? 200;}
    else if(path === '/health') body={ok:true,sources:[]};
    else if(path.startsWith('/v1/explorer/summary')) body=summary;
    else if(path.startsWith('/v1/explorer/subjects')) body={items:[subject],count:1,next_after:null};
    else if(path.includes('/mappings')) body={...mapping,include_expired:path.includes('true')};
    else if(path.startsWith('/v1/explorer/events')) {
      body = path.includes('after=') ? {items:[{...event,canonical_event_id:'evt_2'}],count:1,next_after:null} : {items:[event],count:1,next_after:'evt_1'};
    } else throw new Error('Unexpected path: '+path);
    return {ok:status<400,status,statusText:'Failure',headers:new Map([['x-request-id','req_test']]),text:async()=>JSON.stringify(body)};
  };
  dom.window.eval(script);
  const $ = id => dom.window.document.getElementById(id);
  async function connect() {
    $('api-key').value='secret-never-store';
    await $('connect-form').onsubmit({preventDefault(){}});
  }
  return {dom,$,requests,errors,connect};
}

async function until(condition) {
  for(let i=0;i<100;i++) {if(condition()) return; await new Promise(resolve=>setTimeout(resolve,5));}
  assert.fail('UI did not reach expected state');
}

test('connects, traces mappings, renders source text safely and inspects API responses',async()=>{
  const app=setup();
  try {
    await app.connect();
    assert.equal(app.$('workspace').hidden,false);
    assert.equal(app.$('api-key').value,'');
    assert.equal(app.dom.window.localStorage.length,0);
    assert.equal(app.dom.window.sessionStorage.length,0);
    assert.match(app.$('events').textContent,/<img src=x/);
    assert.equal(app.$('events').querySelector('img'),null);
    app.$('events').querySelector('button').click();
    await until(()=>!!app.$('detail').querySelector('table'));
    assert.match(app.$('detail').textContent,/Fed decisions/);
    assert.equal(app.$('detail').querySelector('a[href^="javascript:"]'),null);
    const checkbox=app.$('detail').querySelector('input[type=checkbox]');
    checkbox.checked=true; checkbox.onchange();
    await until(()=>app.requests.some(request=>request.path.includes('include_expired=true')));
    app.$('calls').querySelector('button').click();
    assert.match(app.$('response').textContent,/"event"/);
    assert.ok(app.requests.filter(request=>request.path.startsWith('/v1/')).every(request=>request.options.headers.Authorization==='Bearer secret-never-store'));
    assert.ok(!app.$('calls').textContent.includes('secret-never-store'));
    assert.equal(app.errors.length,0);
  } finally {app.dom.window.close();}
});

test('theme, subject, search and keyset pagination issue the expected API filters',async()=>{
  const app=setup();
  try {
    await app.connect();
    app.$('themes').querySelector('button').click();
    await until(()=>app.$('events').querySelector('button'));
    assert.ok(app.requests.some(request=>request.path.includes('theme=rates')));
    app.$('subjects').querySelector('button').click();
    await until(()=>app.$('events').querySelector('button'));
    assert.ok(app.requests.some(request=>request.path.includes('subject_id=fam_1')));
    app.$('events-next').click();
    await until(()=>app.$('events').querySelector('[data-event-id="evt_2"]'));
    assert.ok(app.requests.some(request=>request.path.includes('after=evt_1')));
    app.$('search').value='Fed';
    app.$('filters').onsubmit({preventDefault(){}});
    await until(()=>app.$('events').querySelector('[data-event-id="evt_1"]'));
    const lastEvent=app.requests.filter(request=>request.path.startsWith('/v1/explorer/events?')).at(-1).path;
    assert.ok(lastEvent.includes('q=Fed'));
    assert.ok(!lastEvent.includes('after=')&&!lastEvent.includes('subject_id='));
    assert.equal(app.errors.length,0);
  } finally {app.dom.window.close();}
});

test('reports auth failure without showing a connected workspace',async()=>{
  const app=setup(path=>path.includes('/summary')?{status:401,body:{detail:'Valid EventRisk API key required'}}:null);
  try {
    await app.connect();
    assert.equal(app.$('workspace').hidden,true);
    assert.match(app.$('error').textContent,/401/);
    assert.equal(app.$('connect').disabled,false);
  } finally {app.dom.window.close();}
});

test('disconnect prevents an in-flight response from repopulating private data',async()=>{
  let release;
  const app=setup(path=>path.includes('/mappings')?new Promise(resolve=>{release=()=>resolve({body:mapping});}):null);
  try {
    await app.connect();
    app.$('events').querySelector('button').click();
    await until(()=>!!release);
    app.$('disconnect').click();
    release();
    await new Promise(resolve=>setTimeout(resolve,20));
    assert.equal(app.$('workspace').hidden,true);
    assert.equal(app.$('calls').textContent,'');
    assert.equal(app.$('response').textContent,'No requests selected.');
    assert.ok(!app.$('detail').textContent.includes('Fed'));
  } finally {app.dom.window.close();}
});
