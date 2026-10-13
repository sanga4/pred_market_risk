const $ = s => document.querySelector(s);
const $$ = s => [...document.querySelectorAll(s)];
const esc = s => String(s ?? '').replace(/[&<>"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#039;'}[m]));
const fp = x => x == null ? '—' : (x * 100).toFixed(Math.abs(x) < .01 ? 1 : 0) + '%';
const fpp = x => x == null ? '—' : (x >= 0 ? '+' : '') + (x * 100).toFixed(1) + ' pp';
const fb = x => x == null ? 'calibrating' : (x >= 0 ? '+' : '') + (x * 100).toFixed(2) + '% / pp';
const cls = x => x > 0 ? 'pos' : x < 0 ? 'neg' : '';
const money = x => x == null || Number.isNaN(Number(x)) ? '—' : '$' + Number(x).toLocaleString(undefined,{maximumFractionDigits:2});
const pct = x => x == null || Number.isNaN(Number(x)) ? '—' : (Number(x) * 100).toFixed(1) + '%';
const STORE_CURRENT = 'eventrisk_portfolio_current_v1';
const STORE_HISTORY = 'eventrisk_portfolio_snapshots_v1';
let currentRows = [];
let ocrBusy = false;
let activeTab = 'ticker';

function switchTab(tab) {
  activeTab = tab;
  $$('.tab').forEach(b => b.classList.toggle('active', b.dataset.tab === tab));
  $('#tickerView').hidden = tab !== 'ticker';
  $('#portfolioView').hidden = tab !== 'portfolio';
  if (tab === 'portfolio') {
    history.replaceState(null,'', location.pathname + '#portfolio');
    const saved = getCurrentPortfolio();
    if (saved?.holdings?.length) {
      currentRows = saved.holdings.map(x => ({...x}));
      renderEditor('Saved portfolio loaded from this browser.');
      analyzePortfolio(false);
    }
  } else {
    const t = ($('#q')?.value || 'NVDA').trim().toUpperCase();
    history.replaceState(null,'', '?ticker=' + encodeURIComponent(t));
  }
}

$$('.tab').forEach(b => b.addEventListener('click', () => switchTab(b.dataset.tab)));

async function loadTicker(t) {
  t = t.trim().toUpperCase(); if (!t) return;
  if (activeTab === 'ticker') history.replaceState(null,'','?ticker='+encodeURIComponent(t));
  $('#app').innerHTML = '<div class="empty">Loading '+esc(t)+'…</div>';
  try {
    const r = await fetch('/api/ticker/'+encodeURIComponent(t));
    if (!r.ok) throw Error(await r.text());
    const d = await r.json(), s=d.security||{}, q=d.quote||{};
    const price=q.price==null?'—':(q.currency==='USD'?'$':'')+Number(q.price).toFixed(2), chg=q.change;
    let h='<div class="tickerhead"><div><div class="ticker">'+esc(s.ticker)+'</div><div class="muted">'+esc(s.name||q.name||'Security')+(s.sector?' · '+esc(s.sector):'')+'</div></div><div><div class="price">'+price+'</div><div class="'+cls(chg)+'">'+(chg==null?'':(chg>=0?'+':'')+(chg*100).toFixed(2)+'% today')+'</div></div></div><div class="meta"><div class="pill">'+d.events.length+' relevant live events</div><div class="pill">Updated '+(d.latest_event_tick?new Date(d.latest_event_tick).toLocaleString():'—')+'</div><div class="pill">Sensitivity = probability response, not outcome shock</div></div><div class="events">';
    if(!d.events.length) h+='<div class="empty">No relevant unresolved events are mapped yet for this ticker.</div>';
    for(const e of d.events){
      const dp=e.delta_24h??e.delta_1h,b=e.beta_per_1pp,impact=e.estimated_24h_return;
      h+='<div class="event"><div><div class="eventname">'+esc(e.event_title)+'</div><div class="reason">'+esc(e.reason||'Event exposure')+' · '+esc(e.market_title||'')+'</div><div class="bar"><i style="width:'+Math.max(2,Math.min(100,(e.relevance||0)*100))+'%"></i></div></div><div><div class="label">Probability</div><div class="val">'+fp(e.prob)+'</div></div><div><div class="label">24h Δ</div><div class="val '+cls(dp)+'">'+fpp(dp)+'</div></div><div><div class="label">Sensitivity</div><div class="val '+(b==null?'pending':cls(b))+'">'+fb(b)+'</div></div><div><div class="label">Assoc. impact</div><div class="val '+cls(impact)+'">'+(impact==null?'—':(impact>=0?'+':'')+(impact*100).toFixed(2)+'%')+'</div></div></div>';
    }
    h+='</div>'; $('#app').innerHTML=h;
  } catch(e) { $('#app').innerHTML='<div class="empty">Live database unavailable: '+esc(e.message)+'</div>'; }
}

$('#search').addEventListener('submit', e => { e.preventDefault(); loadTicker($('#q').value); });

function getCurrentPortfolio() {
  try { return JSON.parse(localStorage.getItem(STORE_CURRENT) || 'null'); } catch { return null; }
}
function savePortfolio(rows) {
  const snapshot = { id: Date.now(), created_at: new Date().toISOString(), holdings: rows.map(r => ({ticker:r.ticker, name:r.name||null, market_value:r.market_value ?? null, weight:r.weight ?? null})) };
  localStorage.setItem(STORE_CURRENT, JSON.stringify(snapshot));
  let hist=[]; try { hist=JSON.parse(localStorage.getItem(STORE_HISTORY)||'[]'); } catch {}
  hist.unshift(snapshot); localStorage.setItem(STORE_HISTORY, JSON.stringify(hist.slice(0,30)));
  updateSnapshotBadge(snapshot);
}
function updateSnapshotBadge(snapshot) {
  const el=$('#snapshotBadge');
  if (!snapshot) { el.textContent='No saved portfolio on this browser'; return; }
  let n=0; try { n=JSON.parse(localStorage.getItem(STORE_HISTORY)||'[]').length; } catch {}
  el.textContent='Saved locally · '+new Date(snapshot.created_at).toLocaleString()+' · '+n+' snapshot'+(n===1?'':'s');
}

function setOcrStatus(text, progress=null, kind='') {
  const box=$('#ocrStatus');
  box.hidden=false;
  box.className='ocr-status '+kind;
  box.innerHTML='<div>'+esc(text)+'</div>'+(progress==null?'':'<div class="progress"><i style="width:'+Math.max(2,Math.min(100,progress*100))+'%"></i></div>');
}

const OCR_EXCLUDE = new Set(['USD','CASH','TOTAL','TODAY','SHARE','SHARES','MARKET','VALUE','PRICE','COST','GAIN','LOSS','BUY','SELL','INVEST','INVESTING','PORTFOLIO','ACCOUNT','STOCK','STOCKS','EQUITY','EQUITIES','POSITION','POSITIONS','HOLDING','HOLDINGS','RETURN','DAY','DAYS','OPEN','CLOSE','AVG','AVERAGE','PERCENT','PCT','OPTIONS','OPTION','CRYPTO','WATCHLIST','AVAILABLE','BALANCE']);
function parseMoney(s) { const n=Number(String(s).replace(/[$,\s]/g,'')); return Number.isFinite(n)?n:null; }
function parseOCRText(text) {
  const lines=String(text||'').split(/\r?\n/).map(x=>x.trim()).filter(Boolean);
  const candidates=new Map();
  for(let i=0;i<lines.length;i++){
    const upper=lines[i].toUpperCase();
    const tokens=upper.match(/\b[A-Z]{1,5}(?:[.-][A-Z])?\b/g)||[];
    for(const raw of tokens){
      const ticker=raw.replace(/,$/,'');
      if(OCR_EXCLUDE.has(ticker) || ticker.length<1) continue;
      const context=lines.slice(Math.max(0,i-1),Math.min(lines.length,i+4)).join(' ');
      const moneyMatches=context.match(/\$\s*(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d{1,2})?/g)||[];
      const values=moneyMatches.map(parseMoney).filter(x=>x!=null && x>=1);
      const pcts=[...context.matchAll(/(-?\d+(?:\.\d+)?)\s*%/g)].map(m=>Number(m[1])).filter(x=>Number.isFinite(x)&&x>=0&&x<=100);
      const market_value=values.length?Math.max(...values):null;
      const weight=pcts.length?pcts[0]/100:null;
      const prev=candidates.get(ticker);
      if(!prev || (market_value||0)>(prev.market_value||0)) candidates.set(ticker,{ticker,market_value,weight,source:'ocr'});
    }
  }
  return [...candidates.values()];
}

async function validateCandidates(rows) {
  if(!rows.length) return [];
  const symbols=[...new Set(rows.map(x=>x.ticker))];
  const r=await fetch('/api/symbols/validate',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({symbols})});
  if(!r.ok) throw Error('Ticker validation failed');
  const known=await r.json(), map=new Map(known.map(x=>[x.ticker,x]));
  return rows.filter(x=>map.has(x.ticker)).map(x=>({...x,name:map.get(x.ticker).name||null,sector:map.get(x.ticker).sector||null}));
}

function mergeRows(rows) {
  const map=new Map(currentRows.map(x=>[x.ticker,{...x}]));
  for(const r of rows){
    const prev=map.get(r.ticker);
    if(!prev) map.set(r.ticker,{...r});
    else {
      if((r.market_value||0)>(prev.market_value||0)) prev.market_value=r.market_value;
      if(prev.weight==null && r.weight!=null) prev.weight=r.weight;
      if(!prev.name && r.name) prev.name=r.name;
    }
  }
  currentRows=[...map.values()];
  recalcWeightsFromValues();
}

function recalcWeightsFromValues() {
  const total=currentRows.reduce((s,r)=>s+(Number(r.market_value)||0),0);
  if(total>0) currentRows.forEach(r=>r.weight=(Number(r.market_value)||0)/total);
  else {
    const wsum=currentRows.reduce((s,r)=>s+(Number(r.weight)||0),0);
    if(wsum>0) currentRows.forEach(r=>r.weight=(Number(r.weight)||0)/wsum);
  }
}

async function processFiles(files) {
  files=[...files].filter(f=>f.type.startsWith('image/'));
  if(!files.length || ocrBusy) return;
  if(!window.Tesseract){ setOcrStatus('OCR library did not load. Refresh the page and try again.',null,'error'); return; }
  ocrBusy=true; $('#chooseBtn').disabled=true; $('#demoBtn').disabled=true;
  try {
    let parsed=[];
    for(let i=0;i<files.length;i++){
      setOcrStatus('Reading screenshot '+(i+1)+' of '+files.length+' locally in your browser…', i/files.length);
      const res=await Tesseract.recognize(files[i],'eng',{logger:m=>{if(m.status==='recognizing text') setOcrStatus('Reading screenshot '+(i+1)+' of '+files.length+' · '+Math.round((m.progress||0)*100)+'%',(i+(m.progress||0))/files.length);}});
      parsed.push(...parseOCRText(res.data.text));
    }
    setOcrStatus('Validating detected tickers…',.96);
    const valid=await validateCandidates(parsed);
    if(!valid.length){
      setOcrStatus('I could not confidently identify a listed ticker. Add holdings manually below or try a clearer/cropped screenshot.',null,'warn');
      if(!currentRows.length) currentRows=[{ticker:'',name:null,market_value:null,weight:null,source:'manual'}];
    } else {
      mergeRows(valid); setOcrStatus('Found '+valid.length+' recognized holding'+(valid.length===1?'':'s')+'. Review them before analysis.',1,'success');
    }
    renderEditor();
  } catch(e) {
    setOcrStatus('Screenshot reading failed: '+e.message,null,'error');
  } finally { ocrBusy=false; $('#chooseBtn').disabled=false; $('#demoBtn').disabled=false; $('#fileInput').value=''; }
}

function createDemoScreenshot() {
  const c=document.createElement('canvas'); c.width=900; c.height=1120; const x=c.getContext('2d');
  x.fillStyle='#f7f9fc'; x.fillRect(0,0,c.width,c.height); x.fillStyle='#152235'; x.font='700 42px Arial'; x.fillText('My Holdings',56,80);
  x.font='24px Arial'; x.fillStyle='#607085'; x.fillText('Account value  $42,040.00',56,128);
  const rows=[['NVDA','NVIDIA','$18,420.00'],['AAPL','Apple','$9,120.00'],['TSM','Taiwan Semiconductor','$6,800.00'],['XOM','Exxon Mobil','$4,500.00'],['JPM','JPMorgan Chase','$3,200.00']];
  let y=220;
  for(const [t,n,v] of rows){ x.strokeStyle='#dce4ec'; x.beginPath(); x.moveTo(56,y-40); x.lineTo(844,y-40); x.stroke(); x.fillStyle='#152235'; x.font='700 30px Arial'; x.fillText(t,56,y); x.font='21px Arial'; x.fillStyle='#6c7d91'; x.fillText(n,56,y+34); x.font='700 30px Arial'; x.fillStyle='#152235'; x.textAlign='right'; x.fillText(v,844,y+12); x.textAlign='left'; y+=160; }
  c.toBlob(blob=>{ if(blob) processFiles([new File([blob],'synthetic-portfolio.png',{type:'image/png'})]); },'image/png');
}

function renderEditor(note='') {
  const box=$('#holdingsEditor'); box.hidden=false;
  recalcWeightsFromValues();
  const total=currentRows.reduce((s,r)=>s+(Number(r.market_value)||0),0);
  $('#editorSummary').textContent=(currentRows.filter(r=>r.ticker).length||0)+' positions'+(total>0?' · '+money(total)+' detected':'')+(note?' · '+note:'');
  const body=$('#holdingsBody');
  body.innerHTML=currentRows.map((r,i)=>'<tr data-i="'+i+'"><td><input class="cell ticker-input" value="'+esc(r.ticker||'')+'" placeholder="NVDA" maxlength="12"><div class="tiny">'+esc(r.name||'')+'</div></td><td><input class="cell value-input" type="number" min="0" step="0.01" value="'+(r.market_value??'')+'" placeholder="18420"></td><td><input class="cell weight-input" type="number" min="0" max="100" step="0.1" value="'+(r.weight==null?'':(r.weight*100).toFixed(1))+'" placeholder="32.1"></td><td><button class="icon-btn remove-row" title="Remove">×</button></td></tr>').join('');
  $$('.ticker-input').forEach((el,i)=>el.addEventListener('input',()=>{ currentRows[i].ticker=el.value.trim().toUpperCase(); }));
  $$('.value-input').forEach((el,i)=>el.addEventListener('change',()=>{ currentRows[i].market_value=el.value===''?null:Number(el.value); recalcWeightsFromValues(); renderEditor(); }));
  $$('.weight-input').forEach((el,i)=>el.addEventListener('input',()=>{ currentRows[i].weight=el.value===''?null:Number(el.value)/100; }));
  $$('.remove-row').forEach((el,i)=>el.addEventListener('click',()=>{ currentRows.splice(i,1); renderEditor(); }));
}

$('#addRowBtn').addEventListener('click',()=>{ currentRows.push({ticker:'',name:null,market_value:null,weight:null,source:'manual'}); renderEditor(); });
$('#fileInput').addEventListener('change',e=>processFiles(e.target.files));
$('#chooseBtn').addEventListener('click',()=>$('#fileInput').click());
$('#demoBtn').addEventListener('click',createDemoScreenshot);
const dz=$('#dropzone');
['dragenter','dragover'].forEach(ev=>dz.addEventListener(ev,e=>{e.preventDefault();dz.classList.add('drag');}));
['dragleave','drop'].forEach(ev=>dz.addEventListener(ev,e=>{e.preventDefault();dz.classList.remove('drag');}));
dz.addEventListener('drop',e=>processFiles(e.dataTransfer.files));

$('#clearPortfolioBtn').addEventListener('click',()=>{
  if(!confirm('Clear the saved portfolio and local snapshot history from this browser?')) return;
  localStorage.removeItem(STORE_CURRENT); localStorage.removeItem(STORE_HISTORY); currentRows=[]; $('#holdingsEditor').hidden=true; $('#portfolioResults').innerHTML=''; $('#ocrStatus').hidden=true; updateSnapshotBadge(null);
});

async function analyzePortfolio(save=true) {
  currentRows=currentRows.filter(r=>String(r.ticker||'').trim()).map(r=>({...r,ticker:String(r.ticker).trim().toUpperCase()}));
  if(!currentRows.length){ setOcrStatus('Add at least one ticker before analyzing.',null,'warn'); return; }
  recalcWeightsFromValues();
  const totalWeight=currentRows.reduce((s,r)=>s+(Number(r.weight)||0),0);
  if(totalWeight<=0){ const w=1/currentRows.length; currentRows.forEach(r=>r.weight=w); }
  if(save) savePortfolio(currentRows);
  $('#portfolioResults').innerHTML='<div class="empty">Analyzing current event exposure across '+currentRows.length+' holding'+(currentRows.length===1?'':'s')+'…</div>';
  try {
    const payload={holdings:currentRows.map(r=>({ticker:r.ticker,market_value:r.market_value??null,weight:r.weight??null}))};
    const res=await fetch('/api/portfolio/analyze',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});
    if(!res.ok) throw Error(await res.text());
    const d=await res.json(); renderPortfolioResults(d);
  } catch(e){ $('#portfolioResults').innerHTML='<div class="empty">Portfolio analysis unavailable: '+esc(e.message)+'</div>'; }
}
$('#analyzePortfolioBtn').addEventListener('click',()=>analyzePortfolio(true));

function exposureClass(level){ return level==='very high'?'hot':level==='high'?'high':level==='moderate'?'medium':'low'; }
function renderPortfolioResults(d){
  const total=d.total_value, holdings=d.holdings||[], events=d.events||[];
  let h='<div class="portfolio-head"><div><div class="eyebrow">MY PORTFOLIO</div><div class="portfolio-title">'+holdings.length+' holding'+(holdings.length===1?'':'s')+(total?' · '+money(total):'')+'</div><div class="muted">Live event risk using your latest locally saved holdings. Prediction probabilities continue updating without another screenshot.</div></div><div class="updated">Events updated<br><strong>'+(d.latest_event_tick?new Date(d.latest_event_tick).toLocaleString():'—')+'</strong></div></div>';
  h+='<div class="meta"><div class="pill">'+events.length+' portfolio event exposures</div><div class="pill">'+(d.weighting==='equal'?'Equal-weight analysis':'Portfolio-weighted analysis')+'</div><div class="pill">Screenshots are not stored</div></div>';
  if(!events.length){ h+='<div class="empty">No relevant unresolved events are mapped to these holdings yet.</div>'; $('#portfolioResults').innerHTML=h; return; }
  h+='<div class="portfolio-events">';
  for(const e of events){
    const dp=e.delta_24h??e.delta_1h, b=e.coefficient_coverage>0?e.portfolio_beta_per_1pp:null, impact=e.estimated_24h_return;
    const chips=(e.affected_holdings||[]).slice(0,6).map(x=>'<span class="holding-chip"><b>'+esc(x.ticker)+'</b> '+pct(x.weight)+'</span>').join('');
    h+='<div class="portfolio-event"><div class="pe-top"><div><div class="eventname">'+esc(e.event_title)+'</div><div class="reason">Affected holdings '+pct(e.affected_weight)+' of portfolio · '+chips+'</div></div><span class="exposure '+exposureClass(e.exposure_level)+'">'+esc(e.exposure_level).toUpperCase()+'</span></div><div class="pe-grid"><div><div class="label">Probability</div><div class="metric">'+fp(e.prob)+'</div></div><div><div class="label">24h Δ</div><div class="metric '+cls(dp)+'">'+fpp(dp)+'</div></div><div><div class="label">Portfolio sensitivity</div><div class="metric '+(b==null?'pending':cls(b))+'">'+fb(b)+'</div></div><div><div class="label">Assoc. 24h move</div><div class="metric '+cls(impact)+'">'+(impact==null?'—':(impact>=0?'+':'')+(impact*100).toFixed(2)+'%')+'</div></div></div><div class="coverage"><span>Event exposure</span><div class="bar"><i style="width:'+Math.max(2,Math.min(100,(e.weighted_relevance||0)*100))+'%"></i></div><span>Coefficient coverage '+pct(e.coefficient_coverage)+'</span></div></div>';
  }
  h+='</div><div class="footnote">Portfolio sensitivity is weight-scaled and only uses coefficients currently calibrated for each holding. Low coefficient coverage means the estimate is partial; it is not a forecast.</div>';
  $('#portfolioResults').innerHTML=h;
}

const saved=getCurrentPortfolio(); updateSnapshotBadge(saved);
const initial=new URLSearchParams(location.search).get('ticker')||'NVDA'; $('#q').value=initial; loadTicker(initial);
if(location.hash==='#portfolio') switchTab('portfolio');
setInterval(()=>{ if(activeTab==='ticker') loadTicker($('#q').value); else { const s=getCurrentPortfolio(); if(s?.holdings?.length){ currentRows=s.holdings.map(x=>({...x})); analyzePortfolio(false); } } },60000);
fetch('/api/status').then(r=>r.json()).then(d=>{$('#status').textContent='DB '+Number(d.observations||0).toLocaleString()+' observations · latest '+(d.latest_tick?new Date(d.latest_tick).toLocaleTimeString():'—')}).catch(()=>{});
