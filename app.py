import os
import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from db import connect
from pipeline import score_relevance

app = FastAPI(title="Event Risk", version="0.1.0")
app.mount("/static", StaticFiles(directory="static"), name="static")
HTTP = requests.Session()
HTTP.headers.update({"User-Agent":"Mozilla/5.0 EventRiskMVP/0.1"})


def rows_dict(cur):
    cols=[d[0] for d in cur.description]
    return [dict(zip(cols,r)) for r in cur.fetchall()]


def db_or_503():
    try: return connect(read_only=True)
    except Exception as e: raise HTTPException(503, f"Database unavailable: {e}")


def stock_snapshot(ticker):
    try:
        r=HTTP.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}",params={"interval":"5m","range":"1d"},timeout=8)
        r.raise_for_status(); x=r.json()["chart"]["result"][0]; meta=x.get("meta") or {}; q=(x.get("indicators",{}).get("quote") or [{}])[0]
        closes=[v for v in (q.get("close") or []) if v is not None]
        prev=meta.get("chartPreviousClose") or meta.get("previousClose"); last=closes[-1] if closes else meta.get("regularMarketPrice")
        return {"price":last,"previous_close":prev,"change":((last/prev)-1) if last and prev else None,"currency":meta.get("currency"),"exchange":meta.get("exchangeName"),"name":meta.get("longName") or meta.get("shortName")}
    except Exception:
        return {"price":None,"previous_close":None,"change":None,"currency":None,"exchange":None,"name":None}


@app.get("/health")
def health():
    try:
        con=db_or_503(); n=con.execute("SELECT count(*) FROM ticks").fetchone()[0]; latest=con.execute("SELECT max(ts) FROM ticks").fetchone()[0]; con.close()
        return {"ok":True,"ticks":n,"latest_tick":latest}
    except HTTPException as e: return {"ok":False,"detail":e.detail}


@app.get("/api/status")
def status():
    con=db_or_503(); d=rows_dict(con.execute("SELECT count(*) observations,count(DISTINCT market_id) markets,count(DISTINCT event_id) events,min(ts) first_tick,max(ts) latest_tick FROM ticks"))[0]
    try: d["model_runs"]=rows_dict(con.execute("SELECT job_type,status,finished_at,details FROM model_runs ORDER BY started_at DESC LIMIT 6"))
    except Exception: d["model_runs"]=[]
    con.close(); return d


@app.get("/api/search")
def search(q: str = Query(..., min_length=1, max_length=16)):
    q=q.strip().upper(); con=db_or_503()
    try:
        data=rows_dict(con.execute("SELECT ticker,name,sector,industry FROM securities WHERE upper(ticker) LIKE ? OR upper(coalesce(name,'')) LIKE ? ORDER BY CASE WHEN upper(ticker)=? THEN 0 ELSE 1 END,ticker LIMIT 12",[q+'%','%'+q+'%',q]))
    except Exception: data=[]
    con.close()
    if not any(x.get("ticker")==q for x in data): data.insert(0,{"ticker":q,"name":None,"sector":None,"industry":None})
    return data[:12]


@app.get("/api/ticker/{ticker}")
def ticker(ticker: str):
    ticker=ticker.strip().upper()
    if not ticker or len(ticker)>12 or not ticker.replace('.','').replace('-','').isalnum(): raise HTTPException(400,"Invalid ticker")
    quote=stock_snapshot(ticker); con=db_or_503()
    sec=None
    try:
        r=rows_dict(con.execute("SELECT ticker,name,sector,industry,exchange,quote_type FROM securities WHERE ticker=?",[ticker])); sec=r[0] if r else None
    except Exception: pass
    if sec is None: sec={"ticker":ticker,"name":quote.get("name"),"sector":None,"industry":None,"exchange":quote.get("exchange"),"quote_type":None}
    try:
        feed=rows_dict(con.execute("SELECT * FROM ticker_event_feed WHERE ticker=? ORDER BY relevance DESC,abs(coalesce(delta_24h,delta_1h,0)) DESC,coalesce(volume_24h,0) DESC LIMIT 30",[ticker]))
    except Exception: feed=[]
    if not feed:
        events=rows_dict(con.execute("SELECT ce.canonical_event_id,ce.title event_title,ce.category,ce.region,ce.theme,em.market_id,mf.market_title,mf.close_time,mf.latest_ts,mf.prob,mf.delta_1h,mf.delta_24h,mf.volume,mf.volume_24h,mf.spread FROM canonical_events ce JOIN event_markets em USING(canonical_event_id) JOIN market_features mf ON mf.venue=em.venue AND mf.market_id=em.market_id WHERE ce.eligible=TRUE AND mf.close_time>now() ORDER BY coalesce(mf.volume_24h,0) DESC LIMIT 300"))
        for e in events:
            score,sign,reason=score_relevance(e.get("theme"),sec.get("sector"),sec.get("industry"),ticker,e.get("event_title"))
            if score>=.18:
                e.update({"ticker":ticker,"relevance":score,"exposure_sign":sign,"reason":reason,"beta_per_1pp":None,"standard_error":None,"t_stat":None,"r_squared":None,"n_obs":None,"confidence":"calibration_pending","model_version":None,"estimated_24h_return":None}); feed.append(e)
        feed=sorted(feed,key=lambda x:(x.get("relevance") or 0,abs(x.get("delta_24h") or x.get("delta_1h") or 0),x.get("volume_24h") or 0),reverse=True)[:30]
    con.close()
    grouped=[]; seen=set()
    for e in feed:
        cid=e.get("canonical_event_id")
        if cid in seen: continue
        siblings=sorted([x for x in feed if x.get("canonical_event_id")==cid],key=lambda x:(x.get("volume_24h") or 0,x.get("volume") or 0),reverse=True)
        lead=siblings[0].copy(); lead["outcomes"]=siblings[:8]; grouped.append(lead); seen.add(cid)
    grouped=sorted(grouped,key=lambda x:((x.get("relevance") or 0)*(1+min(abs((x.get("delta_24h") or x.get("delta_1h") or 0))*10,1))),reverse=True)[:12]
    latest=max([x.get("latest_ts") for x in grouped if x.get("latest_ts")],default=None)
    return {"security":sec,"quote":quote,"events":grouped,"latest_event_tick":latest,"coefficient_note":"Sensitivity is abnormal stock return per +1 percentage-point event-probability change; missing values mean calibration pending."}


@app.get("/")
def root(): return FileResponse("static/index.html")
