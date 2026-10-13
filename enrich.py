#!/usr/bin/env python3
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import pandas as pd
import requests
from db import connect

GAMMA="https://gamma-api.polymarket.com"
HEADERS={"User-Agent":"Mozilla/5.0 EventRiskMVP/0.3"}


def parse_tokens(v):
    if isinstance(v,list): return v
    try: return json.loads(v or "[]")
    except Exception: return []


def fetch_market(mid,event_id):
    r=requests.get(f"{GAMMA}/markets/{mid}",headers=HEADERS,timeout=25); r.raise_for_status(); m=r.json()
    toks=parse_tokens(m.get("clobTokenIds")); yes=toks[0] if toks else None; no=toks[1] if len(toks)>1 else None
    liq=m.get("liquidityNum") or m.get("liquidity")
    try: liq=float(liq) if liq is not None else None
    except Exception: liq=None
    return ('polymarket',str(mid),str(event_id or ''),m.get('conditionId'),m.get('slug'),yes,no,liq,m.get('description'),bool(m.get('active',True)),datetime.now(timezone.utc))


def main():
    con=connect()
    pending=con.execute("""
      SELECT t.market_id,arg_max(t.event_id,t.ts) event_id
      FROM ticks t LEFT JOIN market_metadata m ON m.venue=t.venue AND m.market_id=t.market_id
      WHERE t.venue='polymarket' AND (m.market_id IS NULL OR m.yes_token_id IS NULL)
      GROUP BY t.market_id
    """).fetchall()
    rows=[]
    with ThreadPoolExecutor(max_workers=10) as ex:
        futs={ex.submit(fetch_market,mid,eid):mid for mid,eid in pending}
        for fut in as_completed(futs):
            try: rows.append(fut.result())
            except Exception as e: print('enrich failure',futs[fut],e)
    if rows:
        df=pd.DataFrame(rows,columns=['venue','market_id','event_id','condition_id','slug','yes_token_id','no_token_id','liquidity','description','active','source_updated_at'])
        con.register('metadata_batch',df)
        try:
            con.execute("""
              INSERT INTO market_metadata
              (venue,market_id,event_id,condition_id,slug,yes_token_id,no_token_id,liquidity,description,active,source_updated_at,fetched_at)
              SELECT venue,market_id,event_id,condition_id,slug,yes_token_id,no_token_id,liquidity,description,active,source_updated_at,now()
              FROM metadata_batch
              ON CONFLICT (venue,market_id) DO UPDATE SET
                event_id=excluded.event_id,condition_id=excluded.condition_id,slug=excluded.slug,
                yes_token_id=excluded.yes_token_id,no_token_id=excluded.no_token_id,liquidity=excluded.liquidity,
                description=excluded.description,active=excluded.active,source_updated_at=excluded.source_updated_at,fetched_at=now()
            """)
        finally: con.unregister('metadata_batch')
    con.close(); print('metadata enriched',len(rows))


if __name__=='__main__': main()
