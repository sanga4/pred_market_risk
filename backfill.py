#!/usr/bin/env python3
"""Best-effort Polymarket history backfill. Bulk loads rows and only fetches max history once per market."""
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import pandas as pd
import requests
from db import connect

CLOB='https://clob.polymarket.com'
HEADERS={'User-Agent':'Mozilla/5.0 EventRiskMVP/0.3'}
LIMIT=int(os.getenv('MAX_BACKFILL_MARKETS','200'))


def fetch_one(mid, token, full):
    interval='max' if full else '1w'
    r=requests.get(f'{CLOB}/prices-history',params={'market':token,'interval':interval,'fidelity':60},headers=HEADERS,timeout=35)
    r.raise_for_status(); hist=(r.json() or {}).get('history') or []
    out=[]
    for p in hist:
        if p.get('t') is None or p.get('p') is None: continue
        out.append(('polymarket',str(mid),datetime.fromtimestamp(int(p['t']),timezone.utc),float(p['p']),'polymarket_clob_history',60))
    return out


def main():
    con=connect()
    markets=con.execute("""
      SELECT m.market_id,m.yes_token_id,
             NOT EXISTS(SELECT 1 FROM probability_history p WHERE p.venue='polymarket' AND p.market_id=m.market_id LIMIT 1) AS needs_full
      FROM market_metadata m
      WHERE m.venue='polymarket' AND m.yes_token_id IS NOT NULL
      ORDER BY coalesce(m.liquidity,0) DESC LIMIT ?
    """,[LIMIT]).fetchall()
    rows=[]
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs={ex.submit(fetch_one,mid,token,full):mid for mid,token,full in markets}
        for fut in as_completed(futs):
            mid=futs[fut]
            try:
                got=fut.result(); rows.extend(got); print('backfill',mid,len(got))
            except Exception as e: print('backfill failure',mid,e)
    if rows:
        df=pd.DataFrame(rows,columns=['venue','market_id','ts','prob','source','resolution_minutes'])
        con.register('history_batch',df)
        try:
            con.execute("""
              INSERT INTO probability_history (venue,market_id,ts,prob,source,resolution_minutes)
              SELECT venue,market_id,ts,prob,source,resolution_minutes FROM history_batch
              ON CONFLICT DO NOTHING
            """)
        finally: con.unregister('history_batch')
    con.close(); print('history rows fetched',len(rows))


if __name__=='__main__': main()
