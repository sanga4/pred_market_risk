#!/usr/bin/env python3
"""Best-effort historical Polymarket backfill. Safe to rerun; rows are de-duplicated."""
from datetime import datetime, timezone
import time, requests
from db import connect

CLOB='https://clob.polymarket.com'
HTTP=requests.Session(); HTTP.headers.update({'User-Agent':'Mozilla/5.0 EventRiskMVP/0.1'})

def main(limit=250):
    con=connect()
    rows=con.execute("""
      SELECT market_id,yes_token_id FROM market_metadata
      WHERE venue='polymarket' AND yes_token_id IS NOT NULL
      ORDER BY coalesce(liquidity,0) DESC LIMIT ?
    """,[limit]).fetchall()
    for mid,token in rows:
        try:
            r=HTTP.get(f'{CLOB}/prices-history',params={'market':token,'interval':'max','fidelity':60},timeout=30)
            if r.status_code!=200: continue
            hist=(r.json() or {}).get('history') or []
            for p in hist:
                ts=p.get('t'); price=p.get('p')
                if ts is None or price is None: continue
                dt=datetime.fromtimestamp(int(ts),timezone.utc)
                con.execute("""
                  INSERT INTO probability_history (venue,market_id,ts,prob,source,resolution_minutes)
                  VALUES ('polymarket',?,?,?,'polymarket_clob_history',60)
                  ON CONFLICT DO NOTHING
                """,[mid,dt,float(price)])
            print('backfill',mid,len(hist)); time.sleep(.08)
        except Exception as e: print('backfill failure',mid,e)
    con.close()

if __name__=='__main__': main()
