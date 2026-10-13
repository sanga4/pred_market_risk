#!/usr/bin/env python3
import json, time
from datetime import datetime, timezone
import requests
from db import connect

GAMMA="https://gamma-api.polymarket.com"
HTTP=requests.Session(); HTTP.headers.update({"User-Agent":"Mozilla/5.0 EventRiskMVP/0.1"})

def parse_tokens(v):
    if isinstance(v,list): return v
    try: return json.loads(v or "[]")
    except Exception: return []

def main():
    con=connect()
    ids=con.execute("SELECT market_id,arg_max(event_id,ts) event_id FROM ticks WHERE venue='polymarket' GROUP BY market_id").fetchall()
    existing={r[0] for r in con.execute("SELECT market_id FROM market_metadata WHERE venue='polymarket' AND yes_token_id IS NOT NULL").fetchall()}
    for mid,event_id in ids:
        if mid in existing: continue
        try:
            r=HTTP.get(f"{GAMMA}/markets/{mid}",timeout=20); r.raise_for_status(); m=r.json()
            tokens=parse_tokens(m.get("clobTokenIds")); yes=tokens[0] if tokens else None; no=tokens[1] if len(tokens)>1 else None
            con.execute("""
              INSERT INTO market_metadata
              (venue,market_id,event_id,condition_id,slug,yes_token_id,no_token_id,liquidity,description,active,source_updated_at,fetched_at)
              VALUES ('polymarket',?,?,?,?,?,?,?,?,?,?,now())
              ON CONFLICT (venue,market_id) DO UPDATE SET
                event_id=excluded.event_id,condition_id=excluded.condition_id,slug=excluded.slug,
                yes_token_id=excluded.yes_token_id,no_token_id=excluded.no_token_id,liquidity=excluded.liquidity,
                description=excluded.description,active=excluded.active,source_updated_at=excluded.source_updated_at,fetched_at=now()
            """,[str(event_id or ''),m.get("conditionId"),m.get("slug"),yes,no,
                  float(m.get("liquidityNum") or m.get("liquidity") or 0) if (m.get("liquidityNum") or m.get("liquidity")) else None,
                  m.get("description"),bool(m.get("active",True)),datetime.now(timezone.utc)])
            time.sleep(.05)
        except Exception as e: print("enrich failure",mid,e)
    con.close()

if __name__=='__main__': main()
