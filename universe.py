#!/usr/bin/env python3
"""Refresh a broad U.S. equity universe with sector/industry metadata."""
import requests
from db import connect

URL='https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=25&offset=0&download=true'
HEADERS={
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122 Safari/537.36',
    'Accept':'application/json,text/plain,*/*',
    'Origin':'https://www.nasdaq.com',
    'Referer':'https://www.nasdaq.com/market-activity/stocks/screener'
}


def main():
    r=requests.get(URL,headers=HEADERS,timeout=45); r.raise_for_status()
    rows=((r.json() or {}).get('data') or {}).get('rows') or []
    con=connect(); count=0
    for x in rows:
        ticker=str(x.get('symbol') or '').strip().upper()
        if not ticker or len(ticker)>12: continue
        name=str(x.get('name') or '').strip() or None
        sector=str(x.get('sector') or '').strip() or None
        industry=str(x.get('industry') or '').strip() or None
        country=str(x.get('country') or '').strip() or None
        con.execute("""
          INSERT INTO securities (ticker,name,sector,industry,country,tracked,source,updated_at)
          VALUES (?,?,?,?,?,FALSE,'nasdaq_screener',now())
          ON CONFLICT (ticker) DO UPDATE SET
            name=coalesce(excluded.name,securities.name),
            sector=coalesce(excluded.sector,securities.sector),
            industry=coalesce(excluded.industry,securities.industry),
            country=coalesce(excluded.country,securities.country),
            source='nasdaq_screener',updated_at=now()
        """,[ticker,name,sector,industry,country]); count+=1
    con.close(); print('universe',count)

if __name__=='__main__': main()
