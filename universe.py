#!/usr/bin/env python3
"""Best-effort refresh of a broad U.S. equity universe with sector/industry metadata."""
import pandas as pd
import requests
from db import connect

URL='https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=25&offset=0&download=true'
HEADERS={
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122 Safari/537.36',
    'Accept':'application/json,text/plain,*/*','Origin':'https://www.nasdaq.com',
    'Referer':'https://www.nasdaq.com/market-activity/stocks/screener'
}


def main():
    try:
        r=requests.get(URL,headers=HEADERS,timeout=45); r.raise_for_status()
        rows=((r.json() or {}).get('data') or {}).get('rows') or []
    except Exception as e:
        print('universe source unavailable; keeping existing securities:',e); return
    data=[]
    for x in rows:
        ticker=str(x.get('symbol') or '').strip().upper()
        if not ticker or len(ticker)>12: continue
        data.append((ticker,str(x.get('name') or '').strip() or None,str(x.get('sector') or '').strip() or None,
                     str(x.get('industry') or '').strip() or None,str(x.get('country') or '').strip() or None))
    if not data: print('universe source returned no rows; keeping existing securities'); return
    df=pd.DataFrame(data,columns=['ticker','name','sector','industry','country'])
    con=connect(); con.register('universe_batch',df)
    try:
        con.execute("""
          INSERT INTO securities (ticker,name,sector,industry,country,tracked,source,updated_at)
          SELECT ticker,name,sector,industry,country,FALSE,'nasdaq_screener',now() FROM universe_batch
          ON CONFLICT (ticker) DO UPDATE SET
            name=coalesce(excluded.name,securities.name),sector=coalesce(excluded.sector,securities.sector),
            industry=coalesce(excluded.industry,securities.industry),country=coalesce(excluded.country,securities.country),
            source=CASE WHEN securities.tracked THEN securities.source ELSE 'nasdaq_screener' END,updated_at=now()
        """)
    finally:
        con.unregister('universe_batch'); con.close()
    print('universe',len(data))


if __name__=='__main__': main()
