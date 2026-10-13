#!/usr/bin/env python3
"""Production collector entrypoint that bounds the very large Kalshi open-market universe."""
import math
import os
import re
import sys
import collector

MAX_KALSHI=int(os.getenv('MAX_KALSHI_MARKETS','1000'))
RELEVANT=re.compile(
    r"taiwan|china|iran|israel|gaza|hamas|hezbollah|russia|ukraine|nato|war|military|"
    r"missile|nuclear|sanction|tariff|trade|fed|fomc|interest rate|rate cut|inflation|"
    r"recession|gdp|unemployment|jobs report|nonfarm|cpi|pce|oil|gas|opec|hormuz|"
    r"shipping|bitcoin|ethereum|crypto|sec |regulation|debt ceiling|government shutdown|"
    r"treasury|election|president|congress|supreme court|merger|acquisition|bankruptcy|"
    r"earnings|ipo|antitrust|export|import|semiconductor|chip",
    re.I,
)

orig_kalshi=collector.kalshi


def bounded_kalshi():
    rows=[]
    for r in orig_kalshi():
        text=f"{r.get('event_title') or ''} {r.get('market_title') or ''}"
        if not RELEVANT.search(text):
            continue
        vol=float(r.get('volume') or 0); vol24=float(r.get('volume_24h') or 0)
        # Keep the important tail but drop near-empty micro-contracts.
        if vol < 100 and vol24 < 10:
            continue
        score=vol24*10 + math.log1p(max(vol,0))
        rows.append((score,r))
    rows.sort(key=lambda x:x[0],reverse=True)
    for _,r in rows[:MAX_KALSHI]:
        yield r


collector.kalshi=bounded_kalshi

if __name__=='__main__':
    sys.exit(collector.main())
