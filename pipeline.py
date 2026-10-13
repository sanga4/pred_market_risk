#!/usr/bin/env python3
import argparse
import math
import re
import time
import uuid
from datetime import datetime, timezone

import numpy as np
import requests
import statsmodels.api as sm

from db import connect

HTTP = requests.Session()
HTTP.headers.update({"User-Agent": "Mozilla/5.0 (compatible; EventRiskMVP/0.1)"})

SEED_SECURITIES = [
    ("NVDA", "NVIDIA", "Technology", "Semiconductors"),
    ("AMD", "Advanced Micro Devices", "Technology", "Semiconductors"),
    ("AVGO", "Broadcom", "Technology", "Semiconductors"),
    ("INTC", "Intel", "Technology", "Semiconductors"),
    ("AAPL", "Apple", "Technology", "Consumer Electronics"),
    ("MSFT", "Microsoft", "Technology", "Software"),
    ("GOOGL", "Alphabet", "Communication Services", "Internet Content"),
    ("META", "Meta Platforms", "Communication Services", "Internet Content"),
    ("AMZN", "Amazon", "Consumer Cyclical", "Internet Retail"),
    ("TSLA", "Tesla", "Consumer Cyclical", "Auto Manufacturers"),
    ("TSM", "Taiwan Semiconductor", "Technology", "Semiconductors"),
    ("ASML", "ASML", "Technology", "Semiconductor Equipment"),
    ("XOM", "Exxon Mobil", "Energy", "Oil & Gas Integrated"),
    ("CVX", "Chevron", "Energy", "Oil & Gas Integrated"),
    ("COP", "ConocoPhillips", "Energy", "Oil & Gas E&P"),
    ("OXY", "Occidental Petroleum", "Energy", "Oil & Gas E&P"),
    ("JPM", "JPMorgan Chase", "Financial Services", "Banks"),
    ("BAC", "Bank of America", "Financial Services", "Banks"),
    ("GS", "Goldman Sachs", "Financial Services", "Capital Markets"),
    ("BA", "Boeing", "Industrials", "Aerospace & Defense"),
    ("LMT", "Lockheed Martin", "Industrials", "Aerospace & Defense"),
    ("RTX", "RTX", "Industrials", "Aerospace & Defense"),
    ("NOC", "Northrop Grumman", "Industrials", "Aerospace & Defense"),
    ("DAL", "Delta Air Lines", "Industrials", "Airlines"),
    ("UAL", "United Airlines", "Industrials", "Airlines"),
    ("CCL", "Carnival", "Consumer Cyclical", "Travel Services"),
    ("MSTR", "Strategy", "Technology", "Software - Bitcoin exposure"),
    ("COIN", "Coinbase", "Financial Services", "Capital Markets - Crypto"),
    ("SPY", "SPDR S&P 500 ETF", "ETF", "Broad Market"),
    ("QQQ", "Invesco QQQ", "ETF", "Nasdaq 100"),
    ("SOXX", "iShares Semiconductor ETF", "ETF", "Semiconductors"),
    ("XLE", "Energy Select Sector SPDR", "ETF", "Energy"),
    ("XLF", "Financial Select Sector SPDR", "ETF", "Financials"),
    ("ITA", "iShares U.S. Aerospace & Defense ETF", "ETF", "Aerospace & Defense"),
]

THEME_PATTERNS = [
    ("taiwan", r"taiwan|taipei|china invade|china.*blockade"),
    ("china_trade", r"china.*trade|china.*tariff|trade deal.*china|chinese.*tariff|export restriction|chip.*restriction|semiconductor.*ban"),
    ("iran_hormuz", r"iran|hormuz|persian gulf|kharg|farsi island|hengam|abu musa|tunb|houthis|houthi"),
    ("israel_middle_east", r"israel|gaza|hamas|hezbollah|lebanon|syria|saudi|middle east"),
    ("russia_ukraine", r"russia|ukraine|nato|putin|zelensky|crimea|donbas|kremlin"),
    ("korea", r"north korea|south korea|kim jong"),
    ("trade", r"tariff|trade deal|trade agreement|customs|import|export"),
    ("energy", r"oil|gas|diesel|pipeline|opec|energy|refinery|shipping|strait"),
    ("rates", r"fed|federal reserve|rate cut|interest rate|fomc|treasury"),
    ("recession", r"recession|gdp|unemployment|economic contraction"),
    ("crypto", r"bitcoin|crypto|ethereum|binance|coinbase"),
    ("defense", r"military|war|strike|invasion|nuclear|missile|clash|attack"),
]


def classify_theme(text):
    s = (text or "").lower()
    for theme, pattern in THEME_PATTERNS:
        if re.search(pattern, s):
            return theme
    return "other"


def classify_region(text):
    s = (text or "").lower()
    if any(x in s for x in ("taiwan", "china", "korea", "japan")): return "asia"
    if any(x in s for x in ("iran", "israel", "gaza", "syria", "saudi", "lebanon", "hormuz", "houth")): return "middle_east"
    if any(x in s for x in ("russia", "ukraine", "nato", "crimea", "donbas")): return "europe"
    if any(x in s for x in ("venezuela", "argentina", "brazil", "mexico", "canada")): return "americas"
    return "global"


def eligible_event(text):
    s = (text or "").lower()
    low_value = ["mrbeast", "ishowspeed", "f1 race", "shake hands", "who will trump meet", "where will trump", "gold cards"]
    return not any(x in s for x in low_value)


def ensure_seeds(con):
    for ticker, name, sector, industry in SEED_SECURITIES:
        con.execute("""
        INSERT INTO securities (ticker,name,sector,industry,tracked,source,updated_at)
        VALUES (?,?,?,?,TRUE,'seed',now())
        ON CONFLICT (ticker) DO UPDATE SET
          name=excluded.name, sector=excluded.sector, industry=excluded.industry,
          tracked=TRUE, updated_at=now()
        """, [ticker, name, sector, industry])


def refresh_market_features(con):
    con.execute("DELETE FROM market_features")
    con.execute("""
    INSERT INTO market_features
    WITH latest AS (
      SELECT venue,market_id,max(ts) AS latest_ts
      FROM ticks WHERE prob IS NOT NULL GROUP BY venue,market_id
    ), base AS (
      SELECT t.*,l.latest_ts FROM ticks t JOIN latest l USING(venue,market_id)
      WHERE t.prob IS NOT NULL
    ), agg AS (
      SELECT
        venue, market_id, latest_ts,
        arg_max(event_id, ts) AS event_id,
        arg_max(event_title, ts) AS event_title,
        arg_max(market_title, ts) AS market_title,
        arg_max(close_time, ts) AS close_time,
        arg_max(prob, ts) AS prob,
        arg_max(prob, ts) FILTER (WHERE ts <= latest_ts - INTERVAL '1 hour') AS prob_1h,
        arg_max(prob, ts) FILTER (WHERE ts <= latest_ts - INTERVAL '24 hours') AS prob_24h,
        arg_max(yes_bid, ts) AS yes_bid,
        arg_max(yes_ask, ts) AS yes_ask,
        arg_max(last, ts) AS last,
        arg_max(volume, ts) AS volume,
        arg_max(volume_24h, ts) AS volume_24h
      FROM base
      GROUP BY venue, market_id, latest_ts
    )
    SELECT venue,market_id,event_id,event_title,market_title,close_time,latest_ts,prob,prob_1h,prob_24h,
           prob-prob_1h AS delta_1h,
           prob-prob_24h AS delta_24h,
           yes_bid,yes_ask,
           CASE WHEN yes_bid IS NOT NULL AND yes_ask IS NOT NULL THEN yes_ask-yes_bid END AS spread,
           last,volume,volume_24h,now()
    FROM agg
    """)


def refresh_canonical(con):
    rows = con.execute("""
      SELECT DISTINCT venue,event_id,arg_max(event_title,latest_ts) AS event_title
      FROM market_features WHERE event_id IS NOT NULL GROUP BY venue,event_id
    """).fetchall()
    for venue, event_id, title in rows:
        cid = f"{venue}:{event_id}"
        theme = classify_theme(title)
        region = classify_region(title)
        con.execute("""
          INSERT INTO canonical_events
          (canonical_event_id,venue,source_event_id,title,category,region,theme,eligible,methodology_version,updated_at)
          VALUES (?,?,?,?,?,?,?,?,?,now())
          ON CONFLICT (canonical_event_id) DO UPDATE SET
            title=excluded.title, region=excluded.region, theme=excluded.theme,
            eligible=excluded.eligible, updated_at=now()
        """, [cid,venue,event_id,title or event_id,"geopolitics",region,theme,eligible_event(title),"rules_v1"])
    con.execute("DELETE FROM event_markets")
    con.execute("""
      INSERT INTO event_markets
      SELECT ce.canonical_event_id,mf.venue,mf.market_id,mf.market_title,mf.close_time,1.0,now()
      FROM market_features mf
      JOIN canonical_events ce ON ce.venue=mf.venue AND ce.source_event_id=mf.event_id
    """)


def score_relevance(theme, sector, industry, ticker, title):
    sector = (sector or "").lower(); industry=(industry or "").lower(); ticker=ticker.upper(); title=(title or "").lower()
    score, sign, why = 0.05, None, "Broad event exposure"
    if theme == "taiwan":
        if "semiconductor" in industry or ticker in {"NVDA","AMD","AVGO","INTC","TSM","ASML","SOXX"}: score,sign,why=.98,-1,"Semiconductor supply-chain exposure to Taiwan"
        elif sector in {"technology","consumer cyclical"}: score,sign,why=.72,-1,"Asian electronics and supply-chain exposure"
    elif theme == "china_trade":
        if sector in {"technology","consumer cyclical","industrials"}: score,sign,why=.82,-1,"China trade/export-policy exposure"
        if ticker in {"NVDA","AMD","AAPL","TSM","ASML","TSLA","SOXX"}: score=max(score,.92); sign=-1
    elif theme in {"iran_hormuz","israel_middle_east","energy"}:
        if sector == "energy" or ticker in {"XOM","CVX","COP","OXY","XLE"}: score,sign,why=.92,1,"Oil supply and Middle East energy exposure"
        elif "airline" in industry or ticker in {"DAL","UAL","CCL"}: score,sign,why=.82,-1,"Fuel-cost and travel-route exposure"
        elif sector in {"industrials","technology"}: score,sign,why=.38,None,"Global risk and supply-chain exposure"
    elif theme in {"russia_ukraine","defense"}:
        if "aerospace" in industry or ticker in {"LMT","RTX","NOC","BA","ITA"}: score,sign,why=.9,1,"Defense-spending and conflict exposure"
        elif sector == "energy": score,sign,why=.62,1,"European energy-supply exposure"
        else: score,sign,why=.3,-1,"Global geopolitical risk exposure"
    elif theme == "trade":
        if sector in {"technology","consumer cyclical","industrials"}: score,sign,why=.7,-1,"Trade-policy and cross-border supply-chain exposure"
    elif theme == "rates":
        if sector in {"financial services","technology"} or ticker in {"SPY","QQQ","XLF"}: score,sign,why=.72,None,"Interest-rate sensitivity"
    elif theme == "recession":
        score,sign,why=.68,-1,"Broad macroeconomic sensitivity"
        if sector in {"consumer cyclical","financial services","industrials"}: score=.82
    elif theme == "crypto":
        if ticker in {"COIN","MSTR"} or "crypto" in industry or "bitcoin" in industry: score,sign,why=.95,1,"Direct crypto-market exposure"
        else: score=.08
    else:
        if any(x in title for x in ("united states", "u.s.", "us ", "china", "global")): score=.18
    return score, sign, why


def refresh_relevance(con):
    securities = con.execute("SELECT ticker,sector,industry FROM securities WHERE tracked=TRUE").fetchall()
    events = con.execute("SELECT canonical_event_id,title,theme FROM canonical_events WHERE eligible=TRUE").fetchall()
    con.execute("DELETE FROM event_security_map")
    for ticker, sector, industry in securities:
        for cid, title, theme in events:
            score, sign, why = score_relevance(theme, sector, industry, ticker, title)
            if score >= .18:
                con.execute("""INSERT INTO event_security_map VALUES (?,?,?,?,?,'rules_v1',now())""", [cid,ticker,score,sign,why])


def yahoo_chart(ticker, interval="5m", range_="1mo"):
    url=f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
    r=HTTP.get(url, params={"interval":interval,"range":range_,"includePrePost":"false","events":"div,splits"}, timeout=25)
    r.raise_for_status()
    result=r.json()["chart"]["result"]
    if not result: return [], {}
    x=result[0]; q=(x.get("indicators",{}).get("quote") or [{}])[0]; stamps=x.get("timestamp") or []
    out=[]
    for i,ts in enumerate(stamps):
        close=(q.get("close") or [None]*len(stamps))[i]
        if close is None: continue
        def at(name):
            values=q.get(name) or []
            return values[i] if i < len(values) else None
        out.append((datetime.fromtimestamp(ts,timezone.utc),at("open"),at("high"),at("low"),close,at("volume")))
    return out, x.get("meta") or {}


def refresh_prices(con):
    tickers=[r[0] for r in con.execute("SELECT ticker FROM securities WHERE tracked=TRUE").fetchall()]
    for ticker in tickers:
        try:
            rows,meta=yahoo_chart(ticker)
            for ts,o,h,l,c,v in rows:
                con.execute("""
                INSERT INTO ticker_prices (ticker,ts,interval,open,high,low,close,volume,source)
                VALUES (?,?,?,?,?,?,?,?, 'yahoo_chart')
                ON CONFLICT DO NOTHING
                """,[ticker,ts,"5m",o,h,l,c,v])
            if meta:
                con.execute("""UPDATE securities SET name=coalesce(?,name), exchange=coalesce(?,exchange),
                  quote_type=coalesce(?,quote_type), updated_at=now() WHERE ticker=?""",
                  [meta.get("longName") or meta.get("shortName"),meta.get("exchangeName"),meta.get("instrumentType"),ticker])
            print("prices",ticker,len(rows)); time.sleep(.15)
        except Exception as e:
            print("price failure",ticker,e)


def fit_coefficients(con):
    con.execute("DELETE FROM market_security_coefficients")
    maps=con.execute("""
      SELECT em.venue,em.market_id,esm.ticker,esm.relevance
      FROM event_security_map esm JOIN event_markets em USING(canonical_event_id)
      WHERE esm.relevance >= .35
    """).fetchall()
    for venue,market_id,ticker,relevance in maps:
        try:
            pred=con.execute("""
              SELECT time_bucket(INTERVAL '5 minutes',ts) b, arg_max(prob,ts) p
              FROM probability_history_all
              WHERE venue=? AND market_id=?
              GROUP BY 1 ORDER BY 1
            """,[venue,market_id]).fetchdf()
            px=con.execute("""
              SELECT time_bucket(INTERVAL '5 minutes',ts) b, arg_max(close,ts) close
              FROM ticker_prices WHERE ticker=? AND interval='5m'
              GROUP BY 1 ORDER BY 1
            """,[ticker]).fetchdf()
            spy=con.execute("""
              SELECT time_bucket(INTERVAL '5 minutes',ts) b, arg_max(close,ts) spy
              FROM ticker_prices WHERE ticker='SPY' AND interval='5m'
              GROUP BY 1 ORDER BY 1
            """).fetchdf()
            if len(pred)<8 or len(px)<8 or len(spy)<8: continue
            d=pred.merge(px,on="b",how="inner").merge(spy,on="b",how="inner").sort_values("b")
            d["dp"]=d["p"].diff(); d["r"]=np.log(d["close"]).diff(); d["mkt"]=np.log(d["spy"]).diff()
            d=d.replace([np.inf,-np.inf],np.nan).dropna()
            d=d[(d.dp.abs()>=0.0005) & (d.r.abs()<.2) & (d.mkt.abs()<.1)]
            if len(d)<20: continue
            X=sm.add_constant(d[["mkt","dp"]]); model=sm.OLS(d["r"],X).fit(cov_type="HC1")
            beta=float(model.params["dp"])*.01; se=float(model.bse["dp"])*.01
            t=float(model.tvalues["dp"]); r2=float(model.rsquared); n=int(model.nobs)
            confidence="high" if n>=100 and abs(t)>=2.58 else "medium" if n>=50 and abs(t)>=1.96 else "low"
            con.execute("""
              INSERT INTO market_security_coefficients
              VALUES (?,?,?,?,?,?,?,?,?,?,?,?,now())
            """,[venue,market_id,ticker,beta,se,t,r2,n,d.b.min(),d.b.max(),confidence,"ols_market_control_v1"])
        except Exception as e:
            print("model failure",ticker,market_id,e)


def run(job):
    con=connect(); run_id=str(uuid.uuid4()); started=datetime.now(timezone.utc)
    con.execute("INSERT INTO model_runs VALUES (?,?,?,NULL,?,?)",[run_id,job,started,"running",None])
    try:
        if job in {"markets","all"}:
            refresh_market_features(con); refresh_canonical(con)
        if job in {"models","all"}:
            ensure_seeds(con); refresh_market_features(con); refresh_canonical(con); refresh_relevance(con); refresh_prices(con); fit_coefficients(con)
        con.execute("UPDATE model_runs SET finished_at=now(),status='success' WHERE run_id=?",[run_id]); con.close()
    except Exception as e:
        con.execute("UPDATE model_runs SET finished_at=now(),status='failed',details=? WHERE run_id=?",[str(e),run_id]); con.close(); raise


if __name__ == "__main__":
    p=argparse.ArgumentParser(); p.add_argument("job",choices=["markets","models","all"]); args=p.parse_args(); run(args.job)
