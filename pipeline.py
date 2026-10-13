#!/usr/bin/env python3
import argparse
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import pandas as pd
import requests

from db import connect

HTTP_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; EventRiskMVP/0.3)"}

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
    ("rates", r"fed|federal reserve|rate cut|interest rate|fomc|treasury"),
    ("recession", r"recession|gdp|unemployment|economic contraction|soft landing"),
    ("crypto", r"bitcoin|crypto|ethereum|binance|coinbase"),
    ("trade", r"tariff|trade deal|trade agreement|customs|import|export"),
    ("energy", r"oil|gas|diesel|pipeline|opec|energy|refinery|shipping|strait"),
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
    low_value = ["mrbeast", "ishowspeed", "f1 race", "shake hands", "where will trump", "gold cards"]
    return not any(x in s for x in low_value)


def score_relevance(theme, sector, industry, ticker, title):
    sector=(sector or "").lower(); industry=(industry or "").lower(); ticker=ticker.upper(); title=(title or "").lower()
    score, sign, why = 0.05, None, "Broad event exposure"
    if theme == "taiwan":
        if "semiconductor" in industry or ticker in {"NVDA","AMD","AVGO","INTC","TSM","ASML","SOXX"}: score,sign,why=.98,-1,"Semiconductor supply-chain exposure to Taiwan"
        elif sector in {"technology","consumer cyclical"}: score,sign,why=.72,-1,"Asian electronics and supply-chain exposure"
    elif theme == "china_trade":
        if sector in {"technology","consumer cyclical","industrials"}: score,sign,why=.82,-1,"China trade/export-policy exposure"
        if ticker in {"NVDA","AMD","AAPL","TSM","ASML","TSLA","SOXX"}: score=max(score,.92); sign=-1; why="China semiconductor/trade-policy exposure"
    elif theme in {"iran_hormuz","israel_middle_east","energy"}:
        if sector == "energy" or ticker in {"XOM","CVX","COP","OXY","XLE"}: score,sign,why=.92,1,"Oil supply and Middle East energy exposure"
        elif "airline" in industry or ticker in {"DAL","UAL","CCL"}: score,sign,why=.82,-1,"Fuel-cost and travel-route exposure"
        elif sector in {"industrials","technology"}: score,sign,why=.38,None,"Global risk and supply-chain exposure"
    elif theme in {"russia_ukraine","defense"}:
        if "aerospace" in industry or ticker in {"LMT","RTX","NOC","BA","ITA"}: score,sign,why=.90,1,"Defense-spending and conflict exposure"
        elif sector == "energy": score,sign,why=.62,1,"European energy-supply exposure"
        else: score,sign,why=.30,-1,"Global geopolitical risk exposure"
    elif theme == "trade":
        if sector in {"technology","consumer cyclical","industrials"}: score,sign,why=.70,-1,"Trade-policy and cross-border supply-chain exposure"
    elif theme == "rates":
        if sector in {"financial services","technology"} or ticker in {"SPY","QQQ","XLF"}: score,sign,why=.72,None,"Interest-rate sensitivity"
    elif theme == "recession":
        score,sign,why=.68,-1,"Broad macroeconomic sensitivity"
        if sector in {"consumer cyclical","financial services","industrials"}: score=.82
    elif theme == "crypto":
        if ticker in {"COIN","MSTR"} or "crypto" in industry or "bitcoin" in industry: score,sign,why=.95,1,"Direct crypto-market exposure"
        else: score=.08
    elif any(x in title for x in ("united states", "u.s.", "us ", "china", "global")):
        score=.18
    return score, sign, why


def bulk_df(con, name, df, insert_sql):
    if df.empty: return
    con.register(name, df)
    try: con.execute(insert_sql)
    finally: con.unregister(name)


def ensure_seeds(con):
    df=pd.DataFrame(SEED_SECURITIES, columns=["ticker","name","sector","industry"])
    bulk_df(con,"seed_batch",df,"""
      INSERT INTO securities (ticker,name,sector,industry,tracked,source,updated_at)
      SELECT ticker,name,sector,industry,TRUE,'seed',now() FROM seed_batch
      ON CONFLICT (ticker) DO UPDATE SET
        name=excluded.name,sector=excluded.sector,industry=excluded.industry,tracked=TRUE,updated_at=now()
    """)


def refresh_market_features(con):
    con.execute("DELETE FROM market_features")
    con.execute("""
      INSERT INTO market_features
      WITH latest AS (
        SELECT venue,market_id,max(ts) latest_ts FROM ticks WHERE prob IS NOT NULL GROUP BY 1,2
      ), base AS (
        SELECT t.*,l.latest_ts FROM ticks t JOIN latest l USING(venue,market_id) WHERE t.prob IS NOT NULL
      ), agg AS (
        SELECT venue,market_id,latest_ts,
          arg_max(event_id,ts) event_id,arg_max(event_title,ts) event_title,
          arg_max(market_title,ts) market_title,arg_max(close_time,ts) close_time,
          arg_max(prob,ts) prob,
          arg_max(prob,ts) FILTER(WHERE ts<=latest_ts-INTERVAL '1 hour') prob_1h,
          arg_max(prob,ts) FILTER(WHERE ts<=latest_ts-INTERVAL '24 hours') prob_24h,
          arg_max(yes_bid,ts) yes_bid,arg_max(yes_ask,ts) yes_ask,arg_max(last,ts) last,
          arg_max(volume,ts) volume,arg_max(volume_24h,ts) volume_24h
        FROM base GROUP BY 1,2,3
      )
      SELECT venue,market_id,event_id,event_title,market_title,close_time,latest_ts,prob,prob_1h,prob_24h,
        prob-prob_1h,prob-prob_24h,yes_bid,yes_ask,
        CASE WHEN yes_bid IS NOT NULL AND yes_ask IS NOT NULL THEN yes_ask-yes_bid END,
        last,volume,volume_24h,now() FROM agg
    """)


def refresh_canonical(con):
    raw=con.execute("""
      SELECT venue,event_id,arg_max(event_title,latest_ts) title
      FROM market_features WHERE event_id IS NOT NULL GROUP BY 1,2
    """).fetchall()
    data=[]
    for venue,event_id,title in raw:
        data.append((f"{venue}:{event_id}",venue,str(event_id),title or str(event_id),"geopolitics",
                     classify_region(title),classify_theme(title),eligible_event(title),"rules_v2"))
    df=pd.DataFrame(data,columns=["canonical_event_id","venue","source_event_id","title","category","region","theme","eligible","methodology_version"])
    con.execute("DELETE FROM canonical_events")
    bulk_df(con,"ce_batch",df,"""
      INSERT INTO canonical_events
      SELECT canonical_event_id,venue,source_event_id,title,category,region,theme,eligible,methodology_version,now()
      FROM ce_batch
    """)
    con.execute("DELETE FROM event_markets")
    con.execute("""
      INSERT INTO event_markets
      SELECT ce.canonical_event_id,mf.venue,mf.market_id,mf.market_title,mf.close_time,1.0,now()
      FROM market_features mf JOIN canonical_events ce
        ON ce.venue=mf.venue AND ce.source_event_id=mf.event_id
    """)


def refresh_relevance(con):
    secs=con.execute("SELECT ticker,sector,industry FROM securities WHERE tracked=TRUE").fetchall()
    evs=con.execute("SELECT canonical_event_id,title,theme FROM canonical_events WHERE eligible=TRUE").fetchall()
    data=[]
    for ticker,sector,industry in secs:
        for cid,title,theme in evs:
            score,sign,why=score_relevance(theme,sector,industry,ticker,title)
            if score>=.18: data.append((cid,ticker,score,sign,why,"rules_v2"))
    df=pd.DataFrame(data,columns=["canonical_event_id","ticker","relevance","exposure_sign","reason","methodology_version"])
    con.execute("DELETE FROM event_security_map")
    bulk_df(con,"rel_batch",df,"""
      INSERT INTO event_security_map
      SELECT canonical_event_id,ticker,relevance,exposure_sign,reason,methodology_version,now() FROM rel_batch
    """)


def yahoo_chart(ticker, interval="1h", range_="2y"):
    r=requests.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}",
                   params={"interval":interval,"range":range_,"includePrePost":"false","events":"div,splits"},
                   headers=HTTP_HEADERS,timeout=25)
    r.raise_for_status(); results=(r.json().get("chart") or {}).get("result") or []
    if not results: return [],{}
    x=results[0]; q=(x.get("indicators",{}).get("quote") or [{}])[0]; stamps=x.get("timestamp") or []
    out=[]
    for i,ts in enumerate(stamps):
        def at(k):
            a=q.get(k) or []
            return a[i] if i<len(a) else None
        c=at("close")
        if c is not None: out.append((ticker,datetime.fromtimestamp(ts,timezone.utc),interval,at("open"),at("high"),at("low"),c,at("volume"),"yahoo_chart"))
    return out,x.get("meta") or {}


def refresh_prices(con):
    tickers=[r[0] for r in con.execute("SELECT ticker FROM securities WHERE tracked=TRUE").fetchall()]
    existing=con.execute("SELECT count(*) FROM ticker_prices").fetchone()[0]
    range_="2y" if existing==0 else "1mo"
    rows=[]; metas=[]
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs={ex.submit(yahoo_chart,t,"1h",range_):t for t in tickers}
        for fut in as_completed(futs):
            t=futs[fut]
            try:
                rs,meta=fut.result(); rows.extend(rs); metas.append((t,meta)); print("prices",t,len(rs))
            except Exception as e: print("price failure",t,e)
    if rows:
        df=pd.DataFrame(rows,columns=["ticker","ts","interval","open","high","low","close","volume","source"])
        bulk_df(con,"price_batch",df,"""
          INSERT INTO ticker_prices (ticker,ts,interval,open,high,low,close,volume,source)
          SELECT ticker,ts,interval,open,high,low,close,volume,source FROM price_batch
          ON CONFLICT DO NOTHING
        """)
    if metas:
        md=[]
        for t,m in metas:
            md.append((t,m.get("longName") or m.get("shortName"),m.get("exchangeName"),m.get("instrumentType")))
        df=pd.DataFrame(md,columns=["ticker","name","exchange","quote_type"])
        bulk_df(con,"meta_batch",df,"""
          UPDATE securities SET
            name=coalesce(meta_batch.name,securities.name),exchange=coalesce(meta_batch.exchange,securities.exchange),
            quote_type=coalesce(meta_batch.quote_type,securities.quote_type),updated_at=now()
          FROM meta_batch WHERE securities.ticker=meta_batch.ticker
        """)


def fit_coefficients(con):
    con.execute("DELETE FROM market_security_coefficients")
    con.execute("""
      INSERT INTO market_security_coefficients
      WITH ph0 AS (
        SELECT venue,market_id,time_bucket(INTERVAL '1 hour',ts) b,arg_max(prob,ts) p
        FROM probability_history_all GROUP BY 1,2,3
      ), ph AS (
        SELECT venue,market_id,b,p-lag(p) OVER(PARTITION BY venue,market_id ORDER BY b) dp FROM ph0
      ), px0 AS (
        SELECT ticker,time_bucket(INTERVAL '1 hour',ts) b,arg_max(close,ts) close
        FROM ticker_prices WHERE interval='1h' AND close>0 GROUP BY 1,2
      ), px AS (
        SELECT ticker,b,ln(close/lag(close) OVER(PARTITION BY ticker ORDER BY b)) r FROM px0
      ), spy AS (SELECT b,r mkt FROM px WHERE ticker='SPY'),
      aligned AS (
        SELECT p.ticker,p.b,p.r,s.mkt FROM px p JOIN spy s USING(b)
        WHERE p.ticker<>'SPY' AND p.r IS NOT NULL AND s.mkt IS NOT NULL AND abs(p.r)<.2 AND abs(s.mkt)<.1
      ), mbeta AS (
        SELECT ticker,regr_slope(r,mkt) beta_mkt FROM aligned GROUP BY ticker HAVING count(*)>=20
      ), abn AS (
        SELECT a.ticker,a.b,a.r-b.beta_mkt*a.mkt abnormal FROM aligned a JOIN mbeta b USING(ticker)
      ), joined AS (
        SELECT em.venue,em.market_id,esm.ticker,p.b,p.dp,a.abnormal
        FROM event_security_map esm JOIN event_markets em USING(canonical_event_id)
        JOIN ph p ON p.venue=em.venue AND p.market_id=em.market_id
        JOIN abn a ON a.ticker=esm.ticker AND a.b=p.b
        WHERE esm.relevance>=.35 AND p.dp IS NOT NULL AND abs(p.dp)>=.0005 AND abs(a.abnormal)<.2
      ), stats AS (
        SELECT venue,market_id,ticker,count(*) n,min(b) sample_start,max(b) sample_end,
               regr_slope(abnormal,dp) slope,regr_r2(abnormal,dp) r2,
               var_samp(dp)*(count(*)-1) sxx,var_samp(abnormal)*(count(*)-1) syy
        FROM joined GROUP BY 1,2,3 HAVING count(*)>=20
      ), calc AS (
        SELECT *,
          CASE WHEN n>2 AND sxx>0 AND r2 IS NOT NULL
            THEN sqrt(greatest(0,(syy*(1-r2)/(n-2))/sxx)) END slope_se
        FROM stats WHERE slope IS NOT NULL
      )
      SELECT venue,market_id,ticker,slope*.01,
             slope_se*.01,
             CASE WHEN slope_se>0 THEN slope/slope_se END,
             r2,n,sample_start,sample_end,
             CASE WHEN n>=100 AND abs(slope/nullif(slope_se,0))>=2.58 THEN 'high'
                  WHEN n>=50 AND abs(slope/nullif(slope_se,0))>=1.96 THEN 'medium' ELSE 'low' END,
             'hourly_market_residual_v2',now()
      FROM calc
    """)


def run(job):
    con=connect(); run_id=str(uuid.uuid4()); started=datetime.now(timezone.utc)
    con.execute("INSERT INTO model_runs VALUES (?,?,?,NULL,?,?)",[run_id,job,started,"running",None])
    try:
        refresh_market_features(con); refresh_canonical(con)
        if job in {"models","all"}:
            ensure_seeds(con); refresh_relevance(con); refresh_prices(con); fit_coefficients(con)
        con.execute("UPDATE model_runs SET finished_at=now(),status='success' WHERE run_id=?",[run_id]); con.close()
    except Exception as e:
        con.execute("UPDATE model_runs SET finished_at=now(),status='failed',details=? WHERE run_id=?",[str(e),run_id]); con.close(); raise


if __name__ == "__main__":
    p=argparse.ArgumentParser(); p.add_argument("job",choices=["markets","models","all"]); args=p.parse_args(); run(args.job)
