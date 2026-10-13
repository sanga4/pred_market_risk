#!/usr/bin/env python3
"""Production entrypoint around pipeline.py with safe DuckDB aliases and retail relevance rules."""
import argparse
import pipeline
from relevance import score_relevance


def refresh_market_features(con):
    con.execute("DELETE FROM market_features")
    con.execute("""
      INSERT INTO market_features
      WITH cur AS (
        SELECT venue,market_id,max(ts) latest_ts,
          arg_max(event_id,ts) event_id,arg_max(event_title,ts) event_title,
          arg_max(market_title,ts) market_title,arg_max(close_time,ts) close_time,
          arg_max(prob,ts) prob,arg_max(yes_bid,ts) yes_bid,arg_max(yes_ask,ts) yes_ask,
          arg_max(last,ts) last_px,arg_max(volume,ts) volume,arg_max(volume_24h,ts) volume_24h
        FROM ticks WHERE prob IS NOT NULL GROUP BY 1,2
      ), hist AS (
        SELECT c.venue,c.market_id,
          arg_max(h.prob,h.ts) FILTER(WHERE h.ts<=c.latest_ts-INTERVAL '1 hour') prob_1h,
          arg_max(h.prob,h.ts) FILTER(WHERE h.ts<=c.latest_ts-INTERVAL '24 hours') prob_24h
        FROM cur c LEFT JOIN probability_history_all h
          ON h.venue=c.venue AND h.market_id=c.market_id
        GROUP BY 1,2
      )
      SELECT c.venue,c.market_id,c.event_id,c.event_title,c.market_title,c.close_time,c.latest_ts,c.prob,
        h.prob_1h,h.prob_24h,c.prob-h.prob_1h,c.prob-h.prob_24h,c.yes_bid,c.yes_ask,
        CASE WHEN c.yes_bid IS NOT NULL AND c.yes_ask IS NOT NULL THEN c.yes_ask-c.yes_bid END,
        c.last_px,c.volume,c.volume_24h,now()
      FROM cur c LEFT JOIN hist h USING(venue,market_id)
    """)


def fit_coefficients(con):
    con.execute("DELETE FROM market_security_coefficients")
    con.execute("""
      INSERT INTO market_security_coefficients
      WITH ph0 AS (
        SELECT venue,market_id,time_bucket(INTERVAL '1 hour',ts) bucket,arg_max(prob,ts) p
        FROM probability_history_all GROUP BY 1,2,3
      ), ph AS (
        SELECT venue,market_id,bucket,p-lag(p) OVER(PARTITION BY venue,market_id ORDER BY bucket) dp FROM ph0
      ), px0 AS (
        SELECT ticker,time_bucket(INTERVAL '1 hour',ts) bucket,arg_max(close,ts) px_close
        FROM ticker_prices WHERE interval='1h' AND close>0 GROUP BY 1,2
      ), px AS (
        SELECT ticker,bucket,ln(px_close/lag(px_close) OVER(PARTITION BY ticker ORDER BY bucket)) stock_return FROM px0
      ), spy AS (
        SELECT bucket,stock_return market_return FROM px WHERE ticker='SPY'
      ), aligned AS (
        SELECT p.ticker,p.bucket,p.stock_return,s.market_return
        FROM px p JOIN spy s USING(bucket)
        WHERE p.ticker<>'SPY' AND p.stock_return IS NOT NULL AND s.market_return IS NOT NULL
          AND abs(p.stock_return)<.2 AND abs(s.market_return)<.1
      ), market_beta AS (
        SELECT ticker,regr_slope(stock_return,market_return) beta_mkt
        FROM aligned GROUP BY ticker HAVING count(*)>=20
      ), abnormal AS (
        SELECT a.ticker,a.bucket,a.stock_return-b.beta_mkt*a.market_return abnormal_return
        FROM aligned a JOIN market_beta b USING(ticker)
      ), joined AS (
        SELECT em.venue,em.market_id,esm.ticker,p.bucket,p.dp,a.abnormal_return
        FROM event_security_map esm
        JOIN event_markets em USING(canonical_event_id)
        JOIN ph p ON p.venue=em.venue AND p.market_id=em.market_id
        JOIN abnormal a ON a.ticker=esm.ticker AND a.bucket=p.bucket
        WHERE esm.relevance>=.35 AND p.dp IS NOT NULL AND abs(p.dp)>=.0005 AND abs(a.abnormal_return)<.2
      ), model_stats AS (
        SELECT venue,market_id,ticker,count(*) n,min(bucket) sample_start,max(bucket) sample_end,
               regr_slope(abnormal_return,dp) slope,regr_r2(abnormal_return,dp) r2,
               var_samp(dp)*(count(*)-1) sxx,var_samp(abnormal_return)*(count(*)-1) syy
        FROM joined GROUP BY 1,2,3 HAVING count(*)>=20
      ), calc AS (
        SELECT *,CASE WHEN n>2 AND sxx>0 AND r2 IS NOT NULL
          THEN sqrt(greatest(0,(syy*(1-r2)/(n-2))/sxx)) END slope_se
        FROM model_stats WHERE slope IS NOT NULL
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


pipeline.refresh_market_features = refresh_market_features
pipeline.fit_coefficients = fit_coefficients
pipeline.score_relevance = score_relevance

if __name__ == '__main__':
    p=argparse.ArgumentParser(); p.add_argument('job',choices=['markets','models','all']); args=p.parse_args()
    pipeline.run(args.job)
