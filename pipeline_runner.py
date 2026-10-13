#!/usr/bin/env python3
"""Production entrypoint around pipeline.py with safe DuckDB aliases."""
import argparse
import pipeline


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
          arg_max(yes_bid,ts) yes_bid,arg_max(yes_ask,ts) yes_ask,arg_max(last,ts) last_px,
          arg_max(volume,ts) volume,arg_max(volume_24h,ts) volume_24h
        FROM base GROUP BY 1,2,3
      )
      SELECT venue,market_id,event_id,event_title,market_title,close_time,latest_ts,prob,prob_1h,prob_24h,
        prob-prob_1h,prob-prob_24h,yes_bid,yes_ask,
        CASE WHEN yes_bid IS NOT NULL AND yes_ask IS NOT NULL THEN yes_ask-yes_bid END,
        last_px,volume,volume_24h,now() FROM agg
    """)


pipeline.refresh_market_features = refresh_market_features

if __name__ == '__main__':
    p=argparse.ArgumentParser(); p.add_argument('job',choices=['markets','models','all']); args=p.parse_args()
    pipeline.run(args.job)
