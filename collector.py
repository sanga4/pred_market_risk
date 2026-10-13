#!/usr/bin/env python3
"""
Snapshot odds for open market-relevant contracts on Kalshi + Polymarket.
Each run appends one bulk snapshot batch to `ticks`.
"""
import json, logging, os, sys
from datetime import datetime, timezone

import duckdb, pyarrow as pa, requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DB = os.getenv("DB", "event_risk.duckdb")
KALSHI_CATEGORIES = [x.strip() for x in os.getenv(
    "KALSHI_CATEGORIES",
    "Politics,Economics,Financials,Companies,Crypto,Science and Technology"
).split(",") if x.strip()]
POLY_TAGS = [x.strip() for x in os.getenv("POLY_TAGS", "geopolitics").split(",") if x.strip()]
MAX_SPREAD = 0.10

KALSHI = "https://external-api.kalshi.com/trade-api/v2"
GAMMA = "https://gamma-api.polymarket.com"
NOW = datetime.now(timezone.utc).replace(microsecond=0)
log = logging.getLogger("collector")

SCHEMA = pa.schema([
    ("ts", pa.timestamp("s", tz="UTC")), ("venue", pa.string()),
    ("market_id", pa.string()), ("event_id", pa.string()),
    ("event_title", pa.string()), ("market_title", pa.string()),
    ("close_time", pa.timestamp("s", tz="UTC")),
    ("prob", pa.float64()), ("yes_bid", pa.float64()), ("yes_ask", pa.float64()),
    ("last", pa.float64()), ("volume", pa.float64()), ("volume_24h", pa.float64()),
])

http = requests.Session()
http.headers.update({"User-Agent": "EventRiskMVP/0.2"})
http.mount("https://", HTTPAdapter(max_retries=Retry(
    total=6, backoff_factor=1.5, status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=("GET",), respect_retry_after_header=True)))


def get(url, **params):
    r = http.get(url, params={k: v for k, v in params.items() if v is not None}, timeout=40)
    r.raise_for_status()
    return r.json()


def num(x):
    try: return float(x)
    except (TypeError, ValueError): return None


def when(x):
    try: return datetime.fromisoformat(str(x).replace("Z", "+00:00"))
    except (TypeError, ValueError): return None


def row(venue, market_id, event_id, event_title, market_title, close, bid, ask, last, vol, vol24):
    if market_id is None:
        return None
    mid = (bid + ask) / 2 if bid is not None and ask is not None and 0 <= ask - bid <= MAX_SPREAD else None
    return dict(ts=NOW, venue=venue, market_id=str(market_id), event_id=str(event_id) if event_id is not None else None,
                event_title=event_title, market_title=market_title, close_time=when(close),
                prob=round(mid, 4) if mid is not None else last,
                yes_bid=bid, yes_ask=ask, last=last, volume=vol, volume_24h=vol24)


# Kalshi's current API exposes category on Series and status=open on markets.
def kalshi():
    series_rows = get(f"{KALSHI}/series").get("series") or []
    allowed = {c.lower() for c in KALSHI_CATEGORIES}
    series_category = {s.get("ticker"): (s.get("category") or "").lower() for s in series_rows}

    cursor = None
    while True:
        d = get(f"{KALSHI}/markets", status="open", limit=1000, cursor=cursor, mve_filter="exclude")
        for m in d.get("markets") or []:
            if allowed and series_category.get(m.get("series_ticker"), "") not in allowed:
                continue
            r = row("kalshi", m.get("ticker"), m.get("event_ticker"),
                    m.get("title") or m.get("event_ticker"),
                    m.get("yes_sub_title") or m.get("subtitle") or m.get("title"),
                    m.get("close_time"), num(m.get("yes_bid_dollars")), num(m.get("yes_ask_dollars")),
                    num(m.get("last_price_dollars")), num(m.get("volume_fp")), num(m.get("volume_24h_fp")))
            if r: yield r
        cursor = d.get("cursor")
        if not cursor: break


def polymarket():
    seen = set()
    for tag in POLY_TAGS:
        cursor = None
        while True:
            d = get(f"{GAMMA}/events/keyset", tag_slug=tag, closed="false", limit=100, after_cursor=cursor)
            for e in d.get("events") or []:
                for m in e.get("markets") or []:
                    if m.get("closed") or not m.get("active") or str(m.get("id")) in seen:
                        continue
                    seen.add(str(m.get("id")))
                    r = row("polymarket", m.get("id"), e.get("id"), e.get("title"),
                            m.get("groupItemTitle") or m.get("question"), m.get("endDate"),
                            num(m.get("bestBid")), num(m.get("bestAsk")), num(m.get("lastTradePrice")),
                            num(m.get("volumeNum") or m.get("volume")), num(m.get("volume24hr")))
                    if r: yield r
            cursor = d.get("next_cursor")
            if not cursor: break


DDL = """CREATE TABLE IF NOT EXISTS ticks (
  ts TIMESTAMPTZ, venue VARCHAR, market_id VARCHAR, event_id VARCHAR,
  event_title VARCHAR, market_title VARCHAR, close_time TIMESTAMPTZ,
  prob DOUBLE, yes_bid DOUBLE, yes_ask DOUBLE, last DOUBLE, volume DOUBLE, volume_24h DOUBLE)"""


def write(rows):
    if DB.startswith("md:"):
        name = DB[3:]
        con = duckdb.connect("md:")
        con.execute(f'CREATE DATABASE IF NOT EXISTS "{name}"')
        con.execute(f'USE "{name}"')
    else:
        con = duckdb.connect(DB)
    con.execute(DDL)
    con.register("batch", pa.Table.from_pylist(rows, schema=SCHEMA))
    con.execute("INSERT INTO ticks SELECT * FROM batch")
    con.close()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    rows, failed = [], False
    for venue in (kalshi, polymarket):
        try:
            got = list(venue()); rows += got
            log.info("%-10s %5d markets", venue.__name__, len(got))
        except Exception:
            log.exception("%s failed", venue.__name__); failed = True
    if rows: write(rows)
    return 1 if failed or not rows else 0


if __name__ == "__main__":
    sys.exit(main())
