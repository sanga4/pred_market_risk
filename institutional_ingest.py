"""Full-fidelity institutional ingestion for EventRisk.

This collector is deliberately separate from the retail `ticks` collector.  It stores:
- immutable raw source payloads on every pull
- versioned contract metadata
- point-in-time market observations
- ingestion-run and source-health records

It does not create security-return or portfolio-impact claims.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from db import connect

log = logging.getLogger("eventrisk.institutional_ingest")

KALSHI = "https://external-api.kalshi.com/trade-api/v2"
POLY_GAMMA = "https://gamma-api.polymarket.com"
POLY_CLOB = "https://clob.polymarket.com"

KALSHI_CATEGORIES = {x.strip().lower() for x in os.getenv(
    "KALSHI_CATEGORIES",
    "Politics,Economics,Financials,Companies,Crypto,Science and Technology",
).split(",") if x.strip()}
POLY_TAGS = [x.strip() for x in os.getenv("POLY_TAGS", "geopolitics,politics,economy").split(",") if x.strip()]
FETCH_POLY_ORDERBOOK = os.getenv("FETCH_POLY_ORDERBOOK", "false").lower() in {"1", "true", "yes"}

HTTP = requests.Session()
HTTP.headers.update({"User-Agent": "EventRiskInstitutional/1.0"})
HTTP.mount("https://", HTTPAdapter(max_retries=Retry(
    total=5,
    backoff_factor=1.0,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=("GET",),
    respect_retry_after_header=True,
)))


def now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def parse_time(value) -> Optional[datetime]:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def number(value) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def stable_id(prefix: str, *parts: object, length: int = 24) -> str:
    raw = "\x1f".join("" if p is None else str(p) for p in parts)
    return f"{prefix}_{hashlib.sha256(raw.encode()).hexdigest()[:length]}"


def canonical_json(payload: object) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


def get_json(url: str, **params):
    response = HTTP.get(url, params={k: v for k, v in params.items() if v is not None}, timeout=40)
    response.raise_for_status()
    return response.json()


@dataclass
class ContractSnapshot:
    source_id: str
    source_market_id: str
    source_event_id: Optional[str]
    title: str
    description: Optional[str]
    status: Optional[str]
    opens_at: Optional[datetime]
    closes_at: Optional[datetime]
    resolves_at: Optional[datetime]
    resolution_authority: Optional[str]
    resolution_rules: Optional[str]
    source_url: Optional[str]
    observed_at: datetime
    yes_bid: Optional[float]
    yes_ask: Optional[float]
    no_bid: Optional[float]
    no_ask: Optional[float]
    last_trade: Optional[float]
    displayed_probability: Optional[float]
    volume: Optional[float]
    volume_24h: Optional[float]
    open_interest: Optional[float]
    liquidity: Optional[float]
    orderbook_depth: Optional[float]
    raw_event: object
    raw_market: object


class KalshiAdapter:
    source_id = "kalshi"

    def iter_snapshots(self) -> Iterable[ContractSnapshot]:
        cursor = None
        while True:
            data = get_json(
                f"{KALSHI}/events",
                status="open",
                with_nested_markets="true",
                limit=200,
                cursor=cursor,
            )
            for event in data.get("events") or []:
                category = str(event.get("category") or "").lower()
                if category and KALSHI_CATEGORIES and category not in KALSHI_CATEGORIES:
                    continue
                for market in event.get("markets") or []:
                    status = str(market.get("status") or "").lower()
                    if status not in {"open", "active"}:
                        continue
                    bid = number(market.get("yes_bid_dollars") or market.get("yes_bid"))
                    ask = number(market.get("yes_ask_dollars") or market.get("yes_ask"))
                    last = number(market.get("last_price_dollars") or market.get("last_price"))
                    if bid is not None and bid > 1.0:
                        bid /= 100.0
                    if ask is not None and ask > 1.0:
                        ask /= 100.0
                    if last is not None and last > 1.0:
                        last /= 100.0
                    midpoint = (bid + ask) / 2 if bid is not None and ask is not None and bid <= ask else None
                    observed = parse_time(market.get("updated_time") or market.get("last_updated_ts")) or now_utc()
                    yield ContractSnapshot(
                        source_id=self.source_id,
                        source_market_id=str(market.get("ticker")),
                        source_event_id=str(event.get("event_ticker") or "") or None,
                        title=str(market.get("title") or market.get("yes_sub_title") or event.get("title") or market.get("ticker")),
                        description=market.get("subtitle") or event.get("sub_title") or event.get("title"),
                        status=status,
                        opens_at=parse_time(market.get("open_time")),
                        closes_at=parse_time(market.get("close_time")),
                        resolves_at=parse_time(market.get("expiration_time") or market.get("settlement_ts")),
                        resolution_authority=market.get("settlement_source") or event.get("settlement_source"),
                        resolution_rules=market.get("rules_primary") or market.get("rules_secondary") or market.get("rules"),
                        source_url=None,
                        observed_at=observed,
                        yes_bid=bid,
                        yes_ask=ask,
                        no_bid=number(market.get("no_bid_dollars") or market.get("no_bid")),
                        no_ask=number(market.get("no_ask_dollars") or market.get("no_ask")),
                        last_trade=last,
                        displayed_probability=midpoint if midpoint is not None else last,
                        volume=number(market.get("volume_fp") or market.get("volume")),
                        volume_24h=number(market.get("volume_24h_fp") or market.get("volume_24h")),
                        open_interest=number(market.get("open_interest_fp") or market.get("open_interest")),
                        liquidity=number(market.get("liquidity")),
                        orderbook_depth=None,
                        raw_event=event,
                        raw_market=market,
                    )
            cursor = data.get("cursor")
            if not cursor:
                break


class PolymarketAdapter:
    source_id = "polymarket"

    def _orderbook_depth(self, market: dict) -> Optional[float]:
        if not FETCH_POLY_ORDERBOOK:
            return None
        token_ids = market.get("clobTokenIds")
        if isinstance(token_ids, str):
            try:
                token_ids = json.loads(token_ids)
            except Exception:
                token_ids = [token_ids]
        if not token_ids:
            return None
        try:
            book = get_json(f"{POLY_CLOB}/book", token_id=token_ids[0])
            depth = 0.0
            for side in ("bids", "asks"):
                for level in book.get(side) or []:
                    price = number(level.get("price")) or 0.0
                    size = number(level.get("size")) or 0.0
                    depth += price * size
            return depth
        except Exception:
            log.exception("Polymarket orderbook fetch failed for %s", market.get("id"))
            return None

    def iter_snapshots(self) -> Iterable[ContractSnapshot]:
        seen = set()
        for tag in POLY_TAGS:
            cursor = None
            while True:
                data = get_json(
                    f"{POLY_GAMMA}/events/keyset",
                    tag_slug=tag,
                    closed="false",
                    limit=100,
                    after_cursor=cursor,
                )
                for event in data.get("events") or []:
                    for market in event.get("markets") or []:
                        market_id = str(market.get("id"))
                        if not market_id or market_id in seen or market.get("closed") or not market.get("active"):
                            continue
                        seen.add(market_id)
                        bid = number(market.get("bestBid"))
                        ask = number(market.get("bestAsk"))
                        last = number(market.get("lastTradePrice"))
                        midpoint = (bid + ask) / 2 if bid is not None and ask is not None and bid <= ask else None
                        observed = parse_time(market.get("updatedAt") or event.get("updatedAt")) or now_utc()
                        rules = market.get("description") or event.get("description") or market.get("question")
                        yield ContractSnapshot(
                            source_id=self.source_id,
                            source_market_id=market_id,
                            source_event_id=str(event.get("id") or "") or None,
                            title=str(market.get("question") or market.get("groupItemTitle") or event.get("title") or market_id),
                            description=market.get("description") or event.get("description"),
                            status="active" if market.get("active") and not market.get("closed") else "closed",
                            opens_at=parse_time(market.get("startDate") or event.get("startDate")),
                            closes_at=parse_time(market.get("endDate") or event.get("endDate")),
                            resolves_at=parse_time(market.get("endDate") or event.get("endDate")),
                            resolution_authority=market.get("resolutionSource") or event.get("resolutionSource"),
                            resolution_rules=rules,
                            source_url=market.get("url") or event.get("url"),
                            observed_at=observed,
                            yes_bid=bid,
                            yes_ask=ask,
                            no_bid=None,
                            no_ask=None,
                            last_trade=last,
                            displayed_probability=midpoint if midpoint is not None else last,
                            volume=number(market.get("volumeNum") or market.get("volume")),
                            volume_24h=number(market.get("volume24hr")),
                            open_interest=number(market.get("openInterest")),
                            liquidity=number(market.get("liquidityNum") or market.get("liquidity")),
                            orderbook_depth=self._orderbook_depth(market),
                            raw_event=event,
                            raw_market=market,
                        )
                cursor = data.get("next_cursor")
                if not cursor:
                    break


ADAPTERS = {
    "kalshi": KalshiAdapter,
    "polymarket": PolymarketAdapter,
}


def ensure_sources(con):
    rows = [
        ("kalshi", "Kalshi", "prediction_market", KALSHI),
        ("polymarket", "Polymarket", "prediction_market", POLY_GAMMA),
    ]
    for source_id, name, source_type, base_url in rows:
        con.execute(
            """INSERT INTO er_sources(source_id,source_name,source_type,base_url)
               VALUES (?,?,?,?)
               ON CONFLICT(source_id) DO UPDATE SET
                 source_name=excluded.source_name,
                 source_type=excluded.source_type,
                 base_url=excluded.base_url,
                 updated_at=now()""",
            [source_id, name, source_type, base_url],
        )


def insert_raw(con, source_id: str, object_type: str, object_id: str, payload: object,
               observed_at: Optional[datetime], run_id: str) -> str:
    received = now_utc()
    text = canonical_json(payload)
    digest = hashlib.sha256(text.encode()).hexdigest()
    snapshot_id = f"raw_{uuid.uuid4().hex}"
    con.execute(
        """INSERT INTO er_raw_snapshots(
             raw_snapshot_id,source_id,source_object_type,source_object_id,
             source_observed_at,received_at,content_hash,payload_json,ingestion_run_id)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        [snapshot_id, source_id, object_type, object_id, observed_at, received, digest, text, run_id],
    )
    return snapshot_id


def contract_metadata(snapshot: ContractSnapshot) -> dict:
    return {
        "title": snapshot.title,
        "description": snapshot.description,
        "status": snapshot.status,
        "opens_at": snapshot.opens_at,
        "closes_at": snapshot.closes_at,
        "resolves_at": snapshot.resolves_at,
        "resolution_authority": snapshot.resolution_authority,
        "resolution_rules": snapshot.resolution_rules,
        "source_url": snapshot.source_url,
    }


def upsert_contract(con, snapshot: ContractSnapshot, raw_snapshot_id: str) -> str:
    contract_id = stable_id("sc", snapshot.source_id, snapshot.source_market_id)
    current = con.execute(
        """SELECT current_title,current_description,status,opens_at,closes_at,resolves_at,
                  resolution_authority,current_resolution_rules,source_url
           FROM er_source_contracts WHERE source_contract_id=?""",
        [contract_id],
    ).fetchone()
    metadata = contract_metadata(snapshot)
    values = [
        metadata["title"], metadata["description"], metadata["status"], metadata["opens_at"],
        metadata["closes_at"], metadata["resolves_at"], metadata["resolution_authority"],
        metadata["resolution_rules"], metadata["source_url"],
    ]
    if current is None:
        con.execute(
            """INSERT INTO er_source_contracts(
                 source_contract_id,source_id,source_market_id,source_event_id,current_title,
                 current_description,status,opens_at,closes_at,resolves_at,resolution_authority,
                 current_resolution_rules,source_url,first_seen_at,last_seen_at,active)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            [contract_id, snapshot.source_id, snapshot.source_market_id, snapshot.source_event_id,
             *values, now_utc(), now_utc(), snapshot.status in {"active", "open"}],
        )
        con.execute(
            """INSERT INTO er_source_contract_versions(
                 source_contract_id,version_no,title,description,status,resolution_rules,
                 opens_at,closes_at,resolves_at,source_observed_at,received_at,valid_from,raw_snapshot_id)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            [contract_id, 1, snapshot.title, snapshot.description, snapshot.status,
             snapshot.resolution_rules, snapshot.opens_at, snapshot.closes_at, snapshot.resolves_at,
             snapshot.observed_at, now_utc(), now_utc(), raw_snapshot_id],
        )
    else:
        changed = tuple(current) != tuple(values)
        con.execute(
            """UPDATE er_source_contracts SET
                 source_event_id=?,current_title=?,current_description=?,status=?,opens_at=?,closes_at=?,
                 resolves_at=?,resolution_authority=?,current_resolution_rules=?,source_url=?,
                 last_seen_at=?,active=?
               WHERE source_contract_id=?""",
            [snapshot.source_event_id, *values, now_utc(), snapshot.status in {"active", "open"}, contract_id],
        )
        if changed:
            prior = con.execute(
                "SELECT coalesce(max(version_no),0) FROM er_source_contract_versions WHERE source_contract_id=?",
                [contract_id],
            ).fetchone()[0]
            con.execute(
                "UPDATE er_source_contract_versions SET valid_to=? WHERE source_contract_id=? AND valid_to IS NULL",
                [now_utc(), contract_id],
            )
            con.execute(
                """INSERT INTO er_source_contract_versions(
                     source_contract_id,version_no,title,description,status,resolution_rules,
                     opens_at,closes_at,resolves_at,source_observed_at,received_at,valid_from,raw_snapshot_id)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                [contract_id, prior + 1, snapshot.title, snapshot.description, snapshot.status,
                 snapshot.resolution_rules, snapshot.opens_at, snapshot.closes_at, snapshot.resolves_at,
                 snapshot.observed_at, now_utc(), now_utc(), raw_snapshot_id],
            )
            con.execute(
                """INSERT INTO er_revisions(revision_id,object_type,object_id,revision_type,reason,previous_version,new_version,effective_at)
                   VALUES (?,?,?,?,?,?,?,?,?)""".replace(",?,?,?,?,?)", ",?,?,?,?)"),
                [f"rev_{uuid.uuid4().hex}", "source_contract", contract_id, "metadata_change",
                 "Source contract metadata changed", str(prior), str(prior + 1), snapshot.observed_at],
            )
    return contract_id


def insert_observation(con, snapshot: ContractSnapshot, contract_id: str, raw_snapshot_id: str):
    midpoint = None
    spread = None
    if snapshot.yes_bid is not None and snapshot.yes_ask is not None:
        spread = snapshot.yes_ask - snapshot.yes_bid
        if snapshot.yes_bid <= snapshot.yes_ask:
            midpoint = (snapshot.yes_bid + snapshot.yes_ask) / 2
    received = now_utc()
    con.execute(
        """INSERT INTO er_market_observations(
             source_id,source_contract_id,observed_at,received_at,processed_at,
             yes_bid,yes_ask,no_bid,no_ask,midpoint,last_trade,displayed_probability,
             volume,volume_24h,open_interest,liquidity,orderbook_depth,spread,
             market_status,raw_snapshot_id,methodology_version)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        [snapshot.source_id, contract_id, snapshot.observed_at, received, received,
         snapshot.yes_bid, snapshot.yes_ask, snapshot.no_bid, snapshot.no_ask, midpoint,
         snapshot.last_trade, snapshot.displayed_probability, snapshot.volume, snapshot.volume_24h,
         snapshot.open_interest, snapshot.liquidity, snapshot.orderbook_depth, spread,
         snapshot.status, raw_snapshot_id, "source_native_v1"],
    )


def update_health(con, source_id: str, run_id: str, status: str, records: int, details: Optional[str] = None):
    previous = con.execute(
        "SELECT consecutive_failures FROM er_source_health WHERE source_id=?",
        [source_id],
    ).fetchone()
    failures = (previous[0] if previous else 0) + 1 if status != "success" else 0
    last_obs = con.execute(
        "SELECT max(observed_at) FROM er_market_observations WHERE source_id=?",
        [source_id],
    ).fetchone()[0]
    con.execute(
        """INSERT INTO er_source_health(
             source_id,last_attempt_at,last_success_at,last_observation_at,last_run_id,last_run_status,
             records_last_run,consecutive_failures,health_status,details,updated_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,now())
           ON CONFLICT(source_id) DO UPDATE SET
             last_attempt_at=excluded.last_attempt_at,
             last_success_at=coalesce(excluded.last_success_at,er_source_health.last_success_at),
             last_observation_at=excluded.last_observation_at,last_run_id=excluded.last_run_id,
             last_run_status=excluded.last_run_status,records_last_run=excluded.records_last_run,
             consecutive_failures=excluded.consecutive_failures,health_status=excluded.health_status,
             details=excluded.details,updated_at=now()""",
        [source_id, now_utc(), now_utc() if status == "success" else None, last_obs, run_id, status,
         records, failures, "healthy" if status == "success" else "degraded", details],
    )


def ingest_source(source_id: str) -> dict:
    if source_id not in ADAPTERS:
        raise ValueError(f"Unknown source {source_id}")
    adapter = ADAPTERS[source_id]()
    run_id = f"ing_{uuid.uuid4().hex}"
    started = now_utc()
    written = 0
    errors = 0
    con = connect()
    try:
        ensure_sources(con)
        con.execute(
            "INSERT INTO er_ingestion_runs(run_id,source_id,started_at,status) VALUES (?,?,?,'running')",
            [run_id, source_id, started],
        )
        try:
            for snapshot in adapter.iter_snapshots():
                try:
                    event_object_id = snapshot.source_event_id or snapshot.source_market_id
                    insert_raw(con, source_id, "event", event_object_id, snapshot.raw_event, snapshot.observed_at, run_id)
                    market_raw_id = insert_raw(
                        con, source_id, "market", snapshot.source_market_id,
                        snapshot.raw_market, snapshot.observed_at, run_id,
                    )
                    contract_id = upsert_contract(con, snapshot, market_raw_id)
                    insert_observation(con, snapshot, contract_id, market_raw_id)
                    written += 1
                except Exception:
                    errors += 1
                    log.exception("Failed to persist %s contract %s", source_id, snapshot.source_market_id)
            status = "success" if errors == 0 else "partial"
            con.execute(
                """UPDATE er_ingestion_runs SET finished_at=?,status=?,records_received=?,records_written=?,error_count=?
                   WHERE run_id=?""",
                [now_utc(), status, written + errors, written, errors, run_id],
            )
            update_health(con, source_id, run_id, status, written, None if not errors else f"{errors} record errors")
            return {"source_id": source_id, "run_id": run_id, "status": status, "records_written": written, "errors": errors}
        except Exception as exc:
            con.execute(
                """UPDATE er_ingestion_runs SET finished_at=?,status='failed',records_received=?,records_written=?,error_count=?,details=?
                   WHERE run_id=?""",
                [now_utc(), written, written, errors + 1, str(exc)[:2000], run_id],
            )
            update_health(con, source_id, run_id, "failed", written, str(exc)[:2000])
            raise
    finally:
        con.close()


def run_once(source_ids: Optional[list[str]] = None) -> list[dict]:
    results = []
    for source_id in source_ids or list(ADAPTERS):
        try:
            results.append(ingest_source(source_id))
        except Exception as exc:
            log.exception("Institutional ingestion failed for %s", source_id)
            results.append({"source_id": source_id, "status": "failed", "error": str(exc)})
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(json.dumps(run_once(), indent=2, default=str))
