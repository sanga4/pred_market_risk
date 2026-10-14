"""Source adapters for the institutional event universe.

Default policy is intentionally `full`: EventRisk collects every supported open event
returned by each source and leaves relevance decisions to institutional clients.
Optional filtering exists only when explicitly configured with
EVENTRISK_COLLECTION_SCOPE=filtered.

Kalshi's public events endpoint is cursor paginated and does not require a category
filter. Polymarket's keyset events endpoint likewise supports listing active events
without a tag; tags are optional filters. The adapters preserve those source semantics.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Iterable, Optional

import institutional_ingest as base

log = logging.getLogger("eventrisk.institutional_universe_adapters")

COLLECTION_SCOPE = os.getenv("EVENTRISK_COLLECTION_SCOPE", "full").strip().lower()
if COLLECTION_SCOPE not in {"full", "filtered"}:
    raise RuntimeError("EVENTRISK_COLLECTION_SCOPE must be 'full' or 'filtered'")

KALSHI_FILTER_CATEGORIES = {
    x.strip().lower()
    for x in os.getenv("KALSHI_CATEGORIES", "").split(",")
    if x.strip()
}
POLY_FILTER_TAGS = [
    x.strip()
    for x in os.getenv("POLY_TAGS", "").split(",")
    if x.strip()
]

ContractSnapshot = base.ContractSnapshot


def collection_policy() -> dict:
    return {
        "scope": COLLECTION_SCOPE,
        "kalshi_categories": sorted(KALSHI_FILTER_CATEGORIES) if COLLECTION_SCOPE == "filtered" else [],
        "polymarket_tags": list(POLY_FILTER_TAGS) if COLLECTION_SCOPE == "filtered" else [],
        "default_behavior": "all supported open events" if COLLECTION_SCOPE == "full" else "explicit source filters",
    }


class KalshiUniverseAdapter:
    source_id = "kalshi"

    def iter_snapshots(self) -> Iterable[ContractSnapshot]:
        cursor = None
        while True:
            data = base.get_json(
                f"{base.KALSHI}/events",
                status="open",
                with_nested_markets="true",
                limit=200,
                cursor=cursor,
            )
            for event in data.get("events") or []:
                category = str(event.get("category") or "").strip().lower()
                if (
                    COLLECTION_SCOPE == "filtered"
                    and KALSHI_FILTER_CATEGORIES
                    and category
                    and category not in KALSHI_FILTER_CATEGORIES
                ):
                    continue
                for market in event.get("markets") or []:
                    status = str(market.get("status") or "").lower()
                    if status not in {"open", "active"}:
                        continue
                    bid = base.number(market.get("yes_bid_dollars") or market.get("yes_bid"))
                    ask = base.number(market.get("yes_ask_dollars") or market.get("yes_ask"))
                    last = base.number(market.get("last_price_dollars") or market.get("last_price"))
                    if bid is not None and bid > 1.0:
                        bid /= 100.0
                    if ask is not None and ask > 1.0:
                        ask /= 100.0
                    if last is not None and last > 1.0:
                        last /= 100.0
                    midpoint = (
                        (bid + ask) / 2
                        if bid is not None and ask is not None and bid <= ask
                        else None
                    )
                    observed = base.parse_time(
                        market.get("updated_time")
                        or market.get("last_updated_ts")
                        or event.get("last_updated_ts")
                    ) or base.now_utc()
                    settlement_sources = event.get("settlement_sources") or []
                    authority = market.get("settlement_source") or event.get("settlement_source")
                    if not authority and settlement_sources:
                        authority = "; ".join(
                            str(item.get("name") or item.get("url") or "").strip()
                            for item in settlement_sources
                            if isinstance(item, dict) and (item.get("name") or item.get("url"))
                        ) or None
                    yield ContractSnapshot(
                        source_id=self.source_id,
                        source_market_id=str(market.get("ticker")),
                        source_event_id=str(event.get("event_ticker") or "") or None,
                        title=str(
                            market.get("title")
                            or market.get("yes_sub_title")
                            or event.get("title")
                            or market.get("ticker")
                        ),
                        description=market.get("subtitle") or event.get("sub_title") or event.get("title"),
                        status=status,
                        opens_at=base.parse_time(market.get("open_time")),
                        closes_at=base.parse_time(market.get("close_time")),
                        resolves_at=base.parse_time(
                            market.get("expiration_time")
                            or market.get("expected_expiration_time")
                            or market.get("settlement_ts")
                        ),
                        resolution_authority=authority,
                        resolution_rules=(
                            market.get("rules_primary")
                            or market.get("rules_secondary")
                            or market.get("rules")
                        ),
                        source_url=None,
                        observed_at=observed,
                        yes_bid=bid,
                        yes_ask=ask,
                        no_bid=base.number(market.get("no_bid_dollars") or market.get("no_bid")),
                        no_ask=base.number(market.get("no_ask_dollars") or market.get("no_ask")),
                        last_trade=last,
                        displayed_probability=midpoint if midpoint is not None else last,
                        volume=base.number(market.get("volume_fp") or market.get("volume")),
                        volume_24h=base.number(market.get("volume_24h_fp") or market.get("volume_24h")),
                        open_interest=base.number(
                            market.get("open_interest_fp") or market.get("open_interest")
                        ),
                        liquidity=base.number(market.get("liquidity")),
                        orderbook_depth=None,
                        raw_event=event,
                        raw_market=market,
                    )
            cursor = data.get("cursor")
            if not cursor:
                break


class PolymarketUniverseAdapter:
    source_id = "polymarket"

    def _orderbook_depth(self, market: dict) -> Optional[float]:
        if not base.FETCH_POLY_ORDERBOOK:
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
            book = base.get_json(f"{base.POLY_CLOB}/book", token_id=token_ids[0])
            depth = 0.0
            for side in ("bids", "asks"):
                for level in book.get(side) or []:
                    price = base.number(level.get("price")) or 0.0
                    size = base.number(level.get("size")) or 0.0
                    depth += price * size
            return depth
        except Exception:
            log.exception("Polymarket orderbook fetch failed for %s", market.get("id"))
            return None

    def iter_snapshots(self) -> Iterable[ContractSnapshot]:
        # Full mode uses the documented untagged keyset feed. Filtered mode may run
        # several explicit tag scopes and deduplicates markets that overlap those tags.
        scopes: list[Optional[str]] = (
            [None]
            if COLLECTION_SCOPE == "full"
            else (list(POLY_FILTER_TAGS) if POLY_FILTER_TAGS else [None])
        )
        seen: set[str] = set()
        for tag in scopes:
            cursor = None
            while True:
                params = {
                    "closed": "false",
                    "limit": 100,
                    "after_cursor": cursor,
                }
                if tag is not None:
                    # Retain the legacy slug filter only as an explicit filtered-mode
                    # compatibility option. Full mode never supplies a tag restriction.
                    params["tag_slug"] = tag
                data = base.get_json(f"{base.POLY_GAMMA}/events/keyset", **params)
                for event in data.get("events") or []:
                    for market in event.get("markets") or []:
                        market_id = str(market.get("id") or "")
                        if (
                            not market_id
                            or market_id in seen
                            or market.get("closed")
                            or not market.get("active")
                        ):
                            continue
                        seen.add(market_id)
                        bid = base.number(market.get("bestBid"))
                        ask = base.number(market.get("bestAsk"))
                        last = base.number(market.get("lastTradePrice"))
                        midpoint = (
                            (bid + ask) / 2
                            if bid is not None and ask is not None and bid <= ask
                            else None
                        )
                        observed = base.parse_time(
                            market.get("updatedAt") or event.get("updatedAt")
                        ) or base.now_utc()
                        rules = (
                            market.get("description")
                            or event.get("description")
                            or market.get("question")
                        )
                        yield ContractSnapshot(
                            source_id=self.source_id,
                            source_market_id=market_id,
                            source_event_id=str(event.get("id") or "") or None,
                            title=str(
                                market.get("question")
                                or market.get("groupItemTitle")
                                or event.get("title")
                                or market_id
                            ),
                            description=market.get("description") or event.get("description"),
                            status=(
                                "active"
                                if market.get("active") and not market.get("closed")
                                else "closed"
                            ),
                            opens_at=base.parse_time(
                                market.get("startDate") or event.get("startDate")
                            ),
                            closes_at=base.parse_time(
                                market.get("endDate") or event.get("endDate")
                            ),
                            resolves_at=base.parse_time(
                                market.get("endDate") or event.get("endDate")
                            ),
                            resolution_authority=(
                                market.get("resolutionSource")
                                or event.get("resolutionSource")
                            ),
                            resolution_rules=rules,
                            source_url=market.get("url") or event.get("url"),
                            observed_at=observed,
                            yes_bid=bid,
                            yes_ask=ask,
                            no_bid=None,
                            no_ask=None,
                            last_trade=last,
                            displayed_probability=midpoint if midpoint is not None else last,
                            volume=base.number(market.get("volumeNum") or market.get("volume")),
                            volume_24h=base.number(market.get("volume24hr")),
                            open_interest=base.number(market.get("openInterest")),
                            liquidity=base.number(
                                market.get("liquidityNum") or market.get("liquidity")
                            ),
                            orderbook_depth=self._orderbook_depth(market),
                            raw_event=event,
                            raw_market=market,
                        )
                cursor = data.get("next_cursor")
                if not cursor:
                    break


ADAPTERS = {
    "kalshi": KalshiUniverseAdapter,
    "polymarket": PolymarketUniverseAdapter,
}
