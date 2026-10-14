"""Cursor-based REST change feed for institutional synchronization.

Unlike the legacy `/v1/changes?since=` convenience endpoint, this endpoint provides an
opaque exclusive resume token and deterministic ordering. Historical replay defaults to
`as_known` canonical mapping so later taxonomy changes do not leak backward into a sync.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal, Optional

from fastapi import APIRouter, Query

from db import connect
from institutional_stream import Cursor, decode_cursor, encode_cursor

router = APIRouter(tags=["institutional-change-feed"])


def _rows(cur):
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _start(cursor: Optional[str], since: Optional[datetime]) -> Cursor:
    if cursor:
        return decode_cursor(cursor)
    if since is not None:
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)
        return Cursor(since)
    return Cursor(datetime.now(timezone.utc))


@router.get("/v1/change-feed")
def change_feed(
    cursor: Optional[str] = None,
    since: Optional[datetime] = None,
    source_id: Optional[str] = None,
    mapping_basis: Literal["as_known", "current_taxonomy"] = "as_known",
    limit: int = Query(1000, ge=1, le=10000),
):
    start = _start(cursor, since)
    con = connect(read_only=True)
    try:
        mapping_join = (
            """LEFT JOIN er_event_mappings m
                 ON m.source_contract_id=o.source_contract_id
                AND m.valid_from<=o.received_at
                AND (m.valid_to IS NULL OR m.valid_to>o.received_at)"""
            if mapping_basis == "as_known"
            else "LEFT JOIN er_latest_contract_mappings m USING(source_contract_id)"
        )
        where = ["""(
          o.received_at > ?
          OR (o.received_at = ? AND o.source_contract_id > ?)
          OR (o.received_at = ? AND o.source_contract_id = ? AND o.observed_at > ?)
        )"""]
        params: list[object] = [
            start.received_at, start.received_at, start.source_contract_id,
            start.received_at, start.source_contract_id, start.observed_at,
        ]
        if source_id:
            where.append("o.source_id=?")
            params.append(source_id)
        params.append(limit)
        rows = _rows(con.execute(
            f"""SELECT
                  m.canonical_event_id,m.mapping_id,m.mapping_type,m.review_status AS mapping_review_status,
                  o.source_id,o.source_contract_id,c.source_market_id,o.observed_at,o.received_at,o.processed_at,
                  o.displayed_probability,o.yes_bid,o.yes_ask,o.no_bid,o.no_ask,o.midpoint,o.last_trade,
                  o.volume,o.volume_24h,o.open_interest,o.liquidity,o.orderbook_depth,o.spread,
                  o.market_status,o.methodology_version
                FROM er_market_observations o
                JOIN er_source_contracts c USING(source_contract_id)
                {mapping_join}
                WHERE {' AND '.join(where)}
                ORDER BY o.received_at,o.source_contract_id,o.observed_at
                LIMIT ?""",
            params,
        ))
    finally:
        con.close()

    next_cursor = start
    if rows:
        last = rows[-1]
        next_cursor = Cursor(last["received_at"], last["source_contract_id"], last["observed_at"])
    return {
        "mapping_basis": mapping_basis,
        "source_filter": source_id,
        "count": len(rows),
        "limit": limit,
        "has_more_possible": len(rows) == limit,
        "cursor": encode_cursor(next_cursor),
        "cursor_semantics": "exclusive resume token",
        "observations": rows,
    }
