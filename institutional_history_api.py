from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from db import connect

router = APIRouter(tags=["institutional-history"])


def rows_dict(cur):
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


@router.get("/v1/events/{event_id}/versions")
def event_versions(event_id: str):
    con = connect(read_only=True)
    try:
        rows = rows_dict(con.execute(
            "SELECT * FROM er_canonical_event_versions WHERE canonical_event_id=? ORDER BY version_no",
            [event_id],
        ))
        if not rows:
            raise HTTPException(404, "Canonical event versions not found")
        return rows
    finally:
        con.close()


@router.get("/v1/event-families/{family_id}/versions")
def family_versions(family_id: str):
    con = connect(read_only=True)
    try:
        rows = rows_dict(con.execute(
            "SELECT * FROM er_event_family_versions WHERE event_family_id=? ORDER BY version_no",
            [family_id],
        ))
        if not rows:
            raise HTTPException(404, "Event family versions not found")
        return rows
    finally:
        con.close()


@router.get("/v1/events/{event_id}/history-point-in-time")
def point_in_time_history(
    event_id: str,
    source_id: Optional[str] = None,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    limit: int = Query(10000, ge=1, le=100000),
):
    """Return only observations mapped to this event at the time EventRisk received them.

    This differs intentionally from `/history`, which is restated through today's active
    mapping and is useful when a researcher wants today's taxonomy applied to older prices.
    """
    con = connect(read_only=True)
    try:
        exists = con.execute(
            "SELECT 1 FROM er_canonical_events WHERE canonical_event_id=? LIMIT 1", [event_id]
        ).fetchone()
        if not exists:
            raise HTTPException(404, "Canonical event not found")
        where = [
            "m.canonical_event_id=?",
            "m.valid_from<=o.received_at",
            "(m.valid_to IS NULL OR m.valid_to>o.received_at)",
        ]
        params = [event_id]
        if source_id:
            where.append("o.source_id=?")
            params.append(source_id)
        if start:
            where.append("o.observed_at>=?")
            params.append(start)
        if end:
            where.append("o.observed_at<=?")
            params.append(end)
        params.append(limit)
        rows = rows_dict(con.execute(
            f"""SELECT
                   o.source_id,o.source_contract_id,c.source_market_id,o.observed_at,o.received_at,
                   o.processed_at,o.yes_bid,o.yes_ask,o.no_bid,o.no_ask,o.midpoint,o.last_trade,
                   o.displayed_probability,o.volume,o.volume_24h,o.open_interest,o.liquidity,
                   o.orderbook_depth,o.spread,o.market_status,o.methodology_version,
                   m.mapping_id,m.mapping_type,m.mapping_method,m.method_version,m.review_status
                 FROM er_market_observations o
                 JOIN er_source_contracts c USING(source_contract_id)
                 JOIN er_event_mappings m USING(source_contract_id)
                 WHERE {' AND '.join(where)}
                 ORDER BY o.observed_at,o.received_at
                 LIMIT ?""",
            params,
        ))
        return {
            "canonical_event_id": event_id,
            "mapping_basis": "point_in_time_as_known",
            "observations": rows,
        }
    finally:
        con.close()


@router.get("/v1/events/{event_id}/as-known")
def event_as_known(event_id: str, at: datetime):
    con = connect(read_only=True)
    try:
        event = rows_dict(con.execute(
            """SELECT * FROM er_canonical_event_versions
               WHERE canonical_event_id=? AND valid_from<=? AND (valid_to IS NULL OR valid_to>?)
               ORDER BY version_no DESC LIMIT 1""",
            [event_id, at, at],
        ))
        if not event:
            raise HTTPException(404, "No canonical event version existed at that timestamp")
        observations = rows_dict(con.execute(
            """WITH eligible AS (
                 SELECT o.*,c.source_market_id,m.mapping_id,m.mapping_type,m.mapping_method,m.method_version,
                        row_number() OVER(
                          PARTITION BY o.source_contract_id ORDER BY o.received_at DESC,o.observed_at DESC
                        ) rn
                 FROM er_market_observations o
                 JOIN er_source_contracts c USING(source_contract_id)
                 JOIN er_event_mappings m USING(source_contract_id)
                 WHERE m.canonical_event_id=?
                   AND m.valid_from<=?
                   AND (m.valid_to IS NULL OR m.valid_to>?)
                   AND o.received_at<=?
               )
               SELECT * EXCLUDE(rn) FROM eligible WHERE rn=1 ORDER BY source_id,source_market_id""",
            [event_id, at, at, at],
        ))
        return {
            "as_of": at,
            "mapping_basis": "point_in_time_as_known",
            "event": event[0],
            "source_observations": observations,
        }
    finally:
        con.close()
