"""First-class event curve endpoints for institutional research.

Curves expose source-native observations. EventRisk does not blend them into a prescribed
consensus. Consumers choose whether to use today's taxonomy (`current_taxonomy`) or the
canonical mapping that EventRisk actually knew when each observation arrived (`as_known`).
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from fastapi import APIRouter, HTTPException, Query

from db import connect

router = APIRouter(tags=["institutional-curves"])


def _rows(cur):
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


@router.get("/v1/events/{event_id}/curve")
def event_curve(
    event_id: str,
    mapping_basis: Literal["as_known", "current_taxonomy"] = "as_known",
    source_id: Optional[str] = None,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    as_of: Optional[datetime] = None,
    limit: int = Query(25000, ge=1, le=250000),
):
    """Return the raw event probability/market-quality curve without aggregation.

    `as_known` is the safe default for historical research because a row is associated with
    the mapping valid when EventRisk received it. `current_taxonomy` intentionally restates
    old rows through today's active canonical mapping.
    """
    con = connect(read_only=True)
    try:
        event = con.execute(
            "SELECT canonical_name,status FROM er_canonical_events WHERE canonical_event_id=?",
            [event_id],
        ).fetchone()
        if not event:
            raise HTTPException(404, "Canonical event not found")

        view = "er_event_curve_as_known" if mapping_basis == "as_known" else "er_event_curve_current_taxonomy"
        where = ["canonical_event_id=?"]
        params: list[object] = [event_id]
        if source_id:
            where.append("source_id=?")
            params.append(source_id)
        if start:
            where.append("observed_at>=?")
            params.append(start)
        if end:
            where.append("observed_at<=?")
            params.append(end)
        if as_of:
            where.append("received_at<=?")
            params.append(as_of)
        params.append(limit)

        rows = _rows(con.execute(
            f"""SELECT * FROM {view}
                WHERE {' AND '.join(where)}
                ORDER BY observed_at,received_at,source_id,source_contract_id
                LIMIT ?""",
            params,
        ))
        source_count = len({r["source_id"] for r in rows})
        received = [r["received_at"] for r in rows if r.get("received_at") is not None]
        observed = [r["observed_at"] for r in rows if r.get("observed_at") is not None]
        return {
            "canonical_event_id": event_id,
            "canonical_name": event[0],
            "event_status": event[1],
            "mapping_basis": mapping_basis,
            "as_of": as_of,
            "source_filter": source_id,
            "observation_count": len(rows),
            "source_count": source_count,
            "min_observed_at": min(observed) if observed else None,
            "max_observed_at": max(observed) if observed else None,
            "max_received_at": max(received) if received else None,
            "limit": limit,
            "truncated": len(rows) == limit,
            "note": "Source-native curve only; no EventRisk consensus probability is imposed.",
            "observations": rows,
        }
    finally:
        con.close()


@router.get("/v1/events/{event_id}/curve/latest")
def latest_curve_state(
    event_id: str,
    mapping_basis: Literal["as_known", "current_taxonomy"] = "as_known",
    at: Optional[datetime] = None,
):
    """Return the latest observation per source contract by an optional point-in-time cutoff."""
    con = connect(read_only=True)
    try:
        exists = con.execute(
            "SELECT 1 FROM er_canonical_events WHERE canonical_event_id=?",
            [event_id],
        ).fetchone()
        if not exists:
            raise HTTPException(404, "Canonical event not found")
        view = "er_event_curve_as_known" if mapping_basis == "as_known" else "er_event_curve_current_taxonomy"
        cutoff = "AND received_at<=?" if at else ""
        params: list[object] = [event_id]
        if at:
            params.append(at)
        rows = _rows(con.execute(
            f"""WITH ranked AS (
                  SELECT *,row_number() OVER(
                    PARTITION BY source_contract_id
                    ORDER BY received_at DESC,observed_at DESC
                  ) rn
                  FROM {view}
                  WHERE canonical_event_id=? {cutoff}
                )
                SELECT * EXCLUDE(rn) FROM ranked WHERE rn=1
                ORDER BY source_id,source_market_id""",
            params,
        ))
        probabilities = [r["displayed_probability"] for r in rows if r.get("displayed_probability") is not None]
        descriptive = None
        if probabilities:
            values = sorted(probabilities)
            n = len(values)
            median = values[n // 2] if n % 2 else (values[n // 2 - 1] + values[n // 2]) / 2
            descriptive = {
                "count": n,
                "min": min(values),
                "max": max(values),
                "median": median,
                "range": max(values) - min(values),
            }
        return {
            "canonical_event_id": event_id,
            "mapping_basis": mapping_basis,
            "as_of": at,
            "descriptive_cross_source_state": descriptive,
            "note": "Descriptive only; source disagreement is preserved and no consensus is prescribed.",
            "sources": rows,
        }
    finally:
        con.close()
