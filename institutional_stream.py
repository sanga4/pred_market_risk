"""WebSocket stream for normalized EventRisk market observations.

The stream is an append-only delivery surface over er_market_observations.  It sends raw
observed/normalized event data only; it does not emit portfolio actions or trade signals.
"""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from db import connect
from institutional_auth import authenticate_key, extract_bearer

router = APIRouter(tags=["institutional-stream"])


def _iso(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _parse_since(value: Optional[str]) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return datetime.now(timezone.utc)


def _rows_since(cursor: datetime, source_id: Optional[str], limit: int = 1000):
    con = connect(read_only=True)
    try:
        where = ["o.received_at > ?"]
        params = [cursor]
        if source_id:
            where.append("o.source_id = ?")
            params.append(source_id)
        params.append(limit)
        cur = con.execute(
            f"""SELECT
                  m.canonical_event_id,o.source_id,o.source_contract_id,c.source_market_id,
                  o.observed_at,o.received_at,o.displayed_probability,o.yes_bid,o.yes_ask,
                  o.midpoint,o.last_trade,o.volume,o.volume_24h,o.open_interest,o.liquidity,
                  o.spread,o.market_status,o.methodology_version
                FROM er_market_observations o
                JOIN er_source_contracts c USING(source_contract_id)
                LEFT JOIN er_latest_contract_mappings m USING(source_contract_id)
                WHERE {' AND '.join(where)}
                ORDER BY o.received_at,o.source_contract_id
                LIMIT ?""",
            params,
        )
        columns = [d[0] for d in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]
    finally:
        con.close()


@router.websocket("/v1/stream")
async def stream(websocket: WebSocket):
    require_auth = os.getenv("EVENTRISK_REQUIRE_AUTH", "false").lower() in {"1", "true", "yes"}
    supplied = extract_bearer(
        websocket.headers.get("authorization"),
        websocket.headers.get("x-api-key") or websocket.query_params.get("api_key"),
    )
    client = authenticate_key(supplied, "stream") if supplied else None
    if require_auth and client is None:
        await websocket.close(code=4401, reason="API key required")
        return

    source_id = websocket.query_params.get("source_id")
    cursor = _parse_since(websocket.query_params.get("since"))
    poll_seconds = max(0.5, min(10.0, float(websocket.query_params.get("poll_seconds", "1"))))
    await websocket.accept()
    await websocket.send_json({
        "type": "hello",
        "schema_version": "v1",
        "data_layer": "observed+normalized",
        "client_id": client.get("client_id") if client else None,
        "cursor": cursor.isoformat(),
    })

    try:
        while True:
            rows = await asyncio.to_thread(_rows_since, cursor, source_id, 1000)
            if rows:
                for row in rows:
                    for key, value in list(row.items()):
                        if isinstance(value, datetime):
                            row[key] = _iso(value)
                next_cursor = max(datetime.fromisoformat(r["received_at"]) for r in rows)
                await websocket.send_text(json.dumps({
                    "type": "observations",
                    "count": len(rows),
                    "cursor": next_cursor.isoformat(),
                    "data": rows,
                }, separators=(",", ":")))
                cursor = next_cursor
                # If the batch was full, immediately drain the next page before sleeping.
                if len(rows) >= 1000:
                    continue
            else:
                await websocket.send_json({"type": "heartbeat", "cursor": cursor.isoformat()})
            await asyncio.sleep(poll_seconds)
    except WebSocketDisconnect:
        return
