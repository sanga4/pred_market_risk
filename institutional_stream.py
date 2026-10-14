"""Lossless WebSocket stream for normalized EventRisk observations.

The stream uses an opaque compound cursor over the same deterministic ordering as the
underlying primary key. Timestamp-only cursors are unsafe when a batch contains many rows
with the same received_at value; the compound cursor makes reconnects/page drains lossless.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from db import connect
from institutional_auth import authenticate_key, extract_bearer

router = APIRouter(tags=["institutional-stream"])


@dataclass(frozen=True)
class Cursor:
    received_at: datetime
    source_contract_id: str = ""
    observed_at: datetime = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _iso(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _parse_time(value: Optional[str], default: datetime) -> datetime:
    if not value:
        return default
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return default


def encode_cursor(cursor: Cursor) -> str:
    raw = json.dumps({
        "r": cursor.received_at.isoformat(),
        "c": cursor.source_contract_id,
        "o": cursor.observed_at.isoformat(),
    }, separators=(",", ":"), sort_keys=True).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def decode_cursor(value: Optional[str], since: Optional[str] = None) -> Cursor:
    if value:
        try:
            padded = value + "=" * (-len(value) % 4)
            data = json.loads(base64.urlsafe_b64decode(padded.encode()).decode())
            received = _parse_time(data.get("r"), datetime.now(timezone.utc))
            observed = _parse_time(data.get("o"), datetime(1970, 1, 1, tzinfo=timezone.utc))
            return Cursor(received, str(data.get("c") or ""), observed)
        except Exception:
            # Invalid resume tokens fail safely to the caller's explicit `since`, or now.
            pass
    received = _parse_time(since, datetime.now(timezone.utc))
    return Cursor(received)


def cursor_from_row(row: dict) -> Cursor:
    return Cursor(row["received_at"], row["source_contract_id"], row["observed_at"])


def _rows_after(cursor: Cursor, source_id: Optional[str], limit: int = 1000):
    con = connect(read_only=True)
    try:
        where = ["""(
            o.received_at > ?
            OR (o.received_at = ? AND o.source_contract_id > ?)
            OR (o.received_at = ? AND o.source_contract_id = ? AND o.observed_at > ?)
        )"""]
        params = [
            cursor.received_at, cursor.received_at, cursor.source_contract_id,
            cursor.received_at, cursor.source_contract_id, cursor.observed_at,
        ]
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
                ORDER BY o.received_at,o.source_contract_id,o.observed_at
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
    cursor = decode_cursor(websocket.query_params.get("cursor"), websocket.query_params.get("since"))
    poll_seconds = max(0.5, min(10.0, float(websocket.query_params.get("poll_seconds", "1"))))
    await websocket.accept()
    await websocket.send_json({
        "type": "hello",
        "schema_version": "v2.1",
        "data_layer": "observed+normalized",
        "client_id": client.get("client_id") if client else None,
        "cursor": encode_cursor(cursor),
        "cursor_semantics": "exclusive resume token",
    })

    try:
        while True:
            rows = await asyncio.to_thread(_rows_after, cursor, source_id, 1000)
            if rows:
                next_cursor = cursor_from_row(rows[-1])
                serializable = []
                for row in rows:
                    serializable.append({
                        key: _iso(value) if isinstance(value, datetime) else value
                        for key, value in row.items()
                    })
                await websocket.send_text(json.dumps({
                    "type": "observations",
                    "count": len(rows),
                    "cursor": encode_cursor(next_cursor),
                    "data": serializable,
                }, separators=(",", ":")))
                cursor = next_cursor
                if len(rows) >= 1000:
                    continue
            else:
                await websocket.send_json({"type": "heartbeat", "cursor": encode_cursor(cursor)})
            await asyncio.sleep(poll_seconds)
    except WebSocketDisconnect:
        return
