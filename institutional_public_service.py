"""Stateless public EventRisk institutional API.

This process is a read/query plane only. Source ingestion, schema migrations, identity mapping,
enrichment, release generation, key administration and raw-payload restoration are owned by
separate controlled workers/admin tooling.

Public boundary:
- only read/query routes are mounted;
- /internal/* is rejected as defense in depth;
- no write-side model modules are imported;
- institutions receive event data and retain downstream scenario/investment decisions.
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from db import connect
from institutional_api import router as data_router
from institutional_auth import authenticate_key, extract_bearer
from institutional_changefeed_api import router as changefeed_router
from institutional_curve_api import router as curve_router
from institutional_history_api import router as history_router
from institutional_public_queries_api import router as public_queries_router
from institutional_readiness_api import router as readiness_router
from institutional_stream import router as stream_router

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("eventrisk.institutional_public_service")

REQUIRED_TABLES = (
    "er_sources",
    "er_source_contracts",
    "er_contract_semantic_versions",
    "er_market_observations",
    "er_canonical_events",
    "er_event_mappings",
    "er_schema_migrations",
)


def _require_warehouse() -> dict:
    con = connect(read_only=True)
    try:
        missing = []
        for table in REQUIRED_TABLES:
            row = con.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_schema='main' AND table_name=?",
                [table],
            ).fetchone()
            if not row or int(row[0]) != 1:
                missing.append(table)
        if missing:
            raise RuntimeError(f"Institutional warehouse schema incomplete; missing tables: {missing}")
        migrations = int(con.execute("SELECT count(*) FROM er_schema_migrations").fetchone()[0])
        return {"schema_migrations": migrations, "required_tables": len(REQUIRED_TABLES)}
    finally:
        con.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    warehouse = _require_warehouse()
    if os.getenv("ENABLE_BACKGROUND_COLLECTOR", "false").lower() in {"1", "true", "yes"}:
        raise RuntimeError(
            "ENABLE_BACKGROUND_COLLECTOR must be false for institutional_public_service; "
            "source collection is owned by MotherDuck Flights"
        )
    log.info("public institutional API ready warehouse=%s", warehouse)
    yield


app = FastAPI(
    title="EventRisk Institutional Data API",
    version="1.0.0-beta",
    description=(
        "Machine-readable observed and normalized event-data primitives. EventRisk supplies "
        "source observations, curves, canonical identity, provenance and explicitly labeled "
        "relationships; institutions own scenarios, portfolio models and investment decisions."
    ),
    lifespan=lifespan,
)


@app.middleware("http")
async def public_boundary_and_auth(request: Request, call_next):
    if request.url.path == "/internal" or request.url.path.startswith("/internal/"):
        return JSONResponse({"detail": "Not found"}, status_code=404)

    require_auth = os.getenv("EVENTRISK_REQUIRE_AUTH", "false").lower() in {"1", "true", "yes"}
    if request.url.path.startswith("/v1") and require_auth:
        supplied = extract_bearer(
            request.headers.get("authorization"), request.headers.get("x-api-key")
        )
        client = authenticate_key(supplied, "read")
        if client is None:
            return JSONResponse({"detail": "Valid EventRisk API key required"}, status_code=401)
        request.state.eventrisk_client = client

    response = await call_next(request)
    response.headers["X-EventRisk-Schema-Version"] = "v3.0"
    response.headers["X-EventRisk-Data-Layer"] = "observed+deterministic-identity+advisory-enrichment"
    response.headers["X-EventRisk-Decision-Boundary"] = "data-only"
    return response


@app.get("/health")
def health():
    """Cheap operational health check; never materialize analytical history views here."""
    try:
        con = connect(read_only=True)
        try:
            migrations = int(con.execute("SELECT count(*) FROM er_schema_migrations").fetchone()[0])
            active_contracts = int(
                con.execute("SELECT count(*) FROM er_source_contracts WHERE active=TRUE").fetchone()[0]
            )
            # `er_served_canonical_events` intentionally includes latest-observation taxonomy and
            # therefore window-scans observation history. Counting it made /health scale with the
            # entire time series. Count the current serving identity directly instead: active
            # contracts + live mappings + non-retired canonical events. This remains semantically
            # appropriate for an operational cardinality metric and is bounded by current state.
            served_events = int(con.execute("""
                SELECT count(DISTINCT m.canonical_event_id)
                FROM er_event_mappings m
                JOIN er_source_contracts c USING(source_contract_id)
                JOIN er_canonical_events e USING(canonical_event_id)
                WHERE m.valid_to IS NULL
                  AND c.active=TRUE
                  AND e.status NOT IN ('superseded','inactive')
            """).fetchone()[0])
            critical_qa = int(
                con.execute(
                    "SELECT count(*) FROM er_quality_issues WHERE status='open' AND severity='critical'"
                ).fetchone()[0]
            )
            source_health = [
                {
                    "source_id": row[0],
                    "health_status": row[1],
                    "last_success_at": row[2],
                    "last_observation_at": row[3],
                }
                for row in con.execute(
                    "SELECT source_id,health_status,last_success_at,last_observation_at "
                    "FROM er_source_health ORDER BY source_id"
                ).fetchall()
            ]
            return {
                "ok": critical_qa == 0,
                "mode": "stateless-read-api",
                "warehouse": os.getenv("DB", ""),
                "schema_migrations": migrations,
                "active_source_contracts": active_contracts,
                "served_canonical_events": served_events,
                "critical_quality_issues": critical_qa,
                "sources": source_health,
                "background_collector": False,
                "readiness": "/v1/readiness",
            }
        finally:
            con.close()
    except Exception as exc:
        raise HTTPException(503, f"Institutional warehouse unavailable: {exc}")


@app.get("/")
def root():
    return {
        "product": "EventRisk Institutional Data API",
        "version": "1.0.0-beta",
        "docs": "/docs",
        "health": "/health",
        "readiness": "/v1/readiness",
        "stream": "/v1/stream",
        "change_feed": "/v1/change-feed",
        "bulk_releases": "/v1/releases",
        "boundary": (
            "EventRisk supplies observed and normalized event data; institutions own downstream "
            "scenarios, security mappings, portfolio models and investment decisions."
        ),
    }


app.include_router(data_router)
app.include_router(curve_router)
app.include_router(changefeed_router)
app.include_router(public_queries_router)
app.include_router(history_router)
app.include_router(readiness_router)
app.include_router(stream_router)
