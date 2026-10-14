"""Standalone EventRisk institutional data service.

This is intentionally separate from the retail FastAPI application. In staging it can
run the collector in-process against a local DuckDB volume. In production the same
collector code can be moved to a dedicated scheduled worker against MotherDuck.
"""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from db import connect
from institutional_api import router as data_router
from institutional_archive import archive_pending
from institutional_graph import run_graph_pass
from institutional_ingest_v2 import run_once
from institutional_mapping_v2 import run_mapping_pass
from institutional_ops_api import router as ops_router
from institutional_qa import run_qa

log = logging.getLogger("eventrisk.institutional_service")

SCHEMA_FILES = [
    "institutional_schema.sql",
    "institutional_schema_002.sql",
    "institutional_schema_003.sql",
]


def ensure_schema():
    root = Path(__file__).parent
    con = connect()
    try:
        for name in SCHEMA_FILES:
            con.execute((root / name).read_text())
    finally:
        con.close()


def pipeline_cycle():
    ingest = run_once()
    archive = archive_pending(limit=int(os.getenv("RAW_ARCHIVE_BATCH_SIZE", "5000")))
    mapping = run_mapping_pass()
    graph = run_graph_pass()
    qa = run_qa(stale_minutes=max(20, int(os.getenv("COLLECT_INTERVAL_SECONDS", "300")) // 60 * 4))
    return {"ingest": ingest, "archive": archive, "mapping": mapping, "graph": graph, "qa": qa}


async def collector_loop(stop: asyncio.Event):
    interval = max(300, int(os.getenv("COLLECT_INTERVAL_SECONDS", "300")))
    initial_delay = max(1, int(os.getenv("COLLECT_INITIAL_DELAY_SECONDS", "3")))
    await asyncio.sleep(initial_delay)
    while not stop.is_set():
        try:
            result = await asyncio.to_thread(pipeline_cycle)
            log.info("institutional collection cycle completed: %s", result)
        except Exception:
            log.exception("institutional collection cycle failed")
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    ensure_schema()
    stop = asyncio.Event()
    task = None
    if os.getenv("ENABLE_BACKGROUND_COLLECTOR", "false").lower() in {"1", "true", "yes"}:
        task = asyncio.create_task(collector_loop(stop))
    yield
    stop.set()
    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


app = FastAPI(
    title="EventRisk Institutional Data API",
    version="1.0.0-alpha",
    description=(
        "Machine-readable event data primitives: source contracts, immutable observations, "
        "canonical events, curves, entity relationships, provenance and quality state. "
        "No portfolio recommendations or prescribed scenario impacts."
    ),
    lifespan=lifespan,
)


@app.middleware("http")
async def optional_api_key(request: Request, call_next):
    expected = os.getenv("EVENTRISK_API_KEY")
    if expected and request.url.path.startswith("/v1"):
        auth = request.headers.get("authorization", "")
        supplied = auth.split(" ", 1)[1] if auth.lower().startswith("bearer ") else request.headers.get("x-api-key")
        if supplied != expected:
            return JSONResponse({"detail": "Invalid API key"}, status_code=401)
    response = await call_next(request)
    response.headers["X-EventRisk-Schema-Version"] = "v1"
    response.headers["X-EventRisk-Data-Layer"] = "observed+normalized"
    return response


@app.get("/health")
def health():
    try:
        con = connect(read_only=True)
        try:
            counts = {}
            for table in [
                "er_raw_snapshots",
                "er_raw_archive_objects",
                "er_source_contracts",
                "er_market_observations",
                "er_canonical_events",
            ]:
                counts[table] = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            sources = con.execute(
                "SELECT source_id,health_status,last_success_at,last_observation_at FROM er_source_health ORDER BY source_id"
            ).fetchall()
            critical = con.execute(
                "SELECT count(*) FROM er_quality_issues WHERE status='open' AND severity='critical'"
            ).fetchone()[0]
            unarchived = con.execute("SELECT count(*) FROM er_unarchived_raw_snapshots").fetchone()[0]
            return {
                "ok": critical == 0,
                "counts": counts,
                "sources": sources,
                "critical_quality_issues": critical,
                "unarchived_raw_snapshots": unarchived,
            }
        finally:
            con.close()
    except Exception as exc:
        raise HTTPException(503, f"Institutional service unavailable: {exc}")


@app.get("/")
def root():
    return {
        "product": "EventRisk Institutional Data API",
        "version": "1.0.0-alpha",
        "docs": "/docs",
        "health": "/health",
        "boundary": "EventRisk supplies observed and normalized event data; institutions own downstream scenarios and investment decisions.",
    }


app.include_router(data_router)
app.include_router(ops_router)
