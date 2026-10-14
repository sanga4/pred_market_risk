"""Standalone EventRisk institutional data service.

The API is isolated from the retail app. Staging may run collection in-process; production
can move the same cycle to a dedicated worker without changing data semantics.
"""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from db import connect
from institutional_admin_api import router as admin_router
from institutional_api import router as data_router
from institutional_archive_v2 import archive_pending
from institutional_auth import authenticate_key, extract_bearer
from institutional_curve_api import router as curve_router
from institutional_graph_v2 import run_graph_pass
from institutional_history_api import router as history_router
from institutional_ingest_v3 import run_once
from institutional_mapping_drift import persist_mapping_drift
from institutional_mapping_v3 import run_mapping_pass
from institutional_migrations import ensure_schema
from institutional_ontology import run_ontology_pass
from institutional_ops_api import router as ops_router
from institutional_qa_v2 import run_qa
from institutional_readiness_api import router as readiness_router
from institutional_reference_versions import sync_reference_versions
from institutional_stream import router as stream_router

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("eventrisk.institutional_service")


def pipeline_cycle():
    ingest = run_once()
    archive = archive_pending(
        max_rows=int(os.getenv("RAW_ARCHIVE_MAX_ROWS", "20000")),
        bundle_rows=int(os.getenv("RAW_ARCHIVE_BATCH_SIZE", "5000")),
    )
    mapping = run_mapping_pass()
    ontology = run_ontology_pass()
    graph = run_graph_pass()
    reference_versions = sync_reference_versions()
    mapping_drift = persist_mapping_drift()
    qa = run_qa(stale_minutes=max(20, int(os.getenv("COLLECT_INTERVAL_SECONDS", "300")) // 60 * 4))
    return {
        "ingest": ingest,
        "archive": archive,
        "mapping": mapping,
        "ontology": ontology,
        "graph": graph,
        "reference_versions": reference_versions,
        "mapping_drift": mapping_drift,
        "qa": qa,
    }


async def collector_loop(stop: asyncio.Event):
    interval = max(300, int(os.getenv("COLLECT_INTERVAL_SECONDS", "300")))
    initial_delay = max(1, int(os.getenv("COLLECT_INITIAL_DELAY_SECONDS", "3")))
    log.info("institutional collector enabled interval_seconds=%s initial_delay_seconds=%s", interval, initial_delay)
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
    migration_state = ensure_schema()
    stop = asyncio.Event()
    task = None
    collector_enabled = os.getenv("ENABLE_BACKGROUND_COLLECTOR", "false").lower() in {"1", "true", "yes"}
    log.info("institutional service startup schema_ready=true migrations=%s collector_enabled=%s", migration_state, collector_enabled)
    if collector_enabled:
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
async def institutional_auth(request: Request, call_next):
    require_auth = os.getenv("EVENTRISK_REQUIRE_AUTH", "false").lower() in {"1", "true", "yes"}
    protected = request.url.path.startswith("/v1")
    if protected and require_auth:
        supplied = extract_bearer(request.headers.get("authorization"), request.headers.get("x-api-key"))
        client = authenticate_key(supplied, "read")
        if client is None:
            return JSONResponse({"detail": "Valid EventRisk API key required"}, status_code=401)
        request.state.eventrisk_client = client
    response = await call_next(request)
    response.headers["X-EventRisk-Schema-Version"] = "v2.0"
    response.headers["X-EventRisk-Data-Layer"] = "observed+normalized"
    return response


@app.get("/health")
def health():
    try:
        con = connect(read_only=True)
        try:
            counts = {}
            for table in [
                "er_raw_snapshots", "er_raw_archive_objects", "er_source_contracts",
                "er_contract_semantic_versions", "er_market_observations", "er_canonical_events",
            ]:
                counts[table] = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            counts["served_canonical_events"] = con.execute("SELECT count(*) FROM er_served_canonical_events").fetchone()[0]
            counts["raw_database_bodies"] = con.execute("SELECT count(*) FROM er_raw_snapshots WHERE payload_json IS NOT NULL").fetchone()[0]
            counts["verified_archive_rows"] = con.execute("SELECT count(*) FROM er_raw_archive_objects WHERE status='archived'").fetchone()[0]
            counts["schema_migrations"] = con.execute("SELECT count(*) FROM er_schema_migrations").fetchone()[0]
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
                "auth_required": os.getenv("EVENTRISK_REQUIRE_AUTH", "false").lower() in {"1", "true", "yes"},
                "collection_interval_seconds": max(300, int(os.getenv("COLLECT_INTERVAL_SECONDS", "300"))),
                "readiness": "/v1/readiness",
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
        "readiness": "/v1/readiness",
        "stream": "/v1/stream",
        "bulk_releases": "/v1/releases",
        "boundary": "EventRisk supplies observed and normalized event data; institutions own downstream scenarios and investment decisions.",
    }


app.include_router(data_router)
app.include_router(curve_router)
app.include_router(ops_router)
app.include_router(history_router)
app.include_router(readiness_router)
app.include_router(admin_router)
app.include_router(stream_router)
