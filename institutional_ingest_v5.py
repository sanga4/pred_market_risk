"""Institutional ingestion v5: parallel source fetch, serialized transactional persistence.

Kalshi and Polymarket are independent external sources. Fetching them sequentially makes a
full-universe cycle unnecessarily equal to the sum of venue latencies. v5 fetches source
universes concurrently using thread-local HTTP sessions, then persists each source batch
serially with the proven v4 set-based writer. Database writes therefore remain deterministic
and non-overlapping while network wall-clock approaches the slower venue rather than the sum.
"""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Optional

import institutional_ingest as base
import institutional_ingest_v3 as v3
import institutional_ingest_v4 as v4
from db import connect
from institutional_http import get_json as thread_safe_get_json
from institutional_universe_adapters import ADAPTERS, collection_policy

log = logging.getLogger("eventrisk.institutional_ingest_v5")
METHOD_VERSION = "parallel_fetch_serial_write_v5"
MAX_FETCH_WORKERS = max(1, int(os.getenv("EVENTRISK_SOURCE_FETCH_WORKERS", "4")))

# Universe adapters resolve `base.get_json` dynamically, so install the thread-local client
# for this ingestion generation without changing the older reference implementations.
base.get_json = thread_safe_get_json


@dataclass
class PreparedRun:
    source_id: str
    run_id: str
    policy: dict


@dataclass
class FetchResult:
    source_id: str
    snapshots: list
    fetch_seconds: float


def _prepare_run(source_id: str) -> PreparedRun:
    run_id = f"ing_{uuid.uuid4().hex}"
    policy = collection_policy()
    con = connect()
    try:
        v3.ensure_sources(con)
        con.execute("""
            INSERT INTO er_ingestion_runs(
              run_id,source_id,started_at,status,collection_scope,universe_complete,collection_policy)
            VALUES (?,?,now(),'running',?,FALSE,?)
        """, [run_id, source_id, policy["scope"], json.dumps(policy, sort_keys=True)])
    finally:
        con.close()
    return PreparedRun(source_id, run_id, policy)


def _fetch_source(source_id: str) -> FetchResult:
    if source_id not in ADAPTERS:
        raise ValueError(f"Unknown source {source_id}")
    started = time.monotonic()
    snapshots = list(ADAPTERS[source_id]().iter_snapshots())
    return FetchResult(source_id, snapshots, time.monotonic() - started)


def _fail_run(prepared: PreparedRun, exc: Exception, *, phase: str, fetch_seconds: float | None = None) -> dict:
    details = {
        "writer": METHOD_VERSION,
        "phase": phase,
        "error": str(exc)[:1800],
        "collection_policy": prepared.policy,
        "fetch_seconds": fetch_seconds,
    }
    con = connect()
    try:
        con.execute("""
            UPDATE er_ingestion_runs
            SET finished_at=now(),status='failed',error_count=1,details=?,universe_complete=FALSE
            WHERE run_id=?
        """, [json.dumps(details, sort_keys=True, default=str), prepared.run_id])
        v3.update_health(con, prepared.source_id, prepared.run_id, "failed", 0, str(exc)[:2000])
    finally:
        con.close()
    return {
        "source_id": prepared.source_id,
        "run_id": prepared.run_id,
        "status": "failed",
        "phase": phase,
        "error": str(exc),
        "writer": METHOD_VERSION,
    }


def _persist_source(prepared: PreparedRun, fetched: FetchResult) -> dict:
    stage_started = time.monotonic()
    raw, contracts, observations = v4._stage_snapshots(
        prepared.source_id, prepared.run_id, fetched.snapshots
    )
    stage_seconds = time.monotonic() - stage_started
    persist_started = time.monotonic()
    con = connect()
    try:
        con.execute("BEGIN TRANSACTION")
        try:
            persisted = v4._persist_batch(
                con, prepared.source_id, prepared.run_id, raw, contracts, observations
            )
            universe_complete = prepared.policy["scope"] == "full"
            presence = v3.reconcile_presence(
                con,
                prepared.source_id,
                prepared.run_id,
                persisted["seen_contract_ids"],
                universe_complete=universe_complete,
            )
            persist_seconds = time.monotonic() - persist_started
            details = {
                "writer": METHOD_VERSION,
                "collection_policy": prepared.policy,
                "raw_rows": raw.num_rows,
                "contract_rows": contracts.num_rows,
                "contract_versions_created": persisted["contract_versions_created"],
                "semantic_versions_created": persisted["semantic_versions_created"],
                "presence": presence,
                "timing": {
                    "fetch_seconds": round(fetched.fetch_seconds, 6),
                    "stage_seconds": round(stage_seconds, 6),
                    "persist_seconds": round(persist_seconds, 6),
                },
            }
            con.execute("""
                UPDATE er_ingestion_runs
                SET finished_at=now(),status='success',records_received=?,records_written=?,error_count=0,
                    details=?,universe_complete=?,collection_scope=?,collection_policy=?
                WHERE run_id=?
            """, [
                contracts.num_rows,
                contracts.num_rows,
                json.dumps(details, sort_keys=True, default=str),
                universe_complete,
                prepared.policy["scope"],
                json.dumps(prepared.policy, sort_keys=True),
                prepared.run_id,
            ])
            v3.update_health(
                con, prepared.source_id, prepared.run_id, "success", contracts.num_rows, None
            )
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
    finally:
        con.close()
    return {
        "source_id": prepared.source_id,
        "run_id": prepared.run_id,
        "status": "success",
        "records_written": contracts.num_rows,
        "raw_rows_written": raw.num_rows,
        "contract_versions_created": persisted["contract_versions_created"],
        "semantic_versions_created": persisted["semantic_versions_created"],
        "collection_policy": prepared.policy,
        "presence": presence,
        "timing": details["timing"],
        "writer": METHOD_VERSION,
    }


def fetch_sources_parallel(source_ids: list[str]) -> tuple[dict[str, FetchResult], dict[str, Exception]]:
    """Fetch independent source universes concurrently; perform no database writes here."""
    fetched: dict[str, FetchResult] = {}
    errors: dict[str, Exception] = {}
    if not source_ids:
        return fetched, errors
    workers = min(MAX_FETCH_WORKERS, len(source_ids))
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="eventrisk-source") as pool:
        futures = {pool.submit(_fetch_source, source_id): source_id for source_id in source_ids}
        for future in as_completed(futures):
            source_id = futures[future]
            try:
                fetched[source_id] = future.result()
            except Exception as exc:
                log.exception("Institutional source fetch failed for %s", source_id)
                errors[source_id] = exc
    return fetched, errors


def run_once(source_ids: Optional[list[str]] = None) -> list[dict]:
    source_ids = list(source_ids or ADAPTERS)
    prepared: dict[str, PreparedRun] = {}
    results: dict[str, dict] = {}
    for source_id in source_ids:
        try:
            prepared[source_id] = _prepare_run(source_id)
        except Exception as exc:
            log.exception("Failed to prepare ingestion run for %s", source_id)
            results[source_id] = {
                "source_id": source_id,
                "status": "failed",
                "phase": "prepare",
                "error": str(exc),
                "writer": METHOD_VERSION,
            }

    fetchable = [source_id for source_id in source_ids if source_id in prepared]
    fetched, fetch_errors = fetch_sources_parallel(fetchable)
    for source_id, exc in fetch_errors.items():
        results[source_id] = _fail_run(prepared[source_id], exc, phase="fetch")

    # Persist in caller/source order. Writes never overlap even though source HTTP fetches did.
    for source_id in source_ids:
        if source_id in results or source_id not in fetched:
            continue
        try:
            results[source_id] = _persist_source(prepared[source_id], fetched[source_id])
        except Exception as exc:
            log.exception("Set-based persistence failed for %s", source_id)
            results[source_id] = _fail_run(
                prepared[source_id], exc, phase="persist", fetch_seconds=fetched[source_id].fetch_seconds
            )
    return [results[source_id] for source_id in source_ids if source_id in results]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(json.dumps(run_once(), indent=2, default=str))
