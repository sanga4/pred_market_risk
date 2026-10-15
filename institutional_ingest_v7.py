"""Institutional ingestion v7: bounded-memory full-universe CDC.

v7 preserves v6 data semantics while changing execution to stay within a bounded Flight
memory envelope. Each source is fetched and persisted before the next source is retained,
and persistence is performed in configurable source chunks. Every chunk uses the proven v6
raw/state CDC filter and v4 transactional writer, while processed snapshot objects and Arrow
relations are released eagerly between chunks.

The complete source universe is still reconciled for presence only after every chunk succeeds;
raw history and market observations remain append-only on change exactly as in v6.
"""

from __future__ import annotations

import gc
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass
from typing import Optional

import institutional_ingest as base
import institutional_ingest_v3 as v3
import institutional_ingest_v4 as v4
import institutional_ingest_v6 as v6
from db import connect
from institutional_http import get_json as thread_safe_get_json
from institutional_universe_adapters import ADAPTERS, collection_policy

log = logging.getLogger("eventrisk.institutional_ingest_v7")
METHOD_VERSION = "change_data_capture_v7_bounded"
CHUNK_ROWS = max(1000, int(os.getenv("EVENTRISK_INGEST_CHUNK_ROWS", "25000")))

# Keep the same thread-local HTTP client used by v5/v6. Sources are intentionally processed
# one at a time in v7 so a second full venue universe is not retained during persistence.
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
        "memory_strategy": "sequential_source_chunked_cdc",
        "chunk_rows": CHUNK_ROWS,
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


def _drop_cdc_temps(con) -> None:
    # v6 materializes filtered relations before returning Arrow tables. Once those Arrow
    # tables exist, none of the filter-stage temp relations are needed for the v4 writer.
    for name in [
        "_batch_raw_cdc", "_batch_observations_cdc", "_raw_resolution",
        "_latest_raw_for_source", "_batch_raw", "_batch_contracts", "_batch_observations",
    ]:
        con.execute(f"DROP TABLE IF EXISTS {name}")


def _persist_source(prepared: PreparedRun, fetched: FetchResult) -> dict:
    source_started = time.monotonic()
    snapshots = fetched.snapshots
    total_contracts = len(snapshots)
    total_raw_received = 0
    total_raw_written = 0
    total_raw_reused = 0
    total_obs_received = 0
    total_obs_written = 0
    total_flat_suppressed = 0
    total_contract_versions = 0
    total_semantic_versions = 0
    seen_ids: set[str] = set()
    stage_seconds = 0.0
    cdc_seconds = 0.0
    db_persist_seconds = 0.0
    chunks = 0

    con = connect()
    try:
        con.execute("SET TimeZone='UTC'")
        con.execute("BEGIN TRANSACTION")
        try:
            for start in range(0, total_contracts, CHUNK_ROWS):
                end = min(total_contracts, start + CHUNK_ROWS)
                chunk = snapshots[start:end]
                chunks += 1

                t0 = time.monotonic()
                raw, contracts, observations = v4._stage_snapshots(
                    prepared.source_id, prepared.run_id, chunk
                )
                stage_seconds += time.monotonic() - t0

                # The staged Arrow tables own the data needed for this chunk now. Release
                # processed source objects immediately instead of retaining a second copy.
                for idx in range(start, end):
                    snapshots[idx] = None
                del chunk
                gc.collect()

                t0 = time.monotonic()
                raw_cdc, contracts_cdc, observations_cdc, cdc_stats = v6._cdc_filter(
                    con, prepared.source_id, raw, contracts, observations
                )
                cdc_seconds += time.monotonic() - t0

                total_raw_received += cdc_stats["raw_rows_received"]
                total_raw_written += cdc_stats["raw_rows_written"]
                total_raw_reused += cdc_stats["raw_rows_reused"]
                total_obs_received += cdc_stats["observation_rows_received"]
                total_obs_written += cdc_stats["observation_rows_written"]
                total_flat_suppressed += cdc_stats["flat_observations_suppressed"]

                # Avoid v6's production OOM pattern: do not keep the original Arrow batch,
                # CDC temp relations and the second v4 staging copy alive simultaneously.
                _drop_cdc_temps(con)
                del raw, contracts, observations
                gc.collect()

                t0 = time.monotonic()
                persisted = v4._persist_batch(
                    con,
                    prepared.source_id,
                    prepared.run_id,
                    raw_cdc,
                    contracts_cdc,
                    observations_cdc,
                )
                db_persist_seconds += time.monotonic() - t0
                total_contract_versions += persisted["contract_versions_created"]
                total_semantic_versions += persisted["semantic_versions_created"]
                seen_ids.update(persisted["seen_contract_ids"])

                del raw_cdc, contracts_cdc, observations_cdc, persisted
                gc.collect()

            universe_complete = prepared.policy["scope"] == "full"
            presence = v3.reconcile_presence(
                con,
                prepared.source_id,
                prepared.run_id,
                seen_ids,
                universe_complete=universe_complete,
            )
            source_seconds = time.monotonic() - source_started
            details = {
                "writer": METHOD_VERSION,
                "collection_policy": prepared.policy,
                "contract_rows": total_contracts,
                "raw_rows_received": total_raw_received,
                "raw_rows_written": total_raw_written,
                "raw_rows_reused": total_raw_reused,
                "observation_rows_received": total_obs_received,
                "observation_rows_written": total_obs_written,
                "flat_observations_suppressed": total_flat_suppressed,
                "contract_versions_created": total_contract_versions,
                "semantic_versions_created": total_semantic_versions,
                "presence": presence,
                "memory_strategy": "sequential_source_chunked_cdc",
                "chunk_rows": CHUNK_ROWS,
                "chunks": chunks,
                "timing": {
                    "fetch_seconds": round(fetched.fetch_seconds, 6),
                    "stage_seconds": round(stage_seconds, 6),
                    "cdc_seconds": round(cdc_seconds, 6),
                    "db_persist_seconds": round(db_persist_seconds, 6),
                    "source_seconds": round(source_seconds, 6),
                },
            }
            con.execute("""
                UPDATE er_ingestion_runs
                SET finished_at=now(),status='success',records_received=?,records_written=?,error_count=0,
                    details=?,universe_complete=?,collection_scope=?,collection_policy=?
                WHERE run_id=?
            """, [
                total_contracts,
                total_contracts,
                json.dumps(details, sort_keys=True, default=str),
                universe_complete,
                prepared.policy["scope"],
                json.dumps(prepared.policy, sort_keys=True),
                prepared.run_id,
            ])
            v3.update_health(
                con, prepared.source_id, prepared.run_id, "success", total_contracts, None
            )
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
    finally:
        con.close()
        fetched.snapshots.clear()
        gc.collect()

    return {
        "source_id": prepared.source_id,
        "run_id": prepared.run_id,
        "status": "success",
        "records_written": total_contracts,
        "raw_rows_received": total_raw_received,
        "raw_rows_written": total_raw_written,
        "raw_rows_reused": total_raw_reused,
        "observation_rows_received": total_obs_received,
        "observation_rows_written": total_obs_written,
        "flat_observations_suppressed": total_flat_suppressed,
        "contract_versions_created": total_contract_versions,
        "semantic_versions_created": total_semantic_versions,
        "collection_policy": prepared.policy,
        "presence": presence,
        "timing": details["timing"],
        "memory_strategy": details["memory_strategy"],
        "chunk_rows": CHUNK_ROWS,
        "chunks": chunks,
        "writer": METHOD_VERSION,
    }


def ingest_source(source_id: str) -> dict:
    prepared = _prepare_run(source_id)
    try:
        fetched = _fetch_source(source_id)
    except Exception as exc:
        _fail_run(prepared, exc, phase="fetch")
        raise
    try:
        return _persist_source(prepared, fetched)
    except Exception as exc:
        _fail_run(prepared, exc, phase="persist", fetch_seconds=fetched.fetch_seconds)
        raise


def run_once(source_ids: Optional[list[str]] = None) -> list[dict]:
    """Process complete sources sequentially to bound retained source-universe memory."""
    results: list[dict] = []
    for source_id in list(source_ids or ADAPTERS):
        try:
            results.append(ingest_source(source_id))
        except Exception as exc:
            log.exception("Bounded CDC ingestion failed for %s", source_id)
            results.append({
                "source_id": source_id,
                "status": "failed",
                "phase": "source",
                "error": str(exc),
                "writer": METHOD_VERSION,
            })
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(json.dumps(run_once(), indent=2, default=str))
