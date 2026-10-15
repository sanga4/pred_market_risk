"""Institutional ingestion v8: bounded CDC with direct registered-temp persistence.

v8 preserves v6/v7 source, raw, semantic, observation and presence semantics while removing
the memory-amplifying Arrow round trip that remained in v7. Each bounded chunk is staged to
DuckDB TEMP relations exactly once, CDC is applied in place, and the filtered TEMP relations
are persisted directly with the v4 SQL body. Python snapshot/Arrow objects and Arrow's native
memory pool are released after every chunk. Source writes remain atomic.
"""

from __future__ import annotations

import gc
import json
import logging
import os
import resource
import time
import uuid
from dataclasses import dataclass
from typing import Optional

import pyarrow as pa

import institutional_ingest as base
import institutional_ingest_v3 as v3
import institutional_ingest_v4 as v4
from db import connect
from institutional_http import get_json as thread_safe_get_json
from institutional_persist_registered import persist_registered_batch
from institutional_presence_v2 import reconcile_presence
from institutional_universe_adapters import ADAPTERS, collection_policy

log = logging.getLogger("eventrisk.institutional_ingest_v8")
METHOD_VERSION = "change_data_capture_v8_direct_temp"
CHUNK_ROWS = max(1000, int(os.getenv("EVENTRISK_INGEST_CHUNK_ROWS", "25000")))

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
        "memory_strategy": "sequential_source_direct_temp_chunked_cdc",
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


def _release_native_memory() -> None:
    gc.collect()
    try:
        pa.default_memory_pool().release_unused()
    except Exception:
        pass


def _rss_mb() -> float:
    # Linux ru_maxrss is KiB. Flight runners are Linux containers.
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0, 2)


def _drop_batch_temps(con) -> None:
    for name in [
        "_batch_raw", "_batch_contracts", "_batch_observations",
        "_contract_changes", "_semantic_changes", "_latest_raw_for_source", "_raw_resolution",
        "_batch_raw_next", "_batch_observations_next",
    ]:
        con.execute(f"DROP TABLE IF EXISTS {name}")


def _apply_cdc_in_place(con, source_id: str) -> dict:
    raw_received = int(con.execute("SELECT count(*) FROM _batch_raw").fetchone()[0])
    obs_received = int(con.execute("SELECT count(*) FROM _batch_observations").fetchone()[0])

    con.execute("""
        CREATE OR REPLACE TEMP TABLE _latest_raw_for_source AS
        SELECT raw_snapshot_id,source_id,source_object_type,source_object_id,content_hash
        FROM er_raw_snapshots
        WHERE source_id=?
        QUALIFY row_number() OVER (
          PARTITION BY source_id,source_object_type,source_object_id
          ORDER BY received_at DESC,raw_snapshot_id DESC
        )=1
    """, [source_id])
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _raw_resolution AS
        SELECT
          b.raw_snapshot_id AS staged_raw_snapshot_id,
          CASE
            WHEN l.raw_snapshot_id IS NOT NULL
             AND l.content_hash IS NOT DISTINCT FROM b.content_hash
            THEN l.raw_snapshot_id
            ELSE b.raw_snapshot_id
          END AS effective_raw_snapshot_id,
          NOT (
            l.raw_snapshot_id IS NOT NULL
            AND l.content_hash IS NOT DISTINCT FROM b.content_hash
          ) AS should_insert
        FROM _batch_raw b
        LEFT JOIN _latest_raw_for_source l
          ON l.source_id=b.source_id
         AND l.source_object_type=b.source_object_type
         AND l.source_object_id=b.source_object_id
    """)
    con.execute("""
        UPDATE _batch_contracts c SET event_raw_id=r.effective_raw_snapshot_id
        FROM _raw_resolution r WHERE c.event_raw_id=r.staged_raw_snapshot_id
    """)
    con.execute("""
        UPDATE _batch_contracts c SET market_raw_id=r.effective_raw_snapshot_id
        FROM _raw_resolution r WHERE c.market_raw_id=r.staged_raw_snapshot_id
    """)
    con.execute("""
        UPDATE _batch_observations o SET raw_snapshot_id=r.effective_raw_snapshot_id
        FROM _raw_resolution r WHERE o.raw_snapshot_id=r.staged_raw_snapshot_id
    """)

    con.execute("""
        CREATE OR REPLACE TEMP TABLE _batch_raw_next AS
        SELECT b.*
        FROM _batch_raw b
        JOIN _raw_resolution r ON r.staged_raw_snapshot_id=b.raw_snapshot_id
        WHERE r.should_insert
    """)
    raw_written = int(con.execute("SELECT count(*) FROM _batch_raw_next").fetchone()[0])
    con.execute("DROP TABLE _batch_raw")
    con.execute("ALTER TABLE _batch_raw_next RENAME TO _batch_raw")

    con.execute("""
        CREATE OR REPLACE TEMP TABLE _batch_observations_next AS
        SELECT b.*
        FROM _batch_observations b
        LEFT JOIN er_latest_market_observations l
          ON l.source_contract_id=b.source_contract_id
        WHERE l.source_contract_id IS NULL
           OR l.yes_bid IS DISTINCT FROM b.yes_bid
           OR l.yes_ask IS DISTINCT FROM b.yes_ask
           OR l.no_bid IS DISTINCT FROM b.no_bid
           OR l.no_ask IS DISTINCT FROM b.no_ask
           OR l.midpoint IS DISTINCT FROM b.midpoint
           OR l.last_trade IS DISTINCT FROM b.last_trade
           OR l.displayed_probability IS DISTINCT FROM b.displayed_probability
           OR l.volume IS DISTINCT FROM b.volume
           OR l.volume_24h IS DISTINCT FROM b.volume_24h
           OR l.open_interest IS DISTINCT FROM b.open_interest
           OR l.liquidity IS DISTINCT FROM b.liquidity
           OR l.orderbook_depth IS DISTINCT FROM b.orderbook_depth
           OR l.spread IS DISTINCT FROM b.spread
           OR l.market_status IS DISTINCT FROM b.market_status
    """)
    obs_written = int(con.execute("SELECT count(*) FROM _batch_observations_next").fetchone()[0])
    con.execute("DROP TABLE _batch_observations")
    con.execute("ALTER TABLE _batch_observations_next RENAME TO _batch_observations")
    con.execute("DROP TABLE IF EXISTS _latest_raw_for_source")
    con.execute("DROP TABLE IF EXISTS _raw_resolution")

    return {
        "raw_rows_received": raw_received,
        "raw_rows_written": raw_written,
        "raw_rows_reused": raw_received - raw_written,
        "observation_rows_received": obs_received,
        "observation_rows_written": obs_written,
        "flat_observations_suppressed": obs_received - obs_written,
    }


def _persist_source(prepared: PreparedRun, fetched: FetchResult) -> dict:
    source_started = time.monotonic()
    snapshots = fetched.snapshots
    total_contracts = len(snapshots)
    totals = {
        "raw_rows_received": 0,
        "raw_rows_written": 0,
        "raw_rows_reused": 0,
        "observation_rows_received": 0,
        "observation_rows_written": 0,
        "flat_observations_suppressed": 0,
    }
    total_contract_versions = 0
    total_semantic_versions = 0
    seen_ids: set[str] = set()
    stage_seconds = 0.0
    register_seconds = 0.0
    cdc_seconds = 0.0
    db_persist_seconds = 0.0
    presence_seconds = 0.0
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

                for idx in range(start, end):
                    snapshots[idx] = None
                del chunk

                t0 = time.monotonic()
                v4._register_batch(con, raw, contracts, observations)
                register_seconds += time.monotonic() - t0
                del raw, contracts, observations
                _release_native_memory()

                t0 = time.monotonic()
                cdc_stats = _apply_cdc_in_place(con, prepared.source_id)
                cdc_seconds += time.monotonic() - t0
                for key in totals:
                    totals[key] += int(cdc_stats[key])

                t0 = time.monotonic()
                persisted = persist_registered_batch(con, method_version=METHOD_VERSION)
                db_persist_seconds += time.monotonic() - t0
                total_contract_versions += persisted["contract_versions_created"]
                total_semantic_versions += persisted["semantic_versions_created"]
                seen_ids.update(persisted["seen_contract_ids"])
                del persisted

                _drop_batch_temps(con)
                _release_native_memory()
                log.info(
                    "ingest chunk completed source=%s chunk=%s rows=%s rss_max_mb=%s arrow_mb=%.2f",
                    prepared.source_id,
                    chunks,
                    end - start,
                    _rss_mb(),
                    pa.total_allocated_bytes() / (1024.0 * 1024.0),
                )

            universe_complete = prepared.policy["scope"] == "full"
            t0 = time.monotonic()
            presence = reconcile_presence(
                con,
                prepared.source_id,
                prepared.run_id,
                seen_ids,
                universe_complete=universe_complete,
            )
            presence_seconds += time.monotonic() - t0
            source_seconds = time.monotonic() - source_started
            details = {
                "writer": METHOD_VERSION,
                "collection_policy": prepared.policy,
                "contract_rows": total_contracts,
                **totals,
                "contract_versions_created": total_contract_versions,
                "semantic_versions_created": total_semantic_versions,
                "presence": presence,
                "memory_strategy": "sequential_source_direct_temp_chunked_cdc",
                "chunk_rows": CHUNK_ROWS,
                "chunks": chunks,
                "rss_max_mb": _rss_mb(),
                "timing": {
                    "fetch_seconds": round(fetched.fetch_seconds, 6),
                    "stage_seconds": round(stage_seconds, 6),
                    "register_seconds": round(register_seconds, 6),
                    "cdc_seconds": round(cdc_seconds, 6),
                    "db_persist_seconds": round(db_persist_seconds, 6),
                    "presence_seconds": round(presence_seconds, 6),
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
            v3.update_health(con, prepared.source_id, prepared.run_id, "success", total_contracts, None)
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
    finally:
        con.close()
        fetched.snapshots.clear()
        _release_native_memory()

    return {
        "source_id": prepared.source_id,
        "run_id": prepared.run_id,
        "status": "success",
        "records_written": total_contracts,
        **totals,
        "contract_versions_created": total_contract_versions,
        "semantic_versions_created": total_semantic_versions,
        "collection_policy": prepared.policy,
        "presence": presence,
        "timing": details["timing"],
        "memory_strategy": details["memory_strategy"],
        "rss_max_mb": details["rss_max_mb"],
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
    results: list[dict] = []
    for source_id in list(source_ids or ADAPTERS):
        try:
            results.append(ingest_source(source_id))
        except Exception as exc:
            log.exception("Direct-temp CDC ingestion failed for %s", source_id)
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
