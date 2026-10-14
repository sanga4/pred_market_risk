"""Institutional ingestion v6: parallel full-universe polling with change-data capture.

Every source run still fetches the complete supported open universe and reconciles presence.
History, however, is append-only on *change* rather than on every poll:
- exact duplicate raw payloads for the same source object reuse the prior immutable evidence row;
- market observations append only when an economically served state field changes;
- contract/semantic versioning remains delegated to the proven v4 set-based writer;
- source fetch remains concurrent while MotherDuck writes remain serialized.

This preserves point-in-time state and provenance while avoiding redundant raw bodies and
flat curve points at institutional universe scale.
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

import pyarrow as pa

import institutional_ingest as base
import institutional_ingest_v3 as v3
import institutional_ingest_v4 as v4
from db import connect
from institutional_http import get_json as thread_safe_get_json
from institutional_universe_adapters import ADAPTERS, collection_policy

log = logging.getLogger("eventrisk.institutional_ingest_v6")
METHOD_VERSION = "change_data_capture_v6"
MAX_FETCH_WORKERS = max(1, int(os.getenv("EVENTRISK_SOURCE_FETCH_WORKERS", "4")))

# Universe adapters resolve base.get_json dynamically. Use one requests Session per thread.
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


def _cdc_filter(con, source_id: str, raw: pa.Table, contracts: pa.Table, observations: pa.Table):
    """Resolve duplicate raw evidence and remove flat market-state observations.

    The full contract table is intentionally retained so presence and last_seen_at reflect the
    complete universe poll. Only history tables are change-filtered.
    """
    v4._register_batch(con, raw, contracts, observations)
    con.execute("SET TimeZone='UTC'")

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

    # Repoint current-poll contract evidence and observations to the prior immutable raw row
    # when the exact source payload bytes are unchanged.
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
        CREATE OR REPLACE TEMP TABLE _batch_raw_cdc AS
        SELECT b.*
        FROM _batch_raw b
        JOIN _raw_resolution r ON r.staged_raw_snapshot_id=b.raw_snapshot_id
        WHERE r.should_insert
    """)

    con.execute("""
        CREATE OR REPLACE TEMP TABLE _batch_observations_cdc AS
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

    raw_out = con.execute("SELECT * FROM _batch_raw_cdc").fetch_arrow_table()
    contracts_out = con.execute("SELECT * FROM _batch_contracts").fetch_arrow_table()
    observations_out = con.execute("SELECT * FROM _batch_observations_cdc").fetch_arrow_table()
    duplicate_raw = raw.num_rows - raw_out.num_rows
    flat_observations = observations.num_rows - observations_out.num_rows
    return raw_out, contracts_out, observations_out, {
        "raw_rows_received": int(raw.num_rows),
        "raw_rows_written": int(raw_out.num_rows),
        "raw_rows_reused": int(duplicate_raw),
        "observation_rows_received": int(observations.num_rows),
        "observation_rows_written": int(observations_out.num_rows),
        "flat_observations_suppressed": int(flat_observations),
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
        cdc_started = time.monotonic()
        raw_cdc, contracts_cdc, observations_cdc, cdc_stats = _cdc_filter(
            con, prepared.source_id, raw, contracts, observations
        )
        cdc_seconds = time.monotonic() - cdc_started

        con.execute("BEGIN TRANSACTION")
        try:
            persisted = v4._persist_batch(
                con, prepared.source_id, prepared.run_id,
                raw_cdc, contracts_cdc, observations_cdc,
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
                "contract_rows": contracts.num_rows,
                **cdc_stats,
                "contract_versions_created": persisted["contract_versions_created"],
                "semantic_versions_created": persisted["semantic_versions_created"],
                "presence": presence,
                "timing": {
                    "fetch_seconds": round(fetched.fetch_seconds, 6),
                    "stage_seconds": round(stage_seconds, 6),
                    "cdc_seconds": round(cdc_seconds, 6),
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
        **cdc_stats,
        "contract_versions_created": persisted["contract_versions_created"],
        "semantic_versions_created": persisted["semantic_versions_created"],
        "collection_policy": prepared.policy,
        "presence": presence,
        "timing": details["timing"],
        "writer": METHOD_VERSION,
    }


def fetch_sources_parallel(source_ids: list[str]) -> tuple[dict[str, FetchResult], dict[str, Exception]]:
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

    # Preserve deterministic serial writes even though independent HTTP fetches overlap.
    for source_id in source_ids:
        if source_id in results or source_id not in fetched:
            continue
        try:
            results[source_id] = _persist_source(prepared[source_id], fetched[source_id])
        except Exception as exc:
            log.exception("CDC persistence failed for %s", source_id)
            results[source_id] = _fail_run(
                prepared[source_id], exc, phase="persist", fetch_seconds=fetched[source_id].fetch_seconds
            )
    return [results[source_id] for source_id in source_ids if source_id in results]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(json.dumps(run_once(), indent=2, default=str))
