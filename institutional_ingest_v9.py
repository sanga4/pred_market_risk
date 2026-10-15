"""Institutional ingestion v9: streamed bounded CDC with staged presence IDs.

v9 preserves v8 CDC, raw-evidence, semantic-version, observation and whole-source atomicity
semantics while removing two scale costs:
1) a source universe is consumed in bounded chunks instead of materializing every snapshot in
   Python before persistence; and
2) observed contract IDs accumulate in a MotherDuck TEMP table during ingestion, so presence
   reconciliation does not round-trip hundreds of thousands of IDs through Python.

A source adapter/pagination failure rolls back the entire source transaction. A full-universe
run that unexpectedly returns zero contracts while the source has active history fails closed.
"""

from __future__ import annotations

import gc
import itertools
import json
import logging
import os
import time
import uuid

import pyarrow as pa

import institutional_ingest_v3 as v3
import institutional_ingest_v4 as v4
import institutional_ingest_v8 as v8
from db import connect
from institutional_persist_registered import persist_registered_batch
from institutional_presence_v3 import (
    SEEN_TABLE,
    ensure_seen_table,
    reconcile_presence_staged,
    stage_seen_from_registered_batch,
)
from institutional_universe_adapters import ADAPTERS, collection_policy

log = logging.getLogger("eventrisk.institutional_ingest_v9")
METHOD_VERSION = "change_data_capture_v9_streamed"
CHUNK_ROWS = max(1000, int(os.getenv("EVENTRISK_INGEST_CHUNK_ROWS", "25000")))


def _prepare_run(source_id: str):
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
    return run_id, policy


def _fail_run(source_id: str, run_id: str, policy: dict, exc: Exception, *, phase: str) -> None:
    details = {
        "writer": METHOD_VERSION,
        "phase": phase,
        "error": str(exc)[:1800],
        "collection_policy": policy,
        "memory_strategy": "streamed_source_direct_temp_chunked_cdc_staged_presence",
        "chunk_rows": CHUNK_ROWS,
    }
    con = connect()
    try:
        con.execute("""
            UPDATE er_ingestion_runs SET finished_at=now(),status='failed',error_count=1,
              details=?,universe_complete=FALSE WHERE run_id=?
        """, [json.dumps(details, sort_keys=True, default=str), run_id])
        v3.update_health(con, source_id, run_id, "failed", 0, str(exc)[:2000])
    finally:
        con.close()


def _release_native_memory() -> None:
    gc.collect()
    try:
        pa.default_memory_pool().release_unused()
    except Exception:
        pass


def _next_chunk(iterator) -> list:
    return list(itertools.islice(iterator, CHUNK_ROWS))


def ingest_source(source_id: str) -> dict:
    if source_id not in ADAPTERS:
        raise ValueError(f"Unknown source {source_id}")

    run_id, policy = _prepare_run(source_id)
    source_started = time.monotonic()
    fetch_seconds = 0.0
    stage_seconds = 0.0
    register_seconds = 0.0
    cdc_seconds = 0.0
    db_persist_seconds = 0.0
    presence_seconds = 0.0
    total_contracts = 0
    total_contract_versions = 0
    total_semantic_versions = 0
    chunks = 0
    totals = {
        "raw_rows_received": 0,
        "raw_rows_written": 0,
        "raw_rows_reused": 0,
        "observation_rows_received": 0,
        "observation_rows_written": 0,
        "flat_observations_suppressed": 0,
    }

    con = connect()
    try:
        con.execute("SET TimeZone='UTC'")
        con.execute("BEGIN TRANSACTION")
        try:
            ensure_seen_table(con)
            iterator = iter(ADAPTERS[source_id]().iter_snapshots())

            while True:
                t0 = time.monotonic()
                chunk = _next_chunk(iterator)
                fetch_seconds += time.monotonic() - t0
                if not chunk:
                    break
                chunks += 1
                total_contracts += len(chunk)

                t0 = time.monotonic()
                raw, contracts, observations = v4._stage_snapshots(source_id, run_id, chunk)
                stage_seconds += time.monotonic() - t0
                del chunk

                t0 = time.monotonic()
                v4._register_batch(con, raw, contracts, observations)
                stage_seen_from_registered_batch(con)
                register_seconds += time.monotonic() - t0
                del raw, contracts, observations
                _release_native_memory()

                t0 = time.monotonic()
                cdc_stats = v8._apply_cdc_in_place(con, source_id)
                cdc_seconds += time.monotonic() - t0
                for key in totals:
                    totals[key] += int(cdc_stats[key])

                t0 = time.monotonic()
                persisted = persist_registered_batch(
                    con, method_version=METHOD_VERSION, collect_seen_ids=False
                )
                db_persist_seconds += time.monotonic() - t0
                total_contract_versions += int(persisted["contract_versions_created"])
                total_semantic_versions += int(persisted["semantic_versions_created"])
                del persisted

                v8._drop_batch_temps(con)
                _release_native_memory()
                log.info(
                    "streamed ingest chunk completed source=%s chunk=%s total_rows=%s rss_max_mb=%s arrow_mb=%.2f",
                    source_id,
                    chunks,
                    total_contracts,
                    v8._rss_mb(),
                    pa.total_allocated_bytes() / (1024.0 * 1024.0),
                )

            universe_complete = policy["scope"] == "full"
            if universe_complete and total_contracts == 0:
                prior_active = int(con.execute(
                    "SELECT count(*) FROM er_source_contracts WHERE source_id=? AND active=TRUE",
                    [source_id],
                ).fetchone()[0])
                if prior_active:
                    raise RuntimeError(
                        f"{source_id} full-universe traversal returned zero contracts while "
                        f"{prior_active} active contracts exist; refusing to reconcile absence"
                    )

            t0 = time.monotonic()
            presence = reconcile_presence_staged(
                con, source_id, run_id, universe_complete=universe_complete
            )
            presence_seconds += time.monotonic() - t0
            con.execute(f"DROP TABLE IF EXISTS {SEEN_TABLE}")

            source_seconds = time.monotonic() - source_started
            details = {
                "writer": METHOD_VERSION,
                "collection_policy": policy,
                "contract_rows": total_contracts,
                **totals,
                "contract_versions_created": total_contract_versions,
                "semantic_versions_created": total_semantic_versions,
                "presence": presence,
                "memory_strategy": "streamed_source_direct_temp_chunked_cdc_staged_presence",
                "chunk_rows": CHUNK_ROWS,
                "chunks": chunks,
                "rss_max_mb": v8._rss_mb(),
                "timing": {
                    "fetch_seconds": round(fetch_seconds, 6),
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
                total_contracts,total_contracts,json.dumps(details, sort_keys=True, default=str),
                universe_complete,policy["scope"],json.dumps(policy, sort_keys=True),run_id,
            ])
            v3.update_health(con, source_id, run_id, "success", total_contracts, None)
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
    except Exception as exc:
        log.exception("Streamed institutional ingestion failed for %s", source_id)
        _fail_run(source_id, run_id, policy, exc, phase="stream_or_persist")
        raise
    finally:
        con.close()
        _release_native_memory()

    return {
        "source_id": source_id,
        "run_id": run_id,
        "status": "success",
        "records_written": total_contracts,
        **totals,
        "contract_versions_created": total_contract_versions,
        "semantic_versions_created": total_semantic_versions,
        "collection_policy": policy,
        "presence": presence,
        "timing": details["timing"],
        "memory_strategy": details["memory_strategy"],
        "rss_max_mb": details["rss_max_mb"],
        "chunk_rows": CHUNK_ROWS,
        "chunks": chunks,
        "writer": METHOD_VERSION,
    }


def run_once(source_ids=None) -> list[dict]:
    results: list[dict] = []
    for source_id in list(source_ids or ADAPTERS):
        try:
            results.append(ingest_source(source_id))
        except Exception as exc:
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
