"""Institutional ingestion v10: streamed CDC backed by transactional current-state pointers.

v10 preserves v9 source completeness, bounded iteration, immutable raw evidence, semantic
versioning, state-change observations, staged presence reconciliation and whole-source rollback.
It replaces the two history-wide latest-state lookups in the CDC hot path with keyed joins to
`er_raw_current_snapshots` and `er_market_current_state`. Those current tables are transactional
accelerators only; append-only raw/observation history remains authoritative.
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

log = logging.getLogger("eventrisk.institutional_ingest_v10")
METHOD_VERSION = "change_data_capture_v10_stateful"
CHUNK_ROWS = max(1000, int(os.getenv("EVENTRISK_INGEST_CHUNK_ROWS", "25000")))


def _release_native_memory() -> None:
    gc.collect()
    try:
        pa.default_memory_pool().release_unused()
    except Exception:
        pass


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
        "memory_strategy": "streamed_source_state_pointer_cdc_staged_presence",
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


def _next_chunk(iterator) -> list:
    return list(itertools.islice(iterator, CHUNK_ROWS))


def _apply_cdc_stateful(con, source_id: str, run_id: str) -> dict:
    raw_received = int(con.execute("SELECT count(*) FROM _batch_raw").fetchone()[0])
    obs_received = int(con.execute("SELECT count(*) FROM _batch_observations").fetchone()[0])

    # Resolve each staged payload to existing immutable evidence if the exact bytes/hash match the
    # current source object. Repeated identical objects inside one bounded source batch share the
    # first staged immutable snapshot rather than duplicating bytes.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _raw_resolution AS
        WITH ranked AS (
          SELECT b.raw_snapshot_id AS staged_raw_snapshot_id,
                 b.source_id,b.source_object_type,b.source_object_id,b.received_at,b.content_hash,
                 b.ingestion_run_id,
                 p.raw_snapshot_id AS prior_raw_snapshot_id,p.content_hash AS prior_content_hash,
                 first_value(b.raw_snapshot_id) OVER(
                   PARTITION BY b.source_id,b.source_object_type,b.source_object_id,b.content_hash
                   ORDER BY b.received_at,b.raw_snapshot_id
                 ) AS first_staged_raw_snapshot_id,
                 row_number() OVER(
                   PARTITION BY b.source_id,b.source_object_type,b.source_object_id,b.content_hash
                   ORDER BY b.received_at,b.raw_snapshot_id
                 ) AS duplicate_rank
          FROM _batch_raw b
          LEFT JOIN er_raw_current_snapshots p
            ON p.source_id=b.source_id
           AND p.source_object_type=b.source_object_type
           AND p.source_object_id=b.source_object_id
        )
        SELECT *,
               CASE
                 WHEN prior_raw_snapshot_id IS NOT NULL
                  AND prior_content_hash IS NOT DISTINCT FROM content_hash
                 THEN prior_raw_snapshot_id
                 ELSE first_staged_raw_snapshot_id
               END AS effective_raw_snapshot_id,
               CASE
                 WHEN prior_raw_snapshot_id IS NOT NULL
                  AND prior_content_hash IS NOT DISTINCT FROM content_hash
                 THEN FALSE
                 ELSE duplicate_rank=1
               END AS should_insert
        FROM ranked
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

    # Update the current raw pointer from the latest staged occurrence for each source object. A
    # hash change starts a new current interval; an unchanged payload only advances last_seen_at.
    con.execute("""
        INSERT INTO er_raw_current_snapshots(
          source_id,source_object_type,source_object_id,raw_snapshot_id,content_hash,
          current_since,last_seen_at,last_ingestion_run_id)
        SELECT source_id,source_object_type,source_object_id,effective_raw_snapshot_id,content_hash,
               received_at,received_at,?
        FROM _raw_resolution
        QUALIFY row_number() OVER(
          PARTITION BY source_id,source_object_type,source_object_id
          ORDER BY received_at DESC,staged_raw_snapshot_id DESC
        )=1
        ON CONFLICT(source_id,source_object_type,source_object_id) DO UPDATE SET
          raw_snapshot_id=excluded.raw_snapshot_id,
          current_since=CASE
            WHEN er_raw_current_snapshots.content_hash IS DISTINCT FROM excluded.content_hash
            THEN excluded.current_since ELSE er_raw_current_snapshots.current_since END,
          content_hash=excluded.content_hash,
          last_seen_at=excluded.last_seen_at,
          last_ingestion_run_id=excluded.last_ingestion_run_id
    """, [run_id])

    # Compare against a keyed materialized current market state rather than the historical window
    # view. The current state itself is refreshed every successful poll, while history receives
    # only actual state changes.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _batch_observations_next AS
        SELECT b.*
        FROM _batch_observations b
        LEFT JOIN er_market_current_state l USING(source_contract_id)
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

    con.execute("""
        INSERT INTO er_market_current_state(
          source_contract_id,source_id,observed_at,received_at,yes_bid,yes_ask,no_bid,no_ask,
          midpoint,last_trade,displayed_probability,volume,volume_24h,open_interest,liquidity,
          orderbook_depth,spread,market_status,raw_snapshot_id,methodology_version,last_polled_at,
          last_ingestion_run_id)
        SELECT source_contract_id,source_id,observed_at,received_at,yes_bid,yes_ask,no_bid,no_ask,
               midpoint,last_trade,displayed_probability,volume,volume_24h,open_interest,liquidity,
               orderbook_depth,spread,market_status,raw_snapshot_id,methodology_version,received_at,?
        FROM _batch_observations
        ON CONFLICT(source_contract_id) DO UPDATE SET
          source_id=excluded.source_id,
          observed_at=excluded.observed_at,
          received_at=excluded.received_at,
          yes_bid=excluded.yes_bid,yes_ask=excluded.yes_ask,no_bid=excluded.no_bid,no_ask=excluded.no_ask,
          midpoint=excluded.midpoint,last_trade=excluded.last_trade,
          displayed_probability=excluded.displayed_probability,volume=excluded.volume,
          volume_24h=excluded.volume_24h,open_interest=excluded.open_interest,
          liquidity=excluded.liquidity,orderbook_depth=excluded.orderbook_depth,
          spread=excluded.spread,market_status=excluded.market_status,
          raw_snapshot_id=excluded.raw_snapshot_id,methodology_version=excluded.methodology_version,
          last_polled_at=excluded.last_polled_at,last_ingestion_run_id=excluded.last_ingestion_run_id
    """, [run_id])

    con.execute("DROP TABLE _batch_raw")
    con.execute("ALTER TABLE _batch_raw_next RENAME TO _batch_raw")
    con.execute("DROP TABLE _batch_observations")
    con.execute("ALTER TABLE _batch_observations_next RENAME TO _batch_observations")
    con.execute("DROP TABLE IF EXISTS _raw_resolution")

    return {
        "raw_rows_received": raw_received,
        "raw_rows_written": raw_written,
        "raw_rows_reused": raw_received - raw_written,
        "observation_rows_received": obs_received,
        "observation_rows_written": obs_written,
        "flat_observations_suppressed": obs_received - obs_written,
    }


def ingest_source(source_id: str) -> dict:
    if source_id not in ADAPTERS:
        raise ValueError(f"Unknown source {source_id}")

    run_id, policy = _prepare_run(source_id)
    source_started = time.monotonic()
    fetch_seconds = stage_seconds = register_seconds = cdc_seconds = db_persist_seconds = presence_seconds = 0.0
    total_contracts = total_contract_versions = total_semantic_versions = chunks = 0
    totals = {
        "raw_rows_received": 0,"raw_rows_written": 0,"raw_rows_reused": 0,
        "observation_rows_received": 0,"observation_rows_written": 0,"flat_observations_suppressed": 0,
    }

    con = connect()
    try:
        con.execute("SET TimeZone='UTC'")
        con.execute("BEGIN TRANSACTION")
        try:
            ensure_seen_table(con)
            iterator = iter(ADAPTERS[source_id]().iter_snapshots())
            while True:
                t0 = time.monotonic(); chunk = _next_chunk(iterator); fetch_seconds += time.monotonic()-t0
                if not chunk:
                    break
                chunks += 1; total_contracts += len(chunk)

                t0=time.monotonic(); raw,contracts,observations=v4._stage_snapshots(source_id,run_id,chunk); stage_seconds+=time.monotonic()-t0
                del chunk
                t0=time.monotonic(); v4._register_batch(con,raw,contracts,observations); stage_seen_from_registered_batch(con); register_seconds+=time.monotonic()-t0
                del raw,contracts,observations; _release_native_memory()

                t0=time.monotonic(); cdc_stats=_apply_cdc_stateful(con,source_id,run_id); cdc_seconds+=time.monotonic()-t0
                for key in totals: totals[key]+=int(cdc_stats[key])

                t0=time.monotonic(); persisted=persist_registered_batch(con,method_version=METHOD_VERSION,collect_seen_ids=False); db_persist_seconds+=time.monotonic()-t0
                total_contract_versions+=int(persisted["contract_versions_created"]); total_semantic_versions+=int(persisted["semantic_versions_created"])
                del persisted; v8._drop_batch_temps(con); _release_native_memory()
                log.info("stateful ingest chunk completed source=%s chunk=%s total_rows=%s rss_max_mb=%s arrow_mb=%.2f",source_id,chunks,total_contracts,v8._rss_mb(),pa.total_allocated_bytes()/(1024*1024))

            universe_complete=policy["scope"]=="full"
            if universe_complete and total_contracts==0:
                prior_active=int(con.execute("SELECT count(*) FROM er_source_contracts WHERE source_id=? AND active=TRUE",[source_id]).fetchone()[0])
                if prior_active:
                    raise RuntimeError(f"{source_id} full-universe traversal returned zero contracts while {prior_active} active contracts exist; refusing to reconcile absence")

            t0=time.monotonic(); presence=reconcile_presence_staged(con,source_id,run_id,universe_complete=universe_complete); presence_seconds+=time.monotonic()-t0
            con.execute(f"DROP TABLE IF EXISTS {SEEN_TABLE}")
            source_seconds=time.monotonic()-source_started
            details={
                "writer":METHOD_VERSION,"collection_policy":policy,"contract_rows":total_contracts,**totals,
                "contract_versions_created":total_contract_versions,"semantic_versions_created":total_semantic_versions,
                "presence":presence,"memory_strategy":"streamed_source_state_pointer_cdc_staged_presence",
                "chunk_rows":CHUNK_ROWS,"chunks":chunks,"rss_max_mb":v8._rss_mb(),
                "timing":{"fetch_seconds":round(fetch_seconds,6),"stage_seconds":round(stage_seconds,6),"register_seconds":round(register_seconds,6),"cdc_seconds":round(cdc_seconds,6),"db_persist_seconds":round(db_persist_seconds,6),"presence_seconds":round(presence_seconds,6),"source_seconds":round(source_seconds,6)},
            }
            con.execute("""
                UPDATE er_ingestion_runs SET finished_at=now(),status='success',records_received=?,records_written=?,error_count=0,
                  details=?,universe_complete=?,collection_scope=?,collection_policy=? WHERE run_id=?
            """,[total_contracts,total_contracts,json.dumps(details,sort_keys=True,default=str),universe_complete,policy["scope"],json.dumps(policy,sort_keys=True),run_id])
            v3.update_health(con,source_id,run_id,"success",total_contracts,None)
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK"); raise
    except Exception as exc:
        log.exception("Stateful institutional ingestion failed for %s",source_id)
        _fail_run(source_id,run_id,policy,exc,phase="stream_or_persist"); raise
    finally:
        con.close(); _release_native_memory()

    return {"source_id":source_id,"run_id":run_id,"status":"success","records_written":total_contracts,**totals,
            "contract_versions_created":total_contract_versions,"semantic_versions_created":total_semantic_versions,
            "collection_policy":policy,"presence":presence,"timing":details["timing"],"memory_strategy":details["memory_strategy"],
            "rss_max_mb":details["rss_max_mb"],"chunk_rows":CHUNK_ROWS,"chunks":chunks,"writer":METHOD_VERSION}


def run_once(source_ids=None) -> list[dict]:
    results=[]
    for source_id in list(source_ids or ADAPTERS):
        try: results.append(ingest_source(source_id))
        except Exception as exc: results.append({"source_id":source_id,"status":"failed","phase":"source","error":str(exc),"writer":METHOD_VERSION})
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,format="%(asctime)s %(levelname)s %(message)s")
    print(json.dumps(run_once(),indent=2,default=str))
