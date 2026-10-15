"""Institutional ingestion v11: v10 stateful CDC with one-chunk source prefetch.

v11 preserves v10's whole-source transaction, immutable raw evidence, state-change CDC,
semantic versioning and staged presence reconciliation. The only execution change is a bounded
producer that fetches the next source chunk while the current chunk is staged and persisted.
The queue holds at most one extra chunk so network latency can overlap database work without
turning source traversal into an unbounded in-memory materialization.
"""

from __future__ import annotations

import json
import logging
import os
import queue
import threading
import time

import pyarrow as pa

import institutional_ingest_v3 as v3
import institutional_ingest_v4 as v4
import institutional_ingest_v8 as v8
import institutional_ingest_v10 as v10
from db import connect
from institutional_persist_registered import persist_registered_batch
from institutional_presence_v3 import (
    SEEN_TABLE,
    ensure_seen_table,
    reconcile_presence_staged,
    stage_seen_from_registered_batch,
)
from institutional_universe_adapters import ADAPTERS, collection_policy

log = logging.getLogger("eventrisk.institutional_ingest_v11")
METHOD_VERSION = "change_data_capture_v11_prefetched"
CHUNK_ROWS = max(1000, int(os.getenv("EVENTRISK_INGEST_CHUNK_ROWS", "25000")))


def _prefetched_chunks(iterator, chunk_rows: int | None = None):
    """Yield bounded chunks while a daemon producer fetches at most one chunk ahead."""
    chunk_rows = max(1, int(chunk_rows or CHUNK_ROWS))
    items: queue.Queue = queue.Queue(maxsize=1)
    stop = threading.Event()

    def put(item) -> bool:
        while not stop.is_set():
            try:
                items.put(item, timeout=0.25)
                return True
            except queue.Full:
                continue
        return False

    def produce() -> None:
        try:
            while not stop.is_set():
                started = time.monotonic()
                chunk = []
                for _ in range(chunk_rows):
                    try:
                        chunk.append(next(iterator))
                    except StopIteration:
                        break
                elapsed = time.monotonic() - started
                if not put(("chunk", chunk, elapsed)):
                    return
                if not chunk:
                    return
        except BaseException as exc:  # propagated to the source transaction consumer
            put(("error", exc, 0.0))

    producer = threading.Thread(
        target=produce,
        name="eventrisk-source-prefetch",
        daemon=True,
    )
    producer.start()
    try:
        while True:
            kind, payload, elapsed = items.get()
            if kind == "error":
                raise payload
            if not payload:
                break
            yield payload, float(elapsed)
    finally:
        stop.set()
        producer.join(timeout=1.0)


def _fail_run(source_id: str, run_id: str, policy: dict, exc: Exception, *, phase: str) -> None:
    details = {
        "writer": METHOD_VERSION,
        "phase": phase,
        "error": str(exc)[:1800],
        "collection_policy": policy,
        "memory_strategy": "streamed_state_pointer_cdc_prefetch1",
        "chunk_rows": CHUNK_ROWS,
    }
    con = connect()
    try:
        con.execute(
            """
            UPDATE er_ingestion_runs SET finished_at=now(),status='failed',error_count=1,
              details=?,universe_complete=FALSE WHERE run_id=?
            """,
            [json.dumps(details, sort_keys=True, default=str), run_id],
        )
        v3.update_health(con, source_id, run_id, "failed", 0, str(exc)[:2000])
    finally:
        con.close()


def ingest_source(source_id: str) -> dict:
    if source_id not in ADAPTERS:
        raise ValueError(f"Unknown source {source_id}")

    run_id, policy = v10._prepare_run(source_id)
    source_started = time.monotonic()
    fetch_seconds = prefetch_wait_seconds = stage_seconds = register_seconds = 0.0
    cdc_seconds = db_persist_seconds = presence_seconds = 0.0
    total_contracts = total_contract_versions = total_semantic_versions = chunks = 0
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
            prefetched = iter(_prefetched_chunks(iterator))
            while True:
                wait_started = time.monotonic()
                try:
                    chunk, chunk_fetch_seconds = next(prefetched)
                except StopIteration:
                    prefetch_wait_seconds += time.monotonic() - wait_started
                    break
                prefetch_wait_seconds += time.monotonic() - wait_started
                fetch_seconds += chunk_fetch_seconds
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
                v10._release_native_memory()

                t0 = time.monotonic()
                cdc_stats = v10._apply_cdc_stateful(con, source_id, run_id)
                cdc_seconds += time.monotonic() - t0
                for key in totals:
                    totals[key] += int(cdc_stats[key])

                t0 = time.monotonic()
                persisted = persist_registered_batch(
                    con,
                    method_version=METHOD_VERSION,
                    collect_seen_ids=False,
                )
                db_persist_seconds += time.monotonic() - t0
                total_contract_versions += int(persisted["contract_versions_created"])
                total_semantic_versions += int(persisted["semantic_versions_created"])
                del persisted
                v8._drop_batch_temps(con)
                v10._release_native_memory()
                log.info(
                    "prefetched ingest chunk completed source=%s chunk=%s total_rows=%s rss_max_mb=%s arrow_mb=%.2f",
                    source_id,
                    chunks,
                    total_contracts,
                    v8._rss_mb(),
                    pa.total_allocated_bytes() / (1024 * 1024),
                )

            universe_complete = policy["scope"] == "full"
            if universe_complete and total_contracts == 0:
                prior_active = int(
                    con.execute(
                        "SELECT count(*) FROM er_source_contracts WHERE source_id=? AND active=TRUE",
                        [source_id],
                    ).fetchone()[0]
                )
                if prior_active:
                    raise RuntimeError(
                        f"{source_id} full-universe traversal returned zero contracts while {prior_active} "
                        "active contracts exist; refusing to reconcile absence"
                    )

            t0 = time.monotonic()
            presence = reconcile_presence_staged(
                con,
                source_id,
                run_id,
                universe_complete=universe_complete,
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
                "memory_strategy": "streamed_state_pointer_cdc_prefetch1",
                "chunk_rows": CHUNK_ROWS,
                "chunks": chunks,
                "rss_max_mb": v8._rss_mb(),
                "timing": {
                    "fetch_seconds": round(fetch_seconds, 6),
                    "prefetch_wait_seconds": round(prefetch_wait_seconds, 6),
                    "stage_seconds": round(stage_seconds, 6),
                    "register_seconds": round(register_seconds, 6),
                    "cdc_seconds": round(cdc_seconds, 6),
                    "db_persist_seconds": round(db_persist_seconds, 6),
                    "presence_seconds": round(presence_seconds, 6),
                    "source_seconds": round(source_seconds, 6),
                },
            }
            con.execute(
                """
                UPDATE er_ingestion_runs SET finished_at=now(),status='success',records_received=?,records_written=?,error_count=0,
                  details=?,universe_complete=?,collection_scope=?,collection_policy=? WHERE run_id=?
                """,
                [
                    total_contracts,
                    total_contracts,
                    json.dumps(details, sort_keys=True, default=str),
                    universe_complete,
                    policy["scope"],
                    json.dumps(policy, sort_keys=True),
                    run_id,
                ],
            )
            v3.update_health(con, source_id, run_id, "success", total_contracts, None)
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
    except Exception as exc:
        log.exception("Prefetched institutional ingestion failed for %s", source_id)
        _fail_run(source_id, run_id, policy, exc, phase="prefetch_stream_or_persist")
        raise
    finally:
        con.close()
        v10._release_native_memory()

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
    results = []
    for source_id in list(source_ids or ADAPTERS):
        try:
            results.append(ingest_source(source_id))
        except Exception as exc:
            results.append(
                {
                    "source_id": source_id,
                    "status": "failed",
                    "phase": "source",
                    "error": str(exc),
                    "writer": METHOD_VERSION,
                }
            )
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(json.dumps(run_once(), indent=2, default=str))
