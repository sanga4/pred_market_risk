"""Production institutional ingestion orchestration.

This module wraps the source adapters and storage primitives in institutional_ingest.py,
but deduplicates event-level raw payloads within an ingestion run. A source event with
many child contracts is archived once per pull, while each child market/contract retains
its own immutable market snapshot and observation.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

import institutional_ingest as base
from db import connect

log = logging.getLogger("eventrisk.institutional_ingest_v2")


def precise_now_utc() -> datetime:
    """Receipt clock for the institutional ledger; preserve microseconds."""
    return datetime.now(timezone.utc)


# The legacy helper intentionally rounded to seconds. The institutional append-only ledger
# can receive multiple versions inside one second, so all storage primitives must use a
# microsecond receipt clock. Source-observed timestamps are never modified.
base.now_utc = precise_now_utc

# Public aliases used by tests and callers.
ContractSnapshot = base.ContractSnapshot
ADAPTERS = base.ADAPTERS
ensure_sources = base.ensure_sources
insert_raw = base.insert_raw
upsert_contract = base.upsert_contract
insert_observation = base.insert_observation
update_health = base.update_health
now_utc = precise_now_utc


def ingest_source(source_id: str) -> dict:
    if source_id not in ADAPTERS:
        raise ValueError(f"Unknown source {source_id}")
    adapter = ADAPTERS[source_id]()
    run_id = f"ing_{uuid.uuid4().hex}"
    started = now_utc()
    written = 0
    errors = 0
    unique_events = 0
    event_raw_ids: dict[str, str] = {}
    con = connect()
    try:
        ensure_sources(con)
        con.execute(
            "INSERT INTO er_ingestion_runs(run_id,source_id,started_at,status) VALUES (?,?,?,'running')",
            [run_id, source_id, started],
        )
        try:
            for snapshot in adapter.iter_snapshots():
                try:
                    event_object_id = snapshot.source_event_id or snapshot.source_market_id
                    # Nested source responses repeat one event payload for every child market.
                    # Preserve the event exactly once per collection run, not once per child.
                    if event_object_id not in event_raw_ids:
                        event_raw_ids[event_object_id] = insert_raw(
                            con,
                            source_id,
                            "event",
                            event_object_id,
                            snapshot.raw_event,
                            snapshot.observed_at,
                            run_id,
                        )
                        unique_events += 1

                    market_raw_id = insert_raw(
                        con,
                        source_id,
                        "market",
                        snapshot.source_market_id,
                        snapshot.raw_market,
                        snapshot.observed_at,
                        run_id,
                    )
                    contract_id = upsert_contract(con, snapshot, market_raw_id)
                    insert_observation(con, snapshot, contract_id, market_raw_id)
                    written += 1
                except Exception:
                    errors += 1
                    log.exception("Failed to persist %s contract %s", source_id, snapshot.source_market_id)

            status = "success" if errors == 0 else "partial"
            details = json.dumps({"unique_events": unique_events, "contract_errors": errors})
            con.execute(
                """UPDATE er_ingestion_runs SET finished_at=?,status=?,records_received=?,records_written=?,error_count=?,details=?
                   WHERE run_id=?""",
                [now_utc(), status, written + errors, written, errors, details, run_id],
            )
            update_health(
                con,
                source_id,
                run_id,
                status,
                written,
                None if errors == 0 else f"{errors} contract persistence errors",
            )
            return {
                "source_id": source_id,
                "run_id": run_id,
                "status": status,
                "records_written": written,
                "unique_events_written": unique_events,
                "errors": errors,
            }
        except Exception as exc:
            con.execute(
                """UPDATE er_ingestion_runs SET finished_at=?,status='failed',records_received=?,records_written=?,error_count=?,details=?
                   WHERE run_id=?""",
                [now_utc(), written, written, errors + 1, str(exc)[:2000], run_id],
            )
            update_health(con, source_id, run_id, "failed", written, str(exc)[:2000])
            raise
    finally:
        con.close()


def run_once(source_ids: Optional[list[str]] = None) -> list[dict]:
    results = []
    for source_id in source_ids or list(ADAPTERS):
        try:
            results.append(ingest_source(source_id))
        except Exception as exc:
            log.exception("Institutional ingestion failed for %s", source_id)
            results.append({"source_id": source_id, "status": "failed", "error": str(exc)})
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(json.dumps(run_once(), indent=2, default=str))
