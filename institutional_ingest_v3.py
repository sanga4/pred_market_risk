"""Institutional ingestion v3 with prospective full source-contract semantic versioning."""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

import institutional_ingest_v2 as v2
from db import connect

log = logging.getLogger("eventrisk.institutional_ingest_v3")

ContractSnapshot = v2.ContractSnapshot
ADAPTERS = v2.ADAPTERS
ensure_sources = v2.ensure_sources
insert_raw = v2.insert_raw
upsert_contract = v2.upsert_contract
insert_observation = v2.insert_observation
link_contract_version_evidence = v2.link_contract_version_evidence
update_health = v2.update_health


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _semantic_values(snapshot: ContractSnapshot) -> tuple:
    return (
        snapshot.source_id,
        snapshot.source_market_id,
        snapshot.source_event_id,
        snapshot.title,
        snapshot.description,
        "binary",
        snapshot.status,
        snapshot.opens_at,
        snapshot.closes_at,
        snapshot.resolves_at,
        snapshot.resolution_authority,
        snapshot.resolution_rules,
        snapshot.source_url,
    )


def sync_semantic_contract_version(
    con,
    snapshot: ContractSnapshot,
    contract_id: str,
    event_raw_id: str,
    market_raw_id: str,
) -> dict:
    """Append a semantic version only when normalized source-contract semantics change."""
    fields = (
        "source_id,source_market_id,source_event_id,title,description,outcome_type,status,"
        "opens_at,closes_at,resolves_at,resolution_authority,resolution_rules,source_url"
    )
    latest = con.execute(
        f"SELECT version_no,{fields} FROM er_contract_semantic_versions "
        "WHERE source_contract_id=? ORDER BY version_no DESC LIMIT 1",
        [contract_id],
    ).fetchone()
    values = _semantic_values(snapshot)
    if latest is not None and tuple(latest[1:]) == values:
        return {"changed": False, "version_no": int(latest[0])}

    received = now_utc()
    next_version = 1 if latest is None else int(latest[0]) + 1
    if latest is not None:
        con.execute(
            "UPDATE er_contract_semantic_versions SET valid_to=? WHERE source_contract_id=? AND valid_to IS NULL",
            [received, contract_id],
        )
    con.execute(
        """INSERT INTO er_contract_semantic_versions(
             source_contract_id,version_no,source_id,source_market_id,source_event_id,title,description,
             outcome_type,status,opens_at,closes_at,resolves_at,resolution_authority,resolution_rules,
             source_url,source_observed_at,received_at,valid_from,event_raw_snapshot_id,
             market_raw_snapshot_id,metadata_completeness)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        [
            contract_id,
            next_version,
            *values,
            snapshot.observed_at,
            received,
            received,
            event_raw_id,
            market_raw_id,
            "full_v2",
        ],
    )
    con.execute(
        """INSERT INTO er_revisions(
             revision_id,object_type,object_id,revision_type,reason,previous_version,new_version,
             effective_at,recorded_at,actor)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        [
            f"rev_{uuid.uuid4().hex}",
            "source_contract_semantics",
            contract_id,
            "initial_full_semantic_capture" if latest is None else "semantic_metadata_change",
            "Prospective full source-contract semantic ledger",
            None if latest is None else str(latest[0]),
            str(next_version),
            received,
            received,
            "institutional_ingest_v3",
        ],
    )
    return {"changed": True, "version_no": next_version}


def ingest_source(source_id: str) -> dict:
    if source_id not in ADAPTERS:
        raise ValueError(f"Unknown source {source_id}")
    adapter = ADAPTERS[source_id]()
    run_id = f"ing_{uuid.uuid4().hex}"
    started = now_utc()
    written = 0
    errors = 0
    unique_events = 0
    semantic_versions = 0
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
                    event_raw_id = event_raw_ids[event_object_id]
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
                    link_contract_version_evidence(con, contract_id, event_raw_id, market_raw_id)
                    semantic = sync_semantic_contract_version(
                        con, snapshot, contract_id, event_raw_id, market_raw_id
                    )
                    semantic_versions += int(semantic["changed"])
                    insert_observation(con, snapshot, contract_id, market_raw_id)
                    written += 1
                except Exception:
                    errors += 1
                    log.exception("Failed to persist %s contract %s", source_id, snapshot.source_market_id)

            status = "success" if errors == 0 else "partial"
            details = json.dumps({
                "unique_events": unique_events,
                "semantic_versions_created": semantic_versions,
                "contract_errors": errors,
            })
            con.execute(
                """UPDATE er_ingestion_runs
                   SET finished_at=?,status=?,records_received=?,records_written=?,error_count=?,details=?
                   WHERE run_id=?""",
                [now_utc(), status, written + errors, written, errors, details, run_id],
            )
            update_health(
                con, source_id, run_id, status, written,
                None if errors == 0 else f"{errors} contract persistence errors",
            )
            return {
                "source_id": source_id,
                "run_id": run_id,
                "status": status,
                "records_written": written,
                "unique_events_written": unique_events,
                "semantic_versions_created": semantic_versions,
                "errors": errors,
            }
        except Exception as exc:
            con.execute(
                """UPDATE er_ingestion_runs
                   SET finished_at=?,status='failed',records_received=?,records_written=?,error_count=?,details=?
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
