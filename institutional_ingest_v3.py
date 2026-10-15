"""Institutional ingestion v3 with prospective full source-contract semantic versioning.

In addition to source-reported metadata, this module records open-universe *presence*.
Absence is never rewritten into a fake source status: only repeated misses across successful
full-universe runs can deactivate a contract from the serving universe, and every transition
is separately auditable.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Optional

import institutional_ingest_v2 as v2
from db import connect
from institutional_universe_adapters import ADAPTERS, collection_policy

log = logging.getLogger("eventrisk.institutional_ingest_v3")

ContractSnapshot = v2.ContractSnapshot
ensure_sources = v2.ensure_sources
insert_raw = v2.insert_raw
upsert_contract = v2.upsert_contract
insert_observation = v2.insert_observation
link_contract_version_evidence = v2.link_contract_version_evidence
update_health = v2.update_health

PRESENCE_MISS_THRESHOLD = max(2, int(os.getenv("PRESENCE_MISS_THRESHOLD", "3")))
PRESENCE_METHOD = "presence_reconciliation_v1"


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


def _transition(con, contract_id: str, source_id: str, run_id: str, transition_type: str,
                previous_state: Optional[str], new_state: str, misses: int, reason: str) -> None:
    con.execute(
        """INSERT INTO er_contract_presence_transitions(
             transition_id,source_contract_id,source_id,run_id,transition_type,
             previous_state,new_state,consecutive_misses,reason)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        [
            f"prs_{uuid.uuid4().hex}", contract_id, source_id, run_id, transition_type,
            previous_state, new_state, misses, reason,
        ],
    )


def reconcile_presence(con, source_id: str, run_id: str, seen_contract_ids: set[str], *, universe_complete: bool) -> dict:
    """Reconcile open-universe presence without treating one omission as a closure."""
    seen = sorted(seen_contract_ids)
    con.execute("DROP TABLE IF EXISTS _er_seen_contracts")
    con.execute("CREATE TEMP TABLE _er_seen_contracts(source_contract_id VARCHAR PRIMARY KEY)")
    if seen:
        # A Python list binds as a DuckDB LIST, avoiding one network round trip per contract.
        con.execute(
            "INSERT INTO _er_seen_contracts SELECT unnest(?)",
            [seen],
        )

    reappeared = con.execute(
        """SELECT p.source_contract_id,p.listing_state,p.consecutive_misses
           FROM er_contract_presence_state p
           JOIN _er_seen_contracts s USING(source_contract_id)
           WHERE p.listing_state<>'observed'"""
    ).fetchall()

    con.execute(
        """INSERT INTO er_contract_presence_state(
             source_contract_id,source_id,listing_state,consecutive_misses,last_seen_run_id,
             last_seen_at,last_evaluated_run_id,deactivated_at,updated_at)
           SELECT c.source_contract_id,c.source_id,'observed',0,?,now(),?,NULL,now()
           FROM er_source_contracts c
           JOIN _er_seen_contracts s USING(source_contract_id)
           WHERE c.source_id=?
           ON CONFLICT(source_contract_id) DO UPDATE SET
             source_id=excluded.source_id,listing_state='observed',consecutive_misses=0,
             last_seen_run_id=excluded.last_seen_run_id,last_seen_at=excluded.last_seen_at,
             last_evaluated_run_id=excluded.last_evaluated_run_id,deactivated_at=NULL,updated_at=now()""",
        [run_id, run_id, source_id],
    )
    for contract_id, previous_state, misses in reappeared:
        _transition(
            con, contract_id, source_id, run_id, "reactivated", previous_state, "observed", 0,
            f"Contract reappeared in source open universe after {misses} consecutive miss(es)",
        )

    if not universe_complete:
        con.execute("DROP TABLE IF EXISTS _er_seen_contracts")
        return {
            "universe_complete": False,
            "seen": len(seen),
            "reactivated": len(reappeared),
            "missed": 0,
            "deactivated": 0,
            "miss_threshold": PRESENCE_MISS_THRESHOLD,
        }

    before_missing = con.execute(
        """SELECT c.source_contract_id,p.listing_state,p.consecutive_misses
           FROM er_source_contracts c
           LEFT JOIN er_contract_presence_state p USING(source_contract_id)
           LEFT JOIN _er_seen_contracts s USING(source_contract_id)
           WHERE c.source_id=? AND c.active=TRUE AND s.source_contract_id IS NULL""",
        [source_id],
    ).fetchall()

    con.execute(
        """INSERT INTO er_contract_presence_state(
             source_contract_id,source_id,listing_state,consecutive_misses,last_evaluated_run_id,updated_at)
           SELECT c.source_contract_id,c.source_id,'missing_unconfirmed',1,?,now()
           FROM er_source_contracts c
           LEFT JOIN _er_seen_contracts s USING(source_contract_id)
           WHERE c.source_id=? AND c.active=TRUE AND s.source_contract_id IS NULL
           ON CONFLICT(source_contract_id) DO UPDATE SET
             source_id=excluded.source_id,
             consecutive_misses=er_contract_presence_state.consecutive_misses+1,
             listing_state=CASE
               WHEN er_contract_presence_state.consecutive_misses+1>=? THEN 'missing_confirmed'
               ELSE 'missing_unconfirmed'
             END,
             last_evaluated_run_id=excluded.last_evaluated_run_id,
             deactivated_at=CASE
               WHEN er_contract_presence_state.consecutive_misses+1>=?
                 AND er_contract_presence_state.listing_state<>'missing_confirmed'
               THEN now() ELSE er_contract_presence_state.deactivated_at END,
             updated_at=now()""",
        [run_id, source_id, PRESENCE_MISS_THRESHOLD, PRESENCE_MISS_THRESHOLD],
    )

    after_missing = con.execute(
        """SELECT p.source_contract_id,p.listing_state,p.consecutive_misses
           FROM er_contract_presence_state p
           LEFT JOIN _er_seen_contracts s USING(source_contract_id)
           WHERE p.source_id=? AND s.source_contract_id IS NULL
             AND p.last_evaluated_run_id=?""",
        [source_id, run_id],
    ).fetchall()
    before = {r[0]: (r[1], int(r[2] or 0)) for r in before_missing}
    deactivated = []
    for contract_id, new_state, misses in after_missing:
        previous_state, previous_misses = before.get(contract_id, (None, 0))
        if previous_state != new_state:
            transition_type = "deactivated" if new_state == "missing_confirmed" else "first_miss"
            _transition(
                con, contract_id, source_id, run_id, transition_type, previous_state, new_state,
                int(misses),
                "Contract absent from complete full-universe source run; source status itself was not rewritten",
            )
        if new_state == "missing_confirmed":
            deactivated.append(contract_id)

    if deactivated:
        con.execute(
            """UPDATE er_source_contracts SET active=FALSE,last_seen_at=last_seen_at
               WHERE source_id=? AND source_contract_id IN (
                 SELECT source_contract_id FROM er_contract_presence_state
                 WHERE source_id=? AND listing_state='missing_confirmed'
               )""",
            [source_id, source_id],
        )

    con.execute("DROP TABLE IF EXISTS _er_seen_contracts")
    return {
        "universe_complete": True,
        "seen": len(seen),
        "reactivated": len(reappeared),
        "missed": len(after_missing),
        "deactivated": len(deactivated),
        "miss_threshold": PRESENCE_MISS_THRESHOLD,
    }


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
    seen_contract_ids: set[str] = set()
    policy = collection_policy()
    con = connect()
    try:
        ensure_sources(con)
        con.execute(
            """INSERT INTO er_ingestion_runs(
                 run_id,source_id,started_at,status,collection_scope,universe_complete,collection_policy)
               VALUES (?,?,?,'running',?,FALSE,?)""",
            [run_id, source_id, started, policy["scope"], json.dumps(policy, sort_keys=True)],
        )
        try:
            for snapshot in adapter.iter_snapshots():
                try:
                    event_object_id = snapshot.source_event_id or snapshot.source_market_id
                    if event_object_id not in event_raw_ids:
                        event_raw_ids[event_object_id] = insert_raw(
                            con, source_id, "event", event_object_id, snapshot.raw_event,
                            snapshot.observed_at, run_id,
                        )
                        unique_events += 1
                    event_raw_id = event_raw_ids[event_object_id]
                    market_raw_id = insert_raw(
                        con, source_id, "market", snapshot.source_market_id, snapshot.raw_market,
                        snapshot.observed_at, run_id,
                    )
                    contract_id = upsert_contract(con, snapshot, market_raw_id)
                    seen_contract_ids.add(contract_id)
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
            universe_complete = status == "success" and policy["scope"] == "full"
            presence = reconcile_presence(
                con, source_id, run_id, seen_contract_ids, universe_complete=universe_complete
            )
            details_obj = {
                "unique_events": unique_events,
                "semantic_versions_created": semantic_versions,
                "contract_errors": errors,
                "collection_policy": policy,
                "presence": presence,
            }
            con.execute(
                """UPDATE er_ingestion_runs
                   SET finished_at=?,status=?,records_received=?,records_written=?,error_count=?,details=?,
                       universe_complete=?,collection_scope=?,collection_policy=?
                   WHERE run_id=?""",
                [
                    now_utc(), status, written + errors, written, errors,
                    json.dumps(details_obj, sort_keys=True, default=str),
                    universe_complete, policy["scope"], json.dumps(policy, sort_keys=True), run_id,
                ],
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
                "collection_policy": policy,
                "presence": presence,
            }
        except Exception as exc:
            con.execute(
                """UPDATE er_ingestion_runs
                   SET finished_at=?,status='failed',records_received=?,records_written=?,error_count=?,details=?,
                       universe_complete=FALSE,collection_scope=?,collection_policy=?
                   WHERE run_id=?""",
                [
                    now_utc(), written, written, errors + 1,
                    json.dumps({"error": str(exc)[:1800], "collection_policy": policy}),
                    policy["scope"], json.dumps(policy, sort_keys=True), run_id,
                ],
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
            results.append({
                "source_id": source_id,
                "status": "failed",
                "error": str(exc),
                "collection_policy": collection_policy(),
            })
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(json.dumps(run_once(), indent=2, default=str))
