"""Restart-safe batched execution of the EventRisk event ontology.

Taxonomy semantics are intentionally unchanged from `institutional_ontology.py`:
classification rules, threshold extraction and `event_ontology_v1` taxonomy labels remain the
same. v2 changes execution only: input-hash work queues, vectorized Arrow staging, set-based
versioning/revisions and set-based family-category aggregation.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone

import pyarrow as pa

import institutional_ontology as legacy
from db import connect

METHOD_VERSION = "event_ontology_v2_batched"
RECOVERY_VERSION = "ontology_run_recovery_v1"
TAXONOMY_VERSION = legacy.TAXONOMY_VERSION
RECOVERY_MINUTES = max(10, int(os.getenv("ONTOLOGY_RUN_RECOVERY_MINUTES", "30")))

DESIRED_SCHEMA = pa.schema([
    ("canonical_event_id", pa.string()),
    ("event_family_id", pa.string()),
    ("input_hash", pa.string()),
    ("event_class", pa.string()),
    ("predicate", pa.string()),
    ("threshold_value", pa.float64()),
    ("threshold_unit", pa.string()),
    ("taxonomy_version", pa.string()),
    ("processed_at", pa.timestamp("us", tz="UTC")),
])


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _rows_dict(cur) -> list[dict]:
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def recover_abandoned_ontology_runs(con) -> dict:
    cutoff = now_utc() - timedelta(minutes=RECOVERY_MINUTES)
    stale = _rows_dict(con.execute("""
        SELECT run_id,method_version,started_at
        FROM er_ontology_runs
        WHERE status='running' AND started_at<?
        ORDER BY started_at
    """, [cutoff]))
    for row in stale:
        con.execute("""
            UPDATE er_ontology_runs SET status='abandoned',finished_at=now(),details=?
            WHERE run_id=? AND status='running'
        """, [json.dumps({
            "recovery": {
                "method_version": RECOVERY_VERSION,
                "reason": "ontology run remained running beyond threshold after prior worker/process termination",
                "threshold_minutes": RECOVERY_MINUTES,
                "recovered_at": now_utc().isoformat(),
            }
        }, sort_keys=True), row["run_id"]])
    running = _rows_dict(con.execute("""
        SELECT run_id,method_version,started_at FROM er_ontology_runs
        WHERE status='running' ORDER BY started_at
    """))
    return {"recovered": len(stale), "stale": stale, "running": running, "threshold_minutes": RECOVERY_MINUTES}


def _desired_rows(work_rows: list[dict]) -> list[dict]:
    processed_at = now_utc()
    output = []
    for row in work_rows:
        text = f"{row.get('canonical_name') or ''}\n{row.get('canonical_definition') or ''}"
        event_class = legacy.classify(text)
        predicate, value, unit = legacy.extract_threshold(text)
        output.append({
            "canonical_event_id": row["canonical_event_id"],
            "event_family_id": row["event_family_id"],
            "input_hash": row["input_hash"],
            "event_class": event_class,
            "predicate": predicate,
            "threshold_value": value,
            "threshold_unit": unit,
            "taxonomy_version": TAXONOMY_VERSION,
            "processed_at": processed_at,
        })
    return output


def _register_desired(con, rows: list[dict]) -> None:
    table = pa.Table.from_pylist(rows, schema=DESIRED_SCHEMA) if rows else pa.Table.from_pylist([], schema=DESIRED_SCHEMA)
    con.register("_ontology_desired_arrow", table)
    con.execute("CREATE OR REPLACE TEMP TABLE _ontology_desired AS SELECT * FROM _ontology_desired_arrow")
    con.unregister("_ontology_desired_arrow")


def _persist_event_changes(con, desired_count: int) -> dict:
    if desired_count == 0:
        return {"events_changed": 0, "revisions_recorded": 0}

    # Capture a point-in-time baseline before any normalization mutation.
    con.execute("""
        INSERT INTO er_canonical_event_versions(
          canonical_event_id,version_no,event_family_id,canonical_name,canonical_definition,
          subject_entity_id,predicate,object_entity_id,event_class,geography_entity_id,
          threshold_value,threshold_unit,measurement_name,observation_period_start,observation_period_end,
          resolution_deadline,outcome_type,status,taxonomy_version,change_type,method,method_version,
          review_status,valid_from)
        SELECT e.canonical_event_id,1,e.event_family_id,e.canonical_name,e.canonical_definition,
               e.subject_entity_id,e.predicate,e.object_entity_id,e.event_class,e.geography_entity_id,
               e.threshold_value,e.threshold_unit,e.measurement_name,e.observation_period_start,e.observation_period_end,
               e.resolution_deadline,e.outcome_type,e.status,e.taxonomy_version,
               'initial','canonical_bootstrap',e.taxonomy_version,'system',e.created_at
        FROM er_canonical_events e
        JOIN _ontology_desired d USING(canonical_event_id)
        LEFT JOIN er_canonical_event_versions v USING(canonical_event_id)
        WHERE v.canonical_event_id IS NULL
    """)

    con.execute("""
        CREATE OR REPLACE TEMP TABLE _ontology_event_changes AS
        WITH latest AS (
          SELECT canonical_event_id,max(version_no)::INTEGER AS latest_version
          FROM er_canonical_event_versions
          GROUP BY canonical_event_id
        )
        SELECT d.*,coalesce(l.latest_version,0)+1 AS next_version,
               e.event_class AS old_event_class,e.predicate AS old_predicate,
               e.threshold_value AS old_threshold_value,e.threshold_unit AS old_threshold_unit,
               e.taxonomy_version AS old_taxonomy_version
        FROM _ontology_desired d
        JOIN er_canonical_events e USING(canonical_event_id)
        LEFT JOIN latest l USING(canonical_event_id)
        WHERE e.event_class IS DISTINCT FROM d.event_class
           OR e.predicate IS DISTINCT FROM d.predicate
           OR e.threshold_value IS DISTINCT FROM d.threshold_value
           OR e.threshold_unit IS DISTINCT FROM d.threshold_unit
           OR e.taxonomy_version IS DISTINCT FROM d.taxonomy_version
    """)
    changed = int(con.execute("SELECT count(*) FROM _ontology_event_changes").fetchone()[0])
    if changed:
        con.execute("""
            UPDATE er_canonical_events e SET
              event_class=d.event_class,predicate=d.predicate,threshold_value=d.threshold_value,
              threshold_unit=d.threshold_unit,taxonomy_version=d.taxonomy_version,updated_at=d.processed_at
            FROM _ontology_event_changes d
            WHERE e.canonical_event_id=d.canonical_event_id
        """)
        con.execute("""
            UPDATE er_canonical_event_versions v SET valid_to=d.processed_at
            FROM _ontology_event_changes d
            WHERE v.canonical_event_id=d.canonical_event_id AND v.valid_to IS NULL
        """)
        con.execute("""
            INSERT INTO er_canonical_event_versions(
              canonical_event_id,version_no,event_family_id,canonical_name,canonical_definition,
              subject_entity_id,predicate,object_entity_id,event_class,geography_entity_id,
              threshold_value,threshold_unit,measurement_name,observation_period_start,observation_period_end,
              resolution_deadline,outcome_type,status,taxonomy_version,change_type,method,method_version,
              review_status,valid_from)
            SELECT e.canonical_event_id,d.next_version,e.event_family_id,e.canonical_name,e.canonical_definition,
                   e.subject_entity_id,e.predicate,e.object_entity_id,e.event_class,e.geography_entity_id,
                   e.threshold_value,e.threshold_unit,e.measurement_name,e.observation_period_start,e.observation_period_end,
                   e.resolution_deadline,e.outcome_type,e.status,e.taxonomy_version,
                   'taxonomy_normalization','event_ontology',?, 'system',d.processed_at
            FROM _ontology_event_changes d
            JOIN er_canonical_events e USING(canonical_event_id)
        """, [METHOD_VERSION])
        con.execute("""
            INSERT INTO er_revisions(
              revision_id,object_type,object_id,revision_type,reason,previous_version,new_version,
              effective_at,recorded_at,actor)
            SELECT 'rev_' || md5(d.canonical_event_id || '|ontology|' || cast(d.next_version AS varchar) || '|' || cast(epoch_us(d.processed_at) AS varchar)),
                   'canonical_event',d.canonical_event_id,'taxonomy_normalization',
                   'Deterministic Layer 2 event classification/threshold normalization',
                   concat_ws('|',coalesce(d.old_event_class,''),coalesce(d.old_predicate,''),
                                  coalesce(cast(d.old_threshold_value AS varchar),''),coalesce(d.old_threshold_unit,''),
                                  coalesce(d.old_taxonomy_version,'')),
                   concat_ws('|',coalesce(d.event_class,''),coalesce(d.predicate,''),
                                  coalesce(cast(d.threshold_value AS varchar),''),coalesce(d.threshold_unit,''),
                                  coalesce(d.taxonomy_version,'')),
                   d.processed_at,d.processed_at,?
            FROM _ontology_event_changes d
            ON CONFLICT(revision_id) DO NOTHING
        """, [METHOD_VERSION])
    return {"events_changed": changed, "revisions_recorded": changed}


def _persist_family_changes(con, desired_count: int) -> int:
    if desired_count == 0:
        return 0
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _ontology_affected_families AS
        SELECT DISTINCT event_family_id FROM _ontology_desired WHERE event_family_id IS NOT NULL
    """)
    con.execute("""
        INSERT INTO er_event_family_versions(
          event_family_id,version_no,canonical_name,category,subcategory,description,
          change_type,method,method_version,review_status,valid_from)
        SELECT f.event_family_id,1,f.canonical_name,f.category,f.subcategory,f.description,
               'initial','canonical_bootstrap',NULL,'system',f.created_at
        FROM er_event_families f
        JOIN _ontology_affected_families a USING(event_family_id)
        LEFT JOIN er_event_family_versions v USING(event_family_id)
        WHERE v.event_family_id IS NULL
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _ontology_family_changes AS
        WITH counts AS (
          SELECT e.event_family_id,e.event_class,count(*) AS n
          FROM er_canonical_events e
          JOIN _ontology_affected_families a USING(event_family_id)
          WHERE e.event_class IS NOT NULL
          GROUP BY e.event_family_id,e.event_class
        ), ranked AS (
          SELECT event_family_id,event_class,n,
                 row_number() OVER(PARTITION BY event_family_id ORDER BY n DESC,event_class) AS rn
          FROM counts
        ), latest AS (
          SELECT event_family_id,max(version_no)::INTEGER AS latest_version
          FROM er_event_family_versions GROUP BY event_family_id
        )
        SELECT f.event_family_id,r.event_class AS desired_category,f.category AS old_category,
               coalesce(l.latest_version,0)+1 AS next_version,now() AS changed_at
        FROM er_event_families f
        JOIN ranked r USING(event_family_id)
        LEFT JOIN latest l USING(event_family_id)
        WHERE r.rn=1 AND f.category IS DISTINCT FROM r.event_class
    """)
    changed = int(con.execute("SELECT count(*) FROM _ontology_family_changes").fetchone()[0])
    if not changed:
        return 0
    con.execute("""
        UPDATE er_event_families f SET category=c.desired_category,updated_at=c.changed_at
        FROM _ontology_family_changes c WHERE f.event_family_id=c.event_family_id
    """)
    con.execute("""
        UPDATE er_event_family_versions v SET valid_to=c.changed_at
        FROM _ontology_family_changes c
        WHERE v.event_family_id=c.event_family_id AND v.valid_to IS NULL
    """)
    con.execute("""
        INSERT INTO er_event_family_versions(
          event_family_id,version_no,canonical_name,category,subcategory,description,
          change_type,method,method_version,review_status,valid_from)
        SELECT f.event_family_id,c.next_version,f.canonical_name,f.category,f.subcategory,f.description,
               'taxonomy_normalization','event_ontology',?,'system',c.changed_at
        FROM _ontology_family_changes c
        JOIN er_event_families f USING(event_family_id)
    """, [METHOD_VERSION])
    return changed


def _mark_state(con, run_id: str) -> None:
    con.execute("""
        INSERT INTO er_ontology_event_state(
          canonical_event_id,input_hash,taxonomy_version,method_version,processed_at,last_run_id)
        SELECT canonical_event_id,input_hash,taxonomy_version,?,processed_at,?
        FROM _ontology_desired
        ON CONFLICT(canonical_event_id) DO UPDATE SET
          input_hash=excluded.input_hash,
          taxonomy_version=excluded.taxonomy_version,
          method_version=excluded.method_version,
          processed_at=excluded.processed_at,
          last_run_id=excluded.last_run_id
    """, [METHOD_VERSION, run_id])


def run_ontology_pass() -> dict:
    run_id = f"ont_{uuid.uuid4().hex}"
    con = connect()
    try:
        recovery = recover_abandoned_ontology_runs(con)
        if recovery["running"]:
            raise RuntimeError(f"another ontology run is still active: {recovery['running']}")
        con.execute("""
            INSERT INTO er_ontology_runs(run_id,method_version,started_at,status)
            VALUES (?,?,now(),'running')
        """, [run_id, METHOD_VERSION])
        started = time.monotonic()
        try:
            total = int(con.execute("SELECT count(*) FROM er_canonical_events").fetchone()[0])
            t0 = time.monotonic()
            work = _rows_dict(con.execute("""
                SELECT canonical_event_id,event_family_id,canonical_name,canonical_definition,
                       event_class,predicate,threshold_value,threshold_unit,taxonomy_version,input_hash
                FROM er_ontology_work_queue_v2
                ORDER BY canonical_event_id
            """))
            desired = _desired_rows(work)
            _register_desired(con, desired)
            classify_seconds = time.monotonic() - t0

            t0 = time.monotonic()
            event_result = _persist_event_changes(con, len(desired))
            event_persist_seconds = time.monotonic() - t0

            t0 = time.monotonic()
            families_changed = _persist_family_changes(con, len(desired))
            family_seconds = time.monotonic() - t0

            t0 = time.monotonic()
            _mark_state(con, run_id)
            state_seconds = time.monotonic() - t0
            elapsed = time.monotonic() - started
            details = {
                "taxonomy_version": TAXONOMY_VERSION,
                "incremental": True,
                "set_based_persistence": True,
                "recovery": recovery,
                "timing": {
                    "classify_seconds": round(classify_seconds, 6),
                    "event_persist_seconds": round(event_persist_seconds, 6),
                    "family_seconds": round(family_seconds, 6),
                    "state_seconds": round(state_seconds, 6),
                    "total_seconds": round(elapsed, 6),
                },
            }
            con.execute("""
                UPDATE er_ontology_runs SET finished_at=now(),status='success',events_total=?,
                  events_processed=?,events_changed=?,families_changed=?,details=? WHERE run_id=?
            """, [
                total,len(desired),event_result["events_changed"],families_changed,
                json.dumps(details,sort_keys=True,default=str),run_id,
            ])
            return {
                "run_id": run_id,
                "events_total": total,
                "events_processed": len(desired),
                "events_normalized": event_result["events_changed"],
                "families_normalized": families_changed,
                "revisions_recorded": event_result["revisions_recorded"],
                "taxonomy_version": TAXONOMY_VERSION,
                "method_version": METHOD_VERSION,
                "timing": details["timing"],
            }
        except Exception as exc:
            con.execute("""
                UPDATE er_ontology_runs SET finished_at=now(),status='failed',details=? WHERE run_id=?
            """, [json.dumps({"error":str(exc)[:4000]},sort_keys=True),run_id])
            raise
    finally:
        con.close()


if __name__ == "__main__":
    print(run_ontology_pass())
