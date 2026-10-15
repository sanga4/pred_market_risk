"""Deterministic canonical identity lane for institutional event normalization.

This module intentionally excludes fuzzy similarity. It gives every active source contract a
source-native canonical identity, then auto-merges only cross-source pairs satisfying the same
strict identity rule used by the prior mapper: exact normalized title, exact non-empty normalized
resolution rules, and <=24h (or missing) deadline compatibility. A separate semantic watermark
lets this lane run independently from slower review-candidate discovery.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pyarrow as pa

import institutional_mapping as base
import institutional_mapping_v2 as v2
import institutional_mapping_v3 as v3
import institutional_mapping_v4 as v4
import institutional_mapping_v5 as v5
import institutional_mapping_v6 as v6
from db import connect

METHOD_VERSION = "identity_mapping_v1_strict_exact"
RECOVERY_VERSION = "identity_mapping_recovery_v1"
RECOVERY_MINUTES = max(10, int(os.getenv("IDENTITY_MAPPING_RECOVERY_MINUTES", "30")))

STATE_SCHEMA = pa.schema([
    ("source_contract_id", pa.string()),
    ("semantic_version_no", pa.int64()),
    ("semantic_received_at", pa.timestamp("us", tz="UTC")),
    ("method_version", pa.string()),
    ("processed_at", pa.timestamp("us", tz="UTC")),
    ("last_pass_id", pa.string()),
])


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


@contextmanager
def _method_scope():
    modules = [base, v2, v3, v4, v5, v6]
    previous = [(module, getattr(module, "METHOD_VERSION", None)) for module in modules]
    try:
        for module, _ in previous:
            module.METHOD_VERSION = METHOD_VERSION
        yield
    finally:
        for module, value in previous:
            if value is not None:
                module.METHOD_VERSION = value


def _rows_dict(cur) -> list[dict]:
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def recover_abandoned_identity_passes(con) -> dict:
    cutoff = now_utc() - timedelta(minutes=RECOVERY_MINUTES)
    stale = _rows_dict(con.execute("""
        SELECT pass_id,method_version,started_at
        FROM er_mapping_identity_passes
        WHERE status='running' AND started_at<?
        ORDER BY started_at
    """, [cutoff]))
    for row in stale:
        con.execute("""
            UPDATE er_mapping_identity_passes SET status='abandoned',finished_at=now(),details=?
            WHERE pass_id=? AND status='running'
        """, [json.dumps({
            "recovery": {
                "method_version": RECOVERY_VERSION,
                "reason": "identity mapping pass remained running beyond threshold after prior process termination",
                "recovered_at": now_utc().isoformat(),
                "threshold_minutes": RECOVERY_MINUTES,
            }
        }, sort_keys=True), row["pass_id"]])
    running = _rows_dict(con.execute("""
        SELECT pass_id,method_version,started_at FROM er_mapping_identity_passes
        WHERE status='running' ORDER BY started_at
    """))
    return {"recovered": len(stale), "running": running, "threshold_minutes": RECOVERY_MINUTES}


def _work_queue(con) -> list[dict]:
    return _rows_dict(con.execute("""
        SELECT source_contract_id,source_id,current_title,current_resolution_rules,
               closes_at,resolves_at,semantic_version_no,semantic_received_at,
               processed_semantic_version_no
        FROM er_mapping_identity_work_queue
        ORDER BY source_id,source_contract_id
    """))


def _register_changed_ids(con, rows: list[dict]) -> None:
    table = pa.Table.from_pylist(
        [{"source_contract_id": row["source_contract_id"]} for row in rows],
        schema=pa.schema([("source_contract_id", pa.string())]),
    )
    con.register("_identity_changed_arrow", table)
    try:
        con.execute("CREATE OR REPLACE TEMP TABLE _identity_changed_ids AS SELECT * FROM _identity_changed_arrow")
    finally:
        con.unregister("_identity_changed_arrow")


def _exact_pair_rows(con, changed_rows: list[dict]) -> list[dict]:
    if not changed_rows:
        return []
    _register_changed_ids(con, changed_rows)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _identity_pair_ids AS
        SELECT DISTINCT least(l.source_contract_id,r.source_contract_id) left_id,
                        greatest(l.source_contract_id,r.source_contract_id) right_id
        FROM _identity_changed_ids ch
        JOIN er_mapping_contract_index l USING(source_contract_id)
        JOIN er_mapping_contract_index r
          ON r.active=TRUE
         AND r.source_id<>l.source_id
         AND r.source_contract_id<>l.source_contract_id
         AND r.normalized_title=l.normalized_title
         AND r.normalized_title<>''
         AND r.normalized_rules=l.normalized_rules
         AND r.normalized_rules<>''
         AND (
              l.deadline_at IS NULL OR r.deadline_at IS NULL
              OR abs(date_diff('hour',l.deadline_at,r.deadline_at))<=24
         )
        WHERE l.active=TRUE
    """)
    return _rows_dict(con.execute("""
        SELECT p.left_id,p.right_id,
               l.semantic_version_no AS left_semantic_version_no,
               r.semantic_version_no AS right_semantic_version_no,
               CASE
                 WHEN l.deadline_at IS NULL OR r.deadline_at IS NULL THEN NULL
                 ELSE abs(date_diff('hour',l.deadline_at,r.deadline_at))::DOUBLE
               END AS deadline_distance_hours
        FROM _identity_pair_ids p
        JOIN er_mapping_contract_index l ON l.source_contract_id=p.left_id
        JOIN er_mapping_contract_index r ON r.source_contract_id=p.right_id
        ORDER BY p.left_id,p.right_id
    """))


def _exact_candidates(rows: list[dict]) -> list[dict]:
    output = []
    for row in rows:
        left_version = row.get("left_semantic_version_no")
        right_version = row.get("right_semantic_version_no")
        candidate_id = base.stable_id("cand", row["left_id"], row["right_id"], METHOD_VERSION)
        candidate_version_id = base.stable_id(
            "candv", row["left_id"], row["right_id"], METHOD_VERSION,
            left_version if left_version is not None else "legacy",
            right_version if right_version is not None else "legacy",
        )
        output.append({
            "candidate_id": candidate_id,
            "candidate_version_id": candidate_version_id,
            "left_source_contract_id": row["left_id"],
            "right_source_contract_id": row["right_id"],
            "suggested_relationship": "same_event",
            "title_similarity": 1.0,
            "deadline_distance_hours": row.get("deadline_distance_hours"),
            "overall_confidence": 1.0,
            "candidate_method": "strict_exact_title+deadline+resolution_rules",
            "method_version": METHOD_VERSION,
            "resolution_similarity": 1.0,
            "resolution_compatible": True,
            "rules_available_both": True,
            "left_semantic_version_no": left_version,
            "right_semantic_version_no": right_version,
        })
    return output


def _mark_processed(con, changed_rows: list[dict], pass_id: str) -> None:
    if not changed_rows:
        return
    processed_at = now_utc()
    rows = [{
        "source_contract_id": row["source_contract_id"],
        "semantic_version_no": row.get("semantic_version_no"),
        "semantic_received_at": row.get("semantic_received_at"),
        "method_version": METHOD_VERSION,
        "processed_at": processed_at,
        "last_pass_id": pass_id,
    } for row in changed_rows]
    table = pa.Table.from_pylist(rows, schema=STATE_SCHEMA)
    con.register("_identity_state_arrow", table)
    try:
        con.execute("""
            INSERT INTO er_mapping_identity_state(
              source_contract_id,semantic_version_no,semantic_received_at,method_version,processed_at,last_pass_id)
            SELECT source_contract_id,semantic_version_no,semantic_received_at,method_version,processed_at,last_pass_id
            FROM _identity_state_arrow
            ON CONFLICT(source_contract_id) DO UPDATE SET
              semantic_version_no=excluded.semantic_version_no,
              semantic_received_at=excluded.semantic_received_at,
              method_version=excluded.method_version,
              processed_at=excluded.processed_at,
              last_pass_id=excluded.last_pass_id
        """)
    finally:
        con.unregister("_identity_state_arrow")


def run_identity_pass() -> dict:
    pass_id = f"idmap_{uuid.uuid4().hex}"
    con = connect()
    try:
        con.execute("SET TimeZone='UTC'")
        recovery = recover_abandoned_identity_passes(con)
        if recovery["running"]:
            raise RuntimeError(f"another identity mapping pass is still running: {recovery['running']}")
        con.execute("""
            INSERT INTO er_mapping_identity_passes(pass_id,method_version,started_at,status)
            VALUES (?,?,now(),'running')
        """, [pass_id, METHOD_VERSION])
        started = time.monotonic()
        try:
            with _method_scope():
                t0 = time.monotonic()
                native = v4._bootstrap_native_events_batch(con)
                bootstrap_seconds = time.monotonic() - t0

                t0 = time.monotonic()
                v5._seed_index_if_needed(con)
                v5._sync_inactive_index(con)
                changed_rows = _work_queue(con)
                if changed_rows:
                    v5._upsert_index_batch(con, changed_rows)
                indexing_seconds = time.monotonic() - t0

                t0 = time.monotonic()
                pair_rows = _exact_pair_rows(con, changed_rows)
                exact_candidates = _exact_candidates(pair_rows)
                candidate_seconds = time.monotonic() - t0

                t0 = time.monotonic()
                inserted = v6._persist_candidates(con, exact_candidates)
                accepted = v6._auto_accept_exact(con)
                _mark_processed(con, changed_rows, pass_id)
                persistence_seconds = time.monotonic() - t0

            elapsed = time.monotonic() - started
            details = {
                "deterministic_identity_only": True,
                "fuzzy_review_candidates_excluded": True,
                "native_bootstrap": native,
                "recovery": recovery,
                "timing": {
                    "bootstrap_seconds": round(bootstrap_seconds, 6),
                    "indexing_seconds": round(indexing_seconds, 6),
                    "exact_candidate_seconds": round(candidate_seconds, 6),
                    "persistence_seconds": round(persistence_seconds, 6),
                    "total_seconds": round(elapsed, 6),
                },
            }
            con.execute("""
                UPDATE er_mapping_identity_passes SET finished_at=now(),status='success',
                  contracts_changed=?,exact_pairs_evaluated=?,exact_candidates_inserted=?,
                  exact_mappings_accepted=?,details=? WHERE pass_id=?
            """, [
                len(changed_rows),len(pair_rows),inserted,accepted,
                json.dumps(details, sort_keys=True, default=str),pass_id,
            ])
            return {
                "pass_id": pass_id,
                "contracts_changed": len(changed_rows),
                "native_events_created": native["events"],
                "exact_pairs_evaluated": len(pair_rows),
                "exact_candidates_inserted": inserted,
                "exact_mappings_accepted": accepted,
                "timing": details["timing"],
                "method_version": METHOD_VERSION,
            }
        except Exception as exc:
            con.execute("""
                UPDATE er_mapping_identity_passes SET finished_at=now(),status='failed',details=?
                WHERE pass_id=?
            """, [json.dumps({"error": str(exc)[:4000]}, sort_keys=True), pass_id])
            raise
    finally:
        con.close()


if __name__ == "__main__":
    print(run_identity_pass())
