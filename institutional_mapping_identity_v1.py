"""Deterministic canonical identity lane for institutional event normalization.

This module intentionally excludes fuzzy similarity. It gives every active source contract a
source-native canonical identity, then auto-merges only cross-source pairs satisfying the same
strict identity rule used by the prior mapper: exact normalized title, exact non-empty normalized
resolution rules, and <=24h (or missing) deadline compatibility. A separate semantic watermark
lets this lane run independently from slower review-candidate discovery.

The implementation deliberately does not import mapping_v5/v6 because those compatibility
modules patch predecessor METHOD_VERSION globals at import time. This lane owns its index and
candidate-history writes so importing it cannot mutate another mapping generation.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional

import pyarrow as pa

import institutional_mapping as base
import institutional_mapping_v2 as v2
import institutional_mapping_v3 as v3
import institutional_mapping_v4 as v4
from db import connect

METHOD_VERSION = "identity_mapping_v1_strict_exact"
RECOVERY_VERSION = "identity_mapping_recovery_v1"
RECOVERY_MINUTES = max(10, int(os.getenv("IDENTITY_MAPPING_RECOVERY_MINUTES", "30")))
INDEX_BATCH_ROWS = max(1000, int(os.getenv("MAPPING_INDEX_BATCH_ROWS", "20000")))

STATE_SCHEMA = pa.schema([
    ("source_contract_id", pa.string()),
    ("semantic_version_no", pa.int64()),
    ("semantic_received_at", pa.timestamp("us", tz="UTC")),
    ("method_version", pa.string()),
    ("processed_at", pa.timestamp("us", tz="UTC")),
    ("last_pass_id", pa.string()),
])
INDEX_SCHEMA = pa.schema([
    ("source_contract_id", pa.string()),
    ("source_id", pa.string()),
    ("semantic_version_no", pa.int64()),
    ("normalized_title", pa.string()),
    ("normalized_rules", pa.string()),
    ("deadline_at", pa.timestamp("us", tz="UTC")),
    ("deadline_bucket", pa.string()),
    ("meaningful_token_count", pa.int32()),
    ("active", pa.bool_()),
    ("indexed_at", pa.timestamp("us", tz="UTC")),
    ("method_version", pa.string()),
])
TOKEN_SCHEMA = pa.schema([
    ("source_contract_id", pa.string()),
    ("source_id", pa.string()),
    ("semantic_version_no", pa.int64()),
    ("token", pa.string()),
    ("method_version", pa.string()),
    ("indexed_at", pa.timestamp("us", tz="UTC")),
])


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


@contextmanager
def _method_scope():
    # v3's stable merge primitive stamps METHOD_VERSION. Save/restore every predecessor global
    # touched by v4 so this lane is process-safe when used beside older modules in tests/tools.
    modules = [base, v2, v3, v4]
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


def _arrow(rows: list[dict], schema: pa.Schema) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=schema) if rows else pa.Table.from_pylist([], schema=schema)


def _deadline(row: dict) -> Optional[datetime]:
    return row.get("resolves_at") or row.get("closes_at")


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


def _index_payload(row: dict) -> tuple[dict, list[dict]]:
    indexed_at = now_utc()
    normalized_title = base.normalize(row.get("current_title") or "")
    normalized_rules = v2.normalized_rules(row.get("current_resolution_rules"))
    tokens = sorted(v2.meaningful_tokens(row.get("current_title") or ""))
    deadline = _deadline(row)
    index_row = {
        "source_contract_id": row["source_contract_id"],
        "source_id": row["source_id"],
        "semantic_version_no": row.get("semantic_version_no"),
        "normalized_title": normalized_title,
        "normalized_rules": normalized_rules,
        "deadline_at": deadline,
        "deadline_bucket": v4._deadline_bucket(deadline),
        "meaningful_token_count": len(tokens),
        "active": True,
        "indexed_at": indexed_at,
        "method_version": METHOD_VERSION,
    }
    token_rows = [{
        "source_contract_id": row["source_contract_id"],
        "source_id": row["source_id"],
        "semantic_version_no": row.get("semantic_version_no"),
        "token": token,
        "method_version": METHOD_VERSION,
        "indexed_at": indexed_at,
    } for token in tokens]
    return index_row, token_rows


def _upsert_index_batch(con, rows: list[dict]) -> dict:
    if not rows:
        return {"contracts": 0, "tokens": 0}
    index_rows: list[dict] = []
    token_rows: list[dict] = []
    for row in rows:
        index_row, tokens = _index_payload(row)
        index_rows.append(index_row)
        token_rows.extend(tokens)

    con.register("_identity_index_arrow", _arrow(index_rows, INDEX_SCHEMA))
    con.register("_identity_tokens_arrow", _arrow(token_rows, TOKEN_SCHEMA))
    try:
        con.execute("""
            DELETE FROM er_mapping_token_index
            WHERE source_contract_id IN (SELECT source_contract_id FROM _identity_index_arrow)
        """)
        con.execute("""
            INSERT INTO er_mapping_contract_index(
              source_contract_id,source_id,semantic_version_no,normalized_title,normalized_rules,
              deadline_at,deadline_bucket,meaningful_token_count,active,indexed_at,method_version)
            SELECT source_contract_id,source_id,semantic_version_no,normalized_title,normalized_rules,
                   deadline_at,deadline_bucket,meaningful_token_count,active,indexed_at,method_version
            FROM _identity_index_arrow
            ON CONFLICT(source_contract_id) DO UPDATE SET
              source_id=excluded.source_id,
              semantic_version_no=excluded.semantic_version_no,
              normalized_title=excluded.normalized_title,
              normalized_rules=excluded.normalized_rules,
              deadline_at=excluded.deadline_at,
              deadline_bucket=excluded.deadline_bucket,
              meaningful_token_count=excluded.meaningful_token_count,
              active=TRUE,
              indexed_at=excluded.indexed_at,
              method_version=excluded.method_version
        """)
        if token_rows:
            con.execute("""
                INSERT INTO er_mapping_token_index(
                  source_contract_id,source_id,semantic_version_no,token,method_version,indexed_at)
                SELECT source_contract_id,source_id,semantic_version_no,token,method_version,indexed_at
                FROM _identity_tokens_arrow
                ON CONFLICT(source_contract_id,token) DO UPDATE SET
                  source_id=excluded.source_id,
                  semantic_version_no=excluded.semantic_version_no,
                  method_version=excluded.method_version,
                  indexed_at=excluded.indexed_at
            """)
    finally:
        con.unregister("_identity_index_arrow")
        con.unregister("_identity_tokens_arrow")
    return {"contracts": len(index_rows), "tokens": len(token_rows)}


def _seed_index_if_needed(con) -> dict:
    existing = int(con.execute("SELECT count(*) FROM er_mapping_contract_index").fetchone()[0])
    if existing:
        return {"seeded": False, "contracts": 0, "tokens": 0, "existing": existing}
    rows = _rows_dict(con.execute("""
        SELECT c.source_contract_id,c.source_id,c.current_title,c.current_resolution_rules,
               c.closes_at,c.resolves_at,s.version_no AS semantic_version_no
        FROM er_source_contracts c
        LEFT JOIN er_latest_contract_semantic_versions s USING(source_contract_id)
        WHERE c.active=TRUE
        ORDER BY c.source_id,c.source_contract_id
    """))
    total_contracts = 0
    total_tokens = 0
    for start in range(0, len(rows), INDEX_BATCH_ROWS):
        result = _upsert_index_batch(con, rows[start:start + INDEX_BATCH_ROWS])
        total_contracts += result["contracts"]
        total_tokens += result["tokens"]
    return {"seeded": True, "contracts": total_contracts, "tokens": total_tokens, "existing": 0}


def _sync_inactive_index(con) -> int:
    before = int(con.execute("SELECT count(*) FROM er_mapping_contract_index WHERE active=TRUE").fetchone()[0])
    con.execute("""
        UPDATE er_mapping_contract_index i SET active=FALSE,indexed_at=now()
        WHERE i.active=TRUE
          AND NOT EXISTS (
            SELECT 1 FROM er_source_contracts c
            WHERE c.source_contract_id=i.source_contract_id AND c.active=TRUE
          )
    """)
    after = int(con.execute("SELECT count(*) FROM er_mapping_contract_index WHERE active=TRUE").fetchone()[0])
    return max(0, before - after)


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


def _persist_exact_candidates(con, rows: list[dict]) -> int:
    if not rows:
        return 0
    table = pa.Table.from_pylist(rows)
    con.register("_identity_candidates_arrow", table)
    try:
        # Preserve review/acceptance state of a prior semantic version before replacing the
        # current pair-level representation.
        con.execute("""
            UPDATE er_mapping_candidate_versions v SET
              status=c.status,review_action=c.review_action,review_notes=c.review_notes,
              reviewed_by=c.reviewed_by,reviewed_at=c.reviewed_at,
              review_relationship_type=c.review_relationship_type,review_direction=c.review_direction,
              superseded_at=CASE
                WHEN c.left_semantic_version_no IS DISTINCT FROM a.left_semantic_version_no
                  OR c.right_semantic_version_no IS DISTINCT FROM a.right_semantic_version_no
                THEN coalesce(v.superseded_at,now()) ELSE v.superseded_at END
            FROM er_mapping_candidates c,_identity_candidates_arrow a
            WHERE c.left_source_contract_id=a.left_source_contract_id
              AND c.right_source_contract_id=a.right_source_contract_id
              AND c.method_version=a.method_version
              AND v.candidate_id=c.candidate_id
              AND v.left_semantic_version_no IS NOT DISTINCT FROM c.left_semantic_version_no
              AND v.right_semantic_version_no IS NOT DISTINCT FROM c.right_semantic_version_no
        """)
        before = int(con.execute(
            "SELECT count(*) FROM er_mapping_candidate_versions WHERE method_version=?", [METHOD_VERSION]
        ).fetchone()[0])
        con.execute("""
            INSERT INTO er_mapping_candidate_versions(
              candidate_version_id,candidate_id,left_source_contract_id,right_source_contract_id,
              left_semantic_version_no,right_semantic_version_no,suggested_relationship,
              title_similarity,deadline_distance_hours,overall_confidence,candidate_method,method_version,
              resolution_similarity,resolution_compatible,rules_available_both,status)
            SELECT candidate_version_id,candidate_id,left_source_contract_id,right_source_contract_id,
                   left_semantic_version_no,right_semantic_version_no,suggested_relationship,
                   title_similarity,deadline_distance_hours,overall_confidence,candidate_method,method_version,
                   resolution_similarity,resolution_compatible,rules_available_both,'pending'
            FROM _identity_candidates_arrow
            ON CONFLICT(candidate_version_id) DO NOTHING
        """)
        con.execute("""
            DELETE FROM er_mapping_candidates c USING _identity_candidates_arrow a
            WHERE c.left_source_contract_id=a.left_source_contract_id
              AND c.right_source_contract_id=a.right_source_contract_id
              AND c.method_version=a.method_version
              AND (
                c.left_semantic_version_no IS DISTINCT FROM a.left_semantic_version_no
                OR c.right_semantic_version_no IS DISTINCT FROM a.right_semantic_version_no
              )
        """)
        con.execute("""
            INSERT INTO er_mapping_candidates(
              candidate_id,left_source_contract_id,right_source_contract_id,suggested_relationship,
              title_similarity,deadline_distance_hours,overall_confidence,candidate_method,method_version,
              resolution_similarity,resolution_compatible,rules_available_both,
              left_semantic_version_no,right_semantic_version_no,status)
            SELECT candidate_id,left_source_contract_id,right_source_contract_id,suggested_relationship,
                   title_similarity,deadline_distance_hours,overall_confidence,candidate_method,method_version,
                   resolution_similarity,resolution_compatible,rules_available_both,
                   left_semantic_version_no,right_semantic_version_no,'pending'
            FROM _identity_candidates_arrow
            ON CONFLICT DO NOTHING
        """)
        after = int(con.execute(
            "SELECT count(*) FROM er_mapping_candidate_versions WHERE method_version=?", [METHOD_VERSION]
        ).fetchone()[0])
        return after - before
    finally:
        con.unregister("_identity_candidates_arrow")


def _auto_accept_exact(con) -> int:
    rows = con.execute("""
        SELECT candidate_id,left_source_contract_id,right_source_contract_id,overall_confidence,
               left_semantic_version_no,right_semantic_version_no
        FROM er_mapping_candidates
        WHERE status='pending' AND suggested_relationship='same_event'
          AND title_similarity=1.0 AND resolution_compatible=TRUE AND rules_available_both=TRUE
          AND (deadline_distance_hours IS NULL OR deadline_distance_hours<=24)
          AND overall_confidence>=0.98 AND method_version=?
        ORDER BY left_source_contract_id,right_source_contract_id
    """, [METHOD_VERSION]).fetchall()
    accepted = 0
    for candidate_id,left_id,right_id,confidence,left_version,right_version in rows:
        result = v3._merge_right_into_left(
            con,left_id,right_id,confidence,"system","auto_exact",
            "Exact normalized title, deadline and resolution rules matched across sources",
            "strict_cross_source_auto",
        )
        con.execute("""
            UPDATE er_mapping_candidates SET
              status='auto_accepted',review_action='same_event',reviewed_by='system',reviewed_at=now(),
              review_relationship_type=NULL,review_direction=NULL
            WHERE candidate_id=?
        """, [candidate_id])
        con.execute("""
            UPDATE er_mapping_candidate_versions SET
              status='auto_accepted',review_action='same_event',reviewed_by='system',reviewed_at=now(),
              review_relationship_type=NULL,review_direction=NULL
            WHERE candidate_id=?
              AND left_semantic_version_no IS NOT DISTINCT FROM ?
              AND right_semantic_version_no IS NOT DISTINCT FROM ?
        """, [candidate_id,left_version,right_version])
        if result["changed"]:
            accepted += 1
    return accepted


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
                seed = _seed_index_if_needed(con)
                inactive = _sync_inactive_index(con)
                changed_rows = _work_queue(con)
                indexed = _upsert_index_batch(con, changed_rows) if changed_rows else {"contracts": 0,"tokens": 0}
                indexing_seconds = time.monotonic() - t0

                t0 = time.monotonic()
                pair_rows = _exact_pair_rows(con, changed_rows)
                exact_candidates = _exact_candidates(pair_rows)
                candidate_seconds = time.monotonic() - t0

                t0 = time.monotonic()
                inserted = _persist_exact_candidates(con, exact_candidates)
                accepted = _auto_accept_exact(con)
                _mark_processed(con, changed_rows, pass_id)
                persistence_seconds = time.monotonic() - t0

            elapsed = time.monotonic() - started
            details = {
                "deterministic_identity_only": True,
                "fuzzy_review_candidates_excluded": True,
                "native_bootstrap": native,
                "index_seed": seed,
                "indexed_changes": indexed,
                "inactive_index_rows": inactive,
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
