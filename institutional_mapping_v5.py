"""Scalable indexed canonical mapping v5.

Judgement is unchanged from mapping v4:
- every active source contract receives a source-native canonical identity;
- automatic `same_event` requires exact normalized title, <=24h deadline compatibility,
  and exact normalized resolution rules from both sources;
- similar/ambiguous contracts remain distinct review candidates;
- broader/narrower/overlap/conditional relationships never merge probabilities.

Execution changes materially. v5 persists normalized title/rule and meaningful-token indexes,
uses the semantic-version watermark as the work queue, discovers cross-source blocks in SQL,
and transfers only bounded candidate pairs to Python for the same v4 scoring. A method-version
upgrade does not by itself reprocess the full contract universe.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

import pyarrow as pa

import institutional_mapping as base
import institutional_mapping_v2 as v2
import institutional_mapping_v3 as v3
import institutional_mapping_v4 as v4
from db import connect

METHOD_VERSION = "mapping_rules_v5_indexed"
RECOVERY_VERSION = "mapping_pass_recovery_v1"
MAX_BLOCK_MEMBERS = max(25, int(os.getenv("MAPPING_MAX_BLOCK_MEMBERS", "250")))
MAX_BLOCK_TOKENS = max(1, int(os.getenv("MAPPING_MAX_BLOCK_TOKENS", "5")))
MIN_TITLE_SIMILARITY = 0.72
INDEX_BATCH_ROWS = max(1000, int(os.getenv("MAPPING_INDEX_BATCH_ROWS", "20000")))
RECOVERY_MINUTES = max(10, int(os.getenv("MAPPING_RUN_RECOVERY_MINUTES", "30")))

# Inherited review and exact-merge helpers must stamp the active method version.
base.METHOD_VERSION = METHOD_VERSION
v2.METHOD_VERSION = METHOD_VERSION
v3.METHOD_VERSION = METHOD_VERSION
v4.METHOD_VERSION = METHOD_VERSION
RELATIONSHIP_TYPES = v3.RELATIONSHIP_TYPES
DIRECTIONS = v3.DIRECTIONS
review_candidate = v3.review_candidate


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
STATE_SCHEMA = pa.schema([
    ("source_contract_id", pa.string()),
    ("semantic_version_no", pa.int64()),
    ("semantic_received_at", pa.timestamp("us", tz="UTC")),
    ("method_version", pa.string()),
    ("processed_at", pa.timestamp("us", tz="UTC")),
    ("last_pass_id", pa.string()),
])
ID_SCHEMA = pa.schema([("source_contract_id", pa.string())])


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _rows_dict(cur) -> list[dict]:
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _deadline(row: dict) -> Optional[datetime]:
    return row.get("resolves_at") or row.get("closes_at")


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


def _arrow(rows: list[dict], schema: pa.Schema) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=schema) if rows else pa.Table.from_pylist([], schema=schema)


def _upsert_index_batch(con, rows: list[dict]) -> dict:
    if not rows:
        return {"contracts": 0, "tokens": 0}
    index_rows: list[dict] = []
    token_rows: list[dict] = []
    for row in rows:
        index_row, tokens = _index_payload(row)
        index_rows.append(index_row)
        token_rows.extend(tokens)

    con.register("_mapping_index_arrow", _arrow(index_rows, INDEX_SCHEMA))
    con.register("_mapping_tokens_arrow", _arrow(token_rows, TOKEN_SCHEMA))
    try:
        con.execute("""
            DELETE FROM er_mapping_token_index
            WHERE source_contract_id IN (SELECT source_contract_id FROM _mapping_index_arrow)
        """)
        con.execute("""
            INSERT INTO er_mapping_contract_index(
              source_contract_id,source_id,semantic_version_no,normalized_title,normalized_rules,
              deadline_at,deadline_bucket,meaningful_token_count,active,indexed_at,method_version)
            SELECT source_contract_id,source_id,semantic_version_no,normalized_title,normalized_rules,
                   deadline_at,deadline_bucket,meaningful_token_count,active,indexed_at,method_version
            FROM _mapping_index_arrow
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
                FROM _mapping_tokens_arrow
                ON CONFLICT(source_contract_id,token) DO UPDATE SET
                  source_id=excluded.source_id,
                  semantic_version_no=excluded.semantic_version_no,
                  method_version=excluded.method_version,
                  indexed_at=excluded.indexed_at
            """)
    finally:
        con.unregister("_mapping_index_arrow")
        con.unregister("_mapping_tokens_arrow")
    return {"contracts": len(index_rows), "tokens": len(token_rows)}


def _seed_index_if_needed(con) -> dict:
    existing = int(con.execute("SELECT count(*) FROM er_mapping_contract_index").fetchone()[0])
    if existing:
        return {"seeded": False, "contracts": 0, "tokens": 0, "existing": existing}

    cur = con.execute("""
        SELECT c.source_contract_id,c.source_id,c.current_title,c.current_resolution_rules,
               c.closes_at,c.resolves_at,s.version_no AS semantic_version_no
        FROM er_source_contracts c
        LEFT JOIN er_latest_contract_semantic_versions s USING(source_contract_id)
        WHERE c.active=TRUE
        ORDER BY c.source_id,c.source_contract_id
    """)
    cols = [d[0] for d in cur.description]
    total_contracts = 0
    total_tokens = 0
    while True:
        batch = cur.fetchmany(INDEX_BATCH_ROWS)
        if not batch:
            break
        rows = [dict(zip(cols, row)) for row in batch]
        result = _upsert_index_batch(con, rows)
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


def recover_abandoned_mapping_passes(con) -> dict:
    cutoff = now_utc() - timedelta(minutes=RECOVERY_MINUTES)
    stale = _rows_dict(con.execute("""
        SELECT pass_id,method_version,started_at
        FROM er_mapping_passes
        WHERE status='running' AND started_at<?
        ORDER BY started_at
    """, [cutoff]))
    for row in stale:
        detail = {
            "recovery": {
                "method_version": RECOVERY_VERSION,
                "reason": "mapping pass remained running beyond threshold after prior worker/process termination",
                "recovered_at": now_utc().isoformat(),
                "threshold_minutes": RECOVERY_MINUTES,
            }
        }
        con.execute("""
            UPDATE er_mapping_passes SET status='abandoned',finished_at=now(),details=?
            WHERE pass_id=? AND status='running'
        """, [json.dumps(detail, sort_keys=True), row["pass_id"]])

    running = _rows_dict(con.execute("""
        SELECT pass_id,method_version,started_at FROM er_mapping_passes
        WHERE status='running' ORDER BY started_at
    """))
    return {"recovered": len(stale), "stale": stale, "running": running, "threshold_minutes": RECOVERY_MINUTES}


def _work_queue(con) -> list[dict]:
    return _rows_dict(con.execute("""
        SELECT source_contract_id,source_id,current_title,current_resolution_rules,
               closes_at,resolves_at,semantic_version_no,semantic_received_at,
               indexed_semantic_version_no,processed_semantic_version_no,processed_method_version
        FROM er_mapping_work_queue_v5
        ORDER BY source_id,source_contract_id
    """))


def _register_changed_ids(con, changed_rows: list[dict]) -> None:
    con.register("_mapping_changed_ids_arrow", _arrow([
        {"source_contract_id": row["source_contract_id"]} for row in changed_rows
    ], ID_SCHEMA))
    con.execute("CREATE OR REPLACE TEMP TABLE _mapping_changed_ids AS SELECT * FROM _mapping_changed_ids_arrow")
    con.unregister("_mapping_changed_ids_arrow")


def _candidate_pair_rows(con, changed_rows: list[dict]) -> tuple[list[dict], dict]:
    if not changed_rows:
        return [], {"pairs_blocked": 0, "pairs_evaluated": 0, "token_frequency_rows": 0}
    _register_changed_ids(con, changed_rows)

    con.execute("""
        CREATE OR REPLACE TEMP TABLE _mapping_token_freq AS
        SELECT t.token,count(*)::BIGINT AS token_frequency
        FROM er_mapping_token_index t
        JOIN er_mapping_contract_index i USING(source_contract_id)
        WHERE i.active=TRUE
        GROUP BY t.token
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _mapping_changed_rare_tokens AS
        WITH ranked AS (
          SELECT t.source_contract_id,t.token,f.token_frequency,
                 row_number() OVER(
                   PARTITION BY t.source_contract_id
                   ORDER BY f.token_frequency,t.token
                 ) AS rn
          FROM er_mapping_token_index t
          JOIN _mapping_changed_ids c USING(source_contract_id)
          JOIN _mapping_token_freq f USING(token)
          WHERE f.token_frequency<=?
        )
        SELECT source_contract_id,token,token_frequency
        FROM ranked WHERE rn<=?
    """, [MAX_BLOCK_MEMBERS, MAX_BLOCK_TOKENS])
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _mapping_pair_ids AS
        SELECT DISTINCT least(left_id,right_id) AS left_id,greatest(left_id,right_id) AS right_id
        FROM (
          SELECT c.source_contract_id AS left_id,p.source_contract_id AS right_id
          FROM _mapping_changed_ids ch
          JOIN er_mapping_contract_index c USING(source_contract_id)
          JOIN er_mapping_contract_index p
            ON p.active=TRUE
           AND p.source_id<>c.source_id
           AND p.source_contract_id<>c.source_contract_id
           AND p.normalized_title=c.normalized_title
           AND p.deadline_bucket=c.deadline_bucket
          WHERE c.active=TRUE AND c.normalized_title<>''

          UNION ALL

          SELECT ct.source_contract_id AS left_id,pt.source_contract_id AS right_id
          FROM _mapping_changed_rare_tokens ct
          JOIN er_mapping_token_index pt USING(token)
          JOIN er_mapping_contract_index c ON c.source_contract_id=ct.source_contract_id
          JOIN er_mapping_contract_index p ON p.source_contract_id=pt.source_contract_id
          WHERE c.active=TRUE AND p.active=TRUE
            AND c.source_id<>p.source_id
            AND c.source_contract_id<>p.source_contract_id
        ) pairs
    """)
    blocked = int(con.execute("SELECT count(*) FROM _mapping_pair_ids").fetchone()[0])
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _mapping_pair_shared AS
        SELECT p.left_id,p.right_id,count(*)::INTEGER AS shared_token_count
        FROM _mapping_pair_ids p
        JOIN er_mapping_token_index l ON l.source_contract_id=p.left_id
        JOIN er_mapping_token_index r ON r.source_contract_id=p.right_id AND r.token=l.token
        GROUP BY p.left_id,p.right_id
    """)
    rows = _rows_dict(con.execute("""
        SELECT p.left_id,p.right_id,
               l.source_id AS left_source,r.source_id AS right_source,
               l.semantic_version_no AS left_semantic_version_no,
               r.semantic_version_no AS right_semantic_version_no,
               l.normalized_title AS left_normalized_title,
               r.normalized_title AS right_normalized_title,
               l.normalized_rules AS left_normalized_rules,
               r.normalized_rules AS right_normalized_rules,
               l.deadline_at AS left_deadline,r.deadline_at AS right_deadline,
               l.meaningful_token_count AS left_token_count,
               r.meaningful_token_count AS right_token_count,
               coalesce(s.shared_token_count,0) AS shared_token_count
        FROM _mapping_pair_ids p
        JOIN er_mapping_contract_index l ON l.source_contract_id=p.left_id
        JOIN er_mapping_contract_index r ON r.source_contract_id=p.right_id
        LEFT JOIN _mapping_pair_shared s USING(left_id,right_id)
        WHERE coalesce(s.shared_token_count,0) >=
          CASE WHEN least(l.meaningful_token_count,r.meaningful_token_count)<=2 THEN 1 ELSE 2 END
        ORDER BY p.left_id,p.right_id
    """))
    freq_rows = int(con.execute("SELECT count(*) FROM _mapping_token_freq").fetchone()[0])
    return rows, {"pairs_blocked": blocked, "pairs_evaluated": len(rows), "token_frequency_rows": freq_rows}


def _candidate_rows(pair_rows: list[dict]) -> list[dict]:
    output: list[dict] = []
    for row in pair_rows:
        title_sim = base.title_similarity(row["left_normalized_title"], row["right_normalized_title"])
        if title_sim < MIN_TITLE_SIMILARITY:
            continue
        hours = base.deadline_hours(row.get("left_deadline"), row.get("right_deadline"))
        if hours is not None and hours > 24 * 120:
            continue
        left_rules = row["left_normalized_rules"] or ""
        right_rules = row["right_normalized_rules"] or ""
        rules_both = bool(left_rules and right_rules)
        rules_sim = base.title_similarity(left_rules, right_rules) if rules_both else None
        rules_compatible = rules_both and left_rules == right_rules
        exact_title = row["left_normalized_title"] == row["right_normalized_title"]
        exact_deadline = hours is None or hours <= 24
        suggestion = "same_event" if exact_title and exact_deadline and rules_compatible else "related_only"
        if rules_sim is None:
            confidence = title_sim * (1.0 if exact_deadline else 0.9)
        else:
            confidence = 0.60 * title_sim + 0.30 * rules_sim + (0.10 if exact_deadline else 0.0)
        confidence = min(1.0, max(0.0, confidence))
        left_version = row.get("left_semantic_version_no")
        right_version = row.get("right_semantic_version_no")
        candidate_id = base.stable_id(
            "cand", row["left_id"], row["right_id"],
            left_version if left_version is not None else "legacy",
            right_version if right_version is not None else "legacy",
            METHOD_VERSION,
        )
        output.append({
            "candidate_id": candidate_id,
            "left_source_contract_id": row["left_id"],
            "right_source_contract_id": row["right_id"],
            "suggested_relationship": suggestion,
            "title_similarity": float(title_sim),
            "deadline_distance_hours": hours,
            "overall_confidence": float(confidence),
            "candidate_method": "indexed_rare_token+exact_title+deadline+resolution_rules",
            "method_version": METHOD_VERSION,
            "resolution_similarity": rules_sim,
            "resolution_compatible": bool(rules_compatible),
            "rules_available_both": bool(rules_both),
            "left_semantic_version_no": left_version,
            "right_semantic_version_no": right_version,
        })
    return output


def _persist_candidates(con, rows: list[dict]) -> int:
    if not rows:
        return 0
    before = int(con.execute(
        "SELECT count(*) FROM er_mapping_candidates WHERE method_version=?", [METHOD_VERSION]
    ).fetchone()[0])
    table = pa.Table.from_pylist(rows)
    con.register("_mapping_candidates_v5_arrow", table)
    try:
        con.execute("""
            INSERT INTO er_mapping_candidates(
              candidate_id,left_source_contract_id,right_source_contract_id,suggested_relationship,
              title_similarity,deadline_distance_hours,overall_confidence,candidate_method,method_version,
              resolution_similarity,resolution_compatible,rules_available_both,
              left_semantic_version_no,right_semantic_version_no)
            SELECT candidate_id,left_source_contract_id,right_source_contract_id,suggested_relationship,
                   title_similarity,deadline_distance_hours,overall_confidence,candidate_method,method_version,
                   resolution_similarity,resolution_compatible,rules_available_both,
                   left_semantic_version_no,right_semantic_version_no
            FROM _mapping_candidates_v5_arrow
            ON CONFLICT(candidate_id) DO NOTHING
        """)
    finally:
        con.unregister("_mapping_candidates_v5_arrow")
    after = int(con.execute(
        "SELECT count(*) FROM er_mapping_candidates WHERE method_version=?", [METHOD_VERSION]
    ).fetchone()[0])
    return after - before


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
    con.register("_mapping_state_v5_arrow", _arrow(rows, STATE_SCHEMA))
    try:
        con.execute("""
            INSERT INTO er_mapping_contract_state(
              source_contract_id,semantic_version_no,semantic_received_at,method_version,processed_at,last_pass_id)
            SELECT source_contract_id,semantic_version_no,semantic_received_at,method_version,processed_at,last_pass_id
            FROM _mapping_state_v5_arrow
            ON CONFLICT(source_contract_id) DO UPDATE SET
              semantic_version_no=excluded.semantic_version_no,
              semantic_received_at=excluded.semantic_received_at,
              method_version=excluded.method_version,
              processed_at=excluded.processed_at,
              last_pass_id=excluded.last_pass_id
        """)
    finally:
        con.unregister("_mapping_state_v5_arrow")


def run_mapping_pass() -> dict:
    pass_id = f"mp_{uuid.uuid4().hex}"
    con = connect()
    try:
        recovery = recover_abandoned_mapping_passes(con)
        if recovery["running"]:
            raise RuntimeError(f"another mapping pass is still running: {recovery['running']}")
        con.execute("""
            INSERT INTO er_mapping_passes(pass_id,method_version,started_at,status)
            VALUES (?,?,now(),'running')
        """, [pass_id, METHOD_VERSION])
        started = time.monotonic()
        try:
            t0 = time.monotonic()
            native = v4._bootstrap_native_events_batch(con)
            bootstrap_seconds = time.monotonic() - t0

            t0 = time.monotonic()
            seed = _seed_index_if_needed(con)
            deactivated_index_rows = _sync_inactive_index(con)
            changed_rows = _work_queue(con)
            if changed_rows:
                _upsert_index_batch(con, changed_rows)
            indexing_seconds = time.monotonic() - t0

            t0 = time.monotonic()
            pair_rows, pair_stats = _candidate_pair_rows(con, changed_rows)
            blocking_seconds = time.monotonic() - t0

            t0 = time.monotonic()
            candidates = _candidate_rows(pair_rows)
            scoring_seconds = time.monotonic() - t0

            t0 = time.monotonic()
            inserted = _persist_candidates(con, candidates)
            auto = v3.auto_accept_exact(con)
            _mark_processed(con, changed_rows, pass_id)
            persistence_seconds = time.monotonic() - t0

            orphan_count = int(con.execute("""
                SELECT count(*) FROM (
                  SELECT e.canonical_event_id
                  FROM er_canonical_events e
                  LEFT JOIN er_event_mappings m
                    ON m.canonical_event_id=e.canonical_event_id AND m.valid_to IS NULL
                  WHERE e.status='active'
                  GROUP BY e.canonical_event_id
                  HAVING count(m.mapping_id)=0
                )
            """).fetchone()[0])
            contracts_total = int(con.execute(
                "SELECT count(*) FROM er_source_contracts WHERE active=TRUE"
            ).fetchone()[0])
            elapsed = time.monotonic() - started
            inherited_current = int(con.execute("""
                SELECT count(*)
                FROM er_mapping_contract_state st
                JOIN er_latest_contract_semantic_versions s USING(source_contract_id)
                WHERE st.method_version<>?
                  AND coalesce(st.semantic_version_no,-1)=coalesce(s.version_no,-1)
            """, [METHOD_VERSION]).fetchone()[0])
            details = {
                "incremental": True,
                "persistent_index": True,
                "semantic_watermark_not_method_upgrade": True,
                "native_bootstrap": native,
                "index_seed": seed,
                "deactivated_index_rows": deactivated_index_rows,
                "prior_current_watermarks_adopted_without_rescore": inherited_current,
                "pairs_blocked": pair_stats["pairs_blocked"],
                "pairs_evaluated": pair_stats["pairs_evaluated"],
                "candidate_rows": len(candidates),
                "token_frequency_rows": pair_stats["token_frequency_rows"],
                "block_member_cap": MAX_BLOCK_MEMBERS,
                "block_tokens_per_contract": MAX_BLOCK_TOKENS,
                "active_orphan_events": orphan_count,
                "recovery": recovery,
                "timing": {
                    "bootstrap_seconds": round(bootstrap_seconds, 6),
                    "indexing_seconds": round(indexing_seconds, 6),
                    "blocking_seconds": round(blocking_seconds, 6),
                    "scoring_seconds": round(scoring_seconds, 6),
                    "persistence_seconds": round(persistence_seconds, 6),
                    "total_seconds": round(elapsed, 6),
                },
            }
            con.execute("""
                UPDATE er_mapping_passes SET finished_at=now(),status='success',contracts_total=?,
                  contracts_changed=?,native_events_created=?,candidate_pairs_evaluated=?,
                  candidates_inserted=?,auto_exact_mappings=?,details=? WHERE pass_id=?
            """, [
                contracts_total,len(changed_rows),native["events"],pair_stats["pairs_evaluated"],
                inserted,auto,json.dumps(details,sort_keys=True,default=str),pass_id,
            ])
            return {
                "pass_id": pass_id,
                "native_events_created": native["events"],
                "contracts_total": contracts_total,
                "contracts_changed": len(changed_rows),
                "candidate_pairs_evaluated": pair_stats["pairs_evaluated"],
                "candidates_generated": len(candidates),
                "candidates_inserted": inserted,
                "auto_exact_mappings": auto,
                "active_orphan_events": orphan_count,
                "index_seed": seed,
                "timing": details["timing"],
                "method_version": METHOD_VERSION,
            }
        except Exception as exc:
            con.execute("""
                UPDATE er_mapping_passes SET finished_at=now(),status='failed',details=?
                WHERE pass_id=?
            """, [json.dumps({"error": str(exc)[:4000]},sort_keys=True),pass_id])
            raise
    finally:
        con.close()


if __name__ == "__main__":
    print(run_mapping_pass())
