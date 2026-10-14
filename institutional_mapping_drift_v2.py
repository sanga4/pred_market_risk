"""Set-based semantic drift detection for active same-event mappings.

The strict equivalence test is unchanged from v1. The warehouse query now fetches only
cross-source contract pairs inside active merged canonical events, and quality-issue
upsert/resolution happens set-wise instead of issuing one query per canonical event.
"""

from __future__ import annotations

import json

import pyarrow as pa

import institutional_mapping_drift as legacy
import institutional_qa as qa_base
from db import connect

METHOD_VERSION = "mapping_drift_v2_set_based"


def _rows_dict(cur) -> list[dict]:
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def find_mapping_drift(con) -> list[dict]:
    pairs = _rows_dict(con.execute("""
        WITH merged AS (
          SELECT canonical_event_id,max(mapped_at) AS decision_at
          FROM er_event_mappings
          WHERE valid_to IS NULL
          GROUP BY canonical_event_id
          HAVING count(*)>1
             AND sum(CASE WHEN mapping_type='same_event' THEN 1 ELSE 0 END)>0
        ), members AS (
          SELECT m.canonical_event_id,m.source_contract_id,m.mapping_type,m.review_status,m.mapped_at,
                 c.source_id,
                 v.title,v.resolution_rules,v.closes_at,v.resolves_at,v.received_at AS semantic_received_at,
                 merged.decision_at
          FROM merged
          JOIN er_event_mappings m USING(canonical_event_id)
          JOIN er_source_contracts c USING(source_contract_id)
          LEFT JOIN er_latest_contract_semantic_versions v USING(source_contract_id)
          WHERE m.valid_to IS NULL
        )
        SELECT l.canonical_event_id,l.decision_at,
               l.source_contract_id AS left_contract_id,r.source_contract_id AS right_contract_id,
               l.source_id AS left_source_id,r.source_id AS right_source_id,
               l.title AS left_title,r.title AS right_title,
               l.resolution_rules AS left_resolution_rules,r.resolution_rules AS right_resolution_rules,
               l.closes_at AS left_closes_at,r.closes_at AS right_closes_at,
               l.resolves_at AS left_resolves_at,r.resolves_at AS right_resolves_at,
               l.semantic_received_at AS left_semantic_received_at,
               r.semantic_received_at AS right_semantic_received_at
        FROM members l
        JOIN members r
          ON r.canonical_event_id=l.canonical_event_id
         AND r.source_contract_id>l.source_contract_id
         AND r.source_id<>l.source_id
        WHERE l.decision_at IS NOT NULL
          AND l.semantic_received_at IS NOT NULL
          AND r.semantic_received_at IS NOT NULL
          AND (l.semantic_received_at>l.decision_at OR r.semantic_received_at>l.decision_at)
        ORDER BY l.canonical_event_id,l.source_contract_id,r.source_contract_id
    """))
    problems: list[dict] = []
    for row in pairs:
        left = {
            "title": row["left_title"],
            "resolution_rules": row["left_resolution_rules"],
            "closes_at": row["left_closes_at"],
            "resolves_at": row["left_resolves_at"],
        }
        right = {
            "title": row["right_title"],
            "resolution_rules": row["right_resolution_rules"],
            "closes_at": row["right_closes_at"],
            "resolves_at": row["right_resolves_at"],
        }
        equivalent, detail = legacy._strictly_equivalent(left, right)
        if not equivalent:
            problems.append({
                "canonical_event_id": row["canonical_event_id"],
                "decision_at": row["decision_at"],
                "left_contract_id": row["left_contract_id"],
                "right_contract_id": row["right_contract_id"],
                "left_semantic_received_at": row["left_semantic_received_at"],
                "right_semantic_received_at": row["right_semantic_received_at"],
                **detail,
            })
    return problems


def _sync_issues(con, problems: list[dict]) -> int:
    grouped: dict[str, list[dict]] = {}
    for problem in problems:
        grouped.setdefault(problem["canonical_event_id"], []).append(problem)
    rows = []
    for event_id, items in grouped.items():
        rows.append({
            "quality_issue_id": qa_base.issue_id("canonical_event", event_id, "same_event_mapping_semantic_drift"),
            "object_id": event_id,
            "details": json.dumps(items, default=str, sort_keys=True)[:12000],
        })
    if rows:
        con.register("_mapping_drift_issues_arrow", pa.Table.from_pylist(rows))
        try:
            con.execute("""
                INSERT INTO er_quality_issues(
                  quality_issue_id,object_type,object_id,rule_code,severity,status,details,detected_at,
                  resolved_at,resolution_notes)
                SELECT quality_issue_id,'canonical_event',object_id,
                       'same_event_mapping_semantic_drift','critical','open',details,now(),NULL,NULL
                FROM _mapping_drift_issues_arrow
                ON CONFLICT(quality_issue_id) DO UPDATE SET
                  severity='critical',status='open',details=excluded.details,detected_at=now(),
                  resolved_at=NULL,resolution_notes=NULL
            """)
            con.execute("""
                UPDATE er_quality_issues q
                SET status='resolved',resolved_at=now(),resolution_notes='Mapped contract semantics satisfy the strict identity criteria again'
                WHERE q.status='open'
                  AND q.object_type='canonical_event'
                  AND q.rule_code='same_event_mapping_semantic_drift'
                  AND NOT EXISTS (
                    SELECT 1 FROM _mapping_drift_issues_arrow d WHERE d.object_id=q.object_id
                  )
            """)
        finally:
            con.unregister("_mapping_drift_issues_arrow")
    else:
        con.execute("""
            UPDATE er_quality_issues
            SET status='resolved',resolved_at=now(),resolution_notes='Mapped contract semantics satisfy the strict identity criteria again'
            WHERE status='open' AND object_type='canonical_event'
              AND rule_code='same_event_mapping_semantic_drift'
        """)
    return len(grouped)


def persist_mapping_drift() -> dict:
    con = connect()
    try:
        problems = find_mapping_drift(con)
        con.execute("BEGIN TRANSACTION")
        try:
            drifted = _sync_issues(con, problems)
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        return {
            "drifted_canonical_events": drifted,
            "incompatible_pairs": len(problems),
            "method_version": METHOD_VERSION,
        }
    finally:
        con.close()


if __name__ == "__main__":
    print(persist_mapping_drift())
