"""Indexed incremental canonical mapping v6 with semantic-version candidate history.

This is the production successor to the unpromoted v5 experiment. It reuses v5's persistent
contract/token index and semantic-version work queue, while preserving the legacy pair-level
review queue contract. Each regenerated candidate is recorded immutably against the exact
left/right semantic versions. A semantic change reopens the current pair-level review item;
prior review state remains in `er_mapping_candidate_versions`.
"""

from __future__ import annotations

from typing import Optional

import pyarrow as pa

import institutional_mapping as base
import institutional_mapping_v2 as v2
import institutional_mapping_v3 as v3
import institutional_mapping_v4 as v4
import institutional_mapping_v5 as v5
from db import connect

METHOD_VERSION = "mapping_rules_v6_indexed_history"
RELATIONSHIP_TYPES = v3.RELATIONSHIP_TYPES
DIRECTIONS = v3.DIRECTIONS

# Make all inherited helpers stamp one coherent active methodology version.
for module in (base, v2, v3, v4, v5):
    module.METHOD_VERSION = METHOD_VERSION

_BASE_AUTO_ACCEPT_EXACT = v3.auto_accept_exact
_BASE_REVIEW_CANDIDATE = v3.review_candidate


def _candidate_rows(pair_rows: list[dict]) -> list[dict]:
    output: list[dict] = []
    for row in pair_rows:
        title_sim = base.title_similarity(row["left_normalized_title"], row["right_normalized_title"])
        if title_sim < v5.MIN_TITLE_SIMILARITY:
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


def _sync_candidate_history_reviews(con, candidate_id: Optional[str] = None) -> None:
    where = "AND c.candidate_id=?" if candidate_id else ""
    params = [candidate_id] if candidate_id else []
    con.execute(f"""
        UPDATE er_mapping_candidate_versions v SET
          status=c.status,
          review_action=c.review_action,
          review_notes=c.review_notes,
          reviewed_by=c.reviewed_by,
          reviewed_at=c.reviewed_at,
          review_relationship_type=c.review_relationship_type,
          review_direction=c.review_direction
        FROM er_mapping_candidates c
        WHERE v.candidate_id=c.candidate_id
          AND v.left_semantic_version_no IS NOT DISTINCT FROM c.left_semantic_version_no
          AND v.right_semantic_version_no IS NOT DISTINCT FROM c.right_semantic_version_no
          {where}
    """, params)


def _persist_candidates(con, rows: list[dict]) -> int:
    if not rows:
        return 0
    table = pa.Table.from_pylist(rows)
    con.register("_mapping_candidates_v6_arrow", table)
    try:
        # Before replacing a current pair-level queue item, persist any review state against
        # the exact semantic-version history row it belonged to.
        con.execute("""
            UPDATE er_mapping_candidate_versions v SET
              status=c.status,
              review_action=c.review_action,
              review_notes=c.review_notes,
              reviewed_by=c.reviewed_by,
              reviewed_at=c.reviewed_at,
              review_relationship_type=c.review_relationship_type,
              review_direction=c.review_direction,
              superseded_at=CASE
                WHEN c.left_semantic_version_no IS DISTINCT FROM a.left_semantic_version_no
                  OR c.right_semantic_version_no IS DISTINCT FROM a.right_semantic_version_no
                THEN coalesce(v.superseded_at,now()) ELSE v.superseded_at END
            FROM er_mapping_candidates c,_mapping_candidates_v6_arrow a
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
            FROM _mapping_candidates_v6_arrow
            ON CONFLICT(candidate_version_id) DO NOTHING
        """)
        after = int(con.execute(
            "SELECT count(*) FROM er_mapping_candidate_versions WHERE method_version=?", [METHOD_VERSION]
        ).fetchone()[0])

        # Semantic changes invalidate the old review judgement for the current queue item.
        # Delete only the stale current representation; immutable history above retains it.
        con.execute("""
            DELETE FROM er_mapping_candidates c
            USING _mapping_candidates_v6_arrow a
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
            FROM _mapping_candidates_v6_arrow
            ON CONFLICT DO NOTHING
        """)
        return after - before
    finally:
        con.unregister("_mapping_candidates_v6_arrow")


def _auto_accept_exact(con) -> int:
    accepted = _BASE_AUTO_ACCEPT_EXACT(con)
    _sync_candidate_history_reviews(con)
    return accepted


def review_candidate(
    candidate_id: str,
    action: str,
    actor: str,
    notes: Optional[str] = None,
    relationship_type: Optional[str] = None,
    direction: str = "undirected",
) -> dict:
    result = _BASE_REVIEW_CANDIDATE(
        candidate_id, action, actor, notes,
        relationship_type=relationship_type, direction=direction,
    )
    con = connect()
    try:
        _sync_candidate_history_reviews(con, candidate_id)
    finally:
        con.close()
    return result


def run_mapping_pass() -> dict:
    # v5's pass orchestration, recovery, persistent index and work queue are reused unchanged;
    # only candidate materialization/persistence and review history are overridden here.
    v5._candidate_rows = _candidate_rows
    v5._persist_candidates = _persist_candidates
    v3.auto_accept_exact = _auto_accept_exact
    result = v5.run_mapping_pass()
    result["method_version"] = METHOD_VERSION
    result["candidate_history"] = "semantic_versioned_v1"
    return result


if __name__ == "__main__":
    print(run_mapping_pass())
