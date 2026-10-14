"""Institutional mapping v3: conservative identity plus explicit reviewed event relationships.

The product boundary is deliberate:
- source contracts are always preserved;
- `same_event` is the only review action that changes canonical identity;
- broader/narrower/overlap/conditional relationships never merge probabilities;
- relationship direction, reviewer, candidate provenance and evidence are recorded;
- canonical events with no remaining active source mappings are marked superseded so they
  do not pollute the default served event universe, while their history remains queryable.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Optional

import institutional_mapping as base
import institutional_mapping_v2 as v2
from db import connect
from institutional_ontology import append_event_version, ensure_event_baseline

METHOD_VERSION = "mapping_rules_v3_reviewable"

RELATIONSHIP_TYPES = {
    "related_to",
    "broader_than",
    "narrower_than",
    "overlaps_with",
    "mutually_exclusive_with",
    "complement_of",
    "conditional_on",
    "prerequisite_for",
}
SYMMETRIC_RELATIONSHIPS = {
    "related_to",
    "overlaps_with",
    "mutually_exclusive_with",
    "complement_of",
}
DIRECTIONS = {"left_to_right", "right_to_left", "undirected"}

# Keep the v2 candidate generator, but version every newly generated candidate under v3.
v2.METHOD_VERSION = METHOD_VERSION
base.METHOD_VERSION = METHOD_VERSION


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _mapping(con, contract_id: str):
    return con.execute(
        """SELECT mapping_id,canonical_event_id
           FROM er_latest_contract_mappings WHERE source_contract_id=?""",
        [contract_id],
    ).fetchone()


def _retire_orphan_event(con, event_id: str, replacement_event_id: str, actor: str, reason: str) -> bool:
    if event_id == replacement_event_id:
        return False
    remaining = con.execute(
        "SELECT count(*) FROM er_event_mappings WHERE canonical_event_id=? AND valid_to IS NULL",
        [event_id],
    ).fetchone()[0]
    if remaining:
        return False
    row = con.execute(
        "SELECT status FROM er_canonical_events WHERE canonical_event_id=?",
        [event_id],
    ).fetchone()
    if not row or row[0] == "superseded":
        return False

    changed_at = now_utc()
    ensure_event_baseline(con, event_id)
    old_status = row[0]
    con.execute(
        "UPDATE er_canonical_events SET status='superseded',updated_at=? WHERE canonical_event_id=?",
        [changed_at, event_id],
    )
    append_event_version(con, event_id, changed_at, "canonical_merge_superseded")
    con.execute(
        """INSERT INTO er_revisions(
             revision_id,object_type,object_id,revision_type,reason,previous_version,new_version,
             effective_at,actor)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        [
            f"rev_{uuid.uuid4().hex}",
            "canonical_event",
            event_id,
            "canonical_merge_superseded",
            reason,
            old_status,
            f"superseded_by:{replacement_event_id}",
            changed_at,
            actor,
        ],
    )
    return True


def _merge_right_into_left(con, left_id: str, right_id: str, confidence: float | None,
                           actor: str, review_status: str, notes: Optional[str], method: str) -> dict:
    left = _mapping(con, left_id)
    right = _mapping(con, right_id)
    if not left or not right:
        raise ValueError("both contracts must have active canonical mappings")
    target_event = left[1]
    previous_event = right[1]
    if target_event == previous_event:
        return {"changed": False, "target_event": target_event, "previous_event": previous_event, "retired": False}

    changed_at = now_utc()
    con.execute("UPDATE er_event_mappings SET valid_to=? WHERE mapping_id=?", [changed_at, right[0]])
    new_mapping_id = f"map_{uuid.uuid4().hex}"
    con.execute(
        """INSERT INTO er_event_mappings(
             mapping_id,source_contract_id,canonical_event_id,mapping_type,confidence,mapping_method,
             method_version,review_status,review_notes,mapped_at,valid_from,supersedes_mapping_id,created_by)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        [
            new_mapping_id,
            right_id,
            target_event,
            "same_event",
            confidence,
            method,
            METHOD_VERSION,
            review_status,
            notes,
            changed_at,
            changed_at,
            right[0],
            actor,
        ],
    )
    con.execute(
        """INSERT INTO er_revisions(
             revision_id,object_type,object_id,revision_type,reason,previous_version,new_version,
             effective_at,actor)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        [
            f"rev_{uuid.uuid4().hex}",
            "event_mapping",
            right_id,
            "canonical_merge",
            notes or "Cross-source contracts reviewed as the same canonical event",
            previous_event,
            target_event,
            changed_at,
            actor,
        ],
    )
    retired = _retire_orphan_event(
        con,
        previous_event,
        target_event,
        actor,
        notes or "Source-native canonical event was superseded by an identity merge",
    )
    return {"changed": True, "target_event": target_event, "previous_event": previous_event, "retired": retired}


def auto_accept_exact(con) -> int:
    """Auto-merge only exact title/deadline candidates whose full normalized rules match."""
    rows = con.execute(
        """SELECT candidate_id,left_source_contract_id,right_source_contract_id,overall_confidence
           FROM er_mapping_candidates
           WHERE status='pending'
             AND suggested_relationship='same_event'
             AND title_similarity=1.0
             AND resolution_compatible=TRUE
             AND rules_available_both=TRUE
             AND (deadline_distance_hours IS NULL OR deadline_distance_hours<=24)
             AND overall_confidence>=0.98
             AND method_version=?""",
        [METHOD_VERSION],
    ).fetchall()
    accepted = 0
    for candidate_id, left_id, right_id, confidence in rows:
        result = _merge_right_into_left(
            con,
            left_id,
            right_id,
            confidence,
            "system",
            "auto_exact",
            "Exact normalized title, deadline and resolution rules matched across sources",
            "strict_cross_source_auto",
        )
        con.execute(
            """UPDATE er_mapping_candidates
               SET status='auto_accepted',review_action='same_event',reviewed_by='system',reviewed_at=now(),
                   review_relationship_type=NULL,review_direction=NULL
               WHERE candidate_id=?""",
            [candidate_id],
        )
        if result["changed"]:
            accepted += 1
    return accepted


def _relationship_orientation(left_event: str, right_event: str, relationship_type: str, direction: str):
    if direction not in DIRECTIONS:
        raise ValueError(f"direction must be one of {sorted(DIRECTIONS)}")
    if relationship_type in SYMMETRIC_RELATIONSHIPS:
        a, b = sorted([left_event, right_event])
        return a, b, "undirected"
    if direction == "undirected":
        raise ValueError(f"{relationship_type} is directional; choose left_to_right or right_to_left")
    if direction == "left_to_right":
        return left_event, right_event, direction
    return right_event, left_event, direction


def _record_relationship(con, candidate_id: str, left_id: str, right_id: str,
                         relationship_type: str, direction: str, confidence: float | None,
                         actor: str, notes: Optional[str]) -> str:
    if relationship_type not in RELATIONSHIP_TYPES:
        raise ValueError(f"relationship_type must be one of {sorted(RELATIONSHIP_TYPES)}")
    left_map = _mapping(con, left_id)
    right_map = _mapping(con, right_id)
    if not left_map or not right_map:
        raise ValueError("both contracts must have active canonical mappings")
    left_event, right_event = left_map[1], right_map[1]
    if left_event == right_event:
        raise ValueError("contracts already resolve to the same canonical event; relationship would be self-referential")
    source_event, target_event, normalized_direction = _relationship_orientation(
        left_event, right_event, relationship_type, direction
    )
    relationship_id = base.stable_id(
        "rel", source_event, relationship_type, target_event, candidate_id, METHOD_VERSION
    )
    con.execute(
        """INSERT INTO er_event_relationships(
             relationship_id,source_event_id,relationship_type,target_event_id,relationship_class,
             confidence,method,method_version,human_reviewed,review_status,review_notes,
             source_candidate_id,created_by)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(relationship_id) DO NOTHING""",
        [
            relationship_id,
            source_event,
            relationship_type,
            target_event,
            "reviewed_relationship",
            confidence,
            "human_review",
            METHOD_VERSION,
            True,
            "reviewed",
            notes,
            candidate_id,
            actor,
        ],
    )

    contract_rows = con.execute(
        """SELECT source_contract_id,source_id,current_title
           FROM er_source_contracts WHERE source_contract_id IN (?,?)""",
        [left_id, right_id],
    ).fetchall()
    for contract_id, source_id, title in contract_rows:
        evidence_id = base.stable_id("relev", relationship_id, contract_id)
        con.execute(
            """INSERT INTO er_event_relationship_evidence(
                 relationship_id,evidence_id,evidence_type,source_id,source_record_id,excerpt)
               VALUES (?,?,?,?,?,?) ON CONFLICT(relationship_id,evidence_id) DO NOTHING""",
            [relationship_id, evidence_id, "mapping_review_source_contract", source_id, contract_id, (title or "")[:1000]],
        )
    return relationship_id


def review_candidate(candidate_id: str, action: str, actor: str, notes: Optional[str] = None,
                     relationship_type: Optional[str] = None, direction: str = "undirected") -> dict:
    """Review one mapping candidate without converting semantic similarity into investment judgement."""
    if action not in {"same_event", "relationship", "related_only", "reject"}:
        raise ValueError("action must be same_event, relationship, related_only, or reject")
    if action == "related_only":
        action = "relationship"
        relationship_type = relationship_type or "related_to"
        direction = "undirected"

    con = connect()
    try:
        row = con.execute(
            """SELECT left_source_contract_id,right_source_contract_id,overall_confidence,status
               FROM er_mapping_candidates WHERE candidate_id=?""",
            [candidate_id],
        ).fetchone()
        if not row:
            raise KeyError(candidate_id)
        left_id, right_id, confidence, status = row
        if status not in {"pending", "auto_accepted"}:
            raise ValueError(f"candidate already reviewed: {status}")

        relationship_id = None
        merge_result = None
        if action == "same_event":
            merge_result = _merge_right_into_left(
                con,
                left_id,
                right_id,
                confidence,
                actor,
                "reviewed",
                notes,
                "human_review",
            )
        elif action == "relationship":
            if not relationship_type:
                raise ValueError("relationship_type is required when action=relationship")
            relationship_id = _record_relationship(
                con,
                candidate_id,
                left_id,
                right_id,
                relationship_type,
                direction,
                confidence,
                actor,
                notes,
            )

        review_relationship_type = relationship_type if action == "relationship" else None
        review_direction = direction if action == "relationship" else None
        con.execute(
            """UPDATE er_mapping_candidates
               SET status='reviewed',review_action=?,review_notes=?,reviewed_by=?,reviewed_at=now(),
                   review_relationship_type=?,review_direction=?
               WHERE candidate_id=?""",
            [action, notes, actor, review_relationship_type, review_direction, candidate_id],
        )
        con.execute(
            """INSERT INTO er_admin_actions(action_id,action_type,object_type,object_id,actor,details)
               VALUES (?,?,?,?,?,?)""",
            [
                f"act_{uuid.uuid4().hex}",
                "mapping_review",
                "mapping_candidate",
                candidate_id,
                actor,
                f"action={action}; relationship_type={relationship_type or ''}; direction={direction}; notes={notes or ''}",
            ],
        )
        return {
            "candidate_id": candidate_id,
            "action": action,
            "relationship_type": relationship_type if action == "relationship" else None,
            "direction": direction if action == "relationship" else None,
            "relationship_id": relationship_id,
            "merge": merge_result,
            "status": "reviewed",
        }
    finally:
        con.close()


def run_mapping_pass() -> dict:
    con = connect()
    try:
        native = base.bootstrap_native_events(con)
        candidates = v2.generate_candidates(con)
        auto = auto_accept_exact(con)
        orphan_active = con.execute(
            """SELECT count(*) FROM er_canonical_events e
               LEFT JOIN er_event_mappings m
                 ON m.canonical_event_id=e.canonical_event_id AND m.valid_to IS NULL
               WHERE e.status='active'
               GROUP BY e.canonical_event_id
               HAVING count(m.mapping_id)=0"""
        ).fetchall()
        return {
            "native_events_created": native,
            "candidates_considered": candidates,
            "auto_exact_mappings": auto,
            "active_orphan_events": len(orphan_active),
            "method_version": METHOD_VERSION,
        }
    finally:
        con.close()


if __name__ == "__main__":
    print(run_mapping_pass())
