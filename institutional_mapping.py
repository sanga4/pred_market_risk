"""Conservative canonicalization and mapping for institutional EventRisk data.

Principles:
- every source contract receives a source-native canonical event so no data is orphaned
- cross-source merges are never guessed silently
- exact/near-exact cross-source matches become review candidates
- only exact semantic matches under strict rules are auto-mapped
- broader/narrower/overlapping candidates remain distinct until reviewed
"""

from __future__ import annotations

import hashlib
import re
import uuid
from datetime import datetime, timezone
from difflib import SequenceMatcher
from typing import Optional

from db import connect

METHOD_VERSION = "mapping_rules_v1"
STOP = {
    "will", "the", "a", "an", "of", "to", "in", "on", "by", "before", "after", "at",
    "is", "are", "be", "does", "do", "during", "this", "that", "and", "or", "for",
}
DATE_WORDS = {
    "january", "february", "march", "april", "may", "june", "july", "august",
    "september", "october", "november", "december",
}


def now_utc():
    return datetime.now(timezone.utc).replace(microsecond=0)


def stable_id(prefix: str, *parts: object, length: int = 24) -> str:
    raw = "\x1f".join("" if p is None else str(p) for p in parts)
    return f"{prefix}_{hashlib.sha256(raw.encode()).hexdigest()[:length]}"


def normalize(text: str) -> str:
    text = (text or "").lower().replace("u.s.", "us").replace("u.s", "us")
    text = re.sub(r"[^a-z0-9%.$]+", " ", text)
    tokens = [t for t in text.split() if t not in STOP]
    return " ".join(tokens)


def family_key(text: str) -> str:
    tokens = []
    for token in normalize(text).split():
        if token in DATE_WORDS or re.fullmatch(r"20\d{2}", token) or re.fullmatch(r"\d{1,2}", token):
            continue
        tokens.append(token)
    return " ".join(tokens)[:300] or normalize(text)[:300]


def title_similarity(left: str, right: str) -> float:
    a = normalize(left)
    b = normalize(right)
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    sa, sb = set(a.split()), set(b.split())
    union = sa | sb
    jaccard = len(sa & sb) / len(union) if union else 0.0
    seq = SequenceMatcher(None, a, b).ratio()
    return round(0.65 * jaccard + 0.35 * seq, 6)


def deadline_hours(a, b) -> Optional[float]:
    if a is None or b is None:
        return None
    return abs((a - b).total_seconds()) / 3600.0


def bootstrap_native_events(con) -> int:
    contracts = con.execute(
        """SELECT c.source_contract_id,c.source_id,c.source_market_id,c.source_event_id,c.current_title,
                  c.current_description,c.current_resolution_rules,c.closes_at,c.resolves_at,c.status
           FROM er_source_contracts c
           LEFT JOIN er_latest_contract_mappings m USING(source_contract_id)
           WHERE m.source_contract_id IS NULL"""
    ).fetchall()
    created = 0
    for row in contracts:
        contract_id, source_id, source_market_id, source_event_id, title, description, rules, closes_at, resolves_at, status = row
        fkey = family_key(title)
        family_id = stable_id("fam", fkey)
        event_id = stable_id("evt", source_id, source_market_id)
        con.execute(
            """INSERT INTO er_event_families(event_family_id,canonical_name,category,description)
               VALUES (?,?,?,?) ON CONFLICT(event_family_id) DO NOTHING""",
            [family_id, fkey.title()[:300], "unclassified", "Automatically created source-native event family; subject to review."],
        )
        definition = rules or description or title
        con.execute(
            """INSERT INTO er_canonical_events(
                 canonical_event_id,event_family_id,canonical_name,canonical_definition,event_class,
                 resolution_deadline,status,taxonomy_version)
               VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(canonical_event_id) DO NOTHING""",
            [event_id, family_id, title, definition, "unclassified", resolves_at or closes_at,
             "active" if status in {"active", "open"} else (status or "unknown"), "event_ontology_v1"],
        )
        mapping_id = stable_id("map", contract_id, event_id, "source_native")
        con.execute(
            """INSERT INTO er_event_mappings(
                 mapping_id,source_contract_id,canonical_event_id,mapping_type,confidence,
                 mapping_method,method_version,review_status,created_by)
               VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(mapping_id) DO NOTHING""",
            [mapping_id, contract_id, event_id, "source_native", 1.0, "deterministic_identity",
             METHOD_VERSION, "source_native", "system"],
        )
        created += 1
    return created


def generate_candidates(con) -> int:
    contracts = con.execute(
        """SELECT source_contract_id,source_id,current_title,closes_at,resolves_at
           FROM er_source_contracts WHERE active=TRUE ORDER BY source_id,source_contract_id"""
    ).fetchall()
    created = 0
    for i, left in enumerate(contracts):
        for right in contracts[i + 1:]:
            if left[1] == right[1]:
                continue
            sim = title_similarity(left[2], right[2])
            if sim < 0.72:
                continue
            hours = deadline_hours(left[4] or left[3], right[4] or right[3])
            if hours is not None and hours > 24 * 120:
                continue
            exact_title = normalize(left[2]) == normalize(right[2])
            exact_deadline = hours is None or hours <= 24
            suggestion = "same_event" if exact_title and exact_deadline else "related_only"
            conf = sim
            if exact_deadline:
                conf = min(1.0, conf + 0.03)
            candidate_id = stable_id("cand", min(left[0], right[0]), max(left[0], right[0]), METHOD_VERSION)
            con.execute(
                """INSERT INTO er_mapping_candidates(
                     candidate_id,left_source_contract_id,right_source_contract_id,suggested_relationship,
                     title_similarity,deadline_distance_hours,overall_confidence,candidate_method,method_version)
                   VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(candidate_id) DO NOTHING""",
                [candidate_id, left[0], right[0], suggestion, sim, hours, conf, "title+deadline_rules", METHOD_VERSION],
            )
            created += 1
    return created


def auto_accept_exact(con) -> int:
    rows = con.execute(
        """SELECT candidate_id,left_source_contract_id,right_source_contract_id,overall_confidence
           FROM er_mapping_candidates
           WHERE status='pending' AND suggested_relationship='same_event'
             AND title_similarity=1.0
             AND (deadline_distance_hours IS NULL OR deadline_distance_hours<=24)
             AND overall_confidence>=0.98"""
    ).fetchall()
    accepted = 0
    for candidate_id, left_id, right_id, confidence in rows:
        left_map = con.execute(
            "SELECT canonical_event_id FROM er_latest_contract_mappings WHERE source_contract_id=?",
            [left_id],
        ).fetchone()
        right_map = con.execute(
            "SELECT mapping_id,canonical_event_id FROM er_latest_contract_mappings WHERE source_contract_id=?",
            [right_id],
        ).fetchone()
        if not left_map or not right_map or left_map[0] == right_map[1]:
            con.execute(
                "UPDATE er_mapping_candidates SET status='auto_accepted',review_action='same_event',reviewed_by='system',reviewed_at=now() WHERE candidate_id=?",
                [candidate_id],
            )
            continue
        target_event = left_map[0]
        old_mapping_id = right_map[0]
        con.execute("UPDATE er_event_mappings SET valid_to=now() WHERE mapping_id=?", [old_mapping_id])
        new_mapping_id = f"map_{uuid.uuid4().hex}"
        con.execute(
            """INSERT INTO er_event_mappings(
                 mapping_id,source_contract_id,canonical_event_id,mapping_type,confidence,mapping_method,
                 method_version,review_status,supersedes_mapping_id,created_by)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            [new_mapping_id, right_id, target_event, "same_event", confidence, "strict_cross_source_auto",
             METHOD_VERSION, "auto_exact", old_mapping_id, "system"],
        )
        con.execute(
            "UPDATE er_mapping_candidates SET status='auto_accepted',review_action='same_event',reviewed_by='system',reviewed_at=now() WHERE candidate_id=?",
            [candidate_id],
        )
        con.execute(
            """INSERT INTO er_revisions(revision_id,object_type,object_id,revision_type,reason,previous_version,new_version,effective_at)
               VALUES (?,?,?,?,?,?,?,now())""",
            [f"rev_{uuid.uuid4().hex}", "event_mapping", right_id, "canonical_merge",
             "Strict exact cross-source semantic match", right_map[1], target_event],
        )
        accepted += 1
    return accepted


def review_candidate(candidate_id: str, action: str, actor: str, notes: Optional[str] = None) -> dict:
    if action not in {"same_event", "related_only", "reject"}:
        raise ValueError("action must be same_event, related_only, or reject")
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
        if action == "same_event":
            left_map = con.execute("SELECT canonical_event_id FROM er_latest_contract_mappings WHERE source_contract_id=?", [left_id]).fetchone()
            right_map = con.execute("SELECT mapping_id,canonical_event_id FROM er_latest_contract_mappings WHERE source_contract_id=?", [right_id]).fetchone()
            if not left_map or not right_map:
                raise ValueError("both contracts must have native mappings")
            if left_map[0] != right_map[1]:
                con.execute("UPDATE er_event_mappings SET valid_to=now() WHERE mapping_id=?", [right_map[0]])
                con.execute(
                    """INSERT INTO er_event_mappings(
                         mapping_id,source_contract_id,canonical_event_id,mapping_type,confidence,mapping_method,
                         method_version,review_status,review_notes,supersedes_mapping_id,created_by)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    [f"map_{uuid.uuid4().hex}", right_id, left_map[0], "same_event", confidence,
                     "human_review", METHOD_VERSION, "reviewed", notes, right_map[0], actor],
                )
        elif action == "related_only":
            left_event = con.execute("SELECT canonical_event_id FROM er_latest_contract_mappings WHERE source_contract_id=?", [left_id]).fetchone()
            right_event = con.execute("SELECT canonical_event_id FROM er_latest_contract_mappings WHERE source_contract_id=?", [right_id]).fetchone()
            if left_event and right_event and left_event[0] != right_event[0]:
                rel_id = stable_id("rel", min(left_event[0], right_event[0]), max(left_event[0], right_event[0]), "related")
                con.execute(
                    """INSERT INTO er_event_relationships(
                         relationship_id,source_event_id,relationship_type,target_event_id,relationship_class,
                         confidence,method,method_version,human_reviewed,review_status)
                       VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT(relationship_id) DO NOTHING""",
                    [rel_id, left_event[0], "related_to", right_event[0], "reviewed_relationship",
                     confidence, "human_review", METHOD_VERSION, True, "reviewed"],
                )
        con.execute(
            """UPDATE er_mapping_candidates SET status='reviewed',review_action=?,review_notes=?,reviewed_by=?,reviewed_at=now()
               WHERE candidate_id=?""",
            [action, notes, actor, candidate_id],
        )
        con.execute(
            """INSERT INTO er_admin_actions(action_id,action_type,object_type,object_id,actor,details)
               VALUES (?,?,?,?,?,?)""",
            [f"act_{uuid.uuid4().hex}", "mapping_review", "mapping_candidate", candidate_id, actor,
             f"action={action}; notes={notes or ''}"],
        )
        return {"candidate_id": candidate_id, "action": action, "status": "reviewed"}
    finally:
        con.close()


def run_mapping_pass() -> dict:
    con = connect()
    try:
        native = bootstrap_native_events(con)
        candidates = generate_candidates(con)
        auto = auto_accept_exact(con)
        return {"native_events_created": native, "candidates_considered": candidates, "auto_exact_mappings": auto}
    finally:
        con.close()


if __name__ == "__main__":
    print(run_mapping_pass())
