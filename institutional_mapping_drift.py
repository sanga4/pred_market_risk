"""Detect post-decision semantic drift in active canonical identity mappings.

A human-reviewed identity decision is not second-guessed merely because wording differs.
Drift is raised only when a source contract receives a newer full-semantic version after
the active identity decision and the current contracts no longer satisfy the same strict
identity criteria used for automatic equivalence.
"""

from __future__ import annotations

import json

import institutional_mapping as base
import institutional_qa as qa_base
from db import connect
from institutional_mapping_v2 import normalized_rules


def _strictly_equivalent(left: dict, right: dict) -> tuple[bool, dict]:
    title_exact = base.normalize(left["title"]) == base.normalize(right["title"])
    left_rules = normalized_rules(left.get("resolution_rules"))
    right_rules = normalized_rules(right.get("resolution_rules"))
    rules_available = bool(left_rules and right_rules)
    rules_exact = rules_available and left_rules == right_rules
    hours = base.deadline_hours(
        left.get("resolves_at") or left.get("closes_at"),
        right.get("resolves_at") or right.get("closes_at"),
    )
    deadline_compatible = hours is None or hours <= 24
    return title_exact and rules_exact and deadline_compatible, {
        "title_exact": title_exact,
        "rules_available_both": rules_available,
        "rules_exact": rules_exact,
        "deadline_distance_hours": hours,
        "deadline_compatible": deadline_compatible,
    }


def find_mapping_drift(con) -> list[dict]:
    event_ids = [r[0] for r in con.execute("""
        SELECT canonical_event_id
        FROM er_event_mappings
        WHERE valid_to IS NULL
        GROUP BY canonical_event_id
        HAVING count(*)>1 AND sum(CASE WHEN mapping_type='same_event' THEN 1 ELSE 0 END)>0
    """).fetchall()]
    problems: list[dict] = []
    for event_id in event_ids:
        rows = con.execute("""
            SELECT m.source_contract_id,m.mapping_type,m.review_status,m.mapped_at,
                   c.source_id,
                   v.title,v.resolution_rules,v.closes_at,v.resolves_at,v.received_at
            FROM er_event_mappings m
            JOIN er_source_contracts c USING(source_contract_id)
            LEFT JOIN er_latest_contract_semantic_versions v USING(source_contract_id)
            WHERE m.canonical_event_id=? AND m.valid_to IS NULL
            ORDER BY c.source_id,m.source_contract_id
        """, [event_id]).fetchall()
        contracts = []
        decision_at = max((r[3] for r in rows if r[3] is not None), default=None)
        for row in rows:
            contracts.append({
                "source_contract_id": row[0],
                "mapping_type": row[1],
                "review_status": row[2],
                "mapped_at": row[3],
                "source_id": row[4],
                "title": row[5],
                "resolution_rules": row[6],
                "closes_at": row[7],
                "resolves_at": row[8],
                "semantic_received_at": row[9],
            })
        if not decision_at or any(c["semantic_received_at"] is None for c in contracts):
            continue
        changed_after_decision = any(c["semantic_received_at"] > decision_at for c in contracts)
        if not changed_after_decision:
            continue
        for i, left in enumerate(contracts):
            for right in contracts[i + 1:]:
                if left["source_id"] == right["source_id"]:
                    continue
                equivalent, detail = _strictly_equivalent(left, right)
                if not equivalent:
                    problems.append({
                        "canonical_event_id": event_id,
                        "decision_at": decision_at,
                        "left_contract_id": left["source_contract_id"],
                        "right_contract_id": right["source_contract_id"],
                        "left_semantic_received_at": left["semantic_received_at"],
                        "right_semantic_received_at": right["semantic_received_at"],
                        **detail,
                    })
    return problems


def persist_mapping_drift() -> dict:
    con = connect()
    try:
        problems = find_mapping_drift(con)
        grouped: dict[str, list[dict]] = {}
        for problem in problems:
            grouped.setdefault(problem["canonical_event_id"], []).append(problem)
        all_events = [r[0] for r in con.execute("SELECT canonical_event_id FROM er_canonical_events").fetchall()]
        for event_id, event_problems in grouped.items():
            qa_base.open_issue(
                con,
                "canonical_event",
                event_id,
                "same_event_mapping_semantic_drift",
                "critical",
                json.dumps(event_problems, default=str, sort_keys=True)[:12000],
            )
        for event_id in all_events:
            if event_id not in grouped:
                qa_base.resolve_issue(
                    con,
                    "canonical_event",
                    event_id,
                    "same_event_mapping_semantic_drift",
                )
        return {
            "drifted_canonical_events": len(grouped),
            "incompatible_pairs": len(problems),
        }
    finally:
        con.close()
