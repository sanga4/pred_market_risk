"""Scalable institutional canonical mapping v4.

The semantic boundary is unchanged from v3:
- every source contract first gets a source-native canonical event;
- only exact normalized title + compatible deadline + exact normalized resolution rules
  can be automatically merged as `same_event`;
- fuzzy similarity creates review candidates only;
- reviewed broader/narrower/overlap/conditional relationships never merge probabilities.

v4 changes execution, not judgement. Native bootstrap and candidate persistence are batched,
and only new/semantically changed contracts are reconsidered on later passes.
"""

from __future__ import annotations

import json
import os
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Optional

import pyarrow as pa

import institutional_mapping as base
import institutional_mapping_v2 as v2
import institutional_mapping_v3 as v3
from db import connect

METHOD_VERSION = "mapping_rules_v4_batched"
MAX_BLOCK_MEMBERS = max(25, int(os.getenv("MAPPING_MAX_BLOCK_MEMBERS", "250")))
MAX_BLOCK_TOKENS = max(1, int(os.getenv("MAPPING_MAX_BLOCK_TOKENS", "5")))
MIN_TITLE_SIMILARITY = 0.72

# Make the inherited exact-merge and human-review helpers write the v4 method version.
base.METHOD_VERSION = METHOD_VERSION
v2.METHOD_VERSION = METHOD_VERSION
v3.METHOD_VERSION = METHOD_VERSION

RELATIONSHIP_TYPES = v3.RELATIONSHIP_TYPES
DIRECTIONS = v3.DIRECTIONS
review_candidate = v3.review_candidate


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _deadline(row) -> Optional[datetime]:
    return row.get("resolves_at") or row.get("closes_at")


def _deadline_bucket(value: Optional[datetime]) -> str:
    if value is None:
        return "none"
    # Month-level exact-title blocking keeps near-adjacent resolution timestamps together
    # without creating giant all-history title blocks.
    return value.astimezone(timezone.utc).strftime("%Y-%m")


def _rows_dict(cur) -> list[dict]:
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _bootstrap_native_events_batch(con) -> dict:
    rows = _rows_dict(con.execute("""
        SELECT c.source_contract_id,c.source_id,c.source_market_id,c.current_title,
               c.current_description,c.current_resolution_rules,c.closes_at,c.resolves_at,
               c.status,c.first_seen_at,s.version_no AS semantic_version_no
        FROM er_source_contracts c
        LEFT JOIN er_latest_contract_mappings m USING(source_contract_id)
        LEFT JOIN er_latest_contract_semantic_versions s USING(source_contract_id)
        WHERE c.active=TRUE AND m.source_contract_id IS NULL
        ORDER BY c.source_id,c.source_contract_id
    """))
    if not rows:
        return {"contracts": 0, "families": 0, "events": 0, "mappings": 0}

    families: dict[str, dict] = {}
    events: list[dict] = []
    mappings: list[dict] = []
    for row in rows:
        title = row["current_title"] or row["source_market_id"]
        fkey = base.family_key(title)
        family_id = base.stable_id("fam", fkey)
        event_id = base.stable_id("evt", row["source_id"], row["source_market_id"])
        families.setdefault(family_id, {
            "event_family_id": family_id,
            "canonical_name": fkey.title()[:300],
            "category": "unclassified",
            "description": "Automatically created source-native event family; subject to review.",
        })
        events.append({
            "canonical_event_id": event_id,
            "event_family_id": family_id,
            "canonical_name": title,
            "canonical_definition": row["current_resolution_rules"] or row["current_description"] or title,
            "event_class": "unclassified",
            "resolution_deadline": row["resolves_at"] or row["closes_at"],
            "status": "active" if (row["status"] or "").lower() in {"active", "open"} else (row["status"] or "unknown"),
            "taxonomy_version": "event_ontology_v1",
        })
        semantic_marker = row.get("semantic_version_no") if row.get("semantic_version_no") is not None else "legacy"
        mappings.append({
            "mapping_id": base.stable_id("map", row["source_contract_id"], event_id, "source_native", semantic_marker),
            "source_contract_id": row["source_contract_id"],
            "canonical_event_id": event_id,
            "mapping_type": "source_native",
            "confidence": 1.0,
            "mapping_method": "deterministic_identity",
            "method_version": METHOD_VERSION,
            "review_status": "source_native",
            "valid_from": row["first_seen_at"] or now_utc(),
            "created_by": "system",
        })

    family_table = pa.Table.from_pylist(list(families.values()))
    event_table = pa.Table.from_pylist(events)
    mapping_table = pa.Table.from_pylist(mappings)
    con.register("_map_families_arrow", family_table)
    con.register("_map_events_arrow", event_table)
    con.register("_map_native_arrow", mapping_table)
    try:
        con.execute("""
            INSERT INTO er_event_families(event_family_id,canonical_name,category,description)
            SELECT event_family_id,canonical_name,category,description FROM _map_families_arrow
            ON CONFLICT(event_family_id) DO NOTHING
        """)
        con.execute("""
            INSERT INTO er_canonical_events(
              canonical_event_id,event_family_id,canonical_name,canonical_definition,event_class,
              resolution_deadline,status,taxonomy_version)
            SELECT canonical_event_id,event_family_id,canonical_name,canonical_definition,event_class,
                   resolution_deadline,status,taxonomy_version FROM _map_events_arrow
            ON CONFLICT(canonical_event_id) DO NOTHING
        """)
        con.execute("""
            INSERT INTO er_event_mappings(
              mapping_id,source_contract_id,canonical_event_id,mapping_type,confidence,mapping_method,
              method_version,review_status,valid_from,created_by)
            SELECT mapping_id,source_contract_id,canonical_event_id,mapping_type,confidence,mapping_method,
                   method_version,review_status,valid_from,created_by FROM _map_native_arrow
            ON CONFLICT(mapping_id) DO NOTHING
        """)
    finally:
        con.unregister("_map_families_arrow")
        con.unregister("_map_events_arrow")
        con.unregister("_map_native_arrow")
    return {
        "contracts": len(rows),
        "families": len(families),
        "events": len(events),
        "mappings": len(mappings),
    }


def _contract_universe(con) -> tuple[list[dict], set[str], list[dict]]:
    rows = _rows_dict(con.execute("""
        SELECT c.source_contract_id,c.source_id,c.current_title,c.current_resolution_rules,
               c.closes_at,c.resolves_at,s.version_no AS semantic_version_no,
               s.received_at AS semantic_received_at,
               st.semantic_version_no AS processed_semantic_version_no,
               st.method_version AS processed_method_version
        FROM er_source_contracts c
        LEFT JOIN er_latest_contract_semantic_versions s USING(source_contract_id)
        LEFT JOIN er_mapping_contract_state st USING(source_contract_id)
        WHERE c.active=TRUE
        ORDER BY c.source_id,c.source_contract_id
    """))
    changed_rows = [
        row for row in rows
        if row["processed_method_version"] != METHOD_VERSION
        or (row["processed_semantic_version_no"] if row["processed_semantic_version_no"] is not None else -1)
           != (row["semantic_version_no"] if row["semantic_version_no"] is not None else -1)
    ]
    return rows, {row["source_contract_id"] for row in changed_rows}, changed_rows


def _candidate_rows(all_rows: list[dict], changed_ids: set[str]) -> tuple[list[dict], dict]:
    if not changed_ids or len({row["source_id"] for row in all_rows}) < 2:
        return [], {
            "pairs_evaluated": 0,
            "candidate_rows": 0,
            "oversized_token_blocks_skipped": 0,
            "block_member_cap": MAX_BLOCK_MEMBERS,
            "block_tokens_per_contract": MAX_BLOCK_TOKENS,
        }

    token_sets: list[set[str]] = []
    normalized_titles: list[str] = []
    normalized_rules: list[str] = []
    token_frequency: Counter[str] = Counter()
    for row in all_rows:
        tokens = v2.meaningful_tokens(row["current_title"] or "")
        token_sets.append(tokens)
        token_frequency.update(tokens)
        normalized_titles.append(base.normalize(row["current_title"] or ""))
        normalized_rules.append(v2.normalized_rules(row["current_resolution_rules"]))

    blocks: dict[tuple, list[int]] = defaultdict(list)
    oversized_skipped = 0
    for idx, row in enumerate(all_rows):
        norm_title = normalized_titles[idx]
        if norm_title:
            blocks[("exact", norm_title, _deadline_bucket(_deadline(row)))].append(idx)
        eligible = sorted(
            (t for t in token_sets[idx] if token_frequency[t] <= MAX_BLOCK_MEMBERS),
            key=lambda t: (token_frequency[t], t),
        )[:MAX_BLOCK_TOKENS]
        for token in eligible:
            blocks[("token", token)].append(idx)

    pair_ids: set[tuple[int, int]] = set()
    for key, members in blocks.items():
        if len(members) > MAX_BLOCK_MEMBERS and key[0] == "token":
            oversized_skipped += 1
            continue
        for pos, left_idx in enumerate(members):
            left = all_rows[left_idx]
            for right_idx in members[pos + 1:]:
                right = all_rows[right_idx]
                if left["source_id"] == right["source_id"]:
                    continue
                if left["source_contract_id"] not in changed_ids and right["source_contract_id"] not in changed_ids:
                    continue
                pair_ids.add((min(left_idx, right_idx), max(left_idx, right_idx)))

    output: list[dict] = []
    pairs_evaluated = 0
    for i, j in pair_ids:
        left, right = all_rows[i], all_rows[j]
        shared = token_sets[i] & token_sets[j]
        required_shared = 1 if min(len(token_sets[i]), len(token_sets[j])) <= 2 else 2
        if len(shared) < required_shared:
            continue
        pairs_evaluated += 1
        title_sim = base.title_similarity(left["current_title"], right["current_title"])
        if title_sim < MIN_TITLE_SIMILARITY:
            continue
        hours = base.deadline_hours(_deadline(left), _deadline(right))
        if hours is not None and hours > 24 * 120:
            continue
        left_rules = normalized_rules[i]
        right_rules = normalized_rules[j]
        rules_both = bool(left_rules and right_rules)
        rules_sim = base.title_similarity(left_rules, right_rules) if rules_both else None
        rules_compatible = rules_both and left_rules == right_rules
        exact_title = normalized_titles[i] == normalized_titles[j]
        exact_deadline = hours is None or hours <= 24
        suggestion = "same_event" if exact_title and exact_deadline and rules_compatible else "related_only"
        if rules_sim is None:
            confidence = title_sim * (1.0 if exact_deadline else 0.9)
        else:
            confidence = 0.60 * title_sim + 0.30 * rules_sim + (0.10 if exact_deadline else 0.0)
        confidence = min(1.0, max(0.0, confidence))
        left_id = left["source_contract_id"]
        right_id = right["source_contract_id"]
        candidate_id = base.stable_id("cand", min(left_id, right_id), max(left_id, right_id), METHOD_VERSION)
        output.append({
            "candidate_id": candidate_id,
            "left_source_contract_id": left_id,
            "right_source_contract_id": right_id,
            "suggested_relationship": suggestion,
            "title_similarity": float(title_sim),
            "deadline_distance_hours": hours,
            "overall_confidence": float(confidence),
            "candidate_method": "rare_token_block+title+deadline+resolution_rules",
            "method_version": METHOD_VERSION,
            "resolution_similarity": rules_sim,
            "resolution_compatible": bool(rules_compatible),
            "rules_available_both": bool(rules_both),
        })

    return output, {
        "pairs_evaluated": pairs_evaluated,
        "candidate_rows": len(output),
        "oversized_token_blocks_skipped": oversized_skipped,
        "block_member_cap": MAX_BLOCK_MEMBERS,
        "block_tokens_per_contract": MAX_BLOCK_TOKENS,
    }


def _persist_candidates(con, rows: list[dict]) -> int:
    if not rows:
        return 0
    before = con.execute(
        "SELECT count(*) FROM er_mapping_candidates WHERE method_version=?", [METHOD_VERSION]
    ).fetchone()[0]
    table = pa.Table.from_pylist(rows)
    con.register("_mapping_candidates_arrow", table)
    try:
        con.execute("""
            INSERT INTO er_mapping_candidates(
              candidate_id,left_source_contract_id,right_source_contract_id,suggested_relationship,
              title_similarity,deadline_distance_hours,overall_confidence,candidate_method,method_version,
              resolution_similarity,resolution_compatible,rules_available_both)
            SELECT candidate_id,left_source_contract_id,right_source_contract_id,suggested_relationship,
                   title_similarity,deadline_distance_hours,overall_confidence,candidate_method,method_version,
                   resolution_similarity,resolution_compatible,rules_available_both
            FROM _mapping_candidates_arrow
            ON CONFLICT(candidate_id) DO NOTHING
        """)
    finally:
        con.unregister("_mapping_candidates_arrow")
    after = con.execute(
        "SELECT count(*) FROM er_mapping_candidates WHERE method_version=?", [METHOD_VERSION]
    ).fetchone()[0]
    return int(after - before)


def _mark_processed(con, changed_rows: list[dict], pass_id: str) -> None:
    if not changed_rows:
        return
    rows = [{
        "source_contract_id": row["source_contract_id"],
        "semantic_version_no": row["semantic_version_no"],
        "semantic_received_at": row["semantic_received_at"],
        "method_version": METHOD_VERSION,
        "processed_at": now_utc(),
        "last_pass_id": pass_id,
    } for row in changed_rows]
    table = pa.Table.from_pylist(rows)
    con.register("_mapping_state_arrow", table)
    try:
        con.execute("""
            INSERT INTO er_mapping_contract_state(
              source_contract_id,semantic_version_no,semantic_received_at,method_version,processed_at,last_pass_id)
            SELECT source_contract_id,semantic_version_no,semantic_received_at,method_version,processed_at,last_pass_id
            FROM _mapping_state_arrow
            ON CONFLICT(source_contract_id) DO UPDATE SET
              semantic_version_no=excluded.semantic_version_no,
              semantic_received_at=excluded.semantic_received_at,
              method_version=excluded.method_version,
              processed_at=excluded.processed_at,
              last_pass_id=excluded.last_pass_id
        """)
    finally:
        con.unregister("_mapping_state_arrow")


def run_mapping_pass() -> dict:
    pass_id = f"mp_{uuid.uuid4().hex}"
    started = now_utc()
    con = connect()
    try:
        con.execute("""
            INSERT INTO er_mapping_passes(pass_id,method_version,started_at,status)
            VALUES (?,?,?,'running')
        """, [pass_id, METHOD_VERSION, started])
        try:
            native = _bootstrap_native_events_batch(con)
            all_rows, changed_ids, changed_rows = _contract_universe(con)
            candidates, candidate_stats = _candidate_rows(all_rows, changed_ids)
            inserted = _persist_candidates(con, candidates)
            auto = v3.auto_accept_exact(con)
            _mark_processed(con, changed_rows, pass_id)
            orphan_count = con.execute("""
                SELECT count(*) FROM (
                  SELECT e.canonical_event_id
                  FROM er_canonical_events e
                  LEFT JOIN er_event_mappings m
                    ON m.canonical_event_id=e.canonical_event_id AND m.valid_to IS NULL
                  WHERE e.status='active'
                  GROUP BY e.canonical_event_id
                  HAVING count(m.mapping_id)=0
                )
            """).fetchone()[0]
            details = {
                **candidate_stats,
                "native_bootstrap": native,
                "active_orphan_events": int(orphan_count),
                "incremental": True,
            }
            con.execute("""
                UPDATE er_mapping_passes SET finished_at=now(),status='success',contracts_total=?,
                  contracts_changed=?,native_events_created=?,candidate_pairs_evaluated=?,
                  candidates_inserted=?,auto_exact_mappings=?,details=? WHERE pass_id=?
            """, [
                len(all_rows), len(changed_rows), native["events"], candidate_stats["pairs_evaluated"],
                inserted, auto, json.dumps(details, sort_keys=True, default=str), pass_id,
            ])
            return {
                "pass_id": pass_id,
                "native_events_created": native["events"],
                "contracts_total": len(all_rows),
                "contracts_changed": len(changed_rows),
                "candidate_pairs_evaluated": candidate_stats["pairs_evaluated"],
                "candidates_generated": candidate_stats["candidate_rows"],
                "candidates_inserted": inserted,
                "auto_exact_mappings": auto,
                "active_orphan_events": int(orphan_count),
                "method_version": METHOD_VERSION,
                "blocking": {
                    "max_block_members": MAX_BLOCK_MEMBERS,
                    "max_block_tokens": MAX_BLOCK_TOKENS,
                    "oversized_token_blocks_skipped": candidate_stats["oversized_token_blocks_skipped"],
                },
            }
        except Exception as exc:
            con.execute(
                "UPDATE er_mapping_passes SET finished_at=now(),status='failed',details=? WHERE pass_id=?",
                [json.dumps({"error": str(exc)[:4000]}, sort_keys=True), pass_id],
            )
            raise
    finally:
        con.close()


if __name__ == "__main__":
    print(run_mapping_pass())
