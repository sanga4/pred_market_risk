"""Version-aware, provenance-preserving institutional knowledge graph.

This graph layer makes no security-return or portfolio-impact claims. It reconstructs
source-asserted event/entity mentions over the intersection of source-contract metadata
versions and canonical-mapping versions. Thus a changed contract or remapping closes the
semantic interval instead of leaving a stale current relationship active.
"""

from __future__ import annotations

from datetime import datetime, timezone

import institutional_graph as base
from db import connect

GRAPH_VERSION = "graph_rules_v2_versioned_mentions"


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _interval_end(a, b):
    if a is None:
        return b
    if b is None:
        return a
    return min(a, b)


def _interval_start(a, b):
    if a is None:
        return b
    if b is None:
        return a
    return max(a, b)


def retire_v1_current_edges(con) -> int:
    cutoff = now_utc()
    rows = con.execute(
        """SELECT edge_id FROM er_graph_edges
           WHERE edge_class='source_asserted' AND method_version='graph_rules_v1' AND valid_to IS NULL"""
    ).fetchall()
    if rows:
        con.execute(
            """UPDATE er_graph_edges SET valid_to=?
               WHERE edge_class='source_asserted' AND method_version='graph_rules_v1' AND valid_to IS NULL""",
            [cutoff],
        )
    return len(rows)


def refresh_versioned_mentions(con) -> dict:
    base.seed_entities(con)
    aliases = con.execute(
        """SELECT i.entity_id,i.identifier_value,e.canonical_name,e.entity_type
           FROM er_entity_identifiers i
           JOIN er_entities e USING(entity_id)
           WHERE i.identifier_type='alias' AND e.active=TRUE
             AND (i.valid_to IS NULL OR i.valid_to>now())"""
    ).fetchall()

    intervals = con.execute(
        """SELECT
             m.mapping_id,m.canonical_event_id,m.valid_from mapping_from,m.valid_to mapping_to,
             v.source_contract_id,v.version_no,v.title,v.description,v.resolution_rules,
             v.source_observed_at,v.valid_from version_from,v.valid_to version_to,
             c.source_id
           FROM er_event_mappings m
           JOIN er_source_contract_versions v USING(source_contract_id)
           JOIN er_source_contracts c USING(source_contract_id)
           WHERE (v.valid_to IS NULL OR v.valid_to>m.valid_from)
             AND (m.valid_to IS NULL OR m.valid_to>v.valid_from)
           ORDER BY m.canonical_event_id,v.source_contract_id,v.version_no"""
    ).fetchall()

    considered = inserted = evidence_written = 0
    for (
        mapping_id, event_id, mapping_from, mapping_to,
        contract_id, version_no, title, description, rules,
        source_observed_at, version_from, version_to, source_id,
    ) in intervals:
        valid_from = _interval_start(mapping_from, version_from)
        valid_to = _interval_end(mapping_to, version_to)
        if valid_to is not None and valid_from is not None and valid_to <= valid_from:
            continue
        text = "\n".join(x for x in [title, description, rules] if x)
        for entity_id, alias, canonical_name, entity_type in aliases:
            considered += 1
            if not base._contains_alias(text, alias):
                continue
            edge_id = base.stable_id(
                "edge", mapping_id, contract_id, version_no, "mentions", entity_id, GRAPH_VERSION
            )
            existed = con.execute("SELECT 1 FROM er_graph_edges WHERE edge_id=?", [edge_id]).fetchone()
            con.execute(
                """INSERT INTO er_graph_edges(
                     edge_id,source_node_id,source_node_type,relationship_type,target_node_id,target_node_type,
                     edge_class,confidence,assertion_text,inference_method,method_version,human_reviewed,
                     review_status,observed_at,valid_from,valid_to)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(edge_id) DO UPDATE SET
                     valid_to=excluded.valid_to,
                     observed_at=excluded.observed_at,
                     assertion_text=excluded.assertion_text,
                     review_status=excluded.review_status""",
                [
                    edge_id,event_id,"canonical_event","mentions",entity_id,entity_type,
                    "source_asserted",1.0,
                    f"Source contract version explicitly mentions {canonical_name}",
                    None,GRAPH_VERSION,False,"source_asserted",
                    source_observed_at,valid_from,valid_to,
                ],
            )
            if not existed:
                inserted += 1

            evidence_rows = con.execute(
                """SELECT evidence_role,raw_snapshot_id
                   FROM er_contract_version_evidence
                   WHERE source_contract_id=? AND version_no=?
                   ORDER BY evidence_role""",
                [contract_id, version_no],
            ).fetchall()
            for role, raw_snapshot_id in evidence_rows:
                evidence_id = base.stable_id("evd", edge_id, role, raw_snapshot_id)
                before = con.execute(
                    "SELECT 1 FROM er_edge_evidence WHERE edge_id=? AND evidence_id=?",
                    [edge_id, evidence_id],
                ).fetchone()
                con.execute(
                    """INSERT INTO er_edge_evidence(
                         edge_id,evidence_id,evidence_type,source_id,source_record_id,excerpt)
                       VALUES (?,?,?,?,?,?) ON CONFLICT(edge_id,evidence_id) DO NOTHING""",
                    [
                        edge_id,evidence_id,f"contract_version_{role}",source_id,raw_snapshot_id,
                        (title or "")[:1000],
                    ],
                )
                if not before:
                    evidence_written += 1
    return {
        "candidate_alias_checks": considered,
        "mention_edges_inserted": inserted,
        "evidence_records_inserted": evidence_written,
    }


def run_graph_pass() -> dict:
    con = connect()
    try:
        identity = base.bootstrap_security_master(con)
        retired_v1 = retire_v1_current_edges(con)
        mentions = refresh_versioned_mentions(con)
        return {
            **identity,
            "legacy_v1_edges_retired": retired_v1,
            **mentions,
            "graph_version": GRAPH_VERSION,
        }
    finally:
        con.close()


if __name__ == "__main__":
    print(run_graph_pass())
