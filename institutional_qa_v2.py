"""Additional institutional-grade QA controls layered on the existing QA suite.

These checks focus on semantic/temporal integrity introduced by canonical mapping,
prospective full contract semantics, reference-master versioning and bulk distribution.
They never mutate source observations or silently repair mappings.
"""

from __future__ import annotations

import institutional_qa as base
from db import connect
from institutional_migrations import MIGRATION_FILES


def check_active_orphan_events(con) -> int:
    rows = con.execute(
        """SELECT e.canonical_event_id,e.canonical_name
           FROM er_canonical_events e
           LEFT JOIN er_event_mappings m
             ON m.canonical_event_id=e.canonical_event_id AND m.valid_to IS NULL
           WHERE e.status='active'
           GROUP BY e.canonical_event_id,e.canonical_name
           HAVING count(m.mapping_id)=0"""
    ).fetchall()
    bad = {r[0] for r in rows}
    for event_id, name in rows:
        base.open_issue(
            con,
            "canonical_event",
            event_id,
            "active_event_without_source_mapping",
            "high",
            f"Active canonical event has no active source contract mapping: {name}",
        )
    for (event_id,) in con.execute("SELECT canonical_event_id FROM er_canonical_events").fetchall():
        if event_id not in bad:
            base.resolve_issue(con, "canonical_event", event_id, "active_event_without_source_mapping")
    return len(rows)


def check_mapping_targets_distributable_events(con) -> int:
    rows = con.execute(
        """SELECT m.source_contract_id,m.canonical_event_id,e.status
           FROM er_event_mappings m
           JOIN er_canonical_events e USING(canonical_event_id)
           WHERE m.valid_to IS NULL AND e.status IN ('superseded','inactive','retired')"""
    ).fetchall()
    bad_contracts = {r[0] for r in rows}
    for contract_id, event_id, status in rows:
        base.open_issue(
            con,
            "source_contract",
            contract_id,
            "active_mapping_targets_nonserved_event",
            "critical",
            f"Active mapping points to canonical_event={event_id} with status={status}",
        )
    for (contract_id,) in con.execute("SELECT source_contract_id FROM er_source_contracts").fetchall():
        if contract_id not in bad_contracts:
            base.resolve_issue(con, "source_contract", contract_id, "active_mapping_targets_nonserved_event")
    return len(rows)


def check_point_in_time_mapping_cardinality(con) -> int:
    rows = con.execute(
        """SELECT o.source_contract_id,o.observed_at,o.received_at,count(m.mapping_id) mapping_count
           FROM er_market_observations o
           LEFT JOIN er_event_mappings m
             ON m.source_contract_id=o.source_contract_id
            AND m.valid_from<=o.received_at
            AND (m.valid_to IS NULL OR m.valid_to>o.received_at)
           WHERE coalesce(o.methodology_version,'')<>'legacy_collector_v0.3'
           GROUP BY o.source_contract_id,o.observed_at,o.received_at
           HAVING count(m.mapping_id)<>1"""
    ).fetchall()
    bad_contracts = {r[0] for r in rows}
    for contract_id, observed_at, received_at, count in rows:
        base.open_issue(
            con,
            "source_contract",
            contract_id,
            "point_in_time_mapping_cardinality",
            "critical",
            f"Observation observed_at={observed_at} received_at={received_at} resolves to {count} mappings",
        )
    for (contract_id,) in con.execute("SELECT source_contract_id FROM er_source_contracts").fetchall():
        if contract_id not in bad_contracts:
            base.resolve_issue(con, "source_contract", contract_id, "point_in_time_mapping_cardinality")
    return len(rows)


def check_reviewed_relationship_provenance(con) -> int:
    rows = con.execute(
        """SELECT r.relationship_id,r.source_event_id,r.target_event_id,r.relationship_type,
                  r.human_reviewed,r.review_status,r.source_candidate_id,r.created_by,
                  count(e.evidence_id) evidence_count
           FROM er_event_relationships r
           LEFT JOIN er_event_relationship_evidence e USING(relationship_id)
           WHERE r.valid_to IS NULL AND r.relationship_class='reviewed_relationship'
           GROUP BY ALL
           HAVING r.source_event_id=r.target_event_id
               OR r.human_reviewed IS DISTINCT FROM TRUE
               OR coalesce(r.review_status,'')<>'reviewed'
               OR r.source_candidate_id IS NULL
               OR r.created_by IS NULL
               OR count(e.evidence_id)=0"""
    ).fetchall()
    bad = {r[0] for r in rows}
    for relationship_id, source_event, target_event, relationship_type, human_reviewed, review_status, candidate_id, actor, evidence_count in rows:
        base.open_issue(
            con,
            "event_relationship",
            relationship_id,
            "reviewed_relationship_provenance_incomplete",
            "high",
            f"type={relationship_type}; source={source_event}; target={target_event}; reviewed={human_reviewed}; "
            f"review_status={review_status}; candidate={candidate_id}; actor={actor}; evidence_count={evidence_count}",
        )
    for (relationship_id,) in con.execute("SELECT relationship_id FROM er_event_relationships").fetchall():
        if relationship_id not in bad:
            base.resolve_issue(con, "event_relationship", relationship_id, "reviewed_relationship_provenance_incomplete")
    return len(rows)


def check_active_contract_semantic_coverage(con) -> int:
    rows = con.execute(
        """SELECT c.source_contract_id,c.source_id,c.source_market_id
           FROM er_source_contracts c
           LEFT JOIN er_latest_contract_semantic_versions v USING(source_contract_id)
           WHERE c.active=TRUE
             AND (v.source_contract_id IS NULL OR coalesce(v.metadata_completeness,'')<>'full_v2'
                  OR v.event_raw_snapshot_id IS NULL OR v.market_raw_snapshot_id IS NULL)"""
    ).fetchall()
    bad = {r[0] for r in rows}
    for contract_id, source_id, market_id in rows:
        base.open_issue(
            con,
            "source_contract",
            contract_id,
            "active_contract_missing_full_semantics",
            "critical",
            f"Active contract {source_id}:{market_id} lacks full_v2 semantic normalization with both raw evidence pointers",
        )
    for (contract_id,) in con.execute("SELECT source_contract_id FROM er_source_contracts").fetchall():
        if contract_id not in bad:
            base.resolve_issue(con, "source_contract", contract_id, "active_contract_missing_full_semantics")
    return len(rows)


def check_reference_master_versions(con) -> int:
    failures = 0
    for current, versioned, object_type, id_col in [
        ("er_sources", "er_latest_source_versions", "source", "source_id"),
        ("er_entities", "er_latest_entity_versions", "entity", "entity_id"),
        ("er_instruments", "er_latest_instrument_versions", "instrument", "instrument_id"),
    ]:
        rows = con.execute(
            f"""SELECT c.{id_col}
                FROM {current} c LEFT JOIN {versioned} v USING({id_col})
                WHERE v.{id_col} IS NULL"""
        ).fetchall()
        failures += len(rows)
        bad = {r[0] for r in rows}
        for (object_id,) in rows:
            base.open_issue(
                con,
                object_type,
                object_id,
                "reference_master_missing_version",
                "high",
                f"Current {object_type} master row has no point-in-time version record",
            )
        for (object_id,) in con.execute(f"SELECT {id_col} FROM {current}").fetchall():
            if object_id not in bad:
                base.resolve_issue(con, object_type, object_id, "reference_master_missing_version")
    return failures


def check_ready_release_manifests(con) -> int:
    rows = con.execute(
        """SELECT r.release_id,r.dataset_count,r.manifest_hash,r.manifest_object_uri,count(m.dataset_name) manifest_rows
           FROM er_data_releases r
           LEFT JOIN er_export_manifests m USING(release_id)
           WHERE r.status='ready'
           GROUP BY r.release_id,r.dataset_count,r.manifest_hash,r.manifest_object_uri
           HAVING r.manifest_hash IS NULL OR r.manifest_object_uri IS NULL
               OR r.dataset_count IS NULL OR r.dataset_count<>count(m.dataset_name)"""
    ).fetchall()
    bad = {r[0] for r in rows}
    for release_id, dataset_count, manifest_hash, manifest_uri, manifest_rows in rows:
        base.open_issue(
            con,
            "data_release",
            release_id,
            "ready_release_manifest_incomplete",
            "critical",
            f"dataset_count={dataset_count}; rows={manifest_rows}; manifest_hash={manifest_hash}; manifest_uri={manifest_uri}",
        )
    for (release_id,) in con.execute("SELECT release_id FROM er_data_releases").fetchall():
        if release_id not in bad:
            base.resolve_issue(con, "data_release", release_id, "ready_release_manifest_incomplete")
    return len(rows)


def check_migration_ledger(con) -> int:
    count = con.execute("SELECT count(*) FROM er_schema_migrations").fetchone()[0]
    if count != len(MIGRATION_FILES):
        base.open_issue(
            con,
            "dataset",
            "institutional_schema",
            "migration_ledger_incomplete",
            "critical",
            f"Applied migration count={count}; expected={len(MIGRATION_FILES)}",
        )
        return 1
    base.resolve_issue(con, "dataset", "institutional_schema", "migration_ledger_incomplete")
    return 0


def run_qa(stale_minutes: int = 20) -> dict:
    result = base.run_qa(stale_minutes=stale_minutes)
    con = connect()
    try:
        extra = {
            "active_orphan_events": check_active_orphan_events(con),
            "mapping_targets": check_mapping_targets_distributable_events(con),
            "point_in_time_mapping_cardinality": check_point_in_time_mapping_cardinality(con),
            "reviewed_relationship_provenance": check_reviewed_relationship_provenance(con),
            "active_contract_semantic_coverage": check_active_contract_semantic_coverage(con),
            "reference_master_versions": check_reference_master_versions(con),
            "ready_release_manifests": check_ready_release_manifests(con),
            "migration_ledger": check_migration_ledger(con),
        }
        open_count = con.execute("SELECT count(*) FROM er_quality_issues WHERE status='open'").fetchone()[0]
        critical_count = con.execute(
            "SELECT count(*) FROM er_quality_issues WHERE status='open' AND severity='critical'"
        ).fetchone()[0]
    finally:
        con.close()
    result["checks"].update(extra)
    result["open_issues"] = open_count
    result["critical_issues"] = critical_count
    result["qa_version"] = "institutional_qa_v2"
    return result


if __name__ == "__main__":
    print(run_qa())
