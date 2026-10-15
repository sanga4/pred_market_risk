"""Institutional QA v7: role-aware contract-version evidence validation.

Each source-contract version intentionally carries two explicit evidence rows: `event_payload` and
`market_payload`. Earlier QA counted evidence rows against version rows, which falsely flagged fully
evidenced versions because two valid evidence rows != one version. v7 validates the required roles
per version and preserves all QA v6 controls.
"""

from __future__ import annotations

import institutional_qa_v4 as v4
import institutional_qa_v5 as v5
import institutional_qa_v6 as v6
from db import connect

QA_VERSION = "institutional_qa_v7_role_aware_contract_evidence"


def check_contract_version_evidence(con) -> int:
    return v4._sync_rule(
        con,
        object_type="source_contract",
        rule_code="contract_version_missing_evidence",
        severity="high",
        sql="""
          WITH per_version AS (
            SELECT v.source_contract_id,v.version_no,
                   count(*) FILTER (
                     WHERE e.evidence_role='event_payload' AND e.raw_snapshot_id IS NOT NULL
                   ) AS event_evidence,
                   count(*) FILTER (
                     WHERE e.evidence_role='market_payload' AND e.raw_snapshot_id IS NOT NULL
                   ) AS market_evidence
            FROM er_source_contract_versions v
            LEFT JOIN er_contract_version_evidence e
              ON e.source_contract_id=v.source_contract_id AND e.version_no=v.version_no
            WHERE v.raw_snapshot_id IS NOT NULL
            GROUP BY v.source_contract_id,v.version_no
          ), bad AS (
            SELECT * FROM per_version
            WHERE event_evidence=0 OR market_evidence=0
          )
          SELECT source_contract_id AS object_id,
                 'version(s) missing required event_payload or market_payload evidence=' ||
                 string_agg(cast(version_no AS varchar),',' ORDER BY version_no) AS details
          FROM bad
          GROUP BY source_contract_id
        """,
    )


def run_qa(stale_minutes: int = 20) -> dict:
    con = connect()
    try:
        con.execute("BEGIN TRANSACTION")
        try:
            checks = {
                "probability_bounds": v4.check_probability_bounds(con),
                "crossed_books": v4.check_crossed_books(con),
                "missing_raw_lineage": v4.check_missing_raw_lineage(con),
                "archive_integrity": v4.check_archive_integrity(con),
                "archive_backlog": v4.check_archive_backlog(con),
                "contract_versions": v4.check_contract_versions(con),
                "contract_version_evidence": check_contract_version_evidence(con),
                "canonical_version_windows": v4.check_canonical_version_windows(con),
                "mapping_cardinality": v4.check_active_mapping_cardinality(con),
                "source_freshness": v5.check_source_freshness(con, stale_minutes),
                "ingestion_volume_drop": v4.check_ingestion_volume_drop(con),
                "observation_clocks": v4.check_observation_clocks(con),
                "mapping_ambiguity": v4.check_mapping_ambiguity(con),
                "inferred_edge_metadata": v4.check_inferred_edge_metadata(con),
                "active_orphan_events": v4.check_active_orphan_events(con),
                "mapping_targets": v4.check_mapping_targets_distributable_events(con),
                "point_in_time_mapping_cardinality": v4.check_point_in_time_mapping_cardinality(con),
                "reviewed_relationship_provenance": v6.check_reviewed_relationship_provenance(con),
                "active_contract_semantic_coverage": v4.check_active_contract_semantic_coverage(con),
                "reference_master_versions": v4.check_reference_master_versions(con),
                "ready_release_manifests": v4.check_ready_release_manifests(con),
                "migration_ledger": v4.check_migration_ledger(con),
                "latest_full_universe_runs": v4.check_latest_full_universe_runs(con),
                "presence_coherence": v4.check_presence_coherence(con),
                "raw_durability": v4.check_raw_durability(con),
                "stuck_ingestion_runs": v4.check_stuck_ingestion_runs(con, stale_minutes),
            }
            open_count = int(con.execute(
                "SELECT count(*) FROM er_quality_issues WHERE status='open'"
            ).fetchone()[0])
            critical_count = int(con.execute(
                "SELECT count(*) FROM er_quality_issues WHERE status='open' AND severity='critical'"
            ).fetchone()[0])
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        return {
            "checks": checks,
            "open_issues": open_count,
            "critical_issues": critical_count,
            "qa_version": QA_VERSION,
            "source_health_ownership": "ingestion_only",
            "contract_evidence_roles": ["event_payload", "market_payload"],
            "graph_evidence_table": "er_edge_evidence",
            "event_relationship_evidence_table": "er_event_relationship_evidence",
        }
    finally:
        con.close()


if __name__ == "__main__":
    print(run_qa())
