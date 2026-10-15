"""Institutional QA v6: concurrency-safe QA with normalized graph-evidence provenance.

v6 preserves QA v5's ingestion-owned source-health boundary and every prior quality rule. It
corrects the reviewed graph-edge provenance check to use `er_edge_evidence(edge_id, evidence_id)`.
`er_event_relationship_evidence` is intentionally separate and keyed by `relationship_id` for the
reviewed event-relationship layer.
"""

from __future__ import annotations

import institutional_qa_v4 as v4
import institutional_qa_v5 as v5
from db import connect

QA_VERSION = "institutional_qa_v6_graph_evidence"


def check_reviewed_relationship_provenance(con) -> int:
    return v4._sync_rule(
        con,
        object_type="graph_edge",
        rule_code="reviewed_relationship_missing_provenance",
        severity="critical",
        sql="""
          SELECT e.edge_id AS object_id,
                 'Reviewed graph edge missing explicit edge evidence or methodology metadata' AS details
          FROM er_graph_edges e
          LEFT JOIN er_edge_evidence r USING(edge_id)
          WHERE e.human_reviewed=TRUE
          GROUP BY e.edge_id,e.inference_method,e.method_version
          HAVING count(r.evidence_id)=0 OR e.method_version IS NULL
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
                "contract_version_evidence": v4.check_contract_version_evidence(con),
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
                "reviewed_relationship_provenance": check_reviewed_relationship_provenance(con),
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
            "graph_evidence_table": "er_edge_evidence",
            "event_relationship_evidence_table": "er_event_relationship_evidence",
        }
    finally:
        con.close()


if __name__ == "__main__":
    print(run_qa())
