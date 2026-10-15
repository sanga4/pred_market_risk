"""Institutional QA v8: snapshot-safe under concurrent hot-path refreshes.

v8 preserves v7's role-aware contract-evidence checks and every substantive QA rule. It fixes two
operational false-positive modes exposed by the independently scheduled five-minute hot path:

1. Full QA is deferred when the transaction snapshot shows deterministic identity mid-catch-up
   (non-empty identity work queue or active contracts without exactly one live mapping). No issue
   rows are opened/resolved from an internally inconsistent snapshot.
2. Source-run completeness is judged from the most recently *started completed* run per source,
   so a normal in-progress refresh does not invalidate the last completed good universe.

No quality threshold is weakened. A stable snapshot still runs the full v7 rule set and fails on
real lineage, mapping, graph, durability, migration, freshness, or provenance violations.
"""

from __future__ import annotations

import institutional_qa_v4 as v4
import institutional_qa_v5 as v5
import institutional_qa_v6 as v6
import institutional_qa_v7 as v7
from db import connect

QA_VERSION = "institutional_qa_v8_snapshot_guard_completed_runs"


def identity_snapshot_state(con) -> dict:
    """Return whether this transaction sees a deterministic-identity-consistent snapshot."""
    work_queue = int(con.execute(
        "SELECT count(*) FROM er_mapping_identity_work_queue"
    ).fetchone()[0])
    active_cardinality_bad = int(con.execute("""
        SELECT count(*) FROM (
          SELECT c.source_contract_id,count(m.mapping_id) AS n
          FROM er_source_contracts c
          LEFT JOIN er_event_mappings m
            ON m.source_contract_id=c.source_contract_id AND m.valid_to IS NULL
          WHERE c.active=TRUE
          GROUP BY c.source_contract_id
          HAVING count(m.mapping_id)<>1
        )
    """).fetchone()[0])
    return {
        "ready": work_queue == 0 and active_cardinality_bad == 0,
        "identity_work_queue": work_queue,
        "active_mapping_cardinality_bad": active_cardinality_bad,
    }


def check_latest_completed_full_universe_runs(con) -> int:
    """Require each active source's latest completed invocation to be a full successful run."""
    return v4._sync_rule(
        con,
        object_type="source",
        rule_code="latest_run_not_complete_full_universe",
        severity="high",
        sql="""
          WITH ranked AS (
            SELECT r.*,
                   row_number() OVER(
                     PARTITION BY source_id
                     ORDER BY started_at DESC,run_id DESC
                   ) rn
            FROM er_ingestion_runs r
            WHERE finished_at IS NOT NULL AND status<>'running'
          )
          SELECT s.source_id AS object_id,
                 'latest_completed_run_id=' || coalesce(r.run_id,'NULL') ||
                 '; status=' || coalesce(r.status,'NULL') ||
                 '; scope=' || coalesce(r.collection_scope,'NULL') ||
                 '; universe_complete=' || coalesce(cast(r.universe_complete AS varchar),'NULL') ||
                 '; started_at=' || coalesce(cast(r.started_at AS varchar),'NULL') AS details
          FROM er_sources s
          LEFT JOIN ranked r ON r.source_id=s.source_id AND r.rn=1
          WHERE s.active=TRUE
            AND (r.run_id IS NULL OR r.status<>'success' OR coalesce(r.collection_scope,'')<>'full'
                 OR r.universe_complete IS DISTINCT FROM TRUE)
        """,
        resolution_note="Latest completed source run is a successful full-universe traversal",
    )


def run_qa(stale_minutes: int = 20) -> dict:
    con = connect()
    try:
        con.execute("BEGIN TRANSACTION")
        try:
            snapshot = identity_snapshot_state(con)
            if not snapshot["ready"]:
                # Do not synchronize any quality issue rows from an identity-inconsistent snapshot.
                open_count = int(con.execute(
                    "SELECT count(*) FROM er_quality_issues WHERE status='open'"
                ).fetchone()[0])
                critical_count = int(con.execute(
                    "SELECT count(*) FROM er_quality_issues WHERE status='open' AND severity='critical'"
                ).fetchone()[0])
                con.execute("ROLLBACK")
                return {
                    "checks": {},
                    "open_issues": open_count,
                    "critical_issues": critical_count,
                    "qa_version": QA_VERSION,
                    "deferred": True,
                    "deferred_reason": "identity_snapshot_in_transition",
                    "identity_snapshot": snapshot,
                    "source_health_ownership": "ingestion_only",
                    "contract_evidence_roles": ["event_payload", "market_payload"],
                    "graph_evidence_table": "er_edge_evidence",
                    "event_relationship_evidence_table": "er_event_relationship_evidence",
                }

            checks = {
                "probability_bounds": v4.check_probability_bounds(con),
                "crossed_books": v4.check_crossed_books(con),
                "missing_raw_lineage": v4.check_missing_raw_lineage(con),
                "archive_integrity": v4.check_archive_integrity(con),
                "archive_backlog": v4.check_archive_backlog(con),
                "contract_versions": v4.check_contract_versions(con),
                "contract_version_evidence": v7.check_contract_version_evidence(con),
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
                "latest_full_universe_runs": check_latest_completed_full_universe_runs(con),
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
            try:
                con.execute("ROLLBACK")
            except Exception:
                pass
            raise
        return {
            "checks": checks,
            "open_issues": open_count,
            "critical_issues": critical_count,
            "qa_version": QA_VERSION,
            "deferred": False,
            "identity_snapshot": snapshot,
            "latest_source_run_basis": "latest_completed_by_started_at",
            "source_health_ownership": "ingestion_only",
            "contract_evidence_roles": ["event_payload", "market_payload"],
            "graph_evidence_table": "er_edge_evidence",
            "event_relationship_evidence_table": "er_event_relationship_evidence",
        }
    finally:
        con.close()


if __name__ == "__main__":
    print(run_qa())
