"""Institutional QA v5: concurrency-safe enrichment QA.

QA v5 preserves every v4 data-quality rule and issue-ledger semantic while removing the
`er_source_health` write from source-freshness evaluation. The frequent ingestion lane is the
sole owner of source-health state; enrichment QA observes that state and records stale-source
violations only in `er_quality_issues`. This prevents advisory QA from conflicting with a live
five-minute ingestion transaction without weakening any freshness control.
"""

from __future__ import annotations

import institutional_qa_v4 as v4
from db import connect

QA_VERSION = "institutional_qa_v5_concurrency_safe"


def check_source_freshness(con, stale_minutes: int) -> int:
    """Record stale-source issues without mutating ingestion-owned source-health rows."""
    return v4._sync_rule(
        con,
        object_type="source",
        rule_code="source_stale",
        severity="high",
        sql="""
          SELECT s.source_id AS object_id,
                 'Last success=' || coalesce(cast(h.last_success_at AS varchar),'NULL') ||
                 '; last observation=' || coalesce(cast(h.last_observation_at AS varchar),'NULL') ||
                 '; last status=' || coalesce(h.last_run_status,'NULL') ||
                 '; age_minutes=' || coalesce(cast(date_diff('minute',h.last_success_at,now()) AS varchar),'NULL') AS details
          FROM er_sources s LEFT JOIN er_source_health h USING(source_id)
          WHERE s.active=TRUE
            AND (h.last_success_at IS NULL OR date_diff('minute',h.last_success_at,now())>?)
        """,
        params=[stale_minutes],
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
                "source_freshness": check_source_freshness(con, stale_minutes),
                "ingestion_volume_drop": v4.check_ingestion_volume_drop(con),
                "observation_clocks": v4.check_observation_clocks(con),
                "mapping_ambiguity": v4.check_mapping_ambiguity(con),
                "inferred_edge_metadata": v4.check_inferred_edge_metadata(con),
                "active_orphan_events": v4.check_active_orphan_events(con),
                "mapping_targets": v4.check_mapping_targets_distributable_events(con),
                "point_in_time_mapping_cardinality": v4.check_point_in_time_mapping_cardinality(con),
                "reviewed_relationship_provenance": v4.check_reviewed_relationship_provenance(con),
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
        }
    finally:
        con.close()


if __name__ == "__main__":
    print(run_qa())
