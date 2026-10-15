"""Institutional QA v10: snapshot-safe QA with interval-based mapping cardinality.

v10 preserves QA v9's two-phase read-snapshot/publication barrier and complete rule set. The only
rule-semantics execution change is point-in-time mapping cardinality. Instead of range-joining every
market observation to every mapping interval, v10 proves the same institutional-era invariant with
an interval-chain check:

1. Mapping intervals for each source contract must be continuous and non-overlapping, with exactly
   one open final interval.
2. The first non-legacy institutional observation must occur at or after the first mapping interval.

Together these conditions imply that every institutional observation is covered by exactly one
canonical mapping. The rule remains critical and uses the same rule code/issue identity as earlier
QA generations; only the execution plan changes.
"""

from __future__ import annotations

import time
from typing import Any

import institutional_qa_v4 as v4
import institutional_qa_v5 as v5
import institutional_qa_v6 as v6
import institutional_qa_v7 as v7
import institutional_qa_v8 as v8
import institutional_qa_v9 as v9
from db import connect

QA_VERSION = "institutional_qa_v10_interval_mapping_coverage"
POINT_IN_TIME_MAPPING_METHOD = "mapping_interval_chain_plus_first_observation_coverage_v1"

POINT_IN_TIME_MAPPING_SQL = r"""
WITH ordered AS (
  SELECT source_contract_id,mapping_id,valid_from,valid_to,
         lead(valid_from) OVER (
           PARTITION BY source_contract_id ORDER BY valid_from,mapping_id
         ) AS next_valid_from,
         row_number() OVER (
           PARTITION BY source_contract_id ORDER BY valid_from DESC,mapping_id DESC
         ) AS rev_rn
  FROM er_event_mappings
), chain_bad AS (
  SELECT source_contract_id AS object_id,
         'Mapping interval chain is not continuous/non-overlapping with exactly one open final mapping' AS details
  FROM ordered
  GROUP BY source_contract_id
  HAVING sum(CASE
               WHEN next_valid_from IS NOT NULL
                AND valid_to IS DISTINCT FROM next_valid_from THEN 1 ELSE 0 END)>0
      OR sum(CASE WHEN rev_rn=1 AND valid_to IS NOT NULL THEN 1 ELSE 0 END)>0
      OR sum(CASE WHEN valid_to IS NULL THEN 1 ELSE 0 END)<>1
), observation_start AS (
  SELECT source_contract_id,min(received_at) AS first_observation_at
  FROM er_market_observations
  WHERE coalesce(methodology_version,'')<>'legacy_collector_v0.3'
  GROUP BY source_contract_id
), mapping_start AS (
  SELECT source_contract_id,min(valid_from) AS first_mapping_at
  FROM er_event_mappings
  GROUP BY source_contract_id
), coverage_bad AS (
  SELECT o.source_contract_id AS object_id,
         'First institutional observation is not covered by the mapping interval chain' AS details
  FROM observation_start o
  LEFT JOIN mapping_start m USING(source_contract_id)
  WHERE m.first_mapping_at IS NULL OR m.first_mapping_at>o.first_observation_at
), violations AS (
  SELECT object_id,details FROM chain_bad
  UNION ALL
  SELECT object_id,details FROM coverage_bad
)
SELECT object_id,string_agg(details,'; ' ORDER BY details) AS details
FROM violations
GROUP BY object_id
"""


def check_point_in_time_mapping_cardinality(con) -> int:
    return v4._sync_rule(
        con,
        object_type="source_contract",
        rule_code="point_in_time_mapping_cardinality",
        severity="critical",
        sql=POINT_IN_TIME_MAPPING_SQL,
        resolution_note="Mapping interval chain and first-observation coverage are valid",
    )


def _collect_rules(read_con, stale_minutes: int):
    """Collect the exact QA v9 rule set, replacing only the expensive mapping-history plan."""
    collected: list[v9.CollectedRule] = []
    rule_timings: dict[str, float] = {}
    original_sync = v4._sync_rule

    def collect_rule(
        con,
        *,
        object_type: str,
        rule_code: str,
        severity: str,
        sql: str,
        params: list[Any] | None = None,
        resolution_note: str = "Condition no longer present",
    ) -> int:
        started = time.monotonic()
        rows = v9._rows_dict(con.execute(sql, params or []))
        elapsed = time.monotonic() - started
        key = f"{object_type}:{rule_code}"
        rule_timings[key] = round(rule_timings.get(key, 0.0) + elapsed, 6)
        collected.append(v9.CollectedRule(
            object_type=object_type,
            rule_code=rule_code,
            severity=severity,
            resolution_note=resolution_note,
            rows=rows,
        ))
        return len(rows)

    v4._sync_rule = collect_rule
    try:
        checks = {
            "probability_bounds": v4.check_probability_bounds(read_con),
            "crossed_books": v4.check_crossed_books(read_con),
            "missing_raw_lineage": v4.check_missing_raw_lineage(read_con),
            "archive_integrity": v4.check_archive_integrity(read_con),
            "archive_backlog": v4.check_archive_backlog(read_con),
            "contract_versions": v4.check_contract_versions(read_con),
            "contract_version_evidence": v7.check_contract_version_evidence(read_con),
            "canonical_version_windows": v4.check_canonical_version_windows(read_con),
            "mapping_cardinality": v4.check_active_mapping_cardinality(read_con),
            "source_freshness": v5.check_source_freshness(read_con, stale_minutes),
            "ingestion_volume_drop": v4.check_ingestion_volume_drop(read_con),
            "observation_clocks": v4.check_observation_clocks(read_con),
            "mapping_ambiguity": v4.check_mapping_ambiguity(read_con),
            "inferred_edge_metadata": v4.check_inferred_edge_metadata(read_con),
            "active_orphan_events": v4.check_active_orphan_events(read_con),
            "mapping_targets": v4.check_mapping_targets_distributable_events(read_con),
            "point_in_time_mapping_cardinality": check_point_in_time_mapping_cardinality(read_con),
            "reviewed_relationship_provenance": v6.check_reviewed_relationship_provenance(read_con),
            "active_contract_semantic_coverage": v4.check_active_contract_semantic_coverage(read_con),
            "reference_master_versions": v4.check_reference_master_versions(read_con),
            "ready_release_manifests": v4.check_ready_release_manifests(read_con),
            "migration_ledger": v4.check_migration_ledger(read_con),
            "latest_full_universe_runs": v8.check_latest_completed_full_universe_runs(read_con),
            "presence_coherence": v4.check_presence_coherence(read_con),
            "raw_durability": v4.check_raw_durability(read_con),
            "stuck_ingestion_runs": v4.check_stuck_ingestion_runs(read_con, stale_minutes),
        }
        return checks, collected, rule_timings
    finally:
        v4._sync_rule = original_sync


def run_qa(stale_minutes: int = 20) -> dict:
    total_started = time.monotonic()

    read_con = connect(read_only=True)
    try:
        read_con.execute("BEGIN TRANSACTION")
        snapshot_before = v9._fingerprint(read_con)
        if not snapshot_before["identity"]["ready"]:
            read_con.execute("ROLLBACK")
            return {
                "checks": {}, "open_issues": None, "critical_issues": None,
                "qa_version": QA_VERSION, "deferred": True,
                "deferred_reason": "identity_snapshot_in_transition",
                "identity_snapshot": snapshot_before["identity"],
                "phase": "precheck",
                "point_in_time_mapping_method": POINT_IN_TIME_MAPPING_METHOD,
            }
        evaluation_started = time.monotonic()
        checks, collected, rule_timings = _collect_rules(read_con, stale_minutes)
        evaluation_seconds = time.monotonic() - evaluation_started
        read_con.execute("COMMIT")
    except Exception:
        try:
            read_con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        read_con.close()

    barrier_con = connect(read_only=True)
    try:
        snapshot_after = v9._fingerprint(barrier_con)
    finally:
        barrier_con.close()
    if snapshot_after != snapshot_before:
        return {
            "checks": checks, "open_issues": None, "critical_issues": None,
            "qa_version": QA_VERSION, "deferred": True,
            "deferred_reason": "core_fingerprint_changed_during_read_evaluation",
            "identity_snapshot": snapshot_after["identity"],
            "read_snapshot_fingerprint": snapshot_before,
            "current_fingerprint": snapshot_after,
            "rule_timings": rule_timings,
            "point_in_time_mapping_method": POINT_IN_TIME_MAPPING_METHOD,
            "timing": {
                "evaluation_seconds": round(evaluation_seconds, 6),
                "issue_sync_seconds": 0.0,
                "total_seconds": round(time.monotonic() - total_started, 6),
            },
        }

    sync_started = time.monotonic()
    write_con = connect()
    try:
        write_con.execute("BEGIN TRANSACTION")
        try:
            synchronized = {
                f"{rule.object_type}:{rule.rule_code}": v9._sync_collected_rule(write_con, rule)
                for rule in collected
            }
            open_count = int(write_con.execute(
                "SELECT count(*) FROM er_quality_issues WHERE status='open'"
            ).fetchone()[0])
            critical_count = int(write_con.execute(
                "SELECT count(*) FROM er_quality_issues WHERE status='open' AND severity='critical'"
            ).fetchone()[0])
            write_con.execute("COMMIT")
        except Exception:
            write_con.execute("ROLLBACK")
            raise
    finally:
        write_con.close()
    sync_seconds = time.monotonic() - sync_started

    return {
        "checks": checks,
        "open_issues": open_count,
        "critical_issues": critical_count,
        "qa_version": QA_VERSION,
        "deferred": False,
        "identity_snapshot": snapshot_after["identity"],
        "source_health_ownership": "ingestion_only",
        "contract_evidence_roles": ["event_payload", "market_payload"],
        "graph_evidence_table": "er_edge_evidence",
        "event_relationship_evidence_table": "er_event_relationship_evidence",
        "execution": "read_only_snapshot_then_short_issue_sync_interval_mapping_coverage",
        "point_in_time_mapping_method": POINT_IN_TIME_MAPPING_METHOD,
        "rules_synchronized": synchronized,
        "rule_timings": rule_timings,
        "timing": {
            "evaluation_seconds": round(evaluation_seconds, 6),
            "issue_sync_seconds": round(sync_seconds, 6),
            "total_seconds": round(time.monotonic() - total_started, 6),
        },
    }
