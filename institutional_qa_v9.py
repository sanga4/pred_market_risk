"""Institutional QA v9: two-phase snapshot evaluation with short issue-ledger sync.

v9 preserves QA v8's complete rule set, severities, issue identifiers, resolution semantics and
identity-snapshot guard. It changes only execution isolation:

1. Every expensive validation query runs against one read-only transaction snapshot. `_sync_rule`
   is temporarily replaced with a collector that records the exact violation set instead of
   writing `er_quality_issues` while the historical scans are executing.
2. After the read snapshot closes, the current core fingerprint is compared with the snapshot
   fingerprint. If source/identity state changed, the audit is deferred and *no issue rows are
   mutated*.
3. Only for a stable snapshot are the collected violation sets synchronized in one short write
   transaction.

This removes the prior multi-minute transaction that mixed large historical scans with quality
ledger writes, while keeping the same investment-grade fail conditions and preventing QA from
publishing findings from an internally inconsistent source/identity snapshot.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import time
from typing import Any

import pyarrow as pa

import institutional_qa_v4 as v4
import institutional_qa_v5 as v5
import institutional_qa_v6 as v6
import institutional_qa_v7 as v7
import institutional_qa_v8 as v8
from db import connect

QA_VERSION = "institutional_qa_v9_two_phase_read_snapshot"


@dataclass
class CollectedRule:
    object_type: str
    rule_code: str
    severity: str
    resolution_note: str
    rows: list[dict]


def _rows_dict(cur) -> list[dict]:
    columns = [description[0] for description in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def _fingerprint(con) -> dict:
    """Small current-state fingerprint used only as a publication barrier."""
    identity = v8.identity_snapshot_state(con)
    completed = [
        {
            "source_id": row[0],
            "run_id": row[1],
            "started_at": str(row[2]),
            "status": row[3],
            "universe_complete": row[4],
            "collection_scope": row[5],
        }
        for row in con.execute("""
            WITH ranked AS (
              SELECT r.*,
                     row_number() OVER(
                       PARTITION BY source_id ORDER BY started_at DESC,run_id DESC
                     ) rn
              FROM er_ingestion_runs r
              WHERE finished_at IS NOT NULL AND status<>'running'
            )
            SELECT s.source_id,r.run_id,r.started_at,r.status,r.universe_complete,r.collection_scope
            FROM er_sources s
            LEFT JOIN ranked r ON r.source_id=s.source_id AND r.rn=1
            WHERE s.active=TRUE
            ORDER BY s.source_id
        """).fetchall()
    ]
    identity_pass = con.execute("""
        SELECT pass_id,status,started_at,finished_at,coalesce(contracts_changed,0)
        FROM er_mapping_identity_passes
        ORDER BY started_at DESC,pass_id DESC LIMIT 1
    """).fetchone()
    return {
        "identity": identity,
        "latest_completed_source_runs": completed,
        "latest_identity_pass": None if identity_pass is None else [str(v) for v in identity_pass],
        "active_contracts": int(con.execute(
            "SELECT count(*) FROM er_source_contracts WHERE active=TRUE"
        ).fetchone()[0]),
        "active_mappings": int(con.execute(
            "SELECT count(*) FROM er_event_mappings WHERE valid_to IS NULL"
        ).fetchone()[0]),
    }


def _collect_rules(read_con, stale_minutes: int) -> tuple[dict, list[CollectedRule], dict[str, float]]:
    collected: list[CollectedRule] = []
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
        rows = _rows_dict(con.execute(sql, params or []))
        elapsed = time.monotonic() - started
        key = f"{object_type}:{rule_code}"
        rule_timings[key] = round(rule_timings.get(key, 0.0) + elapsed, 6)
        collected.append(CollectedRule(
            object_type=object_type,
            rule_code=rule_code,
            severity=severity,
            resolution_note=resolution_note,
            rows=rows,
        ))
        return len(rows)

    v4._sync_rule = collect_rule
    try:
        # This is intentionally the exact QA v8 rule set/order.
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
            "point_in_time_mapping_cardinality": v4.check_point_in_time_mapping_cardinality(read_con),
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


def _sync_collected_rule(con, rule: CollectedRule) -> int:
    staged = []
    for row in rule.rows:
        object_id = str(row["object_id"])
        staged.append({
            "quality_issue_id": v4.qa_base.issue_id(rule.object_type, object_id, rule.rule_code),
            "object_id": object_id,
            "details": str(row.get("details") or "")[:12000],
        })
    temp = f"_qa9_{rule.rule_code}_{rule.object_type}".replace("-", "_")
    if staged:
        con.register(temp + "_arrow", pa.Table.from_pylist(staged))
        try:
            con.execute(f"CREATE OR REPLACE TEMP TABLE {temp} AS SELECT * FROM {temp}_arrow")
        finally:
            con.unregister(temp + "_arrow")
        con.execute(f"""
            INSERT INTO er_quality_issues(
              quality_issue_id,object_type,object_id,rule_code,severity,status,details,detected_at,
              resolved_at,resolution_notes)
            SELECT quality_issue_id,?,object_id,?,?,'open',details,now(),NULL,NULL FROM {temp}
            ON CONFLICT(quality_issue_id) DO UPDATE SET
              severity=excluded.severity,status='open',details=excluded.details,detected_at=now(),
              resolved_at=NULL,resolution_notes=NULL
        """, [rule.object_type, rule.rule_code, rule.severity])
        con.execute(f"""
            UPDATE er_quality_issues q
            SET status='resolved',resolved_at=now(),resolution_notes=?
            WHERE q.status='open' AND q.object_type=? AND q.rule_code=?
              AND NOT EXISTS (SELECT 1 FROM {temp} b WHERE b.object_id=q.object_id)
        """, [rule.resolution_note, rule.object_type, rule.rule_code])
    else:
        con.execute("""
            UPDATE er_quality_issues
            SET status='resolved',resolved_at=now(),resolution_notes=?
            WHERE status='open' AND object_type=? AND rule_code=?
        """, [rule.resolution_note, rule.object_type, rule.rule_code])
    return len(staged)


def run_qa(stale_minutes: int = 20) -> dict:
    total_started = time.monotonic()

    # Phase 1: one consistent read-only snapshot. No issue ledger or source-health writes occur.
    read_con = connect(read_only=True)
    try:
        read_con.execute("BEGIN TRANSACTION")
        snapshot_before = _fingerprint(read_con)
        if not snapshot_before["identity"]["ready"]:
            read_con.execute("ROLLBACK")
            return {
                "checks": {}, "open_issues": None, "critical_issues": None,
                "qa_version": QA_VERSION, "deferred": True,
                "deferred_reason": "identity_snapshot_in_transition",
                "identity_snapshot": snapshot_before["identity"],
                "phase": "precheck",
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

    # Phase 2 publication barrier: do not publish violation sets if the hot lane changed beneath
    # the read snapshot. The next enrichment attempt can simply evaluate the newer stable state.
    barrier_con = connect(read_only=True)
    try:
        snapshot_after = _fingerprint(barrier_con)
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
            "timing": {
                "evaluation_seconds": round(evaluation_seconds, 6),
                "issue_sync_seconds": 0.0,
                "total_seconds": round(time.monotonic() - total_started, 6),
            },
        }

    # Phase 3: compact issue-ledger publication only. No historical validation scan occurs while
    # this write transaction is open.
    sync_started = time.monotonic()
    write_con = connect()
    try:
        write_con.execute("BEGIN TRANSACTION")
        try:
            synchronized = {
                f"{rule.object_type}:{rule.rule_code}": _sync_collected_rule(write_con, rule)
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
        "execution": "read_only_snapshot_then_short_issue_sync",
        "rules_synchronized": synchronized,
        "rule_timings": rule_timings,
        "timing": {
            "evaluation_seconds": round(evaluation_seconds, 6),
            "issue_sync_seconds": round(sync_seconds, 6),
            "total_seconds": round(time.monotonic() - total_started, 6),
        },
    }


if __name__ == "__main__":
    print(json.dumps(run_qa(), default=str, sort_keys=True))
