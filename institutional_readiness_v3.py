"""Institutional distribution readiness v3.

Adds full-universe and raw-evidence controls to the semantic/temporal v2 readiness gate.
This certifies only data-pipeline fitness for distribution; it never certifies an investment
model, scenario or portfolio decision.
"""

from __future__ import annotations

import os

from db import connect
from institutional_readiness_v2 import evaluate_readiness as v2_readiness

READINESS_VERSION = "institutional_readiness_v3"


def _control(name: str, passed: bool, severity: str, detail: str, observed=None, threshold=None):
    return {
        "name": name,
        "passed": bool(passed),
        "severity": severity,
        "detail": detail,
        "observed": observed,
        "threshold": threshold,
    }


def evaluate_readiness(*, stale_minutes: int | None = None, require_archive: bool | None = None) -> dict:
    result = v2_readiness(stale_minutes=stale_minutes, require_archive=require_archive)
    controls = list(result.get("controls") or [])
    threshold = max(2, int(os.getenv("PRESENCE_MISS_THRESHOLD", "3")))

    con = connect(read_only=True)
    try:
        latest_runs = con.execute("""
            WITH ranked AS (
              SELECT r.*,row_number() OVER(PARTITION BY source_id ORDER BY started_at DESC,run_id DESC) rn
              FROM er_ingestion_runs r
            )
            SELECT s.source_id,r.run_id,r.status,r.collection_scope,r.universe_complete
            FROM er_sources s
            LEFT JOIN ranked r ON r.source_id=s.source_id AND r.rn=1
            WHERE s.active=TRUE ORDER BY s.source_id
        """).fetchall()
        incomplete = [r for r in latest_runs if r[1] is None or r[2] != "success" or r[3] != "full" or r[4] is not True]
        controls.append(_control(
            "latest_source_universes_complete",
            not incomplete,
            "critical",
            "Every active source's latest run is a successful complete full-universe collection",
            observed={r[0]: {"run_id": r[1], "status": r[2], "scope": r[3], "complete": r[4]} for r in latest_runs},
            threshold="status=success, scope=full, universe_complete=true",
        ))

        presence_bad = con.execute("""
            SELECT count(*) FROM er_source_contracts c
            JOIN er_contract_presence_state p USING(source_contract_id)
            WHERE (p.listing_state='missing_confirmed' AND c.active=TRUE)
               OR (p.listing_state='missing_confirmed' AND p.consecutive_misses<?)
               OR (p.listing_state='observed' AND (p.consecutive_misses<>0 OR p.deactivated_at IS NOT NULL))
        """, [threshold]).fetchone()[0]
        controls.append(_control(
            "contract_presence_state_coherent",
            presence_bad == 0,
            "critical",
            "Source-contract serving state agrees with conservative full-universe presence reconciliation",
            observed=presence_bad,
            threshold=0,
        ))

        raw_bad = con.execute("""
            SELECT count(*) FROM er_raw_snapshots r
            LEFT JOIN er_raw_archive_objects a USING(raw_snapshot_id)
            WHERE r.payload_json IS NULL
              AND NOT (
                a.status='archived' AND a.archive_key IS NOT NULL AND a.archive_member IS NOT NULL
                AND a.archive_bundle_hash IS NOT NULL AND a.content_hash=r.content_hash
              )
        """).fetchone()[0]
        controls.append(_control(
            "every_raw_payload_recoverable",
            raw_bad == 0,
            "critical",
            "Every raw source payload remains in the analytical store or a hash-verified object archive",
            observed=raw_bad,
            threshold=0,
        ))

        stale = stale_minutes or max(20, int(os.getenv("COLLECT_INTERVAL_SECONDS", "300")) // 60 * 4)
        running_too_long = con.execute("""
            SELECT count(*) FROM er_ingestion_runs
            WHERE status='running' AND started_at < now() - (? * INTERVAL '1 minute')
        """, [max(30, stale * 2)]).fetchone()[0]
        controls.append(_control(
            "no_stuck_ingestion_runs",
            running_too_long == 0,
            "high",
            "No ingestion run has remained in running state beyond the operational threshold",
            observed=running_too_long,
            threshold=0,
        ))
    finally:
        con.close()

    failed = [c for c in controls if not c.get("passed") and c.get("severity") in {"critical", "high"}]
    result["controls"] = controls
    result["failed_controls"] = [c["name"] for c in failed]
    result["ready"] = len(failed) == 0
    result.setdefault("policy", {})["readiness_version"] = READINESS_VERSION
    result["policy"]["distribution_boundary"] = (
        "Readiness covers EventRisk observed/normalized data controls only; institutions own downstream models and decisions."
    )
    return result
