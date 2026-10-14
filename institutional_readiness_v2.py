"""Distribution-readiness gate for the institutional EventRisk data plane.

Extends the base readiness gate with schema-ledger, canonical-universe and point-in-time
mapping controls. Readiness answers whether the dataset is fit to distribute, not whether
any investment decision should be made from it.
"""

from __future__ import annotations

from db import connect
from institutional_migrations import MIGRATION_FILES
from institutional_readiness import evaluate_readiness as base_readiness


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
    result = base_readiness(stale_minutes=stale_minutes, require_archive=require_archive)
    controls = list(result.get("controls") or [])

    con = connect(read_only=True)
    try:
        migration_count = con.execute("SELECT count(*) FROM er_schema_migrations").fetchone()[0]
        controls.append(_control(
            "migration_ledger_current",
            migration_count == len(MIGRATION_FILES),
            "critical",
            "Institutional schema migration ledger matches the repository migration set",
            observed=migration_count,
            threshold=len(MIGRATION_FILES),
        ))

        orphan_active = con.execute(
            """SELECT count(*) FROM (
                 SELECT e.canonical_event_id
                 FROM er_canonical_events e
                 LEFT JOIN er_event_mappings m
                   ON m.canonical_event_id=e.canonical_event_id AND m.valid_to IS NULL
                 WHERE e.status='active'
                 GROUP BY e.canonical_event_id
                 HAVING count(m.mapping_id)=0
               )"""
        ).fetchone()[0]
        controls.append(_control(
            "no_active_orphan_canonical_events",
            orphan_active == 0,
            "high",
            "Every active canonical event has at least one active source mapping",
            observed=orphan_active,
            threshold=0,
        ))

        invalid_targets = con.execute(
            """SELECT count(*) FROM er_event_mappings m
               JOIN er_canonical_events e USING(canonical_event_id)
               WHERE m.valid_to IS NULL AND e.status IN ('superseded','inactive','retired')"""
        ).fetchone()[0]
        controls.append(_control(
            "active_mappings_target_served_events",
            invalid_targets == 0,
            "critical",
            "No active source mapping points at a superseded/inactive canonical event",
            observed=invalid_targets,
            threshold=0,
        ))

        pit_bad = con.execute(
            """SELECT count(*) FROM (
                 SELECT o.source_contract_id,o.observed_at,o.received_at,count(m.mapping_id) n
                 FROM er_market_observations o
                 LEFT JOIN er_event_mappings m
                   ON m.source_contract_id=o.source_contract_id
                  AND m.valid_from<=o.received_at
                  AND (m.valid_to IS NULL OR m.valid_to>o.received_at)
                 WHERE coalesce(o.methodology_version,'')<>'legacy_collector_v0.3'
                 GROUP BY o.source_contract_id,o.observed_at,o.received_at
                 HAVING count(m.mapping_id)<>1
               )"""
        ).fetchone()[0]
        controls.append(_control(
            "point_in_time_mapping_reconstructable",
            pit_bad == 0,
            "critical",
            "Every institutional observation resolves to exactly one mapping at receipt time",
            observed=pit_bad,
            threshold=0,
        ))

        reviewed_bad = con.execute(
            """SELECT count(*) FROM er_event_relationships r
               LEFT JOIN er_event_relationship_evidence e USING(relationship_id)
               WHERE r.valid_to IS NULL AND r.relationship_class='reviewed_relationship'
               GROUP BY r.relationship_id,r.human_reviewed,r.review_status,r.source_candidate_id,r.created_by
               HAVING r.human_reviewed IS DISTINCT FROM TRUE
                   OR coalesce(r.review_status,'')<>'reviewed'
                   OR r.source_candidate_id IS NULL
                   OR r.created_by IS NULL
                   OR count(e.evidence_id)=0"""
        ).fetchall()
        controls.append(_control(
            "reviewed_relationships_have_provenance",
            len(reviewed_bad) == 0,
            "high",
            "Reviewed event relationships carry reviewer/candidate provenance and evidence",
            observed=len(reviewed_bad),
            threshold=0,
        ))
    finally:
        con.close()

    failed = [
        c for c in controls
        if not c.get("passed") and c.get("severity") in {"critical", "high"}
    ]
    result["controls"] = controls
    result["failed_controls"] = [c["name"] for c in failed]
    result["ready"] = len(failed) == 0
    result.setdefault("policy", {})["readiness_version"] = "institutional_readiness_v2"
    return result
