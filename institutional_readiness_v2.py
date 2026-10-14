"""Distribution-readiness gate for the institutional EventRisk data plane.

This gate is deliberately stricter than liveness. It answers whether the normalized
dataset is fit to distribute for institutional research, not whether any investment
decision should be made from it.
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

        orphan_active = con.execute("""
            SELECT count(*) FROM (
              SELECT e.canonical_event_id
              FROM er_canonical_events e
              LEFT JOIN er_event_mappings m
                ON m.canonical_event_id=e.canonical_event_id AND m.valid_to IS NULL
              WHERE e.status='active'
              GROUP BY e.canonical_event_id
              HAVING count(m.mapping_id)=0
            )
        """).fetchone()[0]
        controls.append(_control(
            "no_active_orphan_canonical_events",
            orphan_active == 0,
            "high",
            "Every active canonical event has at least one active source mapping",
            observed=orphan_active,
            threshold=0,
        ))

        invalid_targets = con.execute("""
            SELECT count(*) FROM er_event_mappings m
            JOIN er_canonical_events e USING(canonical_event_id)
            WHERE m.valid_to IS NULL AND e.status IN ('superseded','inactive','retired')
        """).fetchone()[0]
        controls.append(_control(
            "active_mappings_target_served_events",
            invalid_targets == 0,
            "critical",
            "No active source mapping points at a superseded/inactive canonical event",
            observed=invalid_targets,
            threshold=0,
        ))

        pit_bad = con.execute("""
            SELECT count(*) FROM (
              SELECT o.source_contract_id,o.observed_at,o.received_at,count(m.mapping_id) n
              FROM er_market_observations o
              LEFT JOIN er_event_mappings m
                ON m.source_contract_id=o.source_contract_id
               AND m.valid_from<=o.received_at
               AND (m.valid_to IS NULL OR m.valid_to>o.received_at)
              WHERE coalesce(o.methodology_version,'')<>'legacy_collector_v0.3'
              GROUP BY o.source_contract_id,o.observed_at,o.received_at
              HAVING count(m.mapping_id)<>1
            )
        """).fetchone()[0]
        controls.append(_control(
            "point_in_time_mapping_reconstructable",
            pit_bad == 0,
            "critical",
            "Every institutional observation resolves to exactly one mapping at receipt time",
            observed=pit_bad,
            threshold=0,
        ))

        reviewed_bad = con.execute("""
            SELECT count(*) FROM er_event_relationships r
            LEFT JOIN er_event_relationship_evidence e USING(relationship_id)
            WHERE r.valid_to IS NULL AND r.relationship_class='reviewed_relationship'
            GROUP BY r.relationship_id,r.human_reviewed,r.review_status,r.source_candidate_id,r.created_by
            HAVING r.human_reviewed IS DISTINCT FROM TRUE
                OR coalesce(r.review_status,'')<>'reviewed'
                OR r.source_candidate_id IS NULL
                OR r.created_by IS NULL
                OR count(e.evidence_id)=0
        """).fetchall()
        controls.append(_control(
            "reviewed_relationships_have_provenance",
            len(reviewed_bad) == 0,
            "high",
            "Reviewed event relationships carry reviewer/candidate provenance and evidence",
            observed=len(reviewed_bad),
            threshold=0,
        ))

        active_contracts = con.execute(
            "SELECT count(*) FROM er_source_contracts WHERE active=TRUE"
        ).fetchone()[0]
        semantic_contracts = con.execute("""
            SELECT count(*) FROM er_source_contracts c
            JOIN er_latest_contract_semantic_versions v USING(source_contract_id)
            WHERE c.active=TRUE AND v.metadata_completeness='full_v2'
        """).fetchone()[0]
        controls.append(_control(
            "active_contracts_have_full_semantic_versions",
            semantic_contracts == active_contracts,
            "critical",
            "Every currently active source contract has prospective full semantic normalization tied to raw event and market evidence",
            observed={"covered": semantic_contracts, "active": active_contracts},
            threshold="covered == active",
        ))

        semantic_evidence_bad = con.execute("""
            SELECT count(*) FROM er_latest_contract_semantic_versions
            WHERE event_raw_snapshot_id IS NULL OR market_raw_snapshot_id IS NULL
               OR coalesce(metadata_completeness,'')<>'full_v2'
        """).fetchone()[0]
        controls.append(_control(
            "full_semantic_versions_have_raw_evidence",
            semantic_evidence_bad == 0,
            "critical",
            "Full semantic contract versions reference exact immutable event and market raw snapshots",
            observed=semantic_evidence_bad,
            threshold=0,
        ))

        reference_gaps = {}
        for current, versioned, object_id in [
            ("er_sources", "er_latest_source_versions", "source_id"),
            ("er_entities", "er_latest_entity_versions", "entity_id"),
            ("er_instruments", "er_latest_instrument_versions", "instrument_id"),
        ]:
            current_count = con.execute(f"SELECT count(*) FROM {current}").fetchone()[0]
            covered_count = con.execute(
                f"SELECT count(*) FROM {current} c JOIN {versioned} v USING({object_id})"
            ).fetchone()[0]
            reference_gaps[current] = {"covered": covered_count, "current": current_count}
        reference_ok = all(v["covered"] == v["current"] for v in reference_gaps.values())
        controls.append(_control(
            "reference_masters_versioned",
            reference_ok,
            "high",
            "Current source, entity and instrument masters all have point-in-time version records",
            observed=reference_gaps,
            threshold="covered == current for each master",
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
    result["policy"]["distribution_boundary"] = (
        "Readiness certifies EventRisk data-pipeline controls only; institutions own scenarios, models and investment decisions."
    )
    return result
