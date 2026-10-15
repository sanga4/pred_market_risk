"""Layer-specific institutional distribution readiness.

Observed, deterministic identity and advisory enrichment have different operational SLOs. This
module reports them independently so a slow fuzzy/graph enrichment job cannot make the raw event
curves appear unavailable, while consumers that require enriched Layer 2 data can still fail
closed on that separate readiness surface.

No control makes an investment decision. These are data-pipeline fitness checks only.
"""

from __future__ import annotations

import os
from typing import Any

from db import connect

READINESS_VERSION = "institutional_readiness_v4_layered"


def _control(name: str, passed: bool, severity: str, detail: str, observed: Any = None, threshold: Any = None):
    return {
        "name": name,
        "passed": bool(passed),
        "severity": severity,
        "detail": detail,
        "observed": observed,
        "threshold": threshold,
    }


def _finalize(layer: str, controls: list[dict], *, policy: dict | None = None) -> dict:
    failed = [c for c in controls if not c["passed"] and c["severity"] in {"critical", "high"}]
    return {
        "layer": layer,
        "ready": not failed,
        "controls": controls,
        "failed_controls": [c["name"] for c in failed],
        "policy": {
            "readiness_version": READINESS_VERSION,
            "decision_boundary": "Data-pipeline fitness only; EventRisk does not make portfolio or investment decisions.",
            **(policy or {}),
        },
    }


def _table_names(con) -> set[str]:
    return {r[0] for r in con.execute("SHOW TABLES").fetchall()}


def evaluate_observed_readiness(*, stale_minutes: int | None = None, require_archive: bool | None = None) -> dict:
    stale_minutes = stale_minutes or max(20, int(os.getenv("COLLECT_INTERVAL_SECONDS", "300")) // 60 * 4)
    if require_archive is None:
        require_archive = all(os.getenv(k) for k in [
            "RAW_ARCHIVE_ENDPOINT", "RAW_ARCHIVE_BUCKET",
            "RAW_ARCHIVE_ACCESS_KEY_ID", "RAW_ARCHIVE_SECRET_ACCESS_KEY",
        ])

    controls: list[dict] = []
    con = connect(read_only=True)
    try:
        tables = _table_names(con)
        required = {
            "er_sources", "er_ingestion_runs", "er_source_health", "er_raw_snapshots",
            "er_source_contracts", "er_contract_semantic_versions", "er_market_observations",
            "er_contract_presence_state", "er_raw_current_snapshots", "er_market_current_state",
        }
        missing = sorted(required - tables)
        controls.append(_control(
            "observed_schema_complete", not missing, "critical",
            "Observed-data tables exist" if not missing else f"Missing observed-data tables: {missing}",
            observed=sorted(required & tables), threshold=sorted(required),
        ))
        if missing:
            return _finalize("observed", controls, policy={"stale_minutes": stale_minutes, "archive_required": bool(require_archive)})

        latest_runs = con.execute("""
            WITH ranked AS (
              SELECT r.*,row_number() OVER(PARTITION BY source_id ORDER BY started_at DESC,run_id DESC) rn
              FROM er_ingestion_runs r
            )
            SELECT s.source_id,r.status,r.universe_complete,r.collection_scope,r.finished_at,
                   date_diff('minute',coalesce(r.finished_at,TIMESTAMPTZ '1970-01-01'),now()) age_minutes
            FROM er_sources s LEFT JOIN ranked r ON r.source_id=s.source_id AND r.rn=1
            WHERE s.active=TRUE ORDER BY s.source_id
        """).fetchall()
        bad_latest = [r for r in latest_runs if r[1] != "success" or r[2] is not True or r[3] != "full"]
        controls.append(_control(
            "latest_source_runs_full_and_successful", not bad_latest, "critical",
            "Every active source's latest run is a successful complete full-universe traversal",
            observed=[{"source": r[0], "status": r[1], "complete": r[2], "scope": r[3]} for r in latest_runs],
            threshold="status=success, universe_complete=true, collection_scope=full",
        ))
        stale = [r for r in latest_runs if r[4] is None or r[5] is None or r[5] > stale_minutes]
        controls.append(_control(
            "observed_sources_fresh", not stale, "critical",
            "Every active source completed inside the observed-data freshness window",
            observed={r[0]: r[5] for r in latest_runs}, threshold=f"<= {stale_minutes} minutes",
        ))

        raw_count = int(con.execute("SELECT count(*) FROM er_raw_snapshots").fetchone()[0])
        obs_count = int(con.execute("SELECT count(*) FROM er_market_observations").fetchone()[0])
        active_contracts = int(con.execute("SELECT count(*) FROM er_source_contracts WHERE active=TRUE").fetchone()[0])
        controls.extend([
            _control("raw_evidence_present", raw_count > 0, "critical", "Immutable raw source evidence exists", raw_count, 1),
            _control("market_history_present", obs_count > 0, "critical", "Normalized market observation history exists", obs_count, 1),
            _control("active_source_contracts_present", active_contracts > 0, "critical", "Active source contracts exist", active_contracts, 1),
        ])

        semantic_covered = int(con.execute("""
            SELECT count(*) FROM er_source_contracts c
            JOIN er_latest_contract_semantic_versions v USING(source_contract_id)
            WHERE c.active=TRUE AND v.metadata_completeness='full_v2'
              AND v.event_raw_snapshot_id IS NOT NULL AND v.market_raw_snapshot_id IS NOT NULL
        """).fetchone()[0])
        controls.append(_control(
            "active_contract_semantics_have_raw_evidence", semantic_covered == active_contracts, "critical",
            "Every active source contract has a full semantic version tied to exact raw event and market evidence",
            observed={"covered": semantic_covered, "active": active_contracts}, threshold="covered == active",
        ))

        lineage_bad = int(con.execute("""
            SELECT count(*) FROM er_market_observations o
            LEFT JOIN er_raw_snapshots r ON r.raw_snapshot_id=o.raw_snapshot_id
            WHERE coalesce(o.methodology_version,'')<>'legacy_collector_v0.3'
              AND (o.raw_snapshot_id IS NULL OR r.raw_snapshot_id IS NULL)
        """).fetchone()[0])
        controls.append(_control(
            "observation_raw_lineage_complete", lineage_bad == 0, "critical",
            "Every institutional-era market observation resolves to immutable raw evidence",
            lineage_bad, 0,
        ))

        pointer_raw_bad = int(con.execute("""
            SELECT count(*) FROM er_raw_current_snapshots p
            LEFT JOIN er_raw_snapshots r ON r.raw_snapshot_id=p.raw_snapshot_id
            WHERE r.raw_snapshot_id IS NULL OR r.content_hash IS DISTINCT FROM p.content_hash
        """).fetchone()[0])
        controls.append(_control(
            "raw_current_pointers_valid", pointer_raw_bad == 0, "critical",
            "Every current raw pointer resolves to immutable evidence with the same content hash",
            pointer_raw_bad, 0,
        ))

        market_pointer_gaps = int(con.execute("""
            SELECT count(*) FROM er_source_contracts c
            LEFT JOIN er_market_current_state s USING(source_contract_id)
            WHERE c.active=TRUE AND s.source_contract_id IS NULL
        """).fetchone()[0])
        market_pointer_lineage_bad = int(con.execute("""
            SELECT count(*) FROM er_market_current_state s
            LEFT JOIN er_raw_snapshots r ON r.raw_snapshot_id=s.raw_snapshot_id
            WHERE s.raw_snapshot_id IS NULL OR r.raw_snapshot_id IS NULL
        """).fetchone()[0])
        controls.extend([
            _control(
                "active_contracts_have_current_market_state", market_pointer_gaps == 0, "critical",
                "Every active source contract has a current normalized market-state pointer",
                market_pointer_gaps, 0,
            ),
            _control(
                "current_market_state_has_raw_lineage", market_pointer_lineage_bad == 0, "critical",
                "Every current market state resolves to immutable raw evidence",
                market_pointer_lineage_bad, 0,
            ),
        ])

        presence_bad = int(con.execute("""
            SELECT count(*) FROM er_source_contracts c
            LEFT JOIN er_contract_presence_state p USING(source_contract_id)
            WHERE c.active=TRUE AND (
              p.source_contract_id IS NULL OR p.listing_state='missing_confirmed'
            )
        """).fetchone()[0])
        controls.append(_control(
            "active_contract_presence_coherent", presence_bad == 0, "critical",
            "No served active contract is missing-confirmed or absent from the presence ledger",
            presence_bad, 0,
        ))

        unrecoverable = int(con.execute("""
            SELECT count(*) FROM er_raw_snapshots r
            LEFT JOIN er_raw_archive_objects a USING(raw_snapshot_id)
            WHERE r.payload_json IS NULL
              AND (a.status IS DISTINCT FROM 'archived' OR a.archive_key IS NULL OR a.archive_member IS NULL
                   OR a.archive_bundle_hash IS NULL OR a.content_hash IS DISTINCT FROM r.content_hash)
        """).fetchone()[0])
        controls.append(_control(
            "raw_payloads_recoverable", unrecoverable == 0, "critical",
            "Every raw record retains inline payload bytes or a verified matching archive pointer",
            unrecoverable, 0,
        ))
        if require_archive:
            unarchived = int(con.execute("SELECT count(*) FROM er_unarchived_raw_snapshots").fetchone()[0])
            controls.append(_control(
                "verified_object_archive_caught_up", unarchived == 0, "high",
                "All raw snapshots have verified object-storage copies when archive is required",
                unarchived, 0,
            ))

        stuck = int(con.execute("""
            SELECT count(*) FROM er_ingestion_runs
            WHERE status='running' AND started_at < now() - INTERVAL '30 minutes'
        """).fetchone()[0])
        controls.append(_control(
            "no_stuck_source_ingestion", stuck == 0, "critical",
            "No source ingestion has remained running beyond the abandonment threshold",
            stuck, 0,
        ))
    finally:
        con.close()

    return _finalize("observed", controls, policy={
        "stale_minutes": stale_minutes,
        "archive_required": bool(require_archive),
        "layer_definition": "Layer 1 observed source data, raw evidence, normalized market state and curves.",
    })


def evaluate_identity_readiness() -> dict:
    controls: list[dict] = []
    con = connect(read_only=True)
    try:
        tables = _table_names(con)
        required = {
            "er_canonical_events", "er_event_mappings", "er_mapping_identity_passes",
            "er_mapping_identity_state", "er_source_contracts",
        }
        missing = sorted(required - tables)
        controls.append(_control(
            "identity_schema_complete", not missing, "critical",
            "Deterministic identity tables exist" if not missing else f"Missing identity tables: {missing}",
            observed=sorted(required & tables), threshold=sorted(required),
        ))
        if missing:
            return _finalize("identity", controls, policy={"layer_definition": "Layer 2 deterministic canonical event identity."})

        work = int(con.execute("SELECT count(*) FROM er_mapping_identity_work_queue").fetchone()[0])
        controls.append(_control(
            "identity_semantic_queue_caught_up", work == 0, "critical",
            "Every active contract's current semantic version has passed strict deterministic identity normalization",
            work, 0,
        ))

        cardinality_bad = int(con.execute("""
            SELECT count(*) FROM (
              SELECT c.source_contract_id,count(m.mapping_id) n
              FROM er_source_contracts c
              LEFT JOIN er_event_mappings m
                ON m.source_contract_id=c.source_contract_id AND m.valid_to IS NULL
              WHERE c.active=TRUE
              GROUP BY c.source_contract_id
              HAVING count(m.mapping_id)<>1
            )
        """).fetchone()[0])
        controls.append(_control(
            "active_contract_canonical_cardinality_valid", cardinality_bad == 0, "critical",
            "Every active source contract has exactly one active canonical identity",
            cardinality_bad, 0,
        ))

        invalid_targets = int(con.execute("""
            SELECT count(*) FROM er_event_mappings m
            JOIN er_canonical_events e USING(canonical_event_id)
            WHERE m.valid_to IS NULL AND e.status IN ('superseded','inactive','retired')
        """).fetchone()[0])
        controls.append(_control(
            "identity_mappings_target_served_events", invalid_targets == 0, "critical",
            "No active source mapping targets a superseded/inactive canonical event",
            invalid_targets, 0,
        ))

        orphan_active = int(con.execute("""
            SELECT count(*) FROM (
              SELECT e.canonical_event_id
              FROM er_canonical_events e
              LEFT JOIN er_event_mappings m ON m.canonical_event_id=e.canonical_event_id AND m.valid_to IS NULL
              WHERE e.status='active'
              GROUP BY e.canonical_event_id
              HAVING count(m.mapping_id)=0
            )
        """).fetchone()[0])
        controls.append(_control(
            "no_active_orphan_canonical_events", orphan_active == 0, "high",
            "Every active canonical event is supported by at least one active source contract mapping",
            orphan_active, 0,
        ))

        latest = con.execute("""
            SELECT status,finished_at,contracts_changed,exact_pairs_evaluated
            FROM er_mapping_identity_passes ORDER BY started_at DESC LIMIT 1
        """).fetchone()
        latest_ok = latest is not None and latest[0] == "success"
        controls.append(_control(
            "latest_identity_pass_successful", latest_ok, "high",
            "The most recent deterministic identity pass completed successfully",
            observed=None if latest is None else {
                "status": latest[0], "finished_at": str(latest[1]),
                "contracts_changed": latest[2], "exact_pairs_evaluated": latest[3],
            },
            threshold="status=success",
        ))

        stuck = int(con.execute("""
            SELECT count(*) FROM er_mapping_identity_passes
            WHERE status='running' AND started_at < now() - INTERVAL '30 minutes'
        """).fetchone()[0])
        controls.append(_control(
            "no_stuck_identity_pass", stuck == 0, "critical",
            "No deterministic identity pass is abandoned-but-still-marked-running",
            stuck, 0,
        ))
    finally:
        con.close()
    return _finalize("identity", controls, policy={
        "layer_definition": "Layer 2 deterministic source-native canonicalization and strict exact cross-source identity only.",
        "fuzzy_relationships_included": False,
    })


def evaluate_enrichment_readiness() -> dict:
    controls: list[dict] = []
    con = connect(read_only=True)
    try:
        tables = _table_names(con)
        required = {
            "er_mapping_passes", "er_ontology_runs", "er_graph_runs", "er_graph_edges",
            "er_graph_edge_origins", "er_edge_evidence", "er_event_relationships",
            "er_event_relationship_evidence",
        }
        missing = sorted(required - tables)
        controls.append(_control(
            "enrichment_schema_complete", not missing, "critical",
            "Advisory enrichment tables exist" if not missing else f"Missing enrichment tables: {missing}",
            observed=sorted(required & tables), threshold=sorted(required),
        ))
        if missing:
            return _finalize("enrichment", controls, policy={"core_distribution_blocking": False})

        fuzzy_work = int(con.execute("SELECT count(*) FROM er_mapping_work_queue_v5").fetchone()[0])
        ontology_work = int(con.execute("SELECT count(*) FROM er_ontology_work_queue_v2").fetchone()[0])
        graph_work = int(con.execute("SELECT count(*) FROM er_graph_interval_work_queue_v3").fetchone()[0])
        graph_stale = int(con.execute("SELECT count(*) FROM er_graph_stale_intervals_v3").fetchone()[0])
        controls.extend([
            _control(
                "fuzzy_review_discovery_caught_up", fuzzy_work == 0, "high",
                "All current semantic changes have been evaluated for advisory cross-source review candidates",
                fuzzy_work, 0,
            ),
            _control(
                "ontology_caught_up", ontology_work == 0, "high",
                "Canonical ontology classification has processed its current work queue",
                ontology_work, 0,
            ),
            _control(
                "graph_materialization_caught_up", graph_work == 0 and graph_stale == 0, "high",
                "Knowledge-graph interval work and stale-interval retirement are caught up",
                {"work": graph_work, "stale": graph_stale}, {"work": 0, "stale": 0},
            ),
        ])

        graph_bad = int(con.execute("""
            WITH evidence AS (
              SELECT edge_id,count(*)::BIGINT AS evidence_count
              FROM er_edge_evidence
              GROUP BY edge_id
            )
            SELECT count(*)
            FROM er_graph_edges g
            LEFT JOIN er_graph_edge_origins o USING(edge_id)
            LEFT JOIN evidence e USING(edge_id)
            WHERE g.valid_to IS NULL AND (
              o.edge_id IS NULL OR coalesce(e.evidence_count,0)=0 OR g.method_version IS NULL
              OR (g.edge_class='inferred' AND (g.confidence IS NULL OR g.inference_method IS NULL))
            )
        """).fetchone()[0])
        controls.append(_control(
            "current_graph_edges_have_provenance", graph_bad == 0, "critical",
            "Every current graph edge has interval-origin/evidence/version provenance; inferred edges also carry confidence and inference method",
            graph_bad, 0,
        ))

        reviewed_bad_rows = con.execute("""
            SELECT r.relationship_id
            FROM er_event_relationships r
            LEFT JOIN er_event_relationship_evidence e USING(relationship_id)
            WHERE r.valid_to IS NULL AND r.relationship_class='reviewed_relationship'
            GROUP BY r.relationship_id,r.human_reviewed,r.review_status,r.source_candidate_id,r.created_by
            HAVING r.human_reviewed IS DISTINCT FROM TRUE
                OR coalesce(r.review_status,'')<>'reviewed'
                OR r.source_candidate_id IS NULL OR r.created_by IS NULL OR count(e.evidence_id)=0
        """).fetchall()
        controls.append(_control(
            "reviewed_event_relationships_have_evidence", len(reviewed_bad_rows) == 0, "critical",
            "Reviewed non-identity event relationships retain reviewer, candidate and evidence provenance",
            len(reviewed_bad_rows), 0,
        ))

        latest = {}
        for label, table in [
            ("fuzzy_mapping", "er_mapping_passes"),
            ("ontology", "er_ontology_runs"),
            ("graph", "er_graph_runs"),
        ]:
            row = con.execute(f"SELECT status,finished_at FROM {table} ORDER BY started_at DESC LIMIT 1").fetchone()
            latest[label] = None if row is None else {"status": row[0], "finished_at": str(row[1])}
        latest_ok = all(v is not None and v["status"] == "success" for v in latest.values())
        controls.append(_control(
            "latest_enrichment_passes_successful", latest_ok, "high",
            "Latest fuzzy-mapping, ontology and graph passes all completed successfully",
            latest, "all status=success",
        ))
    finally:
        con.close()

    result = _finalize("enrichment", controls, policy={
        "layer_definition": "Layer 2 advisory review discovery, ontology and provenance-aware knowledge-graph enrichment.",
        "core_distribution_blocking": False,
        "inferred_edges_default": "excluded unless explicitly requested",
    })
    return result


def evaluate_readiness(*, stale_minutes: int | None = None, require_archive: bool | None = None) -> dict:
    observed = evaluate_observed_readiness(stale_minutes=stale_minutes, require_archive=require_archive)
    identity = evaluate_identity_readiness()
    enrichment = evaluate_enrichment_readiness()
    core_ready = observed["ready"] and identity["ready"]
    return {
        "ready": core_ready,
        "core_ready": core_ready,
        "enrichment_ready": enrichment["ready"],
        "observed": observed,
        "identity": identity,
        "enrichment": enrichment,
        "policy": {
            "readiness_version": READINESS_VERSION,
            "core_definition": "Observed Layer 1 plus deterministic canonical identity. Advisory fuzzy/graph enrichment is reported independently.",
            "distribution_boundary": "Institutions own scenarios, portfolio models, risk models and investment decisions.",
        },
    }


if __name__ == "__main__":
    import json
    print(json.dumps(evaluate_readiness(), default=str, indent=2))
