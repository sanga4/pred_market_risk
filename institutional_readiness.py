"""Operational readiness gate for EventRisk institutional data.

This is deliberately stricter than /health. Health answers "can the API serve?" while
readiness answers "is the dataset currently fit to hand to an institutional consumer?".
It never repairs data. A failed gate explains exactly which control failed.
"""

from __future__ import annotations

import os
from dataclasses import asdict, dataclass
from typing import Any

from db import connect


@dataclass
class Control:
    name: str
    passed: bool
    severity: str
    detail: str
    observed: Any = None
    threshold: Any = None


def _control(name: str, passed: bool, detail: str, *, severity: str = "critical", observed=None, threshold=None):
    return Control(name, bool(passed), severity, detail, observed, threshold)


def evaluate_readiness(*, stale_minutes: int | None = None, require_archive: bool | None = None) -> dict:
    stale_minutes = stale_minutes or max(20, int(os.getenv("COLLECT_INTERVAL_SECONDS", "300")) // 60 * 4)
    if require_archive is None:
        require_archive = all(os.getenv(k) for k in [
            "RAW_ARCHIVE_ENDPOINT", "RAW_ARCHIVE_BUCKET",
            "RAW_ARCHIVE_ACCESS_KEY_ID", "RAW_ARCHIVE_SECRET_ACCESS_KEY",
        ])

    controls: list[Control] = []
    con = connect(read_only=True)
    try:
        tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        required_tables = {
            "er_sources", "er_source_contracts", "er_source_contract_versions",
            "er_raw_snapshots", "er_raw_archive_objects", "er_market_observations",
            "er_canonical_events", "er_event_mappings", "er_quality_issues",
            "er_ingestion_runs", "er_mapping_candidates", "er_graph_edges",
        }
        missing = sorted(required_tables - tables)
        controls.append(_control(
            "schema_complete", not missing,
            "All required institutional tables exist" if not missing else f"Missing tables: {', '.join(missing)}",
            observed=len(required_tables) - len(missing), threshold=len(required_tables),
        ))

        if missing:
            result = [asdict(x) for x in controls]
            return {"ready": False, "controls": result, "failed_controls": [x["name"] for x in result if not x["passed"]]}

        # Source freshness and last-run success are data-distribution controls, not cosmetic monitoring.
        source_rows = con.execute(
            """SELECT s.source_id,h.last_success_at,h.last_run_status,h.health_status,
                      date_diff('minute',coalesce(h.last_success_at,TIMESTAMPTZ '1970-01-01'),now()) age_minutes
               FROM er_sources s LEFT JOIN er_source_health h USING(source_id)
               WHERE s.active=TRUE ORDER BY s.source_id"""
        ).fetchall()
        stale = [r for r in source_rows if r[1] is None or r[4] is None or r[4] > stale_minutes]
        controls.append(_control(
            "sources_fresh", not stale,
            "Every active source has succeeded inside the freshness window" if not stale else f"Stale sources: {[r[0] for r in stale]}",
            observed={r[0]: r[4] for r in source_rows}, threshold=f"<= {stale_minutes} minutes",
        ))
        failed_runs = [r for r in source_rows if r[2] not in {None, "success"}]
        controls.append(_control(
            "latest_source_runs_successful", not failed_runs,
            "Latest source runs are successful" if not failed_runs else f"Non-success latest runs: {[(r[0], r[2]) for r in failed_runs]}",
            severity="high",
        ))

        raw_count = con.execute("SELECT count(*) FROM er_raw_snapshots").fetchone()[0]
        obs_count = con.execute("SELECT count(*) FROM er_market_observations").fetchone()[0]
        contracts = con.execute("SELECT count(*) FROM er_source_contracts").fetchone()[0]
        controls.append(_control("raw_data_present", raw_count > 0, "Raw source snapshots have been captured", observed=raw_count, threshold=1))
        controls.append(_control("normalized_observations_present", obs_count > 0, "Normalized market observations exist", observed=obs_count, threshold=1))
        controls.append(_control("source_contracts_present", contracts > 0, "Source contracts exist", observed=contracts, threshold=1))

        missing_lineage = con.execute(
            """SELECT count(*) FROM er_market_observations o
               LEFT JOIN er_raw_snapshots r ON r.raw_snapshot_id=o.raw_snapshot_id
               WHERE coalesce(o.methodology_version,'') <> 'legacy_collector_v0.3'
                 AND (o.raw_snapshot_id IS NULL OR r.raw_snapshot_id IS NULL)"""
        ).fetchone()[0]
        controls.append(_control(
            "observation_lineage_complete", missing_lineage == 0,
            "Every institutional-era observation traces to raw evidence",
            observed=missing_lineage, threshold=0,
        ))

        open_critical = con.execute(
            "SELECT count(*) FROM er_quality_issues WHERE status='open' AND severity='critical'"
        ).fetchone()[0]
        controls.append(_control(
            "no_open_critical_quality_issues", open_critical == 0,
            "No critical data-quality issues are open", observed=open_critical, threshold=0,
        ))

        active_mapping_bad = con.execute(
            """SELECT count(*) FROM (
                 SELECT c.source_contract_id,count(m.mapping_id) n
                 FROM er_source_contracts c
                 LEFT JOIN er_event_mappings m ON m.source_contract_id=c.source_contract_id AND m.valid_to IS NULL
                 GROUP BY c.source_contract_id HAVING count(m.mapping_id)<>1
               )"""
        ).fetchone()[0]
        controls.append(_control(
            "mapping_cardinality_valid", active_mapping_bad == 0,
            "Every source contract has exactly one active canonical mapping",
            observed=active_mapping_bad, threshold=0,
        ))

        invalid_versions = con.execute(
            """SELECT count(*) FROM (
                 SELECT source_contract_id,
                        sum(CASE WHEN valid_to IS NULL THEN 1 ELSE 0 END) open_versions,
                        sum(CASE WHEN valid_to IS NOT NULL AND valid_to<=valid_from THEN 1 ELSE 0 END) bad_windows
                 FROM er_source_contract_versions GROUP BY source_contract_id
                 HAVING open_versions<>1 OR bad_windows>0
               )"""
        ).fetchone()[0]
        controls.append(_control(
            "contract_version_windows_valid", invalid_versions == 0,
            "Contract versions have one current record and non-overlapping positive intervals",
            observed=invalid_versions, threshold=0,
        ))

        inferred_bad = con.execute(
            """SELECT count(*) FROM er_graph_edges
               WHERE edge_class='inferred' AND (confidence IS NULL OR inference_method IS NULL OR method_version IS NULL)"""
        ).fetchone()[0]
        controls.append(_control(
            "inferred_edges_labeled", inferred_bad == 0,
            "Every inferred graph edge carries confidence, method and version",
            observed=inferred_bad, threshold=0,
        ))

        if require_archive:
            unarchived = con.execute("SELECT count(*) FROM er_unarchived_raw_snapshots").fetchone()[0]
            archive_bad = con.execute(
                """SELECT count(*) FROM er_raw_snapshots r
                   LEFT JOIN er_raw_archive_objects a USING(raw_snapshot_id)
                   WHERE r.payload_json IS NULL AND
                         (a.status IS DISTINCT FROM 'archived' OR a.archive_key IS NULL OR
                          a.archive_member IS NULL OR a.archive_bundle_hash IS NULL OR a.content_hash<>r.content_hash)"""
            ).fetchone()[0]
            controls.append(_control(
                "raw_archive_caught_up", unarchived == 0,
                "All raw snapshots are present in verified object storage",
                severity="high", observed=unarchived, threshold=0,
            ))
            controls.append(_control(
                "compacted_raw_is_recoverable", archive_bad == 0,
                "Every compacted raw payload has a verified archive pointer and matching hash",
                observed=archive_bad, threshold=0,
            ))

        latest_obs = con.execute("SELECT max(received_at) FROM er_market_observations").fetchone()[0]
        controls.append(_control(
            "latest_observation_exists", latest_obs is not None,
            "At least one normalized observation has a received timestamp",
            observed=str(latest_obs) if latest_obs else None,
        ))
    finally:
        con.close()

    failed = [c for c in controls if not c.passed and c.severity in {"critical", "high"}]
    return {
        "ready": len(failed) == 0,
        "controls": [asdict(c) for c in controls],
        "failed_controls": [c.name for c in failed],
        "policy": {
            "stale_minutes": stale_minutes,
            "archive_required": bool(require_archive),
            "decision_boundary": "Data readiness only; no investment or portfolio decision is made by this gate.",
        },
    }


if __name__ == "__main__":
    import json
    print(json.dumps(evaluate_readiness(), default=str, indent=2))
