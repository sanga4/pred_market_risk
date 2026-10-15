"""Layered institutional readiness v5: completed-run distribution basis.

v5 preserves every v4 lineage, presence, identity and enrichment control. The observed layer
changes one operational detail: a normal in-progress refresh no longer invalidates the last
completed good dataset. Distribution fitness is evaluated from the latest *completed* source run,
while active refreshes are reported separately and stuck refreshes remain a critical failure.
"""

from __future__ import annotations

from typing import Any

from db import connect
import institutional_readiness_v4 as v4

READINESS_VERSION = "institutional_readiness_v5_completed_run_basis"


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


def _retag(result: dict) -> dict:
    out = dict(result)
    policy = dict(out.get("policy") or {})
    policy["readiness_version"] = READINESS_VERSION
    out["policy"] = policy
    return out


def evaluate_observed_readiness(*, stale_minutes: int | None = None, require_archive: bool | None = None) -> dict:
    base = v4.evaluate_observed_readiness(stale_minutes=stale_minutes, require_archive=require_archive)
    controls = list(base.get("controls") or [])
    policy = dict(base.get("policy") or {})
    policy.pop("readiness_version", None)

    schema_control = next((c for c in controls if c.get("name") == "observed_schema_complete"), None)
    if schema_control is not None and not schema_control.get("passed"):
        return _finalize("observed", controls, policy=policy)

    con = connect(read_only=True)
    try:
        latest_completed = con.execute("""
            WITH ranked AS (
              SELECT r.*,
                     row_number() OVER(
                       PARTITION BY source_id
                       ORDER BY finished_at DESC, started_at DESC, run_id DESC
                     ) rn
              FROM er_ingestion_runs r
              WHERE finished_at IS NOT NULL AND status <> 'running'
            )
            SELECT s.source_id,r.status,r.universe_complete,r.collection_scope,r.finished_at,
                   date_diff('minute',coalesce(r.finished_at,TIMESTAMPTZ '1970-01-01'),now()) age_minutes
            FROM er_sources s
            LEFT JOIN ranked r ON r.source_id=s.source_id AND r.rn=1
            WHERE s.active=TRUE
            ORDER BY s.source_id
        """).fetchall()
        bad_completed = [
            r for r in latest_completed
            if r[1] != "success" or r[2] is not True or r[3] != "full"
        ]
        stale = [
            r for r in latest_completed
            if r[4] is None or r[5] is None or r[5] > int(policy.get("stale_minutes") or 20)
        ]
        active_refreshes = con.execute("""
            SELECT r.source_id,r.run_id,r.started_at,
                   date_diff('second',r.started_at,now()) elapsed_seconds
            FROM er_ingestion_runs r
            JOIN er_sources s USING(source_id)
            WHERE s.active=TRUE AND r.status='running'
            ORDER BY r.started_at
        """).fetchall()
    finally:
        con.close()

    replacement = {
        "latest_source_runs_full_and_successful": _control(
            "latest_source_runs_full_and_successful",
            not bad_completed,
            "critical",
            "Every active source's latest completed run is a successful complete full-universe traversal",
            observed=[
                {"source": r[0], "status": r[1], "complete": r[2], "scope": r[3]}
                for r in latest_completed
            ],
            threshold="latest completed status=success, universe_complete=true, collection_scope=full",
        ),
        "observed_sources_fresh": _control(
            "observed_sources_fresh",
            not stale,
            "critical",
            "Every active source's latest completed run is inside the observed-data freshness window",
            observed={r[0]: r[5] for r in latest_completed},
            threshold=f"<= {int(policy.get('stale_minutes') or 20)} minutes",
        ),
    }
    controls = [replacement.get(c.get("name"), c) for c in controls]
    controls.append(_control(
        "source_refreshes_in_progress",
        True,
        "info",
        "In-progress refreshes are reported but do not invalidate the last completed good dataset; stuck refreshes remain a separate critical control",
        observed=[
            {"source": r[0], "run_id": r[1], "started_at": str(r[2]), "elapsed_seconds": r[3]}
            for r in active_refreshes
        ],
        threshold="informational only; no_stuck_source_ingestion remains critical",
    ))
    policy["completed_run_basis"] = True
    policy["in_progress_refreshes_block_distribution"] = False
    return _finalize("observed", controls, policy=policy)


def evaluate_identity_readiness() -> dict:
    return _retag(v4.evaluate_identity_readiness())


def evaluate_enrichment_readiness() -> dict:
    return _retag(v4.evaluate_enrichment_readiness())


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
            "completed_run_basis": True,
        },
    }


if __name__ == "__main__":
    import json
    print(json.dumps(evaluate_readiness(), default=str, indent=2))
