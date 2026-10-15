"""Layered institutional readiness v6: source-run ordering safe for transaction-stable clocks.

MotherDuck/DuckDB `now()` is transaction-stable. A successful source ingestion can therefore
record `finished_at` near transaction start while a later operational recovery of an older run gets
a newer wall-clock `finished_at`. v6 preserves every v5 control but selects the latest completed
source run by run start order, which represents invocation recency, and uses that run's start time
for freshness. This prevents an older recovered/abandoned row from eclipsing a newer successful
full-universe refresh.
"""

from __future__ import annotations

from typing import Any

from db import connect
import institutional_readiness_v5 as v5

READINESS_VERSION = "institutional_readiness_v6_started_run_basis"


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
    base = v5.evaluate_observed_readiness(stale_minutes=stale_minutes, require_archive=require_archive)
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
                       ORDER BY started_at DESC, run_id DESC
                     ) rn
              FROM er_ingestion_runs r
              WHERE finished_at IS NOT NULL AND status <> 'running'
            )
            SELECT s.source_id,r.run_id,r.status,r.universe_complete,r.collection_scope,
                   r.started_at,r.finished_at,
                   date_diff('minute',coalesce(r.started_at,TIMESTAMPTZ '1970-01-01'),now()) age_minutes
            FROM er_sources s
            LEFT JOIN ranked r ON r.source_id=s.source_id AND r.rn=1
            WHERE s.active=TRUE
            ORDER BY s.source_id
        """).fetchall()
        bad_completed = [
            r for r in latest_completed
            if r[2] != "success" or r[3] is not True or r[4] != "full"
        ]
        max_age = int(policy.get("stale_minutes") or 20)
        stale = [
            r for r in latest_completed
            if r[5] is None or r[7] is None or r[7] > max_age
        ]
    finally:
        con.close()

    replacement = {
        "latest_source_runs_full_and_successful": _control(
            "latest_source_runs_full_and_successful",
            not bad_completed,
            "critical",
            "Every active source's most recently started completed run is a successful complete full-universe traversal",
            observed=[
                {
                    "source": r[0], "run_id": r[1], "status": r[2],
                    "complete": r[3], "scope": r[4],
                    "started_at": str(r[5]), "finished_at": str(r[6]),
                }
                for r in latest_completed
            ],
            threshold="latest completed by started_at: status=success, universe_complete=true, collection_scope=full",
        ),
        "observed_sources_fresh": _control(
            "observed_sources_fresh",
            not stale,
            "critical",
            "Every active source's most recently started completed run began inside the observed-data freshness window",
            observed={r[0]: r[7] for r in latest_completed},
            threshold=f"started_at age <= {max_age} minutes",
        ),
    }
    controls = [replacement.get(c.get("name"), c) for c in controls]
    policy["completed_run_basis"] = True
    policy["completed_run_order"] = "started_at_desc_then_run_id"
    policy["freshness_clock"] = "started_at"
    policy["transaction_stable_finished_at_safe"] = True
    return _finalize("observed", controls, policy=policy)


def evaluate_identity_readiness() -> dict:
    return _retag(v5.evaluate_identity_readiness())


def evaluate_enrichment_readiness() -> dict:
    return _retag(v5.evaluate_enrichment_readiness())


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
            "completed_run_order": "started_at_desc_then_run_id",
        },
    }
