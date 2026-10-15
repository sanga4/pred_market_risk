"""Asynchronous institutional enrichment worker v8.

v8 preserves v7's isolated advisory pair discovery, lifecycle/ontology/graph ownership and
bounded identity-stability wait. Full QA is upgraded to institutional_qa_v9 so expensive history
validation executes on a read-only snapshot and only a compact, stable violation set is published
in a short write transaction.
"""

from __future__ import annotations

import logging
import os
import time

from institutional_graph_v3 import run_graph_pass
from institutional_lifecycle_v2 import sync_event_lifecycle
from institutional_mapping_drift_v2 import persist_mapping_drift
from institutional_migrations_v2 import ensure_schema
from institutional_ontology_v2 import run_ontology_pass
from institutional_qa_v9 import run_qa
from institutional_readiness_v6 import evaluate_enrichment_readiness
from institutional_reference_versions_v2 import sync_reference_versions

log = logging.getLogger("eventrisk.enrichment_worker_v8")
RUNTIME_TOPOLOGY = "enrichment_path_v8_local_pairs_two_phase_qa"


def _run_stage(timings: dict[str, float], name: str, fn, *args, **kwargs):
    started = time.monotonic()
    result = fn(*args, **kwargs)
    elapsed = time.monotonic() - started
    timings[name] = round(elapsed, 6)
    log.info("stage completed name=%s elapsed_seconds=%.3f", name, elapsed)
    return result


def _run_advisory_mapping():
    from institutional_mapping_advisory_v2 import run_mapping_pass
    return run_mapping_pass()


def _run_qa_with_stability_wait(timings: dict[str, float], *, stale_minutes: int) -> dict:
    wait_seconds = max(0, int(os.getenv("ENRICHMENT_CORE_STABILITY_WAIT_SECONDS", "600")))
    poll_seconds = max(5, int(os.getenv("ENRICHMENT_CORE_STABILITY_POLL_SECONDS", "20")))
    started = time.monotonic()
    attempts = 0
    while True:
        attempts += 1
        qa = run_qa(stale_minutes=stale_minutes)
        if not qa.get("deferred"):
            timings["qa"] = round(time.monotonic() - started, 6)
            qa["stability_attempts"] = attempts
            qa["stability_wait_seconds"] = round(max(0.0, timings["qa"]), 6)
            log.info(
                "two-phase QA accepted attempts=%s elapsed_seconds=%.3f evaluation=%s sync=%s",
                attempts, timings["qa"],
                (qa.get("timing") or {}).get("evaluation_seconds"),
                (qa.get("timing") or {}).get("issue_sync_seconds"),
            )
            return qa

        elapsed = time.monotonic() - started
        if elapsed >= wait_seconds:
            timings["qa"] = round(elapsed, 6)
            qa["stability_attempts"] = attempts
            qa["stability_wait_seconds"] = round(elapsed, 6)
            log.warning(
                "two-phase QA deferred after bounded wait attempts=%s elapsed_seconds=%.3f reason=%s snapshot=%s",
                attempts, elapsed, qa.get("deferred_reason"), qa.get("identity_snapshot"),
            )
            return qa
        sleep_for = min(float(poll_seconds), max(0.0, wait_seconds - elapsed))
        log.info(
            "two-phase QA deferred reason=%s; retrying in %.1fs snapshot=%s",
            qa.get("deferred_reason"), sleep_for, qa.get("identity_snapshot"),
        )
        time.sleep(sleep_for)


def cycle() -> dict:
    cycle_started = time.monotonic()
    timings: dict[str, float] = {}
    advisory_mapping = _run_stage(timings, "advisory_mapping", _run_advisory_mapping)
    if not advisory_mapping.get("identity_mutations_disabled"):
        raise RuntimeError("Advisory mapping lane did not certify identity mutations disabled")
    if advisory_mapping.get("native_events_created", 0) or advisory_mapping.get("auto_exact_mappings", 0):
        raise RuntimeError(f"Advisory mapping attempted identity mutation: {advisory_mapping}")
    if advisory_mapping.get("pair_execution_location") != "enrichment_flight_local_python":
        raise RuntimeError(f"Advisory pair discovery did not use isolated Flight execution: {advisory_mapping}")

    lifecycle = _run_stage(timings, "lifecycle", sync_event_lifecycle)
    ontology = _run_stage(timings, "ontology", run_ontology_pass)
    graph = _run_stage(timings, "graph", run_graph_pass)
    reference_versions = _run_stage(timings, "reference_versions", sync_reference_versions)
    mapping_drift = _run_stage(timings, "mapping_drift", persist_mapping_drift)
    qa = _run_qa_with_stability_wait(
        timings,
        stale_minutes=max(20, int(os.getenv("COLLECT_INTERVAL_SECONDS", "300")) // 60 * 4),
    )

    if qa.get("deferred"):
        timings["cycle_total"] = round(time.monotonic() - cycle_started, 6)
        return {
            "status": "deferred_core_transition",
            "advisory_mapping": advisory_mapping,
            "lifecycle": lifecycle,
            "ontology": ontology,
            "graph": graph,
            "reference_versions": reference_versions,
            "mapping_drift": mapping_drift,
            "qa": qa,
            "enrichment_readiness": None,
            "timing_seconds": timings,
            "runtime_topology": RUNTIME_TOPOLOGY,
        }

    if qa.get("critical_issues", 0):
        raise RuntimeError(f"Institutional QA has {qa['critical_issues']} open critical issue(s): {qa}")

    enrichment_readiness = _run_stage(timings, "enrichment_readiness", evaluate_enrichment_readiness)
    timings["cycle_total"] = round(time.monotonic() - cycle_started, 6)
    return {
        "status": "success",
        "advisory_mapping": advisory_mapping,
        "lifecycle": lifecycle,
        "ontology": ontology,
        "graph": graph,
        "reference_versions": reference_versions,
        "mapping_drift": mapping_drift,
        "qa": qa,
        "enrichment_readiness": enrichment_readiness,
        "timing_seconds": timings,
        "runtime_topology": RUNTIME_TOPOLOGY,
    }


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    migration_state = ensure_schema()
    log.info("schema ready %s", migration_state)
    interval = max(900, int(os.getenv("ENRICHMENT_INTERVAL_SECONDS", "3600")))
    once = os.getenv("RUN_ONCE", "false").lower() in {"1", "true", "yes"}
    while True:
        started = time.monotonic()
        try:
            result = cycle()
            log.info("enrichment cycle completed %s", result)
        except Exception:
            log.exception("enrichment cycle failed")
            if once:
                raise
        if once:
            break
        elapsed = time.monotonic() - started
        time.sleep(max(1.0, interval - elapsed))


if __name__ == "__main__":
    main()
