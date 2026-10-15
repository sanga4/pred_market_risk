"""Asynchronous institutional enrichment worker v11 with non-inferential graph execution.

v11 preserves v10's bounded stable catch-up, advisory-only mapping, lifecycle/ontology ownership,
reference versioning, mapping drift and snapshot-safe QA. The only semantic hardening is graph v4:
security names can no longer synthesize legal entities or issuer relationships. Instrument/entity
links must be sourced explicitly or carried by separately labeled reviewed/inferred evidence.
"""

from __future__ import annotations

import logging
import os
import time

from institutional_graph_v4 import run_graph_pass
from institutional_lifecycle_v2 import sync_event_lifecycle
from institutional_mapping_drift_v2 import persist_mapping_drift
from institutional_migrations_v2 import ensure_schema
from institutional_ontology_v2 import run_ontology_pass
from institutional_qa_v10 import run_qa
from institutional_readiness_v6 import evaluate_enrichment_readiness, evaluate_identity_readiness
from institutional_reference_versions_v2 import sync_reference_versions

log = logging.getLogger("eventrisk.enrichment_worker_v11")
RUNTIME_TOPOLOGY = "enrichment_path_v11_no_issuer_inference"


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


def _identity_ready() -> dict:
    return evaluate_identity_readiness()


def _wait_for_stable_identity(*, deadline: float, poll_seconds: float) -> dict:
    attempts = 0
    while True:
        attempts += 1
        identity = _identity_ready()
        if identity.get("ready"):
            return {"ready": True, "attempts": attempts, "identity": identity}
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return {"ready": False, "attempts": attempts, "identity": identity}
        sleep_for = min(poll_seconds, remaining)
        log.info(
            "identity not stable for enrichment; retrying in %.1fs failed_controls=%s",
            sleep_for, identity.get("failed_controls"),
        )
        time.sleep(sleep_for)


def _run_enrichment_pass(timings: dict[str, float], attempt: int) -> dict:
    prefix = f"attempt_{attempt}"
    advisory_mapping = _run_stage(timings, f"{prefix}.advisory_mapping", _run_advisory_mapping)
    if not advisory_mapping.get("identity_mutations_disabled"):
        raise RuntimeError("Advisory mapping lane did not certify identity mutations disabled")
    if advisory_mapping.get("native_events_created", 0) or advisory_mapping.get("auto_exact_mappings", 0):
        raise RuntimeError(f"Advisory mapping attempted identity mutation: {advisory_mapping}")
    if advisory_mapping.get("pair_execution_location") != "enrichment_flight_local_python":
        raise RuntimeError(f"Advisory pair discovery did not use isolated Flight execution: {advisory_mapping}")

    lifecycle = _run_stage(timings, f"{prefix}.lifecycle", sync_event_lifecycle)
    ontology = _run_stage(timings, f"{prefix}.ontology", run_ontology_pass)
    graph = _run_stage(timings, f"{prefix}.graph", run_graph_pass)
    if graph.get("execution_version") != "graph_execution_v4_no_issuer_inference":
        raise RuntimeError(f"Enrichment did not use the approved non-inferential graph execution: {graph}")
    reference_versions = _run_stage(timings, f"{prefix}.reference_versions", sync_reference_versions)
    mapping_drift = _run_stage(timings, f"{prefix}.mapping_drift", persist_mapping_drift)
    return {
        "advisory_mapping": advisory_mapping,
        "lifecycle": lifecycle,
        "ontology": ontology,
        "graph": graph,
        "reference_versions": reference_versions,
        "mapping_drift": mapping_drift,
    }


def cycle() -> dict:
    cycle_started = time.monotonic()
    timings: dict[str, float] = {}
    window_seconds = max(60, int(os.getenv("ENRICHMENT_CORE_STABILITY_WAIT_SECONDS", "600")))
    poll_seconds = float(max(5, int(os.getenv("ENRICHMENT_CORE_STABILITY_POLL_SECONDS", "20"))))
    deadline = cycle_started + window_seconds
    attempts = 0
    last_identity = None
    last_pass = None
    last_qa = None
    last_readiness = None

    while True:
        identity_wait = _wait_for_stable_identity(deadline=deadline, poll_seconds=poll_seconds)
        last_identity = identity_wait["identity"]
        if not identity_wait["ready"]:
            timings["cycle_total"] = round(time.monotonic() - cycle_started, 6)
            return {
                "status": "deferred_identity_transition",
                "identity_readiness": last_identity,
                "attempts": attempts,
                "timing_seconds": timings,
                "runtime_topology": RUNTIME_TOPOLOGY,
            }

        attempts += 1
        pass_started = time.monotonic()
        last_pass = _run_enrichment_pass(timings, attempts)

        qa_started = time.monotonic()
        last_qa = run_qa(
            stale_minutes=max(20, int(os.getenv("COLLECT_INTERVAL_SECONDS", "300")) // 60 * 4)
        )
        timings[f"attempt_{attempts}.qa"] = round(time.monotonic() - qa_started, 6)

        if last_qa.get("deferred"):
            log.info(
                "QA snapshot deferred after enrichment attempt=%s reason=%s; rerunning full pass after core stabilizes",
                attempts, last_qa.get("deferred_reason"),
            )
        else:
            if last_qa.get("critical_issues", 0):
                raise RuntimeError(
                    f"Institutional QA has {last_qa['critical_issues']} open critical issue(s): {last_qa}"
                )
            readiness_started = time.monotonic()
            last_readiness = evaluate_enrichment_readiness()
            timings[f"attempt_{attempts}.enrichment_readiness"] = round(
                time.monotonic() - readiness_started, 6
            )
            if last_readiness.get("ready"):
                timings[f"attempt_{attempts}.total"] = round(time.monotonic() - pass_started, 6)
                timings["cycle_total"] = round(time.monotonic() - cycle_started, 6)
                return {
                    "status": "success",
                    **last_pass,
                    "qa": last_qa,
                    "identity_readiness": last_identity,
                    "enrichment_readiness": last_readiness,
                    "attempts": attempts,
                    "timing_seconds": timings,
                    "runtime_topology": RUNTIME_TOPOLOGY,
                }
            log.info(
                "enrichment pass completed but newer core work is pending attempt=%s failed_controls=%s; catching up",
                attempts, last_readiness.get("failed_controls"),
            )

        timings[f"attempt_{attempts}.total"] = round(time.monotonic() - pass_started, 6)
        if time.monotonic() >= deadline:
            timings["cycle_total"] = round(time.monotonic() - cycle_started, 6)
            return {
                "status": "deferred_core_advanced",
                **(last_pass or {}),
                "qa": last_qa,
                "identity_readiness": last_identity,
                "enrichment_readiness": last_readiness,
                "attempts": attempts,
                "timing_seconds": timings,
                "runtime_topology": RUNTIME_TOPOLOGY,
            }
        time.sleep(min(poll_seconds, max(0.0, deadline - time.monotonic())))


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
