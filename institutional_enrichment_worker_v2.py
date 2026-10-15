"""Asynchronous institutional enrichment worker v2.

This worker preserves the complete advisory normalization pipeline while making QA concurrency-safe
with the frequent observed-data lane. Fuzzy mapping remains advisory only; deterministic identity
continues to be owned by the hot path.
"""

from __future__ import annotations

import logging
import os
import time

from institutional_graph_v3 import run_graph_pass
from institutional_lifecycle_v2 import sync_event_lifecycle
from institutional_mapping_drift_v2 import persist_mapping_drift
from institutional_mapping_v8 import run_mapping_pass
from institutional_migrations_v2 import ensure_schema
from institutional_ontology_v2 import run_ontology_pass
from institutional_qa_v5 import run_qa
from institutional_readiness_v5 import evaluate_enrichment_readiness
from institutional_reference_versions_v2 import sync_reference_versions

log = logging.getLogger("eventrisk.enrichment_worker_v2")
RUNTIME_TOPOLOGY = "enrichment_path_v2_concurrency_safe_qa"


def _run_stage(timings: dict[str, float], name: str, fn, *args, **kwargs):
    started = time.monotonic()
    result = fn(*args, **kwargs)
    elapsed = time.monotonic() - started
    timings[name] = round(elapsed, 6)
    log.info("stage completed name=%s elapsed_seconds=%.3f", name, elapsed)
    return result


def cycle() -> dict:
    cycle_started = time.monotonic()
    timings: dict[str, float] = {}

    fuzzy_mapping = _run_stage(timings, "fuzzy_mapping", run_mapping_pass)
    lifecycle = _run_stage(timings, "lifecycle", sync_event_lifecycle)
    ontology = _run_stage(timings, "ontology", run_ontology_pass)
    graph = _run_stage(timings, "graph", run_graph_pass)
    reference_versions = _run_stage(timings, "reference_versions", sync_reference_versions)
    mapping_drift = _run_stage(timings, "mapping_drift", persist_mapping_drift)
    qa = _run_stage(
        timings,
        "qa",
        run_qa,
        stale_minutes=max(20, int(os.getenv("COLLECT_INTERVAL_SECONDS", "300")) // 60 * 4),
    )
    if qa.get("critical_issues", 0):
        raise RuntimeError(f"Institutional QA has {qa['critical_issues']} open critical issue(s): {qa}")

    enrichment_readiness = _run_stage(timings, "enrichment_readiness", evaluate_enrichment_readiness)
    timings["cycle_total"] = round(time.monotonic() - cycle_started, 6)
    return {
        "fuzzy_mapping": fuzzy_mapping,
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
