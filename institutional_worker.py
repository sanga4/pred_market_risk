"""Dedicated production worker for EventRisk institutional ingestion."""

from __future__ import annotations

import logging
import os
import time

from institutional_archive_v2 import archive_pending
from institutional_graph_v3 import run_graph_pass
from institutional_ingest_v8 import run_once
from institutional_lifecycle import sync_event_lifecycle
from institutional_mapping_drift_v2 import persist_mapping_drift
from institutional_mapping_v6 import run_mapping_pass
from institutional_migrations_v2 import ensure_schema
from institutional_ontology_v2 import run_ontology_pass
from institutional_qa_v4 import run_qa
from institutional_reference_versions_v2 import sync_reference_versions
from institutional_run_recovery import recover_abandoned_runs

log = logging.getLogger("eventrisk.worker")


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

    ingest = _run_stage(timings, "ingest", run_once)
    failed_sources = [r for r in ingest if r.get("status") != "success"]
    if failed_sources:
        raise RuntimeError(f"One or more source ingestions failed: {failed_sources}")

    archive = _run_stage(
        timings,
        "archive",
        archive_pending,
        max_rows=int(os.getenv("RAW_ARCHIVE_MAX_ROWS", "20000")),
        bundle_rows=int(os.getenv("RAW_ARCHIVE_BATCH_SIZE", "5000")),
    )
    if archive.get("configured") and archive.get("failed", 0):
        raise RuntimeError(f"Raw archive failed for {archive['failed']} snapshot(s): {archive}")

    mapping = _run_stage(timings, "mapping", run_mapping_pass)
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

    timings["cycle_total"] = round(time.monotonic() - cycle_started, 6)
    return {
        "ingest": ingest,
        "archive": archive,
        "mapping": mapping,
        "lifecycle": lifecycle,
        "ontology": ontology,
        "graph": graph,
        "reference_versions": reference_versions,
        "mapping_drift": mapping_drift,
        "qa": qa,
        "timing_seconds": timings,
    }


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    migration_state = ensure_schema()
    recovery_state = recover_abandoned_runs()
    log.info("schema ready %s run_recovery=%s", migration_state, recovery_state)
    interval = max(300, int(os.getenv("COLLECT_INTERVAL_SECONDS", "300")))
    once = os.getenv("RUN_ONCE", "false").lower() in {"1", "true", "yes"}
    while True:
        started = time.monotonic()
        try:
            result = cycle()
            log.info("cycle completed %s", result)
        except Exception:
            log.exception("cycle failed")
            if once:
                raise
        if once:
            break
        elapsed = time.monotonic() - started
        time.sleep(max(1.0, interval - elapsed))


if __name__ == "__main__":
    main()
