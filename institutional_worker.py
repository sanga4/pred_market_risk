"""Dedicated production worker for EventRisk institutional core data.

The frequent path intentionally contains only work required for fresh observed curves and
strict deterministic canonical identity. Advisory fuzzy discovery, ontology, graph enrichment,
reference synchronization, drift analysis and full QA run in `institutional_enrichment_worker`.
"""

from __future__ import annotations

import logging
import os
import time

from institutional_archive_v2 import archive_pending
from institutional_ingest_v11 import run_once
from institutional_mapping_identity_v1 import run_identity_pass
from institutional_migrations_v2 import ensure_schema
from institutional_readiness_v5 import evaluate_identity_readiness, evaluate_observed_readiness
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

    identity = _run_stage(timings, "identity", run_identity_pass)
    observed_readiness = _run_stage(
        timings,
        "observed_readiness",
        evaluate_observed_readiness,
        stale_minutes=max(20, int(os.getenv("COLLECT_INTERVAL_SECONDS", "300")) // 60 * 4),
        require_archive=None,
    )
    identity_readiness = _run_stage(timings, "identity_readiness", evaluate_identity_readiness)
    if not observed_readiness.get("ready"):
        raise RuntimeError(f"Observed-data readiness failed: {observed_readiness.get('failed_controls')}")
    if not identity_readiness.get("ready"):
        raise RuntimeError(f"Deterministic identity readiness failed: {identity_readiness.get('failed_controls')}")

    timings["cycle_total"] = round(time.monotonic() - cycle_started, 6)
    return {
        "ingest": ingest,
        "archive": archive,
        "identity": identity,
        "observed_readiness": observed_readiness,
        "identity_readiness": identity_readiness,
        "timing_seconds": timings,
        "runtime_topology": "hot_path_v2_v11_identity",
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
