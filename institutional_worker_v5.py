"""Institutional hot-path worker v5.

v5 combines the accepted v11 full-universe CDC + strict deterministic identity path with the v4
transactional single-flight lease and readiness v6's transaction-stable-clock-safe completed-run
selection. Overlapping schedule triggers skip cleanly; an older recovered run cannot eclipse a
newer successful source refresh during readiness evaluation.
"""

from __future__ import annotations

import logging
import os
import time

from institutional_archive_v2 import archive_pending
from institutional_ingest_v11 import run_once
from institutional_mapping_identity_v1 import run_identity_pass
from institutional_migrations_v2 import ensure_schema
from institutional_readiness_v6 import evaluate_identity_readiness, evaluate_observed_readiness
from institutional_run_recovery import recover_abandoned_runs
from institutional_runtime_lease import acquire_runtime_lease, release_runtime_lease

log = logging.getLogger("eventrisk.worker_v5")
RUNTIME_TOPOLOGY = "hot_path_v5_v11_identity_singleflight_readiness6"


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
    pre_readiness_recovery = _run_stage(
        timings, "pre_readiness_recovery", recover_abandoned_runs
    )
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
        "pre_readiness_recovery": pre_readiness_recovery,
        "observed_readiness": observed_readiness,
        "identity_readiness": identity_readiness,
        "timing_seconds": timings,
        "runtime_topology": RUNTIME_TOPOLOGY,
    }


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    migration_state = ensure_schema()
    recovery_state = recover_abandoned_runs()
    log.info("schema ready %s run_recovery=%s", migration_state, recovery_state)

    lease = acquire_runtime_lease(
        "hot_ingestion",
        ttl_seconds=max(600, int(os.getenv("HOT_PATH_LEASE_SECONDS", "900"))),
    )
    log.info("runtime lease result %s", lease.as_dict())
    if not lease.acquired:
        log.info("hot-path invocation skipped because another writer owns the lease")
        return

    try:
        result = cycle()
        result["runtime_lease"] = lease.as_dict()
        log.info("cycle completed %s", result)
    finally:
        try:
            released = release_runtime_lease(lease)
            log.info("runtime lease release %s", released)
        except Exception:
            log.exception("runtime lease release failed; expiry will provide bounded recovery")
            raise


if __name__ == "__main__":
    main()
