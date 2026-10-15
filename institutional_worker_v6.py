"""Institutional hot-path worker v6.

v6 keeps all v5 data/readiness semantics but shortens the single-flight writer lease to the
mutation-critical path only: v12 source capture, raw archive, deterministic identity and run
recovery. Readiness evaluation is read-only and executes after the writer lease is released, so a
slow readiness query cannot cause the next five-minute writer invocation to skip.

v12 also primes one bounded chunk of the next source while the current source is being persisted.
No source writes occur concurrently and the underlying writer remains v11.
"""

from __future__ import annotations

import logging
import os
import time

from institutional_archive_v2 import archive_pending
from institutional_ingest_v12 import run_once
from institutional_mapping_identity_v1 import run_identity_pass
from institutional_migrations_v2 import ensure_schema
from institutional_readiness_v6 import evaluate_identity_readiness, evaluate_observed_readiness
from institutional_run_recovery import recover_abandoned_runs
from institutional_runtime_lease import acquire_runtime_lease, release_runtime_lease

log = logging.getLogger("eventrisk.worker_v6")
RUNTIME_TOPOLOGY = "hot_path_v6_v12_short_writer_lease_readiness6"


def _run_stage(timings: dict[str, float], name: str, fn, *args, **kwargs):
    started = time.monotonic()
    result = fn(*args, **kwargs)
    elapsed = time.monotonic() - started
    timings[name] = round(elapsed, 6)
    log.info("stage completed name=%s elapsed_seconds=%.3f", name, elapsed)
    return result


def write_cycle() -> dict:
    started = time.monotonic()
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
    recovery = _run_stage(timings, "pre_readiness_recovery", recover_abandoned_runs)
    timings["writer_critical_path_total"] = round(time.monotonic() - started, 6)
    return {
        "ingest": ingest,
        "archive": archive,
        "identity": identity,
        "pre_readiness_recovery": recovery,
        "timing_seconds": timings,
    }


def readiness_cycle() -> dict:
    started = time.monotonic()
    timings: dict[str, float] = {}
    observed = _run_stage(
        timings,
        "observed_readiness",
        evaluate_observed_readiness,
        stale_minutes=max(20, int(os.getenv("COLLECT_INTERVAL_SECONDS", "300")) // 60 * 4),
        require_archive=None,
    )
    identity = _run_stage(timings, "identity_readiness", evaluate_identity_readiness)
    if not observed.get("ready"):
        raise RuntimeError(f"Observed-data readiness failed: {observed.get('failed_controls')}")
    if not identity.get("ready"):
        raise RuntimeError(f"Deterministic identity readiness failed: {identity.get('failed_controls')}")
    timings["readiness_total"] = round(time.monotonic() - started, 6)
    return {
        "observed_readiness": observed,
        "identity_readiness": identity,
        "timing_seconds": timings,
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

    cycle_started = time.monotonic()
    write_result = None
    try:
        write_result = write_cycle()
    finally:
        try:
            released = release_runtime_lease(lease)
            log.info("writer lease released before readiness %s", released)
        except Exception:
            log.exception("runtime lease release failed; expiry will provide bounded recovery")
            raise

    # Read-only distribution fitness is intentionally outside the writer lease. The v6 readiness
    # basis ignores non-stuck in-progress source refreshes, so a later scheduled writer can begin
    # without invalidating the last completed good dataset while this check finishes.
    readiness_result = readiness_cycle()
    total = round(time.monotonic() - cycle_started, 6)
    result = {
        **write_result,
        **{k: v for k, v in readiness_result.items() if k != "timing_seconds"},
        "timing_seconds": {
            **write_result["timing_seconds"],
            **readiness_result["timing_seconds"],
            "cycle_total": total,
        },
        "runtime_topology": RUNTIME_TOPOLOGY,
        "runtime_lease": lease.as_dict(),
        "writer_lease_released_before_readiness": True,
    }
    log.info("cycle completed %s", result)


if __name__ == "__main__":
    main()
