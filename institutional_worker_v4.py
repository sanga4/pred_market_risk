"""Institutional hot-path worker v4 with transactional single-flight ownership.

v4 wraps the accepted v3 hot cycle in a database lease. A scheduled invocation that fires while a
prior collector still owns the lease exits successfully without writing; this preserves frequent
triggers without concurrent full-universe writers. Normal completion releases immediately and a
killed owner becomes recoverable after bounded lease expiry.
"""

from __future__ import annotations

import logging
import os

import institutional_worker_v3 as v3
from institutional_migrations_v2 import ensure_schema
from institutional_run_recovery import recover_abandoned_runs
from institutional_runtime_lease import acquire_runtime_lease, release_runtime_lease

log = logging.getLogger("eventrisk.worker_v4")
RUNTIME_TOPOLOGY = "hot_path_v4_v11_identity_singleflight"


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
        result = v3.cycle()
        result["runtime_topology"] = RUNTIME_TOPOLOGY
        result["runtime_lease"] = lease.as_dict()
        log.info("cycle completed %s", result)
    finally:
        try:
            released = release_runtime_lease(lease)
            log.info("runtime lease release %s", released)
        except Exception:
            # Data stages have already failed/succeeded independently. Surface lease release failure
            # while retaining TTL-based recovery rather than silently masking it.
            log.exception("runtime lease release failed; expiry will provide bounded recovery")
            raise


if __name__ == "__main__":
    main()
