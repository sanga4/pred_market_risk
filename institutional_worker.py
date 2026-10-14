"""Dedicated production worker for EventRisk institutional ingestion.

Staging can run collection inside the API process to fit the current Railway plan. A
production deployment should run this worker separately from the read API so source
latency or a mapping pass cannot reduce API availability.
"""

from __future__ import annotations

import logging
import os
import time

from institutional_archive_v2 import archive_pending
from institutional_graph import run_graph_pass
from institutional_ingest_v2 import run_once
from institutional_mapping_v2 import run_mapping_pass
from institutional_migrations import ensure_schema
from institutional_ontology import run_ontology_pass
from institutional_qa import run_qa

log = logging.getLogger("eventrisk.worker")


def cycle() -> dict:
    ingest = run_once()
    archive = archive_pending(
        max_rows=int(os.getenv("RAW_ARCHIVE_MAX_ROWS", "20000")),
        bundle_rows=int(os.getenv("RAW_ARCHIVE_BATCH_SIZE", "5000")),
    )
    mapping = run_mapping_pass()
    ontology = run_ontology_pass()
    graph = run_graph_pass()
    qa = run_qa(stale_minutes=max(20, int(os.getenv("COLLECT_INTERVAL_SECONDS", "300")) // 60 * 4))
    return {
        "ingest": ingest,
        "archive": archive,
        "mapping": mapping,
        "ontology": ontology,
        "graph": graph,
        "qa": qa,
    }


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    migration_state = ensure_schema()
    log.info("schema ready %s", migration_state)
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
            break
        elapsed = time.monotonic() - started
        time.sleep(max(1.0, interval - elapsed))


if __name__ == "__main__":
    main()
