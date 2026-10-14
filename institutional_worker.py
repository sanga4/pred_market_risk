"""Dedicated production worker for EventRisk institutional ingestion."""

from __future__ import annotations

import logging
import os
import time

from institutional_archive_v2 import archive_pending
from institutional_graph_v2 import run_graph_pass
from institutional_ingest_v3 import run_once
from institutional_mapping_drift import persist_mapping_drift
from institutional_mapping_v3 import run_mapping_pass
from institutional_migrations import ensure_schema
from institutional_ontology import run_ontology_pass
from institutional_qa_v2 import run_qa
from institutional_reference_versions import sync_reference_versions

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
    reference_versions = sync_reference_versions()
    mapping_drift = persist_mapping_drift()
    qa = run_qa(stale_minutes=max(20, int(os.getenv("COLLECT_INTERVAL_SECONDS", "300")) // 60 * 4))
    return {
        "ingest": ingest,
        "archive": archive,
        "mapping": mapping,
        "ontology": ontology,
        "graph": graph,
        "reference_versions": reference_versions,
        "mapping_drift": mapping_drift,
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
