"""Institutional ingestion orchestration v13: parallel independent source transactions.

v13 preserves institutional_ingest_v11 as the authoritative per-source writer and changes only
cross-source orchestration. Each configured source executes its existing whole-source transaction
on its own thread/connection. Source rows are source-keyed and the transactions do not share
mutable source state. The enclosing hot worker retains the global EventRisk single-flight lease,
so there is still only one hot worker generation mutating the institutional warehouse at a time.

A failure in either source is returned as a failed source result. The other source may already have
committed, which is the same source-level atomicity boundary used by the sequential writer; core
readiness remains fail-closed until every active source's latest completed run is successful/full.
"""

from __future__ import annotations

import concurrent.futures
import logging
import time
from typing import Iterable

import institutional_ingest as base
import institutional_ingest_v11 as v11
from institutional_http import get_json as thread_safe_get_json

# Each source thread receives its own retry-configured requests.Session.
base.get_json = thread_safe_get_json

log = logging.getLogger("eventrisk.institutional_ingest_v13")
ORCHESTRATION_VERSION = "parallel_source_transactions_v13"


def _run_source(source_id: str) -> dict:
    started = time.monotonic()
    try:
        result = v11.ingest_source(source_id)
        result["orchestration"] = {
            "method_version": ORCHESTRATION_VERSION,
            "parallel_source_transaction": True,
            "wall_seconds": round(time.monotonic() - started, 6),
        }
        return result
    except Exception as exc:
        log.exception("parallel source ingestion failed source=%s", source_id)
        return {
            "source_id": source_id,
            "status": "failed",
            "phase": "source",
            "error": str(exc),
            "writer": v11.METHOD_VERSION,
            "orchestration": {
                "method_version": ORCHESTRATION_VERSION,
                "parallel_source_transaction": True,
                "wall_seconds": round(time.monotonic() - started, 6),
            },
        }


def run_once(source_ids: Iterable[str] | None = None) -> list[dict]:
    ordered_sources = list(source_ids or v11.ADAPTERS)
    unknown = [source_id for source_id in ordered_sources if source_id not in v11.ADAPTERS]
    if unknown:
        raise ValueError(f"Unknown institutional source(s): {unknown}")
    if not ordered_sources:
        return []

    cycle_started = time.monotonic()
    results_by_source: dict[str, dict] = {}
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=len(ordered_sources),
        thread_name_prefix="eventrisk-source",
    ) as executor:
        futures = {executor.submit(_run_source, source_id): source_id for source_id in ordered_sources}
        for future in concurrent.futures.as_completed(futures):
            source_id = futures[future]
            # _run_source is fail-closed and normally returns a failure record rather than raising;
            # preserve a defensive guard so an orchestration bug can never make the cycle appear OK.
            try:
                results_by_source[source_id] = future.result()
            except BaseException as exc:
                results_by_source[source_id] = {
                    "source_id": source_id,
                    "status": "failed",
                    "phase": "orchestration",
                    "error": str(exc),
                    "writer": v11.METHOD_VERSION,
                    "orchestration": {
                        "method_version": ORCHESTRATION_VERSION,
                        "parallel_source_transaction": True,
                    },
                }

    results = [results_by_source[source_id] for source_id in ordered_sources]
    log.info(
        "parallel source cycle completed sources=%s elapsed_seconds=%.3f statuses=%s",
        ordered_sources,
        time.monotonic() - cycle_started,
        {item["source_id"]: item.get("status") for item in results},
    )
    return results
