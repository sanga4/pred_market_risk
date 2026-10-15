"""Institutional ingestion orchestration v12: bounded cross-source first-chunk priming.

The underlying source writer remains institutional_ingest_v11. v12 changes only orchestration:
while source N is being persisted, a daemon thread fetches at most the first bounded chunk for
source N+1. When the next source starts, v11 consumes that already-fetched chunk and resumes the
same source iterator, retaining v11's one-chunk intra-source prefetch, whole-source transaction,
immutable raw lineage, CDC, semantic versioning and presence semantics.

Only one extra source chunk is retained in memory. No source writes occur concurrently.
"""

from __future__ import annotations

import itertools
import logging
import threading
import time
from dataclasses import dataclass
from typing import Any, Iterator

import institutional_ingest_v11 as v11

log = logging.getLogger("eventrisk.institutional_ingest_v12")
ORCHESTRATION_VERSION = "cross_source_prime_v12"


@dataclass
class PrimedSource:
    source_id: str
    first_chunk: list[Any]
    remainder: Iterator[Any]
    elapsed_seconds: float


class _PrimedAdapter:
    def __init__(self, primed: PrimedSource):
        self._primed = primed

    def iter_snapshots(self):
        yield from self._primed.first_chunk
        yield from self._primed.remainder


def _prime_source(source_id: str, *, chunk_rows: int | None = None) -> PrimedSource:
    if source_id not in v11.ADAPTERS:
        raise ValueError(f"Unknown source {source_id}")
    rows = max(1, int(chunk_rows or v11.CHUNK_ROWS))
    started = time.monotonic()
    iterator = iter(v11.ADAPTERS[source_id]().iter_snapshots())
    first_chunk = list(itertools.islice(iterator, rows))
    return PrimedSource(
        source_id=source_id,
        first_chunk=first_chunk,
        remainder=iterator,
        elapsed_seconds=time.monotonic() - started,
    )


def _ingest_with_prime(source_id: str, primed: PrimedSource | None) -> dict:
    if primed is None:
        result = v11.ingest_source(source_id)
        result["orchestration"] = {
            "method_version": ORCHESTRATION_VERSION,
            "cross_source_primed": False,
            "primed_rows": 0,
            "prime_seconds": 0.0,
        }
        return result

    if primed.source_id != source_id:
        raise RuntimeError(
            f"Primed source mismatch: expected {source_id}, received {primed.source_id}"
        )

    original = v11.ADAPTERS[source_id]

    class WarmedAdapter:
        def iter_snapshots(self):
            yield from _PrimedAdapter(primed).iter_snapshots()

    # Single-threaded writer path: the priming thread has already joined before this swap.
    v11.ADAPTERS[source_id] = WarmedAdapter
    try:
        result = v11.ingest_source(source_id)
    finally:
        v11.ADAPTERS[source_id] = original

    result["orchestration"] = {
        "method_version": ORCHESTRATION_VERSION,
        "cross_source_primed": True,
        "primed_rows": len(primed.first_chunk),
        "prime_seconds": round(primed.elapsed_seconds, 6),
    }
    return result


def run_once(source_ids=None) -> list[dict]:
    source_ids = list(source_ids or v11.ADAPTERS)
    results: list[dict] = []
    pending_prime: PrimedSource | None = None
    pending_error: BaseException | None = None

    for index, source_id in enumerate(source_ids):
        # Start source N+1's first bounded fetch before persisting source N.
        next_holder: dict[str, Any] = {}
        primer: threading.Thread | None = None
        if index + 1 < len(source_ids):
            next_source = source_ids[index + 1]

            def prime_next(target=next_source, holder=next_holder):
                try:
                    holder["value"] = _prime_source(target)
                except BaseException as exc:
                    holder["error"] = exc

            primer = threading.Thread(
                target=prime_next,
                name=f"eventrisk-cross-source-prime-{next_source}",
                daemon=True,
            )
            primer.start()

        if pending_error is not None:
            results.append(
                {
                    "source_id": source_id,
                    "status": "failed",
                    "phase": "cross_source_prime",
                    "error": str(pending_error),
                    "writer": v11.METHOD_VERSION,
                    "orchestration": {"method_version": ORCHESTRATION_VERSION},
                }
            )
        else:
            try:
                results.append(_ingest_with_prime(source_id, pending_prime))
            except Exception as exc:
                results.append(
                    {
                        "source_id": source_id,
                        "status": "failed",
                        "phase": "source",
                        "error": str(exc),
                        "writer": v11.METHOD_VERSION,
                        "orchestration": {"method_version": ORCHESTRATION_VERSION},
                    }
                )

        pending_prime = None
        pending_error = None
        if primer is not None:
            primer.join()
            pending_prime = next_holder.get("value")
            pending_error = next_holder.get("error")
            if pending_prime is not None:
                log.info(
                    "cross-source prime ready source=%s rows=%s elapsed_seconds=%.3f",
                    pending_prime.source_id,
                    len(pending_prime.first_chunk),
                    pending_prime.elapsed_seconds,
                )

    return results
