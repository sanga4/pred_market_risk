"""Drain or incrementally maintain the institutional raw archive backlog.

Each iteration archives one bounded batch using institutional_archive_v3, so memory remains bounded
while large backlogs can consume several batches in one cron execution. Set
RAW_ARCHIVE_RUN_ONCE=true for cron maintenance and RAW_ARCHIVE_CYCLES_PER_RUN to cap how many
bounded batches that execution may process. With RUN_ONCE=false the worker continues until caught up.
"""
from __future__ import annotations

import json
import os
import time

from institutional_archive_v3 import archive_pending


def _truthy(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


def main() -> None:
    max_rows = max(1000, int(os.getenv("RAW_ARCHIVE_MAX_ROWS", "20000")))
    bundle_rows = max(100, int(os.getenv("RAW_ARCHIVE_BATCH_SIZE", "5000")))
    pause = max(0.0, float(os.getenv("RAW_ARCHIVE_BACKFILL_PAUSE_SECONDS", "1")))
    run_once = _truthy("RAW_ARCHIVE_RUN_ONCE")
    max_cycles = max(1, int(os.getenv("RAW_ARCHIVE_CYCLES_PER_RUN", "1"))) if run_once else None
    cycles = 0
    total_archived = 0
    total_compacted = 0
    total_bundles = 0

    while True:
        result = archive_pending(max_rows=max_rows, bundle_rows=bundle_rows)
        cycles += 1
        print(json.dumps({
            "cycle": cycles,
            "run_once": run_once,
            "max_cycles": max_cycles,
            **result,
        }, default=str), flush=True)
        if not result.get("configured"):
            raise RuntimeError("raw archive is not configured")
        if int(result.get("failed") or 0) > 0:
            raise RuntimeError(f"raw archive batch failed: {result}")
        archived = int(result.get("archived") or 0)
        total_archived += archived
        total_compacted += int(result.get("compacted") or 0)
        total_bundles += int(result.get("bundles") or 0)
        cycle_cap_reached = run_once and max_cycles is not None and cycles >= max_cycles
        if archived == 0 or cycle_cap_reached:
            print(json.dumps({
                "complete": archived == 0,
                "bounded_run_complete": cycle_cap_reached,
                "cycles": cycles,
                "max_cycles": max_cycles,
                "archived": total_archived,
                "compacted": total_compacted,
                "bundles": total_bundles,
                "archive_version": "v3",
            }), flush=True)
            return
        if pause:
            time.sleep(pause)


if __name__ == "__main__":
    main()
