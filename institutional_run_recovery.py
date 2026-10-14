"""Recover ingestion-run ledger rows left open by external process termination.

A Flight/container can be killed after source work starts but before the run row is finalized.
On the next worker start, runs older than a conservative threshold are marked `abandoned`.
They are never rewritten as success and their existing details are preserved alongside a
recovery explanation. This is operational ledger repair, not source-data repair.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone

from db import connect

METHOD_VERSION = "ingestion_run_recovery_v1"


def recover_abandoned_runs(stale_minutes: int | None = None) -> dict:
    threshold = stale_minutes or max(30, int(os.getenv("COLLECT_INTERVAL_SECONDS", "300")) // 60 * 6)
    con = connect()
    try:
        rows = con.execute("""
            SELECT run_id,source_id,started_at,details
            FROM er_ingestion_runs
            WHERE status='running'
              AND started_at < now() - (? * INTERVAL '1 minute')
            ORDER BY started_at
        """, [threshold]).fetchall()
        recovered = []
        for run_id, source_id, started_at, details in rows:
            try:
                previous = json.loads(details) if details else {}
                if not isinstance(previous, dict):
                    previous = {"previous_details": previous}
            except Exception:
                previous = {"previous_details": details}
            previous["recovery"] = {
                "method_version": METHOD_VERSION,
                "reason": "run remained running beyond threshold after prior worker/process termination",
                "threshold_minutes": threshold,
                "recovered_at": datetime.now(timezone.utc).isoformat(),
            }
            con.execute("""
                UPDATE er_ingestion_runs
                SET status='abandoned',finished_at=now(),error_count=CASE WHEN error_count<1 THEN 1 ELSE error_count END,
                    details=?
                WHERE run_id=? AND status='running'
            """, [json.dumps(previous, sort_keys=True, default=str), run_id])
            recovered.append({"run_id": run_id, "source_id": source_id, "started_at": started_at})
        return {"recovered": len(recovered), "runs": recovered, "threshold_minutes": threshold, "method_version": METHOD_VERSION}
    finally:
        con.close()


if __name__ == "__main__":
    print(recover_abandoned_runs())
