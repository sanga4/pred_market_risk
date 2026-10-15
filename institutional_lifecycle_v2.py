"""Set-based canonical event lifecycle synchronization.

Semantics are intentionally identical to institutional_lifecycle v1:
- active when any currently mapped source contract is active/open;
- resolved when none are active and at least one is resolved/settled/finalized;
- closed when mapped contracts exist but none are active/resolved;
- superseded is reserved for identity merges and is never overwritten here.

v2 changes only execution: target lifecycle state is derived for the full mapped universe in one
set-based query, then only changed canonicals are versioned and revised inside one transaction.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from db import connect
from institutional_ontology import append_event_version, ensure_event_baseline

LIFECYCLE_VERSION = "canonical_lifecycle_v2_set_based"


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _changed_lifecycle_rows(con):
    return con.execute(
        """
        WITH mapped_state AS (
          SELECT
            m.canonical_event_id,
            count(*) AS mapped_contracts,
            bool_or(coalesce(c.active, FALSE) OR lower(coalesce(c.status,'')) IN ('active','open')) AS has_active,
            bool_or(lower(coalesce(c.status,'')) IN ('resolved','settled','finalized','expired_settled')) AS has_resolved
          FROM er_event_mappings m
          JOIN er_source_contracts c USING(source_contract_id)
          WHERE m.valid_to IS NULL
          GROUP BY m.canonical_event_id
        ), targets AS (
          SELECT
            e.canonical_event_id,
            e.status AS current_status,
            CASE
              WHEN s.has_active THEN 'active'
              WHEN s.has_resolved THEN 'resolved'
              ELSE 'closed'
            END AS target_status
          FROM er_canonical_events e
          JOIN mapped_state s USING(canonical_event_id)
          WHERE e.status <> 'superseded'
        )
        SELECT canonical_event_id,current_status,target_status
        FROM targets
        WHERE current_status IS DISTINCT FROM target_status
        ORDER BY canonical_event_id
        """
    ).fetchall()


def sync_event_lifecycle() -> dict:
    con = connect()
    changed = 0
    try:
        changed_rows = _changed_lifecycle_rows(con)
        superseded = int(
            con.execute("SELECT count(*) FROM er_canonical_events WHERE status='superseded'").fetchone()[0]
        )
        if not changed_rows:
            return {
                "events_status_changed": 0,
                "superseded_events_untouched": superseded,
                "method_version": LIFECYCLE_VERSION,
            }

        changed_at = now_utc()
        con.execute("BEGIN TRANSACTION")
        try:
            for event_id, current_status, target_status in changed_rows:
                ensure_event_baseline(con, event_id)
                con.execute(
                    "UPDATE er_canonical_events SET status=?,updated_at=? WHERE canonical_event_id=?",
                    [target_status, changed_at, event_id],
                )
                append_event_version(con, event_id, changed_at, "source_lifecycle_sync")
                con.execute(
                    """INSERT INTO er_revisions(
                         revision_id,object_type,object_id,revision_type,reason,previous_version,new_version,
                         effective_at,recorded_at,actor)
                       VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    [
                        f"rev_{uuid.uuid4().hex}",
                        "canonical_event",
                        event_id,
                        "source_lifecycle_sync",
                        "Canonical service lifecycle derived only from mapped source-contract state",
                        current_status,
                        target_status,
                        changed_at,
                        changed_at,
                        LIFECYCLE_VERSION,
                    ],
                )
                changed += 1
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

        return {
            "events_status_changed": changed,
            "superseded_events_untouched": superseded,
            "method_version": LIFECYCLE_VERSION,
        }
    finally:
        con.close()


if __name__ == "__main__":
    print(sync_event_lifecycle())
