"""Version canonical event lifecycle from mapped source-contract state.

This module does not predict event outcomes. It only keeps the canonical event's service
lifecycle aligned with the source contracts currently mapped to it:
- active: at least one mapped source contract is currently open/active;
- resolved: no active contracts remain and at least one source reports a resolved/settled state;
- closed: mapped contracts exist but none are active/resolved;
- superseded: reserved for canonical identity merges and never overwritten here.

Every status transition is versioned and recorded as a revision so point-in-time clients can
reconstruct what EventRisk knew.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from db import connect
from institutional_ontology import append_event_version, ensure_event_baseline

LIFECYCLE_VERSION = "canonical_lifecycle_v1"
RESOLVED_STATES = {"resolved", "settled", "finalized", "expired_settled"}
ACTIVE_STATES = {"active", "open"}


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _derived_status(rows: list[tuple[bool | None, str | None]]) -> str | None:
    if not rows:
        return None
    if any(bool(active) or (status or "").lower() in ACTIVE_STATES for active, status in rows):
        return "active"
    if any((status or "").lower() in RESOLVED_STATES for _, status in rows):
        return "resolved"
    return "closed"


def sync_event_lifecycle() -> dict:
    con = connect()
    changed = 0
    skipped_superseded = 0
    try:
        events = con.execute(
            "SELECT canonical_event_id,status FROM er_canonical_events ORDER BY canonical_event_id"
        ).fetchall()
        for event_id, current_status in events:
            if current_status == "superseded":
                skipped_superseded += 1
                continue
            source_rows = con.execute(
                """SELECT c.active,c.status
                   FROM er_event_mappings m
                   JOIN er_source_contracts c USING(source_contract_id)
                   WHERE m.canonical_event_id=? AND m.valid_to IS NULL""",
                [event_id],
            ).fetchall()
            target = _derived_status(source_rows)
            if target is None or target == current_status:
                continue

            changed_at = now_utc()
            ensure_event_baseline(con, event_id)
            con.execute(
                "UPDATE er_canonical_events SET status=?,updated_at=? WHERE canonical_event_id=?",
                [target, changed_at, event_id],
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
                    target,
                    changed_at,
                    changed_at,
                    LIFECYCLE_VERSION,
                ],
            )
            changed += 1
        return {
            "events_status_changed": changed,
            "superseded_events_untouched": skipped_superseded,
            "method_version": LIFECYCLE_VERSION,
        }
    finally:
        con.close()


if __name__ == "__main__":
    print(sync_event_lifecycle())
