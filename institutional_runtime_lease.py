"""Transactional single-flight runtime lease for EventRisk institutional writers.

MotherDuck Flights may launch a scheduled invocation while the prior invocation is still active.
This module provides a fail-closed database lease so cadence triggers can remain frequent without
allowing concurrent hot-path writers. A killed process leaves a bounded lease that expires; normal
completion releases it immediately. Lease state is operational coordination only and never alters
source observations or canonical data.
"""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass
from datetime import datetime

from db import connect

METHOD_VERSION = "runtime_singleflight_v1"
DEFAULT_LEASE_NAME = "hot_ingestion"
DEFAULT_TTL_SECONDS = max(300, int(os.getenv("HOT_PATH_LEASE_SECONDS", "900")))


@dataclass(frozen=True)
class LeaseResult:
    acquired: bool
    lease_name: str
    owner_id: str
    current_owner_id: str | None = None
    expires_at: datetime | None = None
    reason: str | None = None

    def as_dict(self) -> dict:
        return {
            "acquired": self.acquired,
            "lease_name": self.lease_name,
            "owner_id": self.owner_id,
            "current_owner_id": self.current_owner_id,
            "expires_at": self.expires_at,
            "reason": self.reason,
            "method_version": METHOD_VERSION,
        }


def _event(con, lease_name: str, owner_id: str, event_type: str, metadata: dict | None = None) -> None:
    con.execute(
        """
        INSERT INTO er_runtime_lease_events(
          lease_event_id,lease_name,owner_id,event_type,occurred_at,metadata
        ) VALUES (?,?,?,?,now(),?)
        """,
        [f"leaseevt_{uuid.uuid4().hex}", lease_name, owner_id, event_type,
         json.dumps(metadata or {}, sort_keys=True, default=str)],
    )


def try_acquire_on_connection(
    con,
    *,
    lease_name: str = DEFAULT_LEASE_NAME,
    owner_id: str,
    ttl_seconds: int = DEFAULT_TTL_SECONDS,
) -> LeaseResult:
    """Atomically acquire a lease on an existing connection.

    Returns acquired=False when another non-expired owner holds the lease. The caller should treat
    that as a clean skipped invocation, not an ingestion failure.
    """
    ttl_seconds = max(60, int(ttl_seconds))
    con.execute("BEGIN TRANSACTION")
    try:
        prior = con.execute(
            "SELECT owner_id,expires_at FROM er_runtime_leases WHERE lease_name=?", [lease_name]
        ).fetchone()
        row = con.execute(
            """
            UPDATE er_runtime_leases
            SET owner_id=?,acquired_at=now(),heartbeat_at=now(),
                expires_at=now() + (? * INTERVAL '1 second'),
                method_version=?
            WHERE lease_name=?
              AND (owner_id IS NULL OR expires_at IS NULL OR expires_at<=now() OR owner_id=?)
            RETURNING owner_id,expires_at
            """,
            [owner_id, ttl_seconds, METHOD_VERSION, lease_name, owner_id],
        ).fetchone()
        if row is None:
            current = con.execute(
                "SELECT owner_id,expires_at FROM er_runtime_leases WHERE lease_name=?", [lease_name]
            ).fetchone()
            _event(con, lease_name, owner_id, "skipped_busy", {
                "current_owner_id": current[0] if current else None,
                "expires_at": current[1] if current else None,
            })
            con.execute("COMMIT")
            return LeaseResult(
                False, lease_name, owner_id,
                current_owner_id=current[0] if current else None,
                expires_at=current[1] if current else None,
                reason="lease_held",
            )
        takeover = bool(prior and prior[0] and prior[0] != owner_id)
        _event(con, lease_name, owner_id, "expired_takeover" if takeover else "acquired", {
            "prior_owner_id": prior[0] if prior else None,
            "prior_expires_at": prior[1] if prior else None,
            "ttl_seconds": ttl_seconds,
        })
        con.execute("COMMIT")
        return LeaseResult(True, lease_name, owner_id, current_owner_id=owner_id, expires_at=row[1])
    except Exception:
        con.execute("ROLLBACK")
        raise


def acquire_runtime_lease(
    lease_name: str = DEFAULT_LEASE_NAME,
    *,
    owner_id: str | None = None,
    ttl_seconds: int = DEFAULT_TTL_SECONDS,
) -> LeaseResult:
    owner_id = owner_id or f"worker_{uuid.uuid4().hex}"
    con = connect()
    try:
        try:
            return try_acquire_on_connection(
                con, lease_name=lease_name, owner_id=owner_id, ttl_seconds=ttl_seconds
            )
        except Exception as exc:
            # Concurrent UPDATE conflicts are equivalent to not acquiring the single-flight lease.
            # Do not retry the write: another invocation is already contending for ownership.
            message = str(exc)
            if "conflict" in message.lower() or "transaction" in message.lower():
                return LeaseResult(False, lease_name, owner_id, reason="transaction_conflict")
            raise
    finally:
        con.close()


def release_runtime_lease(lease: LeaseResult) -> dict:
    if not lease.acquired:
        return {"released": False, "reason": "not_owner", **lease.as_dict()}
    con = connect()
    try:
        con.execute("BEGIN TRANSACTION")
        try:
            row = con.execute(
                """
                UPDATE er_runtime_leases
                SET owner_id=NULL,acquired_at=NULL,heartbeat_at=NULL,expires_at=NULL,
                    last_released_at=now(),method_version=?
                WHERE lease_name=? AND owner_id=?
                RETURNING lease_name
                """,
                [METHOD_VERSION, lease.lease_name, lease.owner_id],
            ).fetchone()
            if row:
                _event(con, lease.lease_name, lease.owner_id, "released")
            con.execute("COMMIT")
            return {"released": bool(row), **lease.as_dict()}
        except Exception:
            con.execute("ROLLBACK")
            raise
    finally:
        con.close()
