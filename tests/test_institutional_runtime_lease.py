from datetime import datetime, timezone
from pathlib import Path

import duckdb

import institutional_runtime_lease as lease_mod
import institutional_worker_v4 as worker_v4

ROOT = Path(__file__).resolve().parents[1]


def text(name: str) -> str:
    return (ROOT / name).read_text()


def _schema(con):
    con.execute("""
        CREATE TABLE er_runtime_leases (
          lease_name VARCHAR PRIMARY KEY,
          owner_id VARCHAR,
          acquired_at TIMESTAMPTZ,
          heartbeat_at TIMESTAMPTZ,
          expires_at TIMESTAMPTZ,
          last_released_at TIMESTAMPTZ,
          method_version VARCHAR NOT NULL DEFAULT 'runtime_singleflight_v1'
        );
        INSERT INTO er_runtime_leases(lease_name) VALUES ('hot_ingestion');
        CREATE TABLE er_runtime_lease_events (
          lease_event_id VARCHAR PRIMARY KEY,
          lease_name VARCHAR NOT NULL,
          owner_id VARCHAR NOT NULL,
          event_type VARCHAR NOT NULL,
          occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          metadata VARCHAR
        );
    """)


def test_singleflight_acquire_skip_release_and_reacquire():
    con = duckdb.connect(":memory:")
    try:
        _schema(con)
        first = lease_mod.try_acquire_on_connection(
            con, lease_name="hot_ingestion", owner_id="owner-a", ttl_seconds=900
        )
        assert first.acquired is True

        second = lease_mod.try_acquire_on_connection(
            con, lease_name="hot_ingestion", owner_id="owner-b", ttl_seconds=900
        )
        assert second.acquired is False
        assert second.current_owner_id == "owner-a"
        assert second.reason == "lease_held"

        # Release using the same SQL semantics as the production helper, but on this fixture.
        con.execute("""
            UPDATE er_runtime_leases SET owner_id=NULL,acquired_at=NULL,heartbeat_at=NULL,expires_at=NULL
            WHERE lease_name='hot_ingestion' AND owner_id='owner-a'
        """)
        third = lease_mod.try_acquire_on_connection(
            con, lease_name="hot_ingestion", owner_id="owner-b", ttl_seconds=900
        )
        assert third.acquired is True

        events = [r[0] for r in con.execute(
            "SELECT event_type FROM er_runtime_lease_events ORDER BY occurred_at,lease_event_id"
        ).fetchall()]
        assert "acquired" in events
        assert "skipped_busy" in events
    finally:
        con.close()


def test_expired_owner_can_be_taken_over():
    con = duckdb.connect(":memory:")
    try:
        _schema(con)
        con.execute("""
            UPDATE er_runtime_leases
            SET owner_id='dead-owner',acquired_at=now()-INTERVAL '20 minutes',
                heartbeat_at=now()-INTERVAL '20 minutes',expires_at=now()-INTERVAL '5 minutes'
            WHERE lease_name='hot_ingestion'
        """)
        result = lease_mod.try_acquire_on_connection(
            con, lease_name="hot_ingestion", owner_id="new-owner", ttl_seconds=900
        )
        assert result.acquired is True
        assert con.execute(
            "SELECT owner_id FROM er_runtime_leases WHERE lease_name='hot_ingestion'"
        ).fetchone()[0] == "new-owner"
        assert con.execute(
            "SELECT count(*) FROM er_runtime_lease_events WHERE event_type='expired_takeover'"
        ).fetchone()[0] == 1
    finally:
        con.close()


def test_worker_v4_skips_cycle_when_lease_is_busy(monkeypatch):
    called = []
    busy = lease_mod.LeaseResult(
        False, "hot_ingestion", "new-owner", current_owner_id="existing-owner", reason="lease_held"
    )
    monkeypatch.setattr(worker_v4, "ensure_schema", lambda: {"ok": True})
    monkeypatch.setattr(worker_v4, "recover_abandoned_runs", lambda: {"recovered": 0})
    monkeypatch.setattr(worker_v4, "acquire_runtime_lease", lambda *args, **kwargs: busy)
    monkeypatch.setattr(worker_v4.v3, "cycle", lambda: called.append("cycle"))
    worker_v4.main()
    assert called == []


def test_singleflight_runtime_is_versioned_and_migrated():
    lease = text("institutional_runtime_lease.py")
    worker = text("institutional_worker_v4.py")
    schema43 = text("institutional_schema_043.sql")
    schema44 = text("institutional_schema_044.sql")
    migrations = text("institutional_migrations_v2.py")
    assert "runtime_singleflight_v1" in lease
    assert "hot_path_v4_v11_identity_singleflight" in worker
    assert "er_runtime_leases" in schema43
    assert "er_runtime_lease_events" in schema43
    assert "hot_path_v4_v11_identity_singleflight" in schema44
    assert '"institutional_schema_043.sql"' in migrations
    assert '"institutional_schema_044.sql"' in migrations
