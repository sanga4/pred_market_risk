from __future__ import annotations

import duckdb

import db
from institutional_migrations import MIGRATION_FILES, ensure_schema


def _use_temp_db(monkeypatch, tmp_path):
    path = tmp_path / "institutional_migrations.duckdb"
    monkeypatch.setattr(db, "DB", str(path))
    return path


def test_fresh_schema_migrates_once_and_restart_is_noop(monkeypatch, tmp_path):
    path = _use_temp_db(monkeypatch, tmp_path)

    first = ensure_schema()
    assert first["migration_count"] == len(MIGRATION_FILES)
    assert first["applied_now"] == MIGRATION_FILES
    assert first["bootstrapped"] == []

    second = ensure_schema()
    assert second["migration_count"] == len(MIGRATION_FILES)
    assert second["applied_now"] == []
    assert second["bootstrapped"] == []

    con = duckdb.connect(str(path))
    try:
        rows = con.execute(
            "SELECT migration_name, bootstrapped FROM er_schema_migrations ORDER BY applied_at, migration_name"
        ).fetchall()
        assert len(rows) == len(MIGRATION_FILES)
        assert all(not row[1] for row in rows)
        assert con.execute(
            "SELECT count(*) FROM information_schema.columns "
            "WHERE table_name='er_raw_lineage_status' AND column_name='archive_member'"
        ).fetchone()[0] == 1
    finally:
        con.close()


def test_legacy_fully_migrated_database_bootstraps_without_replaying_ddl(monkeypatch, tmp_path):
    path = _use_temp_db(monkeypatch, tmp_path)

    # Build a current database, then remove only the new migration ledger to model
    # the staging database created before restart-safe migration tracking existed.
    ensure_schema()
    con = duckdb.connect(str(path))
    try:
        con.execute("DROP TABLE er_schema_migrations")
    finally:
        con.close()

    state = ensure_schema()
    assert state["migration_count"] == len(MIGRATION_FILES)
    assert state["applied_now"] == []
    assert state["bootstrapped"] == MIGRATION_FILES

    con = duckdb.connect(str(path))
    try:
        bootstrapped = con.execute(
            "SELECT count(*) FROM er_schema_migrations WHERE bootstrapped=TRUE"
        ).fetchone()[0]
        assert bootstrapped == len(MIGRATION_FILES)
    finally:
        con.close()
