import os

import db


def test_local_database_connection_uses_utc(monkeypatch, tmp_path):
    monkeypatch.setattr(db, "DB", str(tmp_path / "timezone.duckdb"))
    monkeypatch.setenv("DB_FORCE_READ_WRITE", "true")
    con = db.connect()
    try:
        zone = con.execute("SELECT current_setting('TimeZone')").fetchone()[0]
        assert str(zone).upper() == "UTC"
    finally:
        con.close()
