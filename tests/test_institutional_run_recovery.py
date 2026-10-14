import os
from datetime import datetime, timedelta, timezone

import db


def setup_database(monkeypatch, tmp_path):
    monkeypatch.setattr(db, "DB", str(tmp_path / "recovery.duckdb"))
    os.environ["DB_FORCE_READ_WRITE"] = "true"
    from institutional_migrations import ensure_schema
    ensure_schema()


def test_only_stale_running_runs_are_marked_abandoned(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    from institutional_run_recovery import recover_abandoned_runs

    now = datetime.now(timezone.utc)
    con = db.connect()
    try:
        con.execute("INSERT INTO er_sources(source_id,source_name,source_type) VALUES ('test','Test','prediction_market')")
        con.execute("""
            INSERT INTO er_ingestion_runs(run_id,source_id,started_at,status,records_written,error_count,details)
            VALUES
              ('old','test',?,'running',12,0,'{"phase":"ingest"}'),
              ('fresh','test',?,'running',3,0,NULL),
              ('done','test',?,'success',20,0,NULL)
        """, [now - timedelta(minutes=90), now - timedelta(minutes=5), now - timedelta(minutes=100)])
    finally:
        con.close()

    result = recover_abandoned_runs(stale_minutes=30)
    assert result['recovered'] == 1
    assert result['runs'][0]['run_id'] == 'old'

    con = db.connect()
    try:
        old = con.execute("SELECT status,finished_at,error_count,details FROM er_ingestion_runs WHERE run_id='old'").fetchone()
        fresh = con.execute("SELECT status,finished_at FROM er_ingestion_runs WHERE run_id='fresh'").fetchone()
        done = con.execute("SELECT status FROM er_ingestion_runs WHERE run_id='done'").fetchone()[0]
        assert old[0] == 'abandoned'
        assert old[1] is not None
        assert old[2] >= 1
        assert 'ingestion_run_recovery_v1' in old[3]
        assert '"phase": "ingest"' in old[3]
        assert fresh == ('running', None)
        assert done == 'success'
    finally:
        con.close()
