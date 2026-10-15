import duckdb

import institutional_readiness_v5 as readiness


def _base_result():
    return {
        "layer": "observed",
        "ready": False,
        "controls": [
            {"name": "observed_schema_complete", "passed": True, "severity": "critical", "detail": "ok", "observed": [], "threshold": []},
            {"name": "latest_source_runs_full_and_successful", "passed": False, "severity": "critical", "detail": "old", "observed": None, "threshold": None},
            {"name": "observed_sources_fresh", "passed": False, "severity": "critical", "detail": "old", "observed": None, "threshold": None},
            {"name": "no_stuck_source_ingestion", "passed": True, "severity": "critical", "detail": "ok", "observed": 0, "threshold": 0},
        ],
        "failed_controls": ["latest_source_runs_full_and_successful", "observed_sources_fresh"],
        "policy": {
            "readiness_version": "institutional_readiness_v4_layered",
            "stale_minutes": 20,
            "archive_required": False,
            "layer_definition": "Layer 1 observed source data",
        },
    }


def _db():
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE er_sources(source_id VARCHAR, active BOOLEAN)")
    con.execute("""
        CREATE TABLE er_ingestion_runs(
          run_id VARCHAR, source_id VARCHAR, started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ,
          status VARCHAR, universe_complete BOOLEAN, collection_scope VARCHAR
        )
    """)
    con.execute("INSERT INTO er_sources VALUES ('kalshi',TRUE),('polymarket',TRUE)")
    return con


def test_running_refresh_does_not_invalidate_latest_completed_good_data(monkeypatch):
    con = _db()
    con.execute("""
        INSERT INTO er_ingestion_runs VALUES
          ('k-good','kalshi',now()-INTERVAL '3 minutes',now()-INTERVAL '2 minutes','success',TRUE,'full'),
          ('p-good','polymarket',now()-INTERVAL '3 minutes',now()-INTERVAL '2 minutes','success',TRUE,'full'),
          ('k-running','kalshi',now()-INTERVAL '30 seconds',NULL,'running',FALSE,'full')
    """)
    monkeypatch.setattr(readiness.v4, "evaluate_observed_readiness", lambda **kwargs: _base_result())
    monkeypatch.setattr(readiness, "connect", lambda read_only=True: con)

    result = readiness.evaluate_observed_readiness(stale_minutes=20)
    assert result["ready"] is True
    assert result["policy"]["readiness_version"] == "institutional_readiness_v5_completed_run_basis"
    controls = {c["name"]: c for c in result["controls"]}
    assert controls["latest_source_runs_full_and_successful"]["passed"] is True
    assert controls["observed_sources_fresh"]["passed"] is True
    assert controls["source_refreshes_in_progress"]["passed"] is True
    assert len(controls["source_refreshes_in_progress"]["observed"]) == 1


def test_latest_completed_failed_refresh_still_fails_closed(monkeypatch):
    con = _db()
    con.execute("""
        INSERT INTO er_ingestion_runs VALUES
          ('k-failed','kalshi',now()-INTERVAL '3 minutes',now()-INTERVAL '2 minutes','failed',FALSE,'full'),
          ('p-good','polymarket',now()-INTERVAL '3 minutes',now()-INTERVAL '2 minutes','success',TRUE,'full')
    """)
    monkeypatch.setattr(readiness.v4, "evaluate_observed_readiness", lambda **kwargs: _base_result())
    monkeypatch.setattr(readiness, "connect", lambda read_only=True: con)

    result = readiness.evaluate_observed_readiness(stale_minutes=20)
    assert result["ready"] is False
    assert "latest_source_runs_full_and_successful" in result["failed_controls"]
