from pathlib import Path

import duckdb

import institutional_enrichment_worker_v5 as worker_v5
import institutional_qa_v8 as qa_v8

ROOT = Path(__file__).resolve().parents[1]


def text(name: str) -> str:
    return (ROOT / name).read_text()


def _quality_schema(con):
    con.execute("""
        CREATE TABLE er_quality_issues(
          quality_issue_id VARCHAR PRIMARY KEY,
          object_type VARCHAR,
          object_id VARCHAR,
          rule_code VARCHAR,
          severity VARCHAR,
          status VARCHAR,
          details VARCHAR,
          detected_at TIMESTAMPTZ,
          resolved_at TIMESTAMPTZ,
          resolution_notes VARCHAR
        );
    """)


def test_identity_snapshot_guard_detects_queue_and_mapping_transition():
    con = duckdb.connect(":memory:")
    try:
        con.execute("""
            CREATE TABLE er_mapping_identity_work_queue(source_contract_id VARCHAR);
            CREATE TABLE er_source_contracts(source_contract_id VARCHAR PRIMARY KEY, active BOOLEAN);
            CREATE TABLE er_event_mappings(
              mapping_id VARCHAR PRIMARY KEY,
              source_contract_id VARCHAR,
              valid_to TIMESTAMPTZ
            );
        """)
        con.execute("INSERT INTO er_source_contracts VALUES ('c1',TRUE)")
        con.execute("INSERT INTO er_event_mappings VALUES ('m1','c1',NULL)")
        con.execute("INSERT INTO er_mapping_identity_work_queue VALUES ('c1')")

        state = qa_v8.identity_snapshot_state(con)
        assert state["ready"] is False
        assert state["identity_work_queue"] == 1
        assert state["active_mapping_cardinality_bad"] == 0

        con.execute("DELETE FROM er_mapping_identity_work_queue")
        assert qa_v8.identity_snapshot_state(con)["ready"] is True

        con.execute("DELETE FROM er_event_mappings")
        state = qa_v8.identity_snapshot_state(con)
        assert state["ready"] is False
        assert state["active_mapping_cardinality_bad"] == 1
    finally:
        con.close()


def test_latest_completed_full_universe_rule_ignores_newer_running_refresh():
    con = duckdb.connect(":memory:")
    try:
        _quality_schema(con)
        con.execute("CREATE TABLE er_sources(source_id VARCHAR PRIMARY KEY, active BOOLEAN)")
        con.execute("""
            CREATE TABLE er_ingestion_runs(
              run_id VARCHAR PRIMARY KEY,
              source_id VARCHAR,
              started_at TIMESTAMPTZ,
              finished_at TIMESTAMPTZ,
              status VARCHAR,
              universe_complete BOOLEAN,
              collection_scope VARCHAR
            )
        """)
        con.execute("INSERT INTO er_sources VALUES ('kalshi',TRUE)")
        con.execute("""
            INSERT INTO er_ingestion_runs VALUES
              ('good','kalshi',TIMESTAMPTZ '2026-10-15 10:00:00+00',TIMESTAMPTZ '2026-10-15 10:04:00+00','success',TRUE,'full'),
              ('refresh','kalshi',TIMESTAMPTZ '2026-10-15 10:05:00+00',NULL,'running',FALSE,'full')
        """)
        assert qa_v8.check_latest_completed_full_universe_runs(con) == 0
        assert con.execute(
            "SELECT count(*) FROM er_quality_issues WHERE status='open' AND rule_code='latest_run_not_complete_full_universe'"
        ).fetchone()[0] == 0

        con.execute("UPDATE er_ingestion_runs SET collection_scope='filtered' WHERE run_id='good'")
        assert qa_v8.check_latest_completed_full_universe_runs(con) == 1
        assert con.execute(
            "SELECT status FROM er_quality_issues WHERE object_id='kalshi' AND rule_code='latest_run_not_complete_full_universe'"
        ).fetchone()[0] == 'open'
    finally:
        con.close()


def test_enrichment_worker_retries_deferred_snapshot(monkeypatch):
    responses = iter([
        {"deferred": True, "identity_snapshot": {"ready": False}},
        {"deferred": False, "critical_issues": 0, "identity_snapshot": {"ready": True}},
    ])
    monkeypatch.setattr(worker_v5, "run_qa", lambda **kwargs: next(responses))
    monkeypatch.setattr(worker_v5.time, "sleep", lambda seconds: None)
    monkeypatch.setenv("ENRICHMENT_CORE_STABILITY_WAIT_SECONDS", "60")
    monkeypatch.setenv("ENRICHMENT_CORE_STABILITY_POLL_SECONDS", "5")

    timings = {}
    result = worker_v5._run_qa_with_stability_wait(timings, stale_minutes=20)
    assert result["deferred"] is False
    assert result["stability_attempts"] == 2
    assert "qa" in timings


def test_qa_v8_and_enrichment_v5_are_versioned():
    qa = text("institutional_qa_v8.py")
    worker = text("institutional_enrichment_worker_v5.py")
    schema = text("institutional_schema_048.sql")
    migrations = text("institutional_migrations_v2.py")
    assert "institutional_qa_v8_snapshot_guard_completed_runs" in qa
    assert "identity_snapshot_in_transition" in qa
    assert "latest_completed_by_started_at" in qa
    assert "from institutional_qa_v8 import run_qa" in worker
    assert "enrichment_path_v5_snapshot_guarded_qa" in worker
    assert "ENRICHMENT_CORE_STABILITY_WAIT_SECONDS" in worker
    assert "institutional_qa_v8_snapshot_guard_completed_runs" in schema
    assert '"institutional_schema_048.sql"' in migrations
