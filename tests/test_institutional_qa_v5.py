from pathlib import Path

import duckdb

import institutional_qa_v5 as qa_v5

ROOT = Path(__file__).resolve().parents[1]


def text(name: str) -> str:
    return (ROOT / name).read_text()


def test_source_freshness_records_issue_without_mutating_ingestion_health():
    con = duckdb.connect(":memory:")
    try:
        con.execute("""
            CREATE TABLE er_sources(source_id VARCHAR PRIMARY KEY, active BOOLEAN);
            CREATE TABLE er_source_health(
              source_id VARCHAR PRIMARY KEY,
              last_success_at TIMESTAMPTZ,
              last_observation_at TIMESTAMPTZ,
              last_run_status VARCHAR,
              health_status VARCHAR,
              updated_at TIMESTAMPTZ
            );
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
            INSERT INTO er_sources VALUES ('kalshi', TRUE);
            INSERT INTO er_source_health VALUES (
              'kalshi', now() - INTERVAL '60 minutes', now() - INTERVAL '60 minutes',
              'success', 'healthy', now()
            );
        """)

        assert qa_v5.check_source_freshness(con, 20) == 1
        health = con.execute(
            "SELECT health_status,last_run_status FROM er_source_health WHERE source_id='kalshi'"
        ).fetchone()
        assert health == ("healthy", "success")
        issue = con.execute(
            "SELECT status,severity FROM er_quality_issues WHERE rule_code='source_stale'"
        ).fetchone()
        assert issue == ("open", "high")

        con.execute("UPDATE er_source_health SET last_success_at=now(),last_observation_at=now() WHERE source_id='kalshi'")
        assert qa_v5.check_source_freshness(con, 20) == 0
        status = con.execute(
            "SELECT status FROM er_quality_issues WHERE rule_code='source_stale'"
        ).fetchone()[0]
        assert status == "resolved"
    finally:
        con.close()


def test_qa_v5_and_enrichment_v2_are_versioned_and_wired_without_mutating_v4():
    qa = text("institutional_qa_v5.py")
    worker = text("institutional_enrichment_worker_v2.py")
    schema = text("institutional_schema_040.sql")
    migrations = text("institutional_migrations_v2.py")

    assert "institutional_qa_v5_concurrency_safe" in qa
    assert "UPDATE er_source_health" not in qa
    assert "from institutional_qa_v5 import run_qa" in worker
    assert "from institutional_qa_v4 import run_qa" not in worker
    assert "enrichment_path_v2_concurrency_safe_qa" in worker
    assert "institutional_qa_v5_concurrency_safe" in schema
    assert "enrichment_path_v2_concurrency_safe_qa" in schema
    assert '"institutional_schema_040.sql"' in migrations

    compile(qa, "institutional_qa_v5.py", "exec")
    compile(worker, "institutional_enrichment_worker_v2.py", "exec")
