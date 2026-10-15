from pathlib import Path

import duckdb

import institutional_qa_v7 as qa_v7

ROOT = Path(__file__).resolve().parents[1]


def text(name: str) -> str:
    return (ROOT / name).read_text()


def _schema(con):
    con.execute("""
        CREATE TABLE er_source_contract_versions(
          source_contract_id VARCHAR,
          version_no INTEGER,
          raw_snapshot_id VARCHAR
        );
        CREATE TABLE er_contract_version_evidence(
          source_contract_id VARCHAR,
          version_no INTEGER,
          evidence_role VARCHAR,
          raw_snapshot_id VARCHAR,
          PRIMARY KEY(source_contract_id,version_no,evidence_role)
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
    """)


def test_two_required_evidence_roles_do_not_create_cardinality_false_positive():
    con = duckdb.connect(":memory:")
    try:
        _schema(con)
        con.execute("INSERT INTO er_source_contract_versions VALUES ('c1',1,'raw-market')")
        con.execute("""
            INSERT INTO er_contract_version_evidence VALUES
              ('c1',1,'event_payload','raw-event'),
              ('c1',1,'market_payload','raw-market')
        """)
        assert qa_v7.check_contract_version_evidence(con) == 0
        assert con.execute("SELECT count(*) FROM er_quality_issues WHERE status='open'").fetchone()[0] == 0
    finally:
        con.close()


def test_missing_required_role_is_flagged_and_then_resolved():
    con = duckdb.connect(":memory:")
    try:
        _schema(con)
        con.execute("INSERT INTO er_source_contract_versions VALUES ('c1',1,'raw-market')")
        con.execute("INSERT INTO er_contract_version_evidence VALUES ('c1',1,'market_payload','raw-market')")
        assert qa_v7.check_contract_version_evidence(con) == 1
        assert con.execute(
            "SELECT status FROM er_quality_issues WHERE object_id='c1' AND rule_code='contract_version_missing_evidence'"
        ).fetchone()[0] == "open"

        con.execute("INSERT INTO er_contract_version_evidence VALUES ('c1',1,'event_payload','raw-event')")
        assert qa_v7.check_contract_version_evidence(con) == 0
        assert con.execute(
            "SELECT status FROM er_quality_issues WHERE object_id='c1' AND rule_code='contract_version_missing_evidence'"
        ).fetchone()[0] == "resolved"
    finally:
        con.close()


def test_qa_v7_and_enrichment_v4_are_versioned():
    qa = text("institutional_qa_v7.py")
    worker = text("institutional_enrichment_worker_v4.py")
    schema = text("institutional_schema_045.sql")
    migrations = text("institutional_migrations_v2.py")
    assert "institutional_qa_v7_role_aware_contract_evidence" in qa
    assert "event_payload" in qa and "market_payload" in qa
    assert "from institutional_qa_v7 import run_qa" in worker
    assert "enrichment_path_v4_role_aware_qa" in worker
    assert "institutional_qa_v7_role_aware_contract_evidence" in schema
    assert '"institutional_schema_045.sql"' in migrations
