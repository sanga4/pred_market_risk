import os

import db


def setup_database(monkeypatch, tmp_path):
    monkeypatch.setattr(db, "DB", str(tmp_path / "qa-v4.duckdb"))
    os.environ["DB_FORCE_READ_WRITE"] = "true"
    from institutional_migrations import MIGRATION_FILES
    from institutional_migrations_v2 import EXTRA_MIGRATION_FILES, ensure_schema
    state = ensure_schema()
    assert state["migration_count"] == len(MIGRATION_FILES) + len(EXTRA_MIGRATION_FILES)


def test_qa_v4_accepts_full_extended_migration_ledger(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    from institutional_qa_v4 import check_migration_ledger

    con = db.connect()
    try:
        assert check_migration_ledger(con) == 0
        assert con.execute("""
            SELECT count(*) FROM er_quality_issues
            WHERE status='open' AND rule_code='migration_ledger_incomplete'
        """).fetchone()[0] == 0
    finally:
        con.close()


def test_qa_v4_detects_missing_extension_ledger_entry(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    from institutional_qa_v4 import check_migration_ledger

    con = db.connect()
    try:
        con.execute("DELETE FROM er_schema_migrations WHERE migration_name='institutional_schema_029.sql'")
        assert check_migration_ledger(con) == 1
        issue = con.execute("""
            SELECT severity,status FROM er_quality_issues
            WHERE rule_code='migration_ledger_incomplete'
        """).fetchone()
        assert issue == ("critical", "open")
    finally:
        con.close()
