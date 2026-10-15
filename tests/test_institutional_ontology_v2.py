import os

import db


def setup_database(monkeypatch, tmp_path):
    monkeypatch.setattr(db, "DB", str(tmp_path / "ontology_v2.duckdb"))
    os.environ["DB_FORCE_READ_WRITE"] = "true"
    from institutional_migrations import ensure_schema
    ensure_schema()


def seed_event(con, event_id, family_id, name, definition, event_class="unclassified"):
    con.execute("""
        INSERT INTO er_event_families(event_family_id,canonical_name,category,description)
        VALUES (?,?, 'unclassified','test family')
        ON CONFLICT(event_family_id) DO NOTHING
    """, [family_id, f"Family {family_id}"])
    con.execute("""
        INSERT INTO er_canonical_events(
          canonical_event_id,event_family_id,canonical_name,canonical_definition,event_class,taxonomy_version,status)
        VALUES (?,?,?,?,?,'event_ontology_v1','active')
    """, [event_id, family_id, name, definition, event_class])


def test_ontology_v2_normalizes_events_and_families_then_becomes_incremental(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    con = db.connect()
    try:
        seed_event(
            con, "evt-1", "fam-1",
            "Will China blockade Taiwan before 2028?",
            "Resolves YES if China begins a blockade of Taiwan before 2028.",
        )
    finally:
        con.close()

    from institutional_ontology_v2 import run_ontology_pass
    first = run_ontology_pass()
    assert first["method_version"] == "event_ontology_v2_batched"
    assert first["events_total"] == 1
    assert first["events_processed"] == 1
    assert first["events_normalized"] == 1
    assert first["families_normalized"] == 1

    con = db.connect()
    try:
        event = con.execute("""
            SELECT event_class,taxonomy_version FROM er_canonical_events WHERE canonical_event_id='evt-1'
        """).fetchone()
        assert event == ("geopolitics", "event_ontology_v1")
        assert con.execute(
            "SELECT category FROM er_event_families WHERE event_family_id='fam-1'"
        ).fetchone()[0] == "geopolitics"
        assert con.execute(
            "SELECT count(*) FROM er_canonical_event_versions WHERE canonical_event_id='evt-1'"
        ).fetchone()[0] == 2
        assert con.execute(
            "SELECT count(*) FROM er_event_family_versions WHERE event_family_id='fam-1'"
        ).fetchone()[0] == 2
        assert con.execute(
            "SELECT count(*) FROM er_ontology_event_state WHERE canonical_event_id='evt-1'"
        ).fetchone()[0] == 1
    finally:
        con.close()

    second = run_ontology_pass()
    assert second["events_processed"] == 0
    assert second["events_normalized"] == 0
    assert second["families_normalized"] == 0


def test_ontology_v2_reprocesses_only_changed_classification_inputs(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    con = db.connect()
    try:
        seed_event(con, "evt-1", "fam-1", "Will CPI be above 3%?", "YES if CPI is above 3 percent.")
        seed_event(con, "evt-2", "fam-2", "Will Bitcoin exceed $100000?", "YES if Bitcoin exceeds $100000.")
    finally:
        con.close()

    from institutional_ontology_v2 import run_ontology_pass
    first = run_ontology_pass()
    assert first["events_processed"] == 2

    con = db.connect()
    try:
        con.execute("""
            UPDATE er_canonical_events
            SET canonical_name='Will the Fed cut interest rates?',
                canonical_definition='YES if the FOMC announces an interest rate cut.'
            WHERE canonical_event_id='evt-1'
        """)
    finally:
        con.close()

    second = run_ontology_pass()
    assert second["events_processed"] == 1
    assert second["events_normalized"] == 1
    con = db.connect()
    try:
        assert con.execute(
            "SELECT event_class FROM er_canonical_events WHERE canonical_event_id='evt-1'"
        ).fetchone()[0] == "rates"
        assert con.execute(
            "SELECT event_class FROM er_canonical_events WHERE canonical_event_id='evt-2'"
        ).fetchone()[0] == "crypto"
        assert con.execute(
            "SELECT count(*) FROM er_canonical_event_versions WHERE canonical_event_id='evt-1'"
        ).fetchone()[0] == 3
    finally:
        con.close()


def test_ontology_v2_marks_already_normalized_untracked_event_without_rewriting_history(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    con = db.connect()
    try:
        seed_event(
            con, "evt-1", "fam-1",
            "Will US CPI inflation be above 3%?",
            "YES if CPI inflation is above 3 percent.",
            event_class="inflation",
        )
        con.execute("UPDATE er_event_families SET category='inflation' WHERE event_family_id='fam-1'")
    finally:
        con.close()

    from institutional_ontology_v2 import run_ontology_pass
    result = run_ontology_pass()
    assert result["events_processed"] == 1
    assert result["events_normalized"] == 1  # threshold predicate/value are newly normalized
    second = run_ontology_pass()
    assert second["events_processed"] == 0


def test_ontology_v2_recovers_stale_runs_as_abandoned(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    from institutional_ontology_v2 import recover_abandoned_ontology_runs

    con = db.connect()
    try:
        con.execute("""
            INSERT INTO er_ontology_runs(run_id,method_version,started_at,status)
            VALUES ('stale-ontology','event_ontology_v2_batched',now()-INTERVAL '2 hours','running')
        """)
        recovery = recover_abandoned_ontology_runs(con)
        assert recovery["recovered"] == 1
        assert recovery["running"] == []
        row = con.execute("SELECT status,finished_at,details FROM er_ontology_runs WHERE run_id='stale-ontology'").fetchone()
        assert row[0] == "abandoned"
        assert row[1] is not None
        assert "ontology_run_recovery_v1" in row[2]
    finally:
        con.close()
