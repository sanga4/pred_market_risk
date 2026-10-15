import os

import db


def setup_database(monkeypatch, tmp_path):
    monkeypatch.setattr(db, "DB", str(tmp_path / "lifecycle.duckdb"))
    os.environ["DB_FORCE_READ_WRITE"] = "true"
    from institutional_migrations import ensure_schema
    ensure_schema()


def seed(con):
    con.execute("""
        INSERT INTO er_sources(source_id,source_name,source_type,active)
        VALUES ('test','Test','prediction_market',TRUE)
    """)
    con.execute("""
        INSERT INTO er_source_contracts(
          source_contract_id,source_id,source_market_id,current_title,status,active
        ) VALUES ('sc_1','test','m1','Test event','active',TRUE)
    """)
    con.execute("""
        INSERT INTO er_event_families(event_family_id,canonical_name,category)
        VALUES ('fam_1','Test family','other')
    """)
    con.execute("""
        INSERT INTO er_canonical_events(
          canonical_event_id,event_family_id,canonical_name,canonical_definition,status
        ) VALUES ('evt_1','fam_1','Test event','Test event definition','active')
    """)
    con.execute("""
        INSERT INTO er_event_mappings(
          mapping_id,source_contract_id,canonical_event_id,mapping_type,confidence,
          mapping_method,method_version,review_status,created_by
        ) VALUES ('map_1','sc_1','evt_1','source_native',1.0,
                  'deterministic_identity','test','source_native','system')
    """)


def test_lifecycle_tracks_source_state_and_versions(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    from institutional_lifecycle import sync_event_lifecycle

    con = db.connect()
    try:
        seed(con)
    finally:
        con.close()

    unchanged = sync_event_lifecycle()
    assert unchanged["events_status_changed"] == 0

    con = db.connect()
    try:
        con.execute("UPDATE er_source_contracts SET active=FALSE,status='closed' WHERE source_contract_id='sc_1'")
    finally:
        con.close()
    closed = sync_event_lifecycle()
    assert closed["events_status_changed"] == 1

    con = db.connect()
    try:
        assert con.execute("SELECT status FROM er_canonical_events WHERE canonical_event_id='evt_1'").fetchone()[0] == 'closed'
        versions = con.execute(
            "SELECT version_no,status,valid_to FROM er_canonical_event_versions WHERE canonical_event_id='evt_1' ORDER BY version_no"
        ).fetchall()
        assert len(versions) >= 2
        assert versions[-1][1] == 'closed'
        assert versions[-1][2] is None
        revisions = con.execute(
            "SELECT count(*) FROM er_revisions WHERE object_id='evt_1' AND revision_type='source_lifecycle_sync'"
        ).fetchone()[0]
        assert revisions == 1
        con.execute("UPDATE er_source_contracts SET status='settled' WHERE source_contract_id='sc_1'")
    finally:
        con.close()

    resolved = sync_event_lifecycle()
    assert resolved["events_status_changed"] == 1
    con = db.connect()
    try:
        assert con.execute("SELECT status FROM er_canonical_events WHERE canonical_event_id='evt_1'").fetchone()[0] == 'resolved'
    finally:
        con.close()


def test_lifecycle_never_overwrites_superseded_identity(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    from institutional_lifecycle import sync_event_lifecycle

    con = db.connect()
    try:
        seed(con)
        con.execute("UPDATE er_canonical_events SET status='superseded' WHERE canonical_event_id='evt_1'")
        con.execute("UPDATE er_source_contracts SET active=FALSE,status='closed' WHERE source_contract_id='sc_1'")
    finally:
        con.close()

    result = sync_event_lifecycle()
    assert result["events_status_changed"] == 0
    assert result["superseded_events_untouched"] == 1
    con = db.connect()
    try:
        assert con.execute("SELECT status FROM er_canonical_events WHERE canonical_event_id='evt_1'").fetchone()[0] == 'superseded'
    finally:
        con.close()
