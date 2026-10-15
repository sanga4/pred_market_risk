import os

import db


def setup_database(monkeypatch, tmp_path):
    monkeypatch.setattr(db, "DB", str(tmp_path / "reference_versions_v2.duckdb"))
    os.environ["DB_FORCE_READ_WRITE"] = "true"
    from institutional_migrations import ensure_schema
    ensure_schema()


def test_reference_v2_creates_changes_setwise_and_is_noop_when_unchanged(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    con = db.connect()
    try:
        con.execute("""
            INSERT INTO er_sources(source_id,source_name,source_type,base_url,redistribution_status,attribution_required,active)
            VALUES ('source-x','Source X','prediction_market','https://example.test','unknown',FALSE,TRUE)
        """)
        con.execute("""
            INSERT INTO er_entities(entity_id,entity_type,canonical_name,legal_name,country_code,active)
            VALUES ('ent-x','legal_entity','Example Corp','Example Corp','US',TRUE)
        """)
        con.execute("""
            INSERT INTO er_instruments(
              instrument_id,issuer_entity_id,instrument_type,canonical_name,currency,exchange_mic,active)
            VALUES ('ins-x','ent-x','equity','EXM Example Corp','USD','XNAS',TRUE)
        """)
    finally:
        con.close()

    from institutional_reference_versions_v2 import sync_reference_versions
    first = sync_reference_versions()
    assert first == {
        "source_versions_created": 1,
        "entity_versions_created": 1,
        "instrument_versions_created": 1,
        "method_version": "reference_versions_v2_set_based",
    }

    second = sync_reference_versions()
    assert second["source_versions_created"] == 0
    assert second["entity_versions_created"] == 0
    assert second["instrument_versions_created"] == 0

    con = db.connect()
    try:
        con.execute("UPDATE er_sources SET redistribution_status='permitted' WHERE source_id='source-x'")
        con.execute("UPDATE er_entities SET legal_name='Example Corporation' WHERE entity_id='ent-x'")
        con.execute("UPDATE er_instruments SET exchange_mic='XNYS' WHERE instrument_id='ins-x'")
    finally:
        con.close()

    third = sync_reference_versions()
    assert third["source_versions_created"] == 1
    assert third["entity_versions_created"] == 1
    assert third["instrument_versions_created"] == 1

    con = db.connect()
    try:
        source_versions = con.execute("""
            SELECT version_no,redistribution_status,valid_to IS NULL
            FROM er_source_versions WHERE source_id='source-x' ORDER BY version_no
        """).fetchall()
        entity_versions = con.execute("""
            SELECT version_no,legal_name,valid_to IS NULL
            FROM er_entity_versions WHERE entity_id='ent-x' ORDER BY version_no
        """).fetchall()
        instrument_versions = con.execute("""
            SELECT version_no,exchange_mic,valid_to IS NULL
            FROM er_instrument_versions WHERE instrument_id='ins-x' ORDER BY version_no
        """).fetchall()
        assert source_versions == [(1, "unknown", False), (2, "permitted", True)]
        assert entity_versions == [(1, "Example Corp", False), (2, "Example Corporation", True)]
        assert instrument_versions == [(1, "XNAS", False), (2, "XNYS", True)]
    finally:
        con.close()
