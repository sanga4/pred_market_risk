import importlib
import os

import db


def setup_database(monkeypatch, tmp_path):
    monkeypatch.setattr(db, "DB", str(tmp_path / "security_master_sync.duckdb"))
    os.environ["DB_FORCE_READ_WRITE"] = "true"
    monkeypatch.setenv("EVENTRISK_SECURITY_MASTER_TABLE", "source_securities")
    from institutional_migrations_v2 import ensure_schema
    ensure_schema()
    con = db.connect()
    try:
        con.execute("""
            CREATE TABLE source_securities(
              ticker VARCHAR PRIMARY KEY,
              name VARCHAR,
              exchange VARCHAR,
              quote_type VARCHAR,
              asset_class VARCHAR,
              sector VARCHAR,
              industry VARCHAR,
              country VARCHAR,
              tracked BOOLEAN,
              source VARCHAR,
              last_requested_at TIMESTAMPTZ,
              updated_at TIMESTAMPTZ
            )
        """)
        con.execute("""
            INSERT INTO source_securities(
              ticker,name,exchange,quote_type,asset_class,sector,industry,country,tracked,source,updated_at
            ) VALUES
              ('AAA','Alpha Corp','NASDAQ',NULL,'equity','Technology','Semiconductors','US',FALSE,'test-master','2026-01-01T00:00:00Z'),
              ('BBB','Beta Fund','NYSE',NULL,'equity',NULL,NULL,'US',FALSE,'test-master','2026-01-01T00:00:00Z')
        """)
    finally:
        con.close()


def load_sync_module():
    import institutional_security_master_sync as sync
    return importlib.reload(sync)


def test_security_master_sync_creates_instruments_without_inventing_issuers(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    sync = load_sync_module()

    first = sync.sync_security_master()
    assert first["records_received"] == 2
    assert first["instruments_upserted"] == 2
    assert first["instruments_deactivated"] == 0
    assert first["attribute_versions_created"] == 2
    assert first["instrument_versions_created"] == 2

    con = db.connect()
    try:
        instruments = con.execute("""
            SELECT canonical_name,issuer_entity_id,exchange_mic,active
            FROM er_instruments ORDER BY canonical_name
        """).fetchall()
        assert instruments == [
            ("Alpha Corp", None, None, True),
            ("Beta Fund", None, None, True),
        ]
        assert con.execute("SELECT count(*) FROM er_entities").fetchone()[0] == 0
        identifiers = con.execute("""
            SELECT identifier_type,identifier_value,source
            FROM er_instrument_identifiers ORDER BY identifier_value
        """).fetchall()
        assert identifiers == [
            ("ticker_eventrisk", "AAA", "event_risk.main.securities"),
            ("ticker_eventrisk", "BBB", "event_risk.main.securities"),
        ]
        attrs = con.execute("""
            SELECT source_ticker,exchange_label,sector,country
            FROM er_instrument_reference_attributes ORDER BY source_ticker
        """).fetchall()
        assert attrs == [
            ("AAA", "NASDAQ", "Technology", "US"),
            ("BBB", "NYSE", None, "US"),
        ]
    finally:
        con.close()

    second = sync.sync_security_master()
    assert second["attribute_versions_created"] == 0
    assert second["instrument_versions_created"] == 0


def test_security_master_sync_versions_changes_and_deactivates_only_owned_missing_rows(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    sync = load_sync_module()
    sync.sync_security_master()

    con = db.connect()
    try:
        con.execute("""
            INSERT INTO er_instruments(
              instrument_id,issuer_entity_id,instrument_type,canonical_name,currency,exchange_mic,active
            ) VALUES ('external-instrument',NULL,'equity','External Instrument',NULL,NULL,TRUE)
        """)
        con.execute("""
            UPDATE source_securities
            SET sector='Information Technology',updated_at='2026-02-01T00:00:00Z'
            WHERE ticker='AAA'
        """)
        con.execute("DELETE FROM source_securities WHERE ticker='BBB'")
    finally:
        con.close()

    changed = sync.sync_security_master()
    assert changed["records_received"] == 1
    assert changed["attribute_versions_created"] == 1
    assert changed["instruments_deactivated"] == 1

    con = db.connect()
    try:
        aaa_versions = con.execute("""
            SELECT version_no,sector,valid_to IS NULL
            FROM er_instrument_reference_attribute_versions
            WHERE source_ticker='AAA' ORDER BY version_no
        """).fetchall()
        assert aaa_versions == [
            (1, "Technology", False),
            (2, "Information Technology", True),
        ]
        bbb_active = con.execute("""
            SELECT i.active
            FROM er_instruments i
            JOIN er_instrument_reference_attributes a USING(instrument_id)
            WHERE a.source_ticker='BBB'
        """).fetchone()[0]
        assert bbb_active is False
        assert con.execute("SELECT active FROM er_instruments WHERE instrument_id='external-instrument'").fetchone()[0] is True
        sync_runs = con.execute("""
            SELECT status,records_received,instruments_deactivated
            FROM er_reference_sync_runs ORDER BY started_at
        """).fetchall()
        assert sync_runs[-1] == ("success", 1, 1)
    finally:
        con.close()
