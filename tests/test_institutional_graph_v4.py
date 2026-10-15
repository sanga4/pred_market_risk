import os

import db


def setup_database(monkeypatch, tmp_path):
    monkeypatch.setattr(db, "DB", str(tmp_path / "graph_v4.duckdb"))
    os.environ["DB_FORCE_READ_WRITE"] = "true"
    from institutional_migrations_v2 import ensure_schema
    ensure_schema()


def test_graph_v4_reads_reference_master_but_does_not_import_legacy_securities(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    con = db.connect()
    try:
        # This table deliberately reproduces the legacy trigger that graph v3 could consume.
        con.execute("""
            CREATE TABLE securities(
              ticker VARCHAR,name VARCHAR,exchange VARCHAR,quote_type VARCHAR,asset_class VARCHAR,
              sector VARCHAR,industry VARCHAR,country VARCHAR
            )
        """)
        con.execute("""
            INSERT INTO securities VALUES
              ('BAD','Should Not Become Issuer','NMS',NULL,'equity','Technology','Software','US')
        """)
        con.execute("""
            INSERT INTO er_instruments(
              instrument_id,issuer_entity_id,instrument_type,canonical_name,currency,exchange_mic,active
            ) VALUES ('instr-safe',NULL,'equity','Safe Instrument',NULL,NULL,TRUE)
        """)
        con.execute("""
            INSERT INTO er_instrument_identifiers(
              instrument_id,identifier_type,identifier_value,source,valid_from,valid_to
            ) VALUES ('instr-safe','ticker_eventrisk','SAFE','event_risk.main.securities',now(),NULL)
        """)
    finally:
        con.close()

    from institutional_graph_v4 import run_graph_pass
    result = run_graph_pass()
    assert result["execution_version"] == "graph_execution_v4_no_issuer_inference"
    assert result["identity_policy"] == "read_reference_master_only_no_issuer_inference"
    assert result["instruments"] == 1
    assert result["ticker_identifiers"] == 1
    assert result["issuer_links"] == 0

    con = db.connect()
    try:
        assert con.execute("SELECT count(*) FROM er_instruments WHERE canonical_name='Should Not Become Issuer'").fetchone()[0] == 0
        assert con.execute("SELECT count(*) FROM er_entity_identifiers WHERE source='legacy_security_master'").fetchone()[0] == 0
        assert con.execute("SELECT count(*) FROM er_instrument_identifiers WHERE source='legacy_security_master'").fetchone()[0] == 0
    finally:
        con.close()


def test_graph_v4_reference_status_does_not_create_issuer_links(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    con = db.connect()
    try:
        con.execute("""
            INSERT INTO er_instruments(
              instrument_id,issuer_entity_id,instrument_type,canonical_name,currency,exchange_mic,active
            ) VALUES ('instr-a',NULL,'equity','Alpha',NULL,NULL,TRUE)
        """)
        from institutional_graph_v4 import _reference_master_status
        status = _reference_master_status(con)
        assert status["instruments"] == 1
        assert status["issuer_links"] == 0
        assert status["identity_policy"] == "read_reference_master_only_no_issuer_inference"
        assert con.execute("SELECT count(*) FROM er_entities").fetchone()[0] == 0
    finally:
        con.close()
