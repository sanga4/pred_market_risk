import importlib
import os
from datetime import timedelta

import db


def setup_database(monkeypatch, tmp_path):
    monkeypatch.setattr(db, "DB", str(tmp_path / "instrument_api.duckdb"))
    os.environ["DB_FORCE_READ_WRITE"] = "true"
    monkeypatch.setenv("EVENTRISK_SECURITY_MASTER_TABLE", "source_securities")
    from institutional_migrations_v2 import ensure_schema
    ensure_schema()
    con = db.connect()
    try:
        con.execute("""
            CREATE TABLE source_securities(
              ticker VARCHAR PRIMARY KEY,name VARCHAR,exchange VARCHAR,quote_type VARCHAR,
              asset_class VARCHAR,sector VARCHAR,industry VARCHAR,country VARCHAR,tracked BOOLEAN,
              source VARCHAR,last_requested_at TIMESTAMPTZ,updated_at TIMESTAMPTZ
            )
        """)
        con.execute("""
            INSERT INTO source_securities(
              ticker,name,exchange,asset_class,sector,industry,country,source,updated_at
            ) VALUES
              ('AAA','Alpha Corp','NMS','equity','Technology','Semiconductors','US','test-master','2026-01-01T00:00:00Z'),
              ('BBB','Beta Corp',NULL,'equity','Financials','Banks','GB','test-master','2026-01-02T00:00:00Z')
        """)
    finally:
        con.close()
    import institutional_security_master_sync as sync
    importlib.reload(sync).sync_security_master()


def test_instrument_list_is_keyset_paginated_and_filterable(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    import institutional_public_queries_api as api
    api = importlib.reload(api)

    first = api.list_instruments(ticker=None, sector=None, country=None, active=True, after=None, limit=1)
    assert first["count"] == 1
    assert first["next_after"] == first["items"][0]["instrument_id"]
    second = api.list_instruments(
        ticker=None, sector=None, country=None, active=True, after=first["next_after"], limit=1
    )
    assert second["count"] == 1
    assert second["items"][0]["instrument_id"] != first["items"][0]["instrument_id"]

    filtered = api.list_instruments(ticker="AAA", sector="Technology", country="US", active=True, after=None, limit=10)
    assert filtered["count"] == 1
    assert filtered["items"][0]["source_ticker"] == "AAA"
    assert filtered["items"][0]["issuer_entity_id"] is None
    assert filtered["items"][0]["exchange_mic"] is None
    assert filtered["items"][0]["exchange_label"] == "NMS"


def test_instrument_detail_and_as_of_preserve_unasserted_fields(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    import institutional_public_queries_api as api
    api = importlib.reload(api)

    listed = api.list_instruments(ticker="AAA", sector=None, country=None, active=True, after=None, limit=10)
    instrument_id = listed["items"][0]["instrument_id"]
    detail = api.get_instrument(instrument_id)
    assert detail["instrument"]["source_ticker"] == "AAA"
    assert detail["instrument"]["issuer_entity_id"] is None
    assert detail["instrument"]["exchange_mic"] is None
    assert len(detail["identifiers"]) == 1
    assert detail["identifiers"][0]["identifier_type"] == "ticker_eventrisk"
    assert len(detail["instrument_versions"]) == 1
    assert len(detail["reference_versions"]) == 1

    at = detail["reference_versions"][0]["valid_from"] + timedelta(seconds=1)
    historical = api.instrument_as_of(instrument_id, at)
    assert historical["instrument_id"] == instrument_id
    assert historical["instrument"]["issuer_entity_id"] is None
    assert historical["reference"]["source_ticker"] == "AAA"
    assert historical["identifiers"][0]["identifier_value"] == "AAA"
