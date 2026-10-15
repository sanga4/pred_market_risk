import importlib
import os
from datetime import timedelta

import db


def setup_database(monkeypatch, tmp_path):
    monkeypatch.setattr(db, "DB", str(tmp_path / "exports_v4.duckdb"))
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
            ) VALUES ('AAA','Alpha Corp','NMS','equity','Technology','Semiconductors','US','test-master','2026-01-01T00:00:00Z')
        """)
    finally:
        con.close()
    import institutional_security_master_sync as sync
    importlib.reload(sync).sync_security_master()


def test_exports_v4_adds_point_in_time_instrument_reference_datasets(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    import institutional_exports_v4 as exports
    exports = importlib.reload(exports)

    assert "instrument_reference_attribute_versions" in exports.DATASETS
    assert "instrument_reference_attributes_as_of" in exports.DATASETS
    assert exports.GENERATOR_VERSION == "institutional_exports_v4_instrument_reference"

    con = db.connect()
    try:
        version = con.execute("""
            SELECT source_updated_at,valid_from,recorded_at
            FROM er_instrument_reference_attribute_versions
            WHERE source_ticker='AAA'
        """).fetchone()
        assert version[1] > version[0]
        at = version[2] + timedelta(seconds=1)
        sql = exports.DATASETS["instrument_reference_attributes_as_of"]
        rows = con.execute(sql, exports.v3._params(sql, at)).fetchall()
        assert len(rows) == 1
        names = [d[0] for d in con.description]
        row = dict(zip(names, rows[0]))
        assert row["source_ticker"] == "AAA"
        assert row["sector"] == "Technology"
        assert row["valid_to"] is None
    finally:
        con.close()


def test_admin_release_path_promotes_v4_without_rewriting_v3():
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    admin = (root / "institutional_admin_api.py").read_text()
    v3 = (root / "institutional_exports_v3.py").read_text()
    v4 = (root / "institutional_exports_v4.py").read_text()
    schema59 = (root / "institutional_schema_059.sql").read_text()

    assert "from institutional_exports_v4 import generate_release" in admin
    assert "institutional_exports_v4_instrument_reference" in v4
    assert "instrument_reference_attribute_versions" in v4
    assert "instrument_reference_attributes_as_of" in v4
    assert "institutional_exports_v4_instrument_reference" in schema59
    assert "GENERATOR_VERSION = \"institutional_exports_v3\"" in v3
