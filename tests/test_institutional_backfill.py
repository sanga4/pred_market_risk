import os
from pathlib import Path
from datetime import datetime, timezone

import db


def setup_legacy_database(tmp_path):
    db.DB = str(tmp_path / "legacy_backfill_test.duckdb")
    os.environ["DB_FORCE_READ_WRITE"] = "true"
    root = Path(__file__).resolve().parents[1]
    con = db.connect()
    try:
        con.execute((root / "institutional_schema.sql").read_text())
        con.execute((root / "institutional_schema_002.sql").read_text())
        con.execute((root / "institutional_schema_003.sql").read_text())
        con.execute("""CREATE TABLE ticks(
          ts TIMESTAMPTZ, venue VARCHAR, market_id VARCHAR, event_id VARCHAR,
          event_title VARCHAR, market_title VARCHAR, close_time TIMESTAMPTZ,
          prob DOUBLE, yes_bid DOUBLE, yes_ask DOUBLE, last DOUBLE,
          volume DOUBLE, volume_24h DOUBLE
        )""")
        con.execute("""CREATE TABLE canonical_events(
          canonical_event_id VARCHAR, title VARCHAR, category VARCHAR, region VARCHAR,
          theme VARCHAR, eligible BOOLEAN, updated_at TIMESTAMPTZ
        )""")
        con.execute("""CREATE TABLE event_markets(
          canonical_event_id VARCHAR, venue VARCHAR, market_id VARCHAR
        )""")
        ts = datetime(2026, 10, 13, 12, 0, tzinfo=timezone.utc)
        close = datetime(2027, 12, 31, tzinfo=timezone.utc)
        con.execute(
            "INSERT INTO ticks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [ts, "kalshi", "KX-LEGACY", "KX-EVENT", "Legacy Taiwan event", "Taiwan event outcome",
             close, 0.21, 0.20, 0.22, 0.21, 1000.0, 100.0],
        )
        con.execute(
            "INSERT INTO canonical_events VALUES (?,?,?,?,?,?,?)",
            ["legacy_evt_1", "Legacy Taiwan event", "geopolitics", "asia", "taiwan", True, ts],
        )
        con.execute("INSERT INTO event_markets VALUES (?,?,?)", ["legacy_evt_1", "kalshi", "KX-LEGACY"])
    finally:
        con.close()


def test_legacy_backfill_is_explicit_and_nonfabricated(tmp_path):
    setup_legacy_database(tmp_path)
    from institutional_backfill import main as backfill
    from institutional_qa import run_qa

    backfill()
    con = db.connect()
    try:
        contract = con.execute(
            "SELECT source_contract_id FROM er_source_contracts WHERE source_id='kalshi' AND source_market_id='KX-LEGACY'"
        ).fetchone()
        assert contract is not None
        contract_id = contract[0]

        version = con.execute(
            "SELECT version_no,raw_snapshot_id FROM er_source_contract_versions WHERE source_contract_id=?",
            [contract_id],
        ).fetchone()
        assert version == (1, None)

        obs = con.execute(
            "SELECT methodology_version,raw_snapshot_id FROM er_market_observations WHERE source_contract_id=?",
            [contract_id],
        ).fetchone()
        assert obs == ("legacy_collector_v0.3", None)

        mapping = con.execute(
            "SELECT review_status FROM er_event_mappings WHERE source_contract_id=?",
            [contract_id],
        ).fetchone()
        assert mapping == ("legacy_unreviewed",)

        disclosure = con.execute(
            "SELECT severity,status FROM er_quality_issues WHERE quality_issue_id='quality:legacy_raw_payload_unavailable'"
        ).fetchone()
        assert disclosure == ("info", "open")
    finally:
        con.close()

    qa = run_qa(stale_minutes=10**7)
    assert qa["checks"]["missing_raw_lineage"] == 0
