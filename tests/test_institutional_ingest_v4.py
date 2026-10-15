import os
from datetime import datetime, timezone

import db


def setup_database(monkeypatch, tmp_path):
    monkeypatch.setattr(db, "DB", str(tmp_path / "bulk.duckdb"))
    os.environ["DB_FORCE_READ_WRITE"] = "true"
    os.environ["EVENTRISK_COLLECTION_SCOPE"] = "full"
    from institutional_migrations import ensure_schema
    ensure_schema()


def snap(market_id, title, *, event_id="evt-shared", description=None):
    from institutional_ingest_v3 import ContractSnapshot
    close = datetime(2027, 12, 31, tzinfo=timezone.utc)
    return ContractSnapshot(
        source_id="kalshi", source_market_id=market_id, source_event_id=event_id,
        title=title, description=description or f"Description {title}", status="active",
        opens_at=datetime(2026, 1, 1, tzinfo=timezone.utc), closes_at=close, resolves_at=close,
        resolution_authority="test", resolution_rules=f"YES if {title}", source_url=None,
        observed_at=datetime(2026, 10, 14, 21, 0, tzinfo=timezone.utc),
        yes_bid=0.20, yes_ask=0.22, no_bid=None, no_ask=None, last_trade=0.21,
        displayed_probability=0.21, volume=1000.0, volume_24h=100.0,
        open_interest=500.0, liquidity=200.0, orderbook_depth=None,
        raw_event={"id": event_id, "title": "Shared event"},
        raw_market={"id": market_id, "question": title, "bestBid": 0.20, "bestAsk": 0.22},
    )


def test_v4_bulk_ingestion_preserves_raw_version_and_observation_semantics(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    import institutional_ingest_v4 as ingest

    rows = [snap("MKT-A", "Outcome A"), snap("MKT-B", "Outcome B")]

    class FakeAdapter:
        def iter_snapshots(self):
            yield from rows

    monkeypatch.setitem(ingest.ADAPTERS, "kalshi", FakeAdapter)
    result = ingest.ingest_source("kalshi")
    assert result["status"] == "success"
    assert result["records_written"] == 2
    assert result["raw_rows_written"] == 3  # one shared event payload + two market payloads
    assert result["semantic_versions_created"] == 2
    assert result["presence"]["universe_complete"] is True

    con = db.connect()
    try:
        assert con.execute("SELECT count(*) FROM er_raw_snapshots").fetchone()[0] == 3
        assert con.execute("SELECT count(*) FROM er_market_observations").fetchone()[0] == 2
        assert con.execute("SELECT count(*) FROM er_source_contracts").fetchone()[0] == 2
        assert con.execute("SELECT count(*) FROM er_source_contract_versions").fetchone()[0] == 2
        assert con.execute("SELECT count(*) FROM er_contract_semantic_versions").fetchone()[0] == 2
        assert con.execute("SELECT count(*) FROM er_contract_version_evidence").fetchone()[0] == 4
        assert con.execute(
            "SELECT count(*) FROM er_contract_presence_state WHERE listing_state='observed' AND consecutive_misses=0"
        ).fetchone()[0] == 2
        run = con.execute(
            "SELECT status,collection_scope,universe_complete,records_written FROM er_ingestion_runs"
        ).fetchone()
        assert run == ("success", "full", True, 2)
        missing_lineage = con.execute("""
            SELECT count(*) FROM er_market_observations o
            LEFT JOIN er_raw_snapshots r ON r.raw_snapshot_id=o.raw_snapshot_id
            WHERE r.raw_snapshot_id IS NULL
        """).fetchone()[0]
        assert missing_lineage == 0
    finally:
        con.close()


def test_v4_only_versions_changed_semantics(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    import institutional_ingest_v4 as ingest

    current = [snap("MKT-A", "Outcome A")]

    class FakeAdapter:
        def iter_snapshots(self):
            yield from current

    monkeypatch.setitem(ingest.ADAPTERS, "kalshi", FakeAdapter)
    first = ingest.ingest_source("kalshi")
    assert first["semantic_versions_created"] == 1

    second = ingest.ingest_source("kalshi")
    assert second["semantic_versions_created"] == 0

    current[0] = snap("MKT-A", "Outcome A", description="Changed source description")
    third = ingest.ingest_source("kalshi")
    assert third["semantic_versions_created"] == 1

    con = db.connect()
    try:
        versions = con.execute(
            "SELECT version_no,description,valid_to FROM er_contract_semantic_versions ORDER BY version_no"
        ).fetchall()
        assert len(versions) == 2
        assert versions[0][2] is not None
        assert versions[1][1] == "Changed source description"
        assert versions[1][2] is None
        # Observations remain append-only even when contract metadata is unchanged.
        assert con.execute("SELECT count(*) FROM er_market_observations").fetchone()[0] == 3
    finally:
        con.close()
