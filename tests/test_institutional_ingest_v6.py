import os
from datetime import datetime, timezone

import db


def setup_database(monkeypatch, tmp_path):
    monkeypatch.setattr(db, "DB", str(tmp_path / "cdc.duckdb"))
    os.environ["DB_FORCE_READ_WRITE"] = "true"
    os.environ["EVENTRISK_COLLECTION_SCOPE"] = "full"
    from institutional_migrations import ensure_schema
    ensure_schema()


def snap(market_id, title, *, event_id="evt-shared", bid=0.20, ask=0.22, volume=1000.0):
    from institutional_ingest_v3 import ContractSnapshot
    close = datetime(2027, 12, 31, tzinfo=timezone.utc)
    midpoint = (bid + ask) / 2
    return ContractSnapshot(
        source_id="kalshi", source_market_id=market_id, source_event_id=event_id,
        title=title, description=f"Description {title}", status="active",
        opens_at=datetime(2026, 1, 1, tzinfo=timezone.utc), closes_at=close, resolves_at=close,
        resolution_authority="test", resolution_rules=f"YES if {title}", source_url=None,
        observed_at=datetime(2026, 10, 14, 21, 0, tzinfo=timezone.utc),
        yes_bid=bid, yes_ask=ask, no_bid=None, no_ask=None, last_trade=midpoint,
        displayed_probability=midpoint, volume=volume, volume_24h=100.0,
        open_interest=500.0, liquidity=200.0, orderbook_depth=None,
        raw_event={"id": event_id, "title": "Shared event"},
        raw_market={"id": market_id, "question": title, "bestBid": bid, "bestAsk": ask, "volume": volume},
    )


def test_v6_identical_poll_reuses_raw_and_suppresses_flat_observations(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    import institutional_ingest_v6 as ingest

    rows = [snap("MKT-A", "Outcome A"), snap("MKT-B", "Outcome B")]

    class FakeAdapter:
        def iter_snapshots(self):
            yield from rows

    monkeypatch.setitem(ingest.ADAPTERS, "kalshi", FakeAdapter)

    first = ingest.ingest_source("kalshi")
    assert first["status"] == "success"
    assert first["raw_rows_received"] == 3
    assert first["raw_rows_written"] == 3
    assert first["raw_rows_reused"] == 0
    assert first["observation_rows_received"] == 2
    assert first["observation_rows_written"] == 2
    assert first["flat_observations_suppressed"] == 0
    assert first["semantic_versions_created"] == 2

    second = ingest.ingest_source("kalshi")
    assert second["status"] == "success"
    assert second["raw_rows_received"] == 3
    assert second["raw_rows_written"] == 0
    assert second["raw_rows_reused"] == 3
    assert second["observation_rows_received"] == 2
    assert second["observation_rows_written"] == 0
    assert second["flat_observations_suppressed"] == 2
    assert second["semantic_versions_created"] == 0
    assert second["presence"]["seen"] == 2
    assert second["presence"]["universe_complete"] is True

    con = db.connect()
    try:
        assert con.execute("SELECT count(*) FROM er_raw_snapshots").fetchone()[0] == 3
        assert con.execute("SELECT count(*) FROM er_market_observations").fetchone()[0] == 2
        assert con.execute("SELECT count(*) FROM er_contract_semantic_versions").fetchone()[0] == 2
        assert con.execute("SELECT count(*) FROM er_ingestion_runs WHERE status='success'").fetchone()[0] == 2
        assert con.execute(
            "SELECT count(*) FROM er_contract_presence_state WHERE listing_state='observed' AND consecutive_misses=0"
        ).fetchone()[0] == 2
    finally:
        con.close()


def test_v6_market_state_change_appends_only_changed_raw_and_curve_point(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    import institutional_ingest_v6 as ingest

    current = [snap("MKT-A", "Outcome A")]

    class FakeAdapter:
        def iter_snapshots(self):
            yield from current

    monkeypatch.setitem(ingest.ADAPTERS, "kalshi", FakeAdapter)
    first = ingest.ingest_source("kalshi")
    assert first["raw_rows_written"] == 2
    assert first["observation_rows_written"] == 1

    current[0] = snap("MKT-A", "Outcome A", bid=0.25, ask=0.27, volume=1100.0)
    second = ingest.ingest_source("kalshi")
    assert second["raw_rows_received"] == 2
    assert second["raw_rows_written"] == 1  # event payload reused; market payload changed
    assert second["raw_rows_reused"] == 1
    assert second["observation_rows_written"] == 1
    assert second["flat_observations_suppressed"] == 0
    assert second["semantic_versions_created"] == 0

    con = db.connect()
    try:
        assert con.execute("SELECT count(*) FROM er_raw_snapshots").fetchone()[0] == 3
        assert con.execute("SELECT count(*) FROM er_market_observations").fetchone()[0] == 2
        latest = con.execute(
            "SELECT yes_bid,yes_ask,volume FROM er_latest_market_observations"
        ).fetchone()
        assert latest == (0.25, 0.27, 1100.0)
        missing_lineage = con.execute("""
            SELECT count(*) FROM er_market_observations o
            LEFT JOIN er_raw_snapshots r ON r.raw_snapshot_id=o.raw_snapshot_id
            WHERE r.raw_snapshot_id IS NULL
        """).fetchone()[0]
        assert missing_lineage == 0
    finally:
        con.close()


def test_v6_methodologies_are_registered(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    con = db.connect()
    try:
        assert con.execute(
            "SELECT count(*) FROM er_methodology_registry WHERE methodology_id='source_ingestion' AND methodology_version='change_data_capture_v6'"
        ).fetchone()[0] == 1
        assert con.execute(
            "SELECT count(*) FROM er_methodology_registry WHERE methodology_id='observation_sampling' AND methodology_version='state_change_points_v1'"
        ).fetchone()[0] == 1
    finally:
        con.close()
