import os
from datetime import datetime, timezone

import pytest

import db


def setup_database(monkeypatch, tmp_path):
    monkeypatch.setattr(db, "DB", str(tmp_path / "cdc-v9.duckdb"))
    os.environ["DB_FORCE_READ_WRITE"] = "true"
    os.environ["EVENTRISK_COLLECTION_SCOPE"] = "full"
    from institutional_migrations_v2 import ensure_schema
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


def test_v9_streams_before_entire_adapter_is_consumed(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    import institutional_ingest_v9 as ingest

    monkeypatch.setattr(ingest, "CHUNK_ROWS", 2)
    rows = [snap(f"MKT-{i}", f"Outcome {i}") for i in range(5)]
    events = []

    class FakeAdapter:
        def iter_snapshots(self):
            for i, item in enumerate(rows):
                events.append(f"yield-{i}")
                yield item

    monkeypatch.setitem(ingest.ADAPTERS, "kalshi", FakeAdapter)
    original = ingest.persist_registered_batch

    def observed_persist(*args, **kwargs):
        events.append("persist")
        return original(*args, **kwargs)

    monkeypatch.setattr(ingest, "persist_registered_batch", observed_persist)
    result = ingest.ingest_source("kalshi")

    assert result["status"] == "success"
    assert result["chunks"] == 3
    assert result["memory_strategy"] == "streamed_source_direct_temp_chunked_cdc_staged_presence"
    assert events[:3] == ["yield-0", "yield-1", "persist"]
    assert events.index("persist") < events.index("yield-4")
    assert result["presence"]["method_version"] == "presence_reconciliation_v3_staged_seen"


def test_v9_identical_poll_preserves_v8_cdc_semantics(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    import institutional_ingest_v9 as ingest

    monkeypatch.setattr(ingest, "CHUNK_ROWS", 2)
    rows = [snap(f"MKT-{i}", f"Outcome {i}") for i in range(5)]

    class FakeAdapter:
        def iter_snapshots(self):
            yield from rows

    monkeypatch.setitem(ingest.ADAPTERS, "kalshi", FakeAdapter)
    first = ingest.ingest_source("kalshi")
    second = ingest.ingest_source("kalshi")

    assert first["raw_rows_received"] == 8
    assert first["raw_rows_written"] == 6
    assert first["observation_rows_written"] == 5
    assert first["semantic_versions_created"] == 5
    assert second["raw_rows_received"] == 8
    assert second["raw_rows_written"] == 0
    assert second["raw_rows_reused"] == 8
    assert second["observation_rows_written"] == 0
    assert second["flat_observations_suppressed"] == 5
    assert second["semantic_versions_created"] == 0
    assert second["presence"]["seen"] == 5


def test_v9_later_chunk_failure_rolls_back_whole_source(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    import institutional_ingest_v9 as ingest

    monkeypatch.setattr(ingest, "CHUNK_ROWS", 1)
    rows = [snap("MKT-A", "Outcome A"), snap("MKT-B", "Outcome B"), snap("MKT-C", "Outcome C")]

    class FakeAdapter:
        def iter_snapshots(self):
            yield from rows

    monkeypatch.setitem(ingest.ADAPTERS, "kalshi", FakeAdapter)
    original = ingest.persist_registered_batch
    calls = {"n": 0}

    def fail_second_chunk(*args, **kwargs):
        calls["n"] += 1
        result = original(*args, **kwargs)
        if calls["n"] == 2:
            raise RuntimeError("synthetic streamed later-chunk failure")
        return result

    monkeypatch.setattr(ingest, "persist_registered_batch", fail_second_chunk)
    with pytest.raises(RuntimeError, match="synthetic streamed later-chunk failure"):
        ingest.ingest_source("kalshi")

    con = db.connect()
    try:
        assert con.execute("SELECT count(*) FROM er_raw_snapshots").fetchone()[0] == 0
        assert con.execute("SELECT count(*) FROM er_market_observations").fetchone()[0] == 0
        assert con.execute("SELECT count(*) FROM er_source_contracts").fetchone()[0] == 0
        assert con.execute("SELECT status,error_count FROM er_ingestion_runs").fetchone() == ("failed", 1)
    finally:
        con.close()


def test_v9_empty_full_universe_fails_closed_when_active_history_exists(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    import institutional_ingest_v9 as ingest

    current = [snap("MKT-A", "Outcome A")]

    class MutableAdapter:
        def iter_snapshots(self):
            yield from current

    monkeypatch.setitem(ingest.ADAPTERS, "kalshi", MutableAdapter)
    assert ingest.ingest_source("kalshi")["status"] == "success"
    current.clear()

    with pytest.raises(RuntimeError, match="returned zero contracts"):
        ingest.ingest_source("kalshi")

    con = db.connect()
    try:
        contract = con.execute(
            "SELECT active FROM er_source_contracts WHERE source_market_id='MKT-A'"
        ).fetchone()
        assert contract == (True,)
        presence = con.execute(
            "SELECT listing_state,consecutive_misses FROM er_contract_presence_state"
        ).fetchone()
        assert presence == ("observed", 0)
    finally:
        con.close()


def test_v9_methodologies_registered(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    con = db.connect()
    try:
        assert con.execute(
            "SELECT count(*) FROM er_methodology_registry WHERE methodology_id='source_ingestion' AND methodology_version='change_data_capture_v9_streamed'"
        ).fetchone()[0] == 1
        assert con.execute(
            "SELECT count(*) FROM er_methodology_registry WHERE methodology_id='presence_reconciliation' AND methodology_version='presence_reconciliation_v3_staged_seen'"
        ).fetchone()[0] == 1
        assert con.execute(
            "SELECT count(*) FROM er_schema_migrations WHERE migration_name='institutional_schema_031.sql'"
        ).fetchone()[0] == 1
    finally:
        con.close()
