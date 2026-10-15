import os
from datetime import datetime, timezone

import pytest

import db


def setup_database(monkeypatch, tmp_path):
    monkeypatch.setattr(db, "DB", str(tmp_path / "cdc-v8.duckdb"))
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


def adapter_for(rows):
    class FakeAdapter:
        def iter_snapshots(self):
            yield from rows
    return FakeAdapter


def test_v8_direct_temp_chunked_poll_preserves_cdc_semantics(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    import institutional_ingest_v8 as ingest

    monkeypatch.setattr(ingest, "CHUNK_ROWS", 2)
    rows = [snap(f"MKT-{i}", f"Outcome {i}") for i in range(5)]
    monkeypatch.setitem(ingest.ADAPTERS, "kalshi", adapter_for(rows))

    first = ingest.ingest_source("kalshi")
    assert first["status"] == "success"
    assert first["chunks"] == 3
    assert first["memory_strategy"] == "sequential_source_direct_temp_chunked_cdc"
    assert first["raw_rows_received"] == 8
    assert first["raw_rows_written"] == 6
    assert first["raw_rows_reused"] == 2
    assert first["observation_rows_received"] == 5
    assert first["observation_rows_written"] == 5
    assert first["flat_observations_suppressed"] == 0
    assert first["semantic_versions_created"] == 5
    assert first["presence"]["seen"] == 5
    assert first["presence"]["method_version"] == "presence_reconciliation_v2_set_based"

    second = ingest.ingest_source("kalshi")
    assert second["status"] == "success"
    assert second["chunks"] == 3
    assert second["raw_rows_received"] == 8
    assert second["raw_rows_written"] == 0
    assert second["raw_rows_reused"] == 8
    assert second["observation_rows_received"] == 5
    assert second["observation_rows_written"] == 0
    assert second["flat_observations_suppressed"] == 5
    assert second["semantic_versions_created"] == 0
    assert second["presence"]["seen"] == 5
    assert second["presence"]["universe_complete"] is True

    con = db.connect()
    try:
        assert con.execute("SELECT count(*) FROM er_raw_snapshots").fetchone()[0] == 6
        assert con.execute("SELECT count(*) FROM er_market_observations").fetchone()[0] == 5
        assert con.execute("SELECT count(*) FROM er_contract_semantic_versions").fetchone()[0] == 5
        assert con.execute("SELECT count(*) FROM er_ingestion_runs WHERE status='success'").fetchone()[0] == 2
        missing_lineage = con.execute("""
            SELECT count(*) FROM er_market_observations o
            LEFT JOIN er_raw_snapshots r ON r.raw_snapshot_id=o.raw_snapshot_id
            WHERE r.raw_snapshot_id IS NULL
        """).fetchone()[0]
        assert missing_lineage == 0
    finally:
        con.close()


def test_v8_later_chunk_failure_rolls_back_source_transaction(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    import institutional_ingest_v8 as ingest

    monkeypatch.setattr(ingest, "CHUNK_ROWS", 1)
    rows = [snap("MKT-A", "Outcome A"), snap("MKT-B", "Outcome B"), snap("MKT-C", "Outcome C")]
    monkeypatch.setitem(ingest.ADAPTERS, "kalshi", adapter_for(rows))

    original = ingest.persist_registered_batch
    calls = {"n": 0}

    def fail_second_chunk(*args, **kwargs):
        calls["n"] += 1
        result = original(*args, **kwargs)
        if calls["n"] == 2:
            raise RuntimeError("synthetic later-chunk failure")
        return result

    monkeypatch.setattr(ingest, "persist_registered_batch", fail_second_chunk)
    with pytest.raises(RuntimeError, match="synthetic later-chunk failure"):
        ingest.ingest_source("kalshi")

    con = db.connect()
    try:
        assert con.execute("SELECT count(*) FROM er_raw_snapshots").fetchone()[0] == 0
        assert con.execute("SELECT count(*) FROM er_market_observations").fetchone()[0] == 0
        assert con.execute("SELECT count(*) FROM er_source_contracts").fetchone()[0] == 0
        failed = con.execute("SELECT status,error_count FROM er_ingestion_runs").fetchone()
        assert failed == ("failed", 1)
    finally:
        con.close()


def test_v8_set_based_presence_preserves_miss_and_reactivation_state_machine(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    import institutional_ingest_v8 as ingest

    monkeypatch.setattr(ingest, "CHUNK_ROWS", 2)
    all_rows = [snap("MKT-A", "Outcome A"), snap("MKT-B", "Outcome B")]
    current = list(all_rows)

    class MutableAdapter:
        def iter_snapshots(self):
            yield from current

    monkeypatch.setitem(ingest.ADAPTERS, "kalshi", MutableAdapter)

    first = ingest.ingest_source("kalshi")
    assert first["presence"]["seen"] == 2

    current[:] = [all_rows[0]]
    miss1 = ingest.ingest_source("kalshi")
    assert miss1["presence"]["missed"] == 1
    assert miss1["presence"]["deactivated"] == 0

    miss2 = ingest.ingest_source("kalshi")
    assert miss2["presence"]["missed"] == 1
    assert miss2["presence"]["deactivated"] == 0

    miss3 = ingest.ingest_source("kalshi")
    assert miss3["presence"]["missed"] == 1
    assert miss3["presence"]["deactivated"] == 1

    current[:] = list(all_rows)
    reappeared = ingest.ingest_source("kalshi")
    assert reappeared["presence"]["reactivated"] == 1

    con = db.connect()
    try:
        state = con.execute("""
            SELECT p.listing_state,p.consecutive_misses,c.active
            FROM er_contract_presence_state p
            JOIN er_source_contracts c USING(source_contract_id)
            WHERE c.source_market_id='MKT-B'
        """).fetchone()
        assert state == ("observed", 0, True)
        transitions = con.execute("""
            SELECT transition_type,count(*)
            FROM er_contract_presence_transitions
            GROUP BY transition_type
            ORDER BY transition_type
        """).fetchall()
        counts = dict(transitions)
        assert counts.get("first_miss") == 1
        assert counts.get("deactivated") == 1
        assert counts.get("reactivated") == 1
    finally:
        con.close()


def test_v8_methodologies_are_registered(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    con = db.connect()
    try:
        assert con.execute(
            "SELECT count(*) FROM er_methodology_registry WHERE methodology_id='source_ingestion' AND methodology_version='change_data_capture_v8_direct_temp'"
        ).fetchone()[0] == 1
        assert con.execute(
            "SELECT count(*) FROM er_methodology_registry WHERE methodology_id='presence_reconciliation' AND methodology_version='presence_reconciliation_v2_set_based'"
        ).fetchone()[0] == 1
        assert con.execute(
            "SELECT count(*) FROM er_schema_migrations WHERE migration_name='institutional_schema_028.sql'"
        ).fetchone()[0] == 1
    finally:
        con.close()
