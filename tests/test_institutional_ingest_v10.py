import os
from datetime import datetime, timezone

import pytest

import db


def setup_database(monkeypatch, tmp_path):
    monkeypatch.setattr(db, "DB", str(tmp_path / "cdc-v10.duckdb"))
    os.environ["DB_FORCE_READ_WRITE"] = "true"
    os.environ["EVENTRISK_COLLECTION_SCOPE"] = "full"
    from institutional_migrations_v2 import ensure_schema
    ensure_schema()


def snap(market_id="MKT-A", *, bid=0.20, ask=0.22, volume=1000.0, marker="a"):
    from institutional_ingest_v3 import ContractSnapshot
    close = datetime(2027, 12, 31, tzinfo=timezone.utc)
    midpoint = (bid + ask) / 2
    return ContractSnapshot(
        source_id="kalshi", source_market_id=market_id, source_event_id="evt-shared",
        title="Will Outcome A happen?", description="Description A", status="active",
        opens_at=datetime(2026, 1, 1, tzinfo=timezone.utc), closes_at=close, resolves_at=close,
        resolution_authority="test", resolution_rules="YES if Outcome A happens", source_url=None,
        observed_at=datetime(2026, 10, 14, 21, 0, tzinfo=timezone.utc),
        yes_bid=bid, yes_ask=ask, no_bid=None, no_ask=None, last_trade=midpoint,
        displayed_probability=midpoint, volume=volume, volume_24h=100.0,
        open_interest=500.0, liquidity=200.0, orderbook_depth=None,
        raw_event={"id": "evt-shared", "title": "Shared event", "marker": "stable-event"},
        raw_market={"id": market_id, "question": "Outcome A", "bestBid": bid, "bestAsk": ask, "volume": volume, "marker": marker},
    )


def test_v10_identical_poll_uses_current_pointers_without_duplicate_history(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    import institutional_ingest_v10 as ingest

    rows = [snap()]
    class FakeAdapter:
        def iter_snapshots(self):
            yield from rows
    monkeypatch.setitem(ingest.ADAPTERS, "kalshi", FakeAdapter)
    monkeypatch.setattr(ingest, "CHUNK_ROWS", 1)

    first = ingest.ingest_source("kalshi")
    second = ingest.ingest_source("kalshi")

    assert first["raw_rows_written"] == 2
    assert first["observation_rows_written"] == 1
    assert second["raw_rows_written"] == 0
    assert second["raw_rows_reused"] == 2
    assert second["observation_rows_written"] == 0
    assert second["flat_observations_suppressed"] == 1
    assert second["memory_strategy"] == "streamed_source_state_pointer_cdc_staged_presence"

    con = db.connect()
    try:
        assert con.execute("SELECT count(*) FROM er_raw_snapshots").fetchone()[0] == 2
        assert con.execute("SELECT count(*) FROM er_market_observations").fetchone()[0] == 1
        assert con.execute("SELECT count(*) FROM er_raw_current_snapshots").fetchone()[0] == 2
        assert con.execute("SELECT count(*) FROM er_market_current_state").fetchone()[0] == 1
        current_run = con.execute(
            "SELECT last_ingestion_run_id FROM er_market_current_state WHERE source_contract_id IN (SELECT source_contract_id FROM er_source_contracts WHERE source_market_id='MKT-A')"
        ).fetchone()[0]
        assert current_run == second["run_id"]
    finally:
        con.close()


def test_v10_changed_market_updates_pointer_and_appends_one_change_point(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    import institutional_ingest_v10 as ingest

    rows = [snap(marker="v1")]
    class FakeAdapter:
        def iter_snapshots(self):
            yield from rows
    monkeypatch.setitem(ingest.ADAPTERS, "kalshi", FakeAdapter)
    monkeypatch.setattr(ingest, "CHUNK_ROWS", 1)

    first = ingest.ingest_source("kalshi")
    con = db.connect()
    try:
        old_market_raw = con.execute(
            "SELECT raw_snapshot_id FROM er_raw_current_snapshots WHERE source_object_type='market'"
        ).fetchone()[0]
    finally:
        con.close()

    rows[:] = [snap(bid=0.30, ask=0.32, volume=1200.0, marker="v2")]
    second = ingest.ingest_source("kalshi")
    assert second["raw_rows_written"] == 1  # event unchanged, market payload changed
    assert second["observation_rows_written"] == 1

    con = db.connect()
    try:
        new_market_raw = con.execute(
            "SELECT raw_snapshot_id FROM er_raw_current_snapshots WHERE source_object_type='market'"
        ).fetchone()[0]
        assert new_market_raw != old_market_raw
        state = con.execute(
            "SELECT yes_bid,yes_ask,volume,raw_snapshot_id,last_ingestion_run_id FROM er_market_current_state"
        ).fetchone()
        assert state[:3] == (0.30, 0.32, 1200.0)
        assert state[3] == new_market_raw
        assert state[4] == second["run_id"]
        assert con.execute("SELECT count(*) FROM er_market_observations").fetchone()[0] == 2
    finally:
        con.close()


def test_v10_pointer_updates_roll_back_with_source_transaction(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    import institutional_ingest_v10 as ingest

    rows = [snap(marker="v1")]
    class FakeAdapter:
        def iter_snapshots(self):
            yield from rows
    monkeypatch.setitem(ingest.ADAPTERS, "kalshi", FakeAdapter)
    monkeypatch.setattr(ingest, "CHUNK_ROWS", 1)
    ingest.ingest_source("kalshi")

    con = db.connect()
    try:
        before = con.execute(
            "SELECT raw_snapshot_id,content_hash,last_ingestion_run_id FROM er_raw_current_snapshots WHERE source_object_type='market'"
        ).fetchone()
        obs_before = con.execute("SELECT count(*) FROM er_market_observations").fetchone()[0]
    finally:
        con.close()

    rows[:] = [snap(bid=0.40, ask=0.42, marker="v2")]
    original = ingest.persist_registered_batch
    def fail_after_pointer_update(*args, **kwargs):
        raise RuntimeError("synthetic persistence failure after pointer update")
    monkeypatch.setattr(ingest, "persist_registered_batch", fail_after_pointer_update)

    with pytest.raises(RuntimeError, match="synthetic persistence failure"):
        ingest.ingest_source("kalshi")

    con = db.connect()
    try:
        after = con.execute(
            "SELECT raw_snapshot_id,content_hash,last_ingestion_run_id FROM er_raw_current_snapshots WHERE source_object_type='market'"
        ).fetchone()
        assert after == before
        assert con.execute("SELECT count(*) FROM er_market_observations").fetchone()[0] == obs_before
        assert con.execute("SELECT yes_bid FROM er_market_current_state").fetchone()[0] == 0.20
    finally:
        con.close()
    monkeypatch.setattr(ingest, "persist_registered_batch", original)


def test_schema33_backfills_current_pointers_from_existing_history(monkeypatch, tmp_path):
    monkeypatch.setattr(db, "DB", str(tmp_path / "pointer-backfill.duckdb"))
    os.environ["DB_FORCE_READ_WRITE"] = "true"
    import institutional_migrations_v2 as migrations
    original_files = list(migrations.EXTRA_MIGRATION_FILES)
    monkeypatch.setattr(migrations, "EXTRA_MIGRATION_FILES", original_files[:-1])
    migrations.ensure_schema()

    con = db.connect()
    try:
        con.execute("INSERT INTO er_sources(source_id,source_name,source_type) VALUES ('kalshi','Kalshi','prediction_market') ON CONFLICT DO NOTHING")
        con.execute("""
            INSERT INTO er_source_contracts(source_contract_id,source_id,source_market_id,current_title,first_seen_at,last_seen_at)
            VALUES ('sc-a','kalshi','MKT-A','A',now(),now())
        """)
        con.execute("""
            INSERT INTO er_raw_snapshots(raw_snapshot_id,source_id,source_object_type,source_object_id,received_at,content_hash,payload_json,ingestion_run_id)
            VALUES ('raw-old','kalshi','market','MKT-A',TIMESTAMPTZ '2026-10-14 20:00:00+00','h1','{}','r1'),
                   ('raw-new','kalshi','market','MKT-A',TIMESTAMPTZ '2026-10-14 21:00:00+00','h2','{}','r2')
        """)
        con.execute("""
            INSERT INTO er_market_observations(source_id,source_contract_id,observed_at,received_at,yes_bid,yes_ask,raw_snapshot_id)
            VALUES ('kalshi','sc-a',TIMESTAMPTZ '2026-10-14 20:00:00+00',TIMESTAMPTZ '2026-10-14 20:00:00+00',0.1,0.2,'raw-old'),
                   ('kalshi','sc-a',TIMESTAMPTZ '2026-10-14 21:00:00+00',TIMESTAMPTZ '2026-10-14 21:00:00+00',0.3,0.4,'raw-new')
        """)
    finally:
        con.close()

    monkeypatch.setattr(migrations, "EXTRA_MIGRATION_FILES", original_files)
    migrations.ensure_schema()
    con = db.connect()
    try:
        assert con.execute(
            "SELECT raw_snapshot_id,content_hash FROM er_raw_current_snapshots WHERE source_object_id='MKT-A'"
        ).fetchone() == ("raw-new", "h2")
        assert con.execute(
            "SELECT yes_bid,yes_ask,raw_snapshot_id FROM er_market_current_state WHERE source_contract_id='sc-a'"
        ).fetchone() == (0.3, 0.4, "raw-new")
        assert con.execute(
            "SELECT count(*) FROM er_schema_migrations WHERE migration_name='institutional_schema_033.sql'"
        ).fetchone()[0] == 1
    finally:
        con.close()
