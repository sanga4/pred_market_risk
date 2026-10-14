import os
from pathlib import Path

import db


def setup_database(tmp_path):
    db.DB = str(tmp_path / "institutional_test.duckdb")
    os.environ["DB_FORCE_READ_WRITE"] = "true"
    root = Path(__file__).resolve().parents[1]
    con = db.connect()
    try:
        con.execute((root / "institutional_schema.sql").read_text())
        con.execute((root / "institutional_schema_002.sql").read_text())
    finally:
        con.close()
    return db.DB


def snapshot(source, market_id, title, close, bid=0.2, ask=0.22):
    from institutional_ingest import ContractSnapshot
    from datetime import datetime, timezone
    return ContractSnapshot(
        source_id=source,
        source_market_id=market_id,
        source_event_id=f"event-{market_id}",
        title=title,
        description=f"Description for {title}",
        status="active",
        opens_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        closes_at=close,
        resolves_at=close,
        resolution_authority="test-source",
        resolution_rules=f"Resolves YES if: {title}",
        source_url=None,
        observed_at=datetime(2026, 10, 14, 17, 0, tzinfo=timezone.utc),
        yes_bid=bid,
        yes_ask=ask,
        no_bid=None,
        no_ask=None,
        last_trade=0.21,
        displayed_probability=0.21,
        volume=1000.0,
        volume_24h=100.0,
        open_interest=500.0,
        liquidity=200.0,
        orderbook_depth=None,
        raw_event={"id": f"event-{market_id}", "title": title},
        raw_market={"id": market_id, "question": title, "bestBid": bid, "bestAsk": ask},
    )


def persist_snapshot(con, snap, run_id="test-run"):
    from institutional_ingest import ensure_sources, insert_observation, insert_raw, upsert_contract
    ensure_sources(con)
    con.execute(
        "INSERT INTO er_ingestion_runs(run_id,source_id,started_at,status) VALUES (?,?,now(),'running') ON CONFLICT(run_id) DO NOTHING",
        [run_id + snap.source_id, snap.source_id],
    )
    raw_id = insert_raw(con, snap.source_id, "market", snap.source_market_id, snap.raw_market, snap.observed_at, run_id + snap.source_id)
    contract_id = upsert_contract(con, snap, raw_id)
    insert_observation(con, snap, contract_id, raw_id)
    return contract_id


def test_raw_lineage_versioning_mapping_graph_and_qa(tmp_path):
    setup_database(tmp_path)
    from datetime import datetime, timezone
    from institutional_graph import run_graph_pass
    from institutional_mapping import run_mapping_pass
    from institutional_qa import run_qa

    close = datetime(2027, 12, 31, tzinfo=timezone.utc)
    con = db.connect()
    try:
        left = snapshot("kalshi", "KX-TAIWAN", "Will China blockade Taiwan before 2028?", close)
        right = snapshot("polymarket", "PM-TAIWAN", "Will China blockade Taiwan before 2028?", close, 0.19, 0.23)
        left_id = persist_snapshot(con, left)
        right_id = persist_snapshot(con, right)

        # A metadata change creates a new version rather than rewriting version 1.
        left_changed = snapshot("kalshi", "KX-TAIWAN", "Will China blockade Taiwan before 2028?", close)
        left_changed.description = "Updated source description"
        persist_snapshot(con, left_changed, "test-run-2")
        versions = con.execute(
            "SELECT version_no,valid_to FROM er_source_contract_versions WHERE source_contract_id=? ORDER BY version_no",
            [left_id],
        ).fetchall()
        assert len(versions) == 2
        assert versions[0][1] is not None
        assert versions[1][1] is None

        # Every observation is traceable to a raw snapshot.
        missing = con.execute(
            """SELECT count(*) FROM er_market_observations o
               LEFT JOIN er_raw_snapshots r ON r.raw_snapshot_id=o.raw_snapshot_id
               WHERE r.raw_snapshot_id IS NULL"""
        ).fetchone()[0]
        assert missing == 0
    finally:
        con.close()

    mapping = run_mapping_pass()
    assert mapping["native_events_created"] >= 2
    assert mapping["auto_exact_mappings"] >= 1

    con = db.connect()
    try:
        mapped = con.execute(
            "SELECT canonical_event_id FROM er_latest_contract_mappings WHERE source_contract_id IN (?,?)",
            [left_id, right_id],
        ).fetchall()
        assert len(mapped) == 2
        assert len({r[0] for r in mapped}) == 1
    finally:
        con.close()

    graph = run_graph_pass()
    assert graph["mention_edges_considered"] >= 2
    con = db.connect()
    try:
        edge_types = {r[0] for r in con.execute("SELECT DISTINCT relationship_type FROM er_graph_edges").fetchall()}
        edge_classes = {r[0] for r in con.execute("SELECT DISTINCT edge_class FROM er_graph_edges").fetchall()}
        assert "mentions" in edge_types
        assert "source_asserted" in edge_classes
        assert "inferred" not in edge_classes
    finally:
        con.close()

    qa = run_qa(stale_minutes=10**7)
    assert qa["critical_issues"] == 0


def test_similar_but_not_identical_contracts_are_not_silently_merged(tmp_path):
    setup_database(tmp_path)
    from datetime import datetime, timezone
    from institutional_mapping import run_mapping_pass

    con = db.connect()
    try:
        a = snapshot("kalshi", "KX-A", "Will China blockade Taiwan before 2028?", datetime(2027, 12, 31, tzinfo=timezone.utc))
        b = snapshot("polymarket", "PM-B", "Will China invade or blockade Taiwan before 2028?", datetime(2027, 12, 31, tzinfo=timezone.utc))
        a_id = persist_snapshot(con, a)
        b_id = persist_snapshot(con, b)
    finally:
        con.close()

    run_mapping_pass()
    con = db.connect()
    try:
        mappings = con.execute(
            "SELECT source_contract_id,canonical_event_id FROM er_latest_contract_mappings WHERE source_contract_id IN (?,?)",
            [a_id, b_id],
        ).fetchall()
        assert len({m[1] for m in mappings}) == 2
        pending = con.execute("SELECT count(*) FROM er_mapping_candidates WHERE status='pending'").fetchone()[0]
        assert pending >= 1
    finally:
        con.close()
