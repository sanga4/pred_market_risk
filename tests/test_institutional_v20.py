from __future__ import annotations

import os
from datetime import datetime, timezone

import db


def setup_db(monkeypatch, tmp_path):
    path = tmp_path / "institutional_v20.duckdb"
    monkeypatch.setattr(db, "DB", str(path))
    os.environ["DB_FORCE_READ_WRITE"] = "true"
    from institutional_migrations import MIGRATION_FILES, ensure_schema
    state = ensure_schema()
    assert state["migration_count"] == len(MIGRATION_FILES)
    return path


def snap(source: str, market_id: str, title: str, rules: str):
    from institutional_ingest_v3 import ContractSnapshot
    close = datetime(2027, 12, 31, tzinfo=timezone.utc)
    return ContractSnapshot(
        source_id=source,
        source_market_id=market_id,
        source_event_id=f"event-{market_id}",
        title=title,
        description=f"Description: {title}",
        status="active",
        opens_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        closes_at=close,
        resolves_at=close,
        resolution_authority="test-source",
        resolution_rules=rules,
        source_url=f"https://example.test/{market_id}",
        observed_at=datetime(2026, 10, 14, 18, 0, tzinfo=timezone.utc),
        yes_bid=0.20,
        yes_ask=0.22,
        no_bid=None,
        no_ask=None,
        last_trade=0.21,
        displayed_probability=0.21,
        volume=1000.0,
        volume_24h=100.0,
        open_interest=500.0,
        liquidity=200.0,
        orderbook_depth=None,
        raw_event={"id": f"event-{market_id}", "title": title, "rules": rules},
        raw_market={"id": market_id, "question": title, "rules": rules, "bestBid": 0.20, "bestAsk": 0.22},
    )


def persist_semantic(con, snapshot, run_id):
    from institutional_ingest_v3 import (
        ensure_sources,
        insert_observation,
        insert_raw,
        link_contract_version_evidence,
        sync_semantic_contract_version,
        upsert_contract,
    )
    ensure_sources(con)
    full_run_id = f"{run_id}-{snapshot.source_id}"
    if not con.execute("SELECT 1 FROM er_ingestion_runs WHERE run_id=?", [full_run_id]).fetchone():
        con.execute(
            "INSERT INTO er_ingestion_runs(run_id,source_id,started_at,status) VALUES (?,?,now(),'running')",
            [full_run_id, snapshot.source_id],
        )
    event_raw = insert_raw(
        con,
        snapshot.source_id,
        "event",
        snapshot.source_event_id,
        snapshot.raw_event,
        snapshot.observed_at,
        full_run_id,
    )
    market_raw = insert_raw(
        con,
        snapshot.source_id,
        "market",
        snapshot.source_market_id,
        snapshot.raw_market,
        snapshot.observed_at,
        full_run_id,
    )
    contract_id = upsert_contract(con, snapshot, market_raw)
    link_contract_version_evidence(con, contract_id, event_raw, market_raw)
    semantic = sync_semantic_contract_version(con, snapshot, contract_id, event_raw, market_raw)
    insert_observation(con, snapshot, contract_id, market_raw)
    return contract_id, event_raw, market_raw, semantic


def test_full_semantic_ledger_drives_versioned_graph(monkeypatch, tmp_path):
    setup_db(monkeypatch, tmp_path)
    from institutional_graph_v2 import run_graph_pass
    from institutional_mapping_v3 import run_mapping_pass
    from institutional_reference_versions import sync_reference_versions

    first = snap(
        "kalshi",
        "KX-GRAPH",
        "Will China blockade Taiwan before 2028?",
        "YES if China prevents commercial shipping to Taiwan for at least 24 hours.",
    )
    con = db.connect()
    try:
        contract_id, event_raw, market_raw, semantic = persist_semantic(con, first, "graph1")
        assert semantic["changed"] is True
    finally:
        con.close()

    run_mapping_pass()
    graph_result = run_graph_pass()
    sync_reference_versions()
    assert graph_result["graph_version"] == "graph_rules_v2_versioned_mentions"

    con = db.connect()
    try:
        version = con.execute(
            """SELECT metadata_completeness,event_raw_snapshot_id,market_raw_snapshot_id
               FROM er_latest_contract_semantic_versions WHERE source_contract_id=?""",
            [contract_id],
        ).fetchone()
        assert version == ("full_v2", event_raw, market_raw)

        edge = con.execute(
            """SELECT edge_id,valid_to FROM er_graph_edges
               WHERE method_version='graph_rules_v2_versioned_mentions'
                 AND relationship_type='mentions' AND target_node_id='geo_taiwan'
                 AND valid_to IS NULL"""
        ).fetchone()
        assert edge is not None
        evidence = con.execute(
            "SELECT count(*) FROM er_edge_evidence WHERE edge_id=?",
            [edge[0]],
        ).fetchone()[0]
        assert evidence == 2
    finally:
        con.close()

    second = snap(
        "kalshi",
        "KX-GRAPH",
        "Will China impose new export rules before 2028?",
        "YES if China announces new export licensing rules before 2028.",
    )
    con = db.connect()
    try:
        _, _, _, semantic2 = persist_semantic(con, second, "graph2")
        assert semantic2["changed"] is True
    finally:
        con.close()

    run_graph_pass()
    con = db.connect()
    try:
        current_taiwan_edges = con.execute(
            """SELECT count(*) FROM er_graph_edges
               WHERE method_version='graph_rules_v2_versioned_mentions'
                 AND relationship_type='mentions' AND target_node_id='geo_taiwan'
                 AND valid_to IS NULL"""
        ).fetchone()[0]
        assert current_taiwan_edges == 0
        retired_taiwan_edges = con.execute(
            """SELECT count(*) FROM er_graph_edges
               WHERE method_version='graph_rules_v2_versioned_mentions'
                 AND relationship_type='mentions' AND target_node_id='geo_taiwan'
                 AND valid_to IS NOT NULL"""
        ).fetchone()[0]
        assert retired_taiwan_edges >= 1
    finally:
        con.close()


def test_post_decision_contract_change_opens_mapping_drift_issue(monkeypatch, tmp_path):
    setup_db(monkeypatch, tmp_path)
    from institutional_mapping_drift import persist_mapping_drift
    from institutional_mapping_v3 import run_mapping_pass

    title = "Will China blockade Taiwan before 2028?"
    rules = "YES if China prevents commercial shipping to Taiwan for at least 24 hours."
    con = db.connect()
    try:
        left_id, *_ = persist_semantic(con, snap("kalshi", "KX-DRIFT", title, rules), "drift1")
        right_id, *_ = persist_semantic(con, snap("polymarket", "PM-DRIFT", title, rules), "drift1")
    finally:
        con.close()

    merged = run_mapping_pass()
    assert merged["auto_exact_mappings"] == 1

    changed = snap(
        "polymarket",
        "PM-DRIFT",
        "Will China blockade or quarantine Taiwan before 2028?",
        "YES if China formally announces either a blockade or a quarantine of Taiwan.",
    )
    con = db.connect()
    try:
        changed_contract_id, *_ = persist_semantic(con, changed, "drift2")
        assert changed_contract_id == right_id
    finally:
        con.close()

    drift = persist_mapping_drift()
    assert drift["drifted_canonical_events"] == 1
    assert drift["incompatible_pairs"] >= 1

    con = db.connect()
    try:
        issue = con.execute(
            """SELECT severity,status FROM er_quality_issues
               WHERE rule_code='same_event_mapping_semantic_drift' AND status='open'"""
        ).fetchone()
        assert issue == ("critical", "open")
        current_events = con.execute(
            "SELECT count(DISTINCT canonical_event_id) FROM er_latest_contract_mappings WHERE source_contract_id IN (?,?)",
            [left_id, right_id],
        ).fetchone()[0]
        assert current_events == 1  # no automatic split; human review is required
    finally:
        con.close()


def test_every_bulk_release_query_binds_on_current_schema(monkeypatch, tmp_path):
    setup_db(monkeypatch, tmp_path)
    from institutional_exports_v3 import DATASETS, _params

    as_of = datetime(2026, 10, 14, 20, 0, tzinfo=timezone.utc)
    con = db.connect()
    try:
        for name, sql in DATASETS.items():
            try:
                con.execute(sql, _params(sql, as_of)).to_arrow_table()
            except Exception as exc:
                raise AssertionError(f"bulk dataset {name} failed to bind/execute: {exc}") from exc
    finally:
        con.close()


def test_readiness_requires_full_semantic_and_reference_coverage(monkeypatch, tmp_path):
    setup_db(monkeypatch, tmp_path)
    from institutional_mapping_v3 import run_mapping_pass
    from institutional_reference_versions import sync_reference_versions
    from institutional_readiness_v2 import evaluate_readiness

    con = db.connect()
    try:
        persist_semantic(
            con,
            snap(
                "kalshi",
                "KX-READY",
                "Will Taiwan hold an election before 2028?",
                "YES if the specified Taiwan election occurs before 2028.",
            ),
            "ready1",
        )
    finally:
        con.close()
    run_mapping_pass()
    sync_reference_versions()

    readiness = evaluate_readiness(stale_minutes=10**7, require_archive=False)
    semantic = next(c for c in readiness["controls"] if c["name"] == "active_contracts_have_full_semantic_versions")
    refs = next(c for c in readiness["controls"] if c["name"] == "reference_masters_versioned")
    assert semantic["passed"] is True
    assert refs["passed"] is True
