from __future__ import annotations

import os
from datetime import datetime, timezone

import db


def setup_current_database(monkeypatch, tmp_path):
    path = tmp_path / "institutional_current.duckdb"
    monkeypatch.setattr(db, "DB", str(path))
    os.environ["DB_FORCE_READ_WRITE"] = "true"
    from institutional_migrations import MIGRATION_FILES, ensure_schema
    state = ensure_schema()
    assert state["migration_count"] == len(MIGRATION_FILES)
    return path


def snapshot(source, market_id, title, close, *, rules=None, bid=0.20, ask=0.22):
    from institutional_ingest_v2 import ContractSnapshot
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
        resolution_rules=rules or f"Resolves YES if: {title}",
        source_url=None,
        observed_at=datetime(2026, 10, 14, 17, 0, tzinfo=timezone.utc),
        yes_bid=bid,
        yes_ask=ask,
        no_bid=None,
        no_ask=None,
        last_trade=(bid + ask) / 2,
        displayed_probability=(bid + ask) / 2,
        volume=1000.0,
        volume_24h=100.0,
        open_interest=500.0,
        liquidity=200.0,
        orderbook_depth=None,
        raw_event={"id": f"event-{market_id}", "title": title},
        raw_market={"id": market_id, "question": title, "bestBid": bid, "bestAsk": ask},
    )


def persist(con, snap, run_id):
    from institutional_ingest_v2 import (
        ensure_sources,
        insert_observation,
        insert_raw,
        link_contract_version_evidence,
        upsert_contract,
    )
    ensure_sources(con)
    full_run_id = f"{run_id}-{snap.source_id}"
    con.execute(
        "INSERT INTO er_ingestion_runs(run_id,source_id,started_at,status) VALUES (?,?,now(),'running')",
        [full_run_id, snap.source_id],
    )
    event_raw = insert_raw(
        con, snap.source_id, "event", snap.source_event_id, snap.raw_event, snap.observed_at, full_run_id
    )
    market_raw = insert_raw(
        con, snap.source_id, "market", snap.source_market_id, snap.raw_market, snap.observed_at, full_run_id
    )
    contract_id = upsert_contract(con, snap, market_raw)
    link_contract_version_evidence(con, contract_id, event_raw, market_raw)
    insert_observation(con, snap, contract_id, market_raw)
    return contract_id


def test_exact_merge_supersedes_orphan_and_preserves_point_in_time_curve(monkeypatch, tmp_path):
    setup_current_database(monkeypatch, tmp_path)
    from institutional_mapping_v3 import run_mapping_pass

    close = datetime(2027, 12, 31, tzinfo=timezone.utc)
    title = "Will China blockade Taiwan before 2028?"
    con = db.connect()
    try:
        left_id = persist(con, snapshot("kalshi", "KX-TAIWAN", title, close), "r1")
        right_id = persist(con, snapshot("polymarket", "PM-TAIWAN", title, close), "r1")
    finally:
        con.close()

    result = run_mapping_pass()
    assert result["auto_exact_mappings"] == 1
    assert result["active_orphan_events"] == 0

    con = db.connect()
    try:
        mappings = con.execute(
            "SELECT source_contract_id,canonical_event_id FROM er_latest_contract_mappings WHERE source_contract_id IN (?,?) ORDER BY source_contract_id",
            [left_id, right_id],
        ).fetchall()
        assert len(mappings) == 2
        target_event = mappings[0][1]
        assert {row[1] for row in mappings} == {target_event}

        superseded = con.execute(
            "SELECT canonical_event_id,status FROM er_canonical_events WHERE status='superseded'"
        ).fetchall()
        assert len(superseded) == 1
        superseded_event = superseded[0][0]
        assert superseded_event != target_event

        served = con.execute(
            "SELECT canonical_event_id,active_contract_count,source_count FROM er_served_canonical_events"
        ).fetchall()
        assert served == [(target_event, 2, 2)]

        current_target = con.execute(
            "SELECT count(*) FROM er_event_curve_current_taxonomy WHERE canonical_event_id=?",
            [target_event],
        ).fetchone()[0]
        assert current_target == 2

        as_known_target = con.execute(
            "SELECT count(*) FROM er_event_curve_as_known WHERE canonical_event_id=?",
            [target_event],
        ).fetchone()[0]
        as_known_superseded = con.execute(
            "SELECT count(*) FROM er_event_curve_as_known WHERE canonical_event_id=?",
            [superseded_event],
        ).fetchone()[0]
        assert as_known_target == 1
        assert as_known_superseded == 1
    finally:
        con.close()


def test_reviewed_relationship_does_not_merge_and_has_evidence(monkeypatch, tmp_path):
    setup_current_database(monkeypatch, tmp_path)
    from institutional_mapping_v3 import review_candidate, run_mapping_pass

    close = datetime(2027, 12, 31, tzinfo=timezone.utc)
    con = db.connect()
    try:
        left_id = persist(
            con,
            snapshot(
                "kalshi", "KX-REL", "Will China blockade Taiwan before 2028?", close,
                rules="YES only if commercial shipping is physically prevented for at least 24 hours.",
            ),
            "r2",
        )
        right_id = persist(
            con,
            snapshot(
                "polymarket", "PM-REL", "Will China blockade or quarantine Taiwan before 2028?", close,
                rules="YES if China formally announces a blockade or quarantine of Taiwan.",
            ),
            "r2",
        )
    finally:
        con.close()

    run_mapping_pass()
    con = db.connect()
    try:
        candidate = con.execute(
            """SELECT candidate_id FROM er_mapping_candidates
               WHERE left_source_contract_id IN (?,?) AND right_source_contract_id IN (?,?)
                 AND status='pending' ORDER BY overall_confidence DESC LIMIT 1""",
            [left_id, right_id, left_id, right_id],
        ).fetchone()
        assert candidate is not None
        candidate_id = candidate[0]
        before = con.execute(
            "SELECT canonical_event_id FROM er_latest_contract_mappings WHERE source_contract_id IN (?,?)",
            [left_id, right_id],
        ).fetchall()
        assert len({r[0] for r in before}) == 2
    finally:
        con.close()

    reviewed = review_candidate(
        candidate_id,
        "relationship",
        "test-reviewer",
        "Definitions overlap but are not identical.",
        relationship_type="overlaps_with",
        direction="undirected",
    )
    assert reviewed["relationship_id"]

    con = db.connect()
    try:
        after = con.execute(
            "SELECT canonical_event_id FROM er_latest_contract_mappings WHERE source_contract_id IN (?,?)",
            [left_id, right_id],
        ).fetchall()
        assert len({r[0] for r in after}) == 2

        rel = con.execute(
            """SELECT relationship_type,relationship_class,human_reviewed,review_status,
                      source_candidate_id,created_by
               FROM er_event_relationships WHERE relationship_id=?""",
            [reviewed["relationship_id"]],
        ).fetchone()
        assert rel == (
            "overlaps_with", "reviewed_relationship", True, "reviewed", candidate_id, "test-reviewer"
        )
        evidence_count = con.execute(
            "SELECT count(*) FROM er_event_relationship_evidence WHERE relationship_id=?",
            [reviewed["relationship_id"]],
        ).fetchone()[0]
        assert evidence_count == 2
    finally:
        con.close()


def test_v2_qa_and_readiness_cover_latest_schema(monkeypatch, tmp_path):
    setup_current_database(monkeypatch, tmp_path)
    from institutional_migrations import MIGRATION_FILES
    from institutional_qa_v2 import run_qa
    from institutional_readiness_v2 import evaluate_readiness

    qa = run_qa(stale_minutes=10**7)
    assert qa["checks"]["migration_ledger"] == 0
    assert qa["qa_version"] == "institutional_qa_v2"

    readiness = evaluate_readiness(stale_minutes=10**7, require_archive=False)
    migration = next(c for c in readiness["controls"] if c["name"] == "migration_ledger_current")
    assert migration["passed"] is True
    assert migration["observed"] == len(MIGRATION_FILES)
    assert readiness["policy"]["readiness_version"] == "institutional_readiness_v2"
