import os
from datetime import datetime, timezone

import db


def setup_database(monkeypatch, tmp_path):
    monkeypatch.setattr(db, "DB", str(tmp_path / "mapping-v7.duckdb"))
    os.environ["DB_FORCE_READ_WRITE"] = "true"
    from institutional_migrations_v2 import ensure_schema
    ensure_schema()


def row(contract_id, source_id, title, rules, close, version=1):
    return {
        "source_contract_id": contract_id,
        "source_id": source_id,
        "current_title": title,
        "current_resolution_rules": rules,
        "closes_at": close,
        "resolves_at": close,
        "semantic_version_no": version,
        "semantic_received_at": close,
    }


def test_v7_pair_filter_matches_v6_blocking_semantics(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    import institutional_mapping_v5 as v5
    import institutional_mapping_v7 as v7

    close = datetime(2027, 12, 31, tzinfo=timezone.utc)
    rows = [
        row("k-a", "kalshi", "Will China blockade Taiwan before 2028?", "YES if blockade begins.", close),
        row("p-a", "polymarket", "Will China blockade Taiwan before 2028?", "YES if blockade begins.", close),
        row("p-b", "polymarket", "Will China blockade Taiwan shipping before 2028?", "YES if shipping stops.", close),
        row("k-b", "kalshi", "Will US CPI exceed 3 percent in 2027?", "YES if CPI exceeds 3 percent.", close),
        row("p-c", "polymarket", "Will US CPI exceed 4 percent in 2027?", "YES if CPI exceeds 4 percent.", close),
        row("p-d", "polymarket", "Will Taiwan win a baseball game?", "YES if Taiwan wins.", close),
    ]
    con = db.connect()
    try:
        v5._upsert_index_batch(con, rows)
        original_pairs, original_stats = v5._candidate_pair_rows(con, rows)
        optimized_pairs, optimized_stats = v7._candidate_pair_rows(con, rows)
    finally:
        con.close()

    original = {(r["left_id"], r["right_id"]) for r in original_pairs}
    optimized = {(r["left_id"], r["right_id"]) for r in optimized_pairs}
    assert optimized == original
    assert optimized_stats["pairs_blocked"] == original_stats["pairs_blocked"]
    assert optimized_stats["pairs_evaluated"] == original_stats["pairs_evaluated"]
    assert optimized_stats["overlap_filter"] == "python_meaningful_tokens_v1"


def test_v7_preserves_v6_exact_merge_and_incremental_second_pass(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    from institutional_mapping_v7 import run_mapping_pass

    received = datetime(2026, 10, 14, 20, 0, tzinfo=timezone.utc)
    close = datetime(2027, 12, 31, tzinfo=timezone.utc)
    title = "Will China blockade Taiwan before 2028?"
    rules = "Resolves YES if China begins a blockade of Taiwan before 2028."
    con = db.connect()
    try:
        for contract_id, source_id, market_id in [
            ("sc-k", "kalshi", "KX-TW"),
            ("sc-p", "polymarket", "PM-TW"),
        ]:
            event_raw_id = f"raw-event-{contract_id}"
            market_raw_id = f"raw-market-{contract_id}"
            con.execute("""
                INSERT INTO er_sources(source_id,source_name,source_type,active)
                VALUES (?,?, 'prediction_market', TRUE) ON CONFLICT(source_id) DO NOTHING
            """, [source_id, source_id.title()])
            con.execute("""
                INSERT INTO er_raw_snapshots(
                  raw_snapshot_id,source_id,source_object_type,source_object_id,source_observed_at,
                  received_at,content_hash,payload_json,ingestion_run_id)
                VALUES
                  (?,?,'event',?,?,?,NULL,'{}','test-run'),
                  (?,?,'market',?,?,?,NULL,'{}','test-run')
            """, [event_raw_id, source_id, f"event-{market_id}", received, received,
                  market_raw_id, source_id, market_id, received, received])
            con.execute("""
                INSERT INTO er_source_contracts(
                  source_contract_id,source_id,source_market_id,current_title,current_description,status,
                  closes_at,resolves_at,current_resolution_rules,first_seen_at,last_seen_at,active)
                VALUES (?,?,?,?,?,'active',?,?,?,?,?,TRUE)
            """, [contract_id, source_id, market_id, title, title, close, close, rules, received, received])
            con.execute("""
                INSERT INTO er_contract_semantic_versions(
                  source_contract_id,version_no,source_id,source_market_id,title,description,outcome_type,status,
                  closes_at,resolves_at,resolution_rules,received_at,valid_from,event_raw_snapshot_id,
                  market_raw_snapshot_id,metadata_completeness)
                VALUES (?,1,?,?,?,?, 'binary','active',?,?,?,?,?,?,?,'full_v2')
            """, [contract_id, source_id, market_id, title, title, close, close, rules,
                  received, received, event_raw_id, market_raw_id])
    finally:
        con.close()

    first = run_mapping_pass()
    assert first["method_version"] == "mapping_rules_v7_python_overlap"
    assert first["blocking_execution"] == "python_overlap_v1"
    assert first["contracts_changed"] == 2
    assert first["candidate_pairs_evaluated"] >= 1
    assert first["auto_exact_mappings"] == 1

    second = run_mapping_pass()
    assert second["contracts_changed"] == 0
    assert second["candidate_pairs_evaluated"] == 0
    assert second["candidates_generated"] == 0
    assert second["auto_exact_mappings"] == 0
