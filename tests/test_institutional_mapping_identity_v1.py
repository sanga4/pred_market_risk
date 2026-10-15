import os
from datetime import datetime, timezone

import db


def setup_database(monkeypatch, tmp_path):
    monkeypatch.setattr(db, "DB", str(tmp_path / "identity-map.duckdb"))
    os.environ["DB_FORCE_READ_WRITE"] = "true"
    from institutional_migrations_v2 import ensure_schema
    ensure_schema()


def seed_contract(con, contract_id, source_id, market_id, title, rules, close, received, version=1):
    event_raw_id = f"raw-event-{contract_id}-{version}"
    market_raw_id = f"raw-market-{contract_id}-{version}"
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
    """, [event_raw_id, source_id, f"event-{market_id}-{version}", received, received,
          market_raw_id, source_id, f"{market_id}-{version}", received, received])
    if version == 1:
        con.execute("""
            INSERT INTO er_source_contracts(
              source_contract_id,source_id,source_market_id,current_title,current_description,status,
              closes_at,resolves_at,current_resolution_rules,first_seen_at,last_seen_at,active)
            VALUES (?,?,?,?,?,'active',?,?,?,?,?,TRUE)
        """, [contract_id, source_id, market_id, title, title, close, close, rules, received, received])
    else:
        con.execute("""
            UPDATE er_source_contracts SET current_title=?,current_description=?,current_resolution_rules=?,
              closes_at=?,resolves_at=?,last_seen_at=? WHERE source_contract_id=?
        """, [title,title,rules,close,close,received,contract_id])
        con.execute(
            "UPDATE er_contract_semantic_versions SET valid_to=? WHERE source_contract_id=? AND valid_to IS NULL",
            [received, contract_id],
        )
    con.execute("""
        INSERT INTO er_contract_semantic_versions(
          source_contract_id,version_no,source_id,source_market_id,title,description,outcome_type,status,
          closes_at,resolves_at,resolution_rules,received_at,valid_from,event_raw_snapshot_id,
          market_raw_snapshot_id,metadata_completeness)
        VALUES (?,?,?,?,?,?, 'binary','active',?,?,?,?,?,?,?,'full_v2')
    """, [contract_id, version, source_id, market_id, title, title, close, close, rules,
          received, received, event_raw_id, market_raw_id])


def test_identity_lane_exact_merge_without_fuzzy_candidates(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    from institutional_mapping_identity_v1 import run_identity_pass

    received = datetime(2026, 10, 14, 20, 0, tzinfo=timezone.utc)
    close = datetime(2027, 12, 31, tzinfo=timezone.utc)
    exact_title = "Will China blockade Taiwan before 2028?"
    exact_rules = "Resolves YES if China begins a blockade of Taiwan before 2028."

    con = db.connect()
    try:
        seed_contract(con, "sc-k", "kalshi", "KX-TW", exact_title, exact_rules, close, received)
        seed_contract(con, "sc-p", "polymarket", "PM-TW", exact_title, exact_rules, close, received)
        seed_contract(
            con, "sc-fuzzy", "polymarket", "PM-TW-SHIPPING",
            "Will China blockade Taiwan shipping before 2028?",
            "Resolves YES if commercial shipping is halted.", close, received,
        )
    finally:
        con.close()

    result = run_identity_pass()
    assert result["contracts_changed"] == 3
    assert result["exact_pairs_evaluated"] == 1
    assert result["exact_mappings_accepted"] == 1
    assert result["method_version"] == "identity_mapping_v1_strict_exact"

    con = db.connect()
    try:
        exact_events = con.execute("""
            SELECT count(DISTINCT canonical_event_id)
            FROM er_latest_contract_mappings
            WHERE source_contract_id IN ('sc-k','sc-p')
        """).fetchone()[0]
        assert exact_events == 1
        fuzzy_event = con.execute(
            "SELECT canonical_event_id FROM er_latest_contract_mappings WHERE source_contract_id='sc-fuzzy'"
        ).fetchone()
        assert fuzzy_event is not None
        fuzzy_candidate_count = con.execute("""
            SELECT count(*) FROM er_mapping_candidates
            WHERE method_version='identity_mapping_v1_strict_exact'
              AND (left_source_contract_id='sc-fuzzy' OR right_source_contract_id='sc-fuzzy')
        """).fetchone()[0]
        assert fuzzy_candidate_count == 0
    finally:
        con.close()


def test_identity_watermark_reprocesses_only_semantic_change(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    from institutional_mapping_identity_v1 import run_identity_pass

    received = datetime(2026, 10, 14, 20, 0, tzinfo=timezone.utc)
    close = datetime(2027, 12, 31, tzinfo=timezone.utc)
    con = db.connect()
    try:
        seed_contract(con, "sc-a", "kalshi", "A", "Will A happen?", "YES if A happens.", close, received)
    finally:
        con.close()

    first = run_identity_pass()
    assert first["contracts_changed"] == 1
    warm = run_identity_pass()
    assert warm["contracts_changed"] == 0
    assert warm["exact_pairs_evaluated"] == 0

    changed_at = datetime(2026, 10, 14, 21, 0, tzinfo=timezone.utc)
    con = db.connect()
    try:
        seed_contract(
            con, "sc-a", "kalshi", "A", "Will A happen before 2028?",
            "YES if A happens before 2028.", close, changed_at, version=2,
        )
    finally:
        con.close()

    changed = run_identity_pass()
    assert changed["contracts_changed"] == 1
    con = db.connect()
    try:
        assert con.execute(
            "SELECT semantic_version_no FROM er_mapping_identity_state WHERE source_contract_id='sc-a'"
        ).fetchone() == (2,)
    finally:
        con.close()


def test_schema32_seeds_identity_state_from_current_mapping_watermark(monkeypatch, tmp_path):
    # Build specifically through schema 031 first. Future migrations must not change this fixture's
    # boundary: it is testing the one-time schema-032 adoption of the old mapping watermark.
    monkeypatch.setattr(db, "DB", str(tmp_path / "identity-seed.duckdb"))
    os.environ["DB_FORCE_READ_WRITE"] = "true"
    import institutional_migrations_v2 as migrations

    original = list(migrations.EXTRA_MIGRATION_FILES)
    schema32_index = original.index("institutional_schema_032.sql")
    monkeypatch.setattr(migrations, "EXTRA_MIGRATION_FILES", original[:schema32_index])
    migrations.ensure_schema()

    received = datetime(2026, 10, 14, 20, 0, tzinfo=timezone.utc)
    close = datetime(2027, 12, 31, tzinfo=timezone.utc)
    con = db.connect()
    try:
        seed_contract(con, "sc-a", "kalshi", "A", "Will A happen?", "YES if A happens.", close, received)
        con.execute("""
            INSERT INTO er_mapping_contract_state(
              source_contract_id,semantic_version_no,semantic_received_at,method_version,processed_at,last_pass_id)
            VALUES ('sc-a',1,?,'mapping_rules_v6_indexed_history',?,'old-pass')
        """, [received, received])
    finally:
        con.close()

    monkeypatch.setattr(migrations, "EXTRA_MIGRATION_FILES", original)
    migrations.ensure_schema()
    con = db.connect()
    try:
        assert con.execute(
            "SELECT semantic_version_no FROM er_mapping_identity_state WHERE source_contract_id='sc-a'"
        ).fetchone() == (1,)
        assert con.execute("SELECT count(*) FROM er_mapping_identity_work_queue").fetchone()[0] == 0
    finally:
        con.close()
