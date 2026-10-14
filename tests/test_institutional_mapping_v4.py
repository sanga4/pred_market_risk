import os
from datetime import datetime, timedelta, timezone

import db


def setup_database(monkeypatch, tmp_path):
    monkeypatch.setattr(db, "DB", str(tmp_path / "mapping_v4.duckdb"))
    os.environ["DB_FORCE_READ_WRITE"] = "true"
    from institutional_migrations import ensure_schema
    ensure_schema()


def seed_contract(con, contract_id, source_id, market_id, title, rules, close, received):
    event_raw_id = f"raw_event_{contract_id}"
    market_raw_id = f"raw_market_{contract_id}"
    con.execute("""
        INSERT INTO er_sources(source_id,source_name,source_type,active)
        VALUES (?,?, 'prediction_market', TRUE)
        ON CONFLICT(source_id) DO NOTHING
    """, [source_id, source_id.title()])
    con.execute("""
        INSERT INTO er_raw_snapshots(
          raw_snapshot_id,source_id,source_object_type,source_object_id,source_observed_at,
          received_at,content_hash,payload_json,ingestion_run_id)
        VALUES
          (?,?,'event',?,?,?,NULL,'{}','test-run'),
          (?,?,'market',?,?,?,NULL,'{}','test-run')
    """, [
        event_raw_id, source_id, f"event-{market_id}", received, received,
        market_raw_id, source_id, market_id, received, received,
    ])
    con.execute("""
        INSERT INTO er_source_contracts(
          source_contract_id,source_id,source_market_id,current_title,current_description,status,
          opens_at,closes_at,resolves_at,current_resolution_rules,first_seen_at,last_seen_at,active)
        VALUES (?,?,?,?,?,'active',?,?,?,?,?,?,TRUE)
    """, [
        contract_id, source_id, market_id, title, f"Description {title}",
        received - timedelta(days=30), close, close, rules, received, received,
    ])
    con.execute("""
        INSERT INTO er_contract_semantic_versions(
          source_contract_id,version_no,source_id,source_market_id,title,description,outcome_type,status,
          opens_at,closes_at,resolves_at,resolution_rules,received_at,valid_from,
          event_raw_snapshot_id,market_raw_snapshot_id,metadata_completeness)
        VALUES (?,?,?,?,?,?,'binary','active',?,?,?,?,?,?,?,?,'full_v2')
    """, [
        contract_id, 1, source_id, market_id, title, f"Description {title}",
        received - timedelta(days=30), close, close, rules, received, received,
        event_raw_id, market_raw_id,
    ])


def test_exact_cross_source_identity_is_batched_then_incremental(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    from institutional_mapping_v4 import run_mapping_pass

    received = datetime(2026, 10, 14, 20, 0, tzinfo=timezone.utc)
    close = datetime(2027, 12, 31, tzinfo=timezone.utc)
    title = "Will China blockade Taiwan before 2028?"
    rules = "Resolves YES if China begins a blockade of Taiwan before 2028."
    con = db.connect()
    try:
        seed_contract(con, "sc_k", "kalshi", "KX-TW", title, rules, close, received)
        seed_contract(con, "sc_p", "polymarket", "PM-TW", title, rules, close, received)
    finally:
        con.close()

    first = run_mapping_pass()
    assert first["method_version"] == "mapping_rules_v4_batched"
    assert first["native_events_created"] == 2
    assert first["contracts_changed"] == 2
    assert first["candidates_generated"] >= 1
    assert first["auto_exact_mappings"] == 1

    con = db.connect()
    try:
        mappings = con.execute("""
            SELECT source_contract_id,canonical_event_id,mapping_type,method_version
            FROM er_latest_contract_mappings
            WHERE source_contract_id IN ('sc_k','sc_p') ORDER BY source_contract_id
        """).fetchall()
        assert len(mappings) == 2
        assert len({r[1] for r in mappings}) == 1
        assert {r[3] for r in mappings} == {"mapping_rules_v4_batched"}
        assert con.execute("SELECT count(*) FROM er_mapping_contract_state").fetchone()[0] == 2
        assert con.execute(
            "SELECT count(*) FROM er_mapping_passes WHERE status='success'"
        ).fetchone()[0] == 1
    finally:
        con.close()

    second = run_mapping_pass()
    assert second["native_events_created"] == 0
    assert second["contracts_changed"] == 0
    assert second["candidates_generated"] == 0
    assert second["auto_exact_mappings"] == 0


def test_same_title_different_rules_remains_separate_review_candidate(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    from institutional_mapping_v4 import run_mapping_pass

    received = datetime(2026, 10, 14, 20, 0, tzinfo=timezone.utc)
    close = datetime(2027, 12, 31, tzinfo=timezone.utc)
    title = "Will China blockade Taiwan before 2028?"
    con = db.connect()
    try:
        seed_contract(
            con, "sc_k", "kalshi", "KX-TW", title,
            "YES only if commercial shipping is physically prevented for 24 hours.", close, received,
        )
        seed_contract(
            con, "sc_p", "polymarket", "PM-TW", title,
            "YES if China formally announces a blockade, even before shipping is interrupted.", close, received,
        )
    finally:
        con.close()

    result = run_mapping_pass()
    assert result["auto_exact_mappings"] == 0
    con = db.connect()
    try:
        mappings = con.execute("""
            SELECT canonical_event_id FROM er_latest_contract_mappings
            WHERE source_contract_id IN ('sc_k','sc_p')
        """).fetchall()
        assert len({r[0] for r in mappings}) == 2
        candidate = con.execute("""
            SELECT suggested_relationship,resolution_compatible,rules_available_both,method_version,status
            FROM er_mapping_candidates
        """).fetchone()
        assert candidate == ("related_only", False, True, "mapping_rules_v4_batched", "pending")
    finally:
        con.close()


def test_only_changed_semantics_reenter_mapping_work_queue(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    from institutional_mapping_v4 import run_mapping_pass

    received = datetime(2026, 10, 14, 20, 0, tzinfo=timezone.utc)
    close = datetime(2027, 12, 31, tzinfo=timezone.utc)
    con = db.connect()
    try:
        seed_contract(
            con, "sc_k", "kalshi", "KX-A", "Will US CPI be above 3%?",
            "YES if CPI is above 3 percent.", close, received,
        )
    finally:
        con.close()

    first = run_mapping_pass()
    assert first["contracts_changed"] == 1
    second = run_mapping_pass()
    assert second["contracts_changed"] == 0

    changed_at = received + timedelta(hours=1)
    con = db.connect()
    try:
        con.execute("""
            UPDATE er_contract_semantic_versions SET valid_to=?
            WHERE source_contract_id='sc_k' AND version_no=1
        """, [changed_at])
        con.execute("""
            INSERT INTO er_contract_semantic_versions(
              source_contract_id,version_no,source_id,source_market_id,title,description,outcome_type,status,
              opens_at,closes_at,resolves_at,resolution_rules,received_at,valid_from,
              event_raw_snapshot_id,market_raw_snapshot_id,metadata_completeness)
            SELECT source_contract_id,2,source_id,source_market_id,title,'Updated description',outcome_type,status,
                   opens_at,closes_at,resolves_at,'YES if headline CPI is above 3 percent.',?, ?,
                   event_raw_snapshot_id,market_raw_snapshot_id,'full_v2'
            FROM er_contract_semantic_versions WHERE source_contract_id='sc_k' AND version_no=1
        """, [changed_at, changed_at])
        con.execute("""
            UPDATE er_source_contracts
            SET current_description='Updated description',current_resolution_rules='YES if headline CPI is above 3 percent.'
            WHERE source_contract_id='sc_k'
        """)
    finally:
        con.close()

    third = run_mapping_pass()
    assert third["contracts_changed"] == 1
    con = db.connect()
    try:
        state = con.execute("""
            SELECT semantic_version_no,method_version FROM er_mapping_contract_state
            WHERE source_contract_id='sc_k'
        """).fetchone()
        assert state == (2, "mapping_rules_v4_batched")
    finally:
        con.close()
