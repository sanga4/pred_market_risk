import os
from datetime import datetime, timedelta, timezone

import db


def setup_database(monkeypatch, tmp_path):
    monkeypatch.setattr(db, "DB", str(tmp_path / "mapping_v6.duckdb"))
    os.environ["DB_FORCE_READ_WRITE"] = "true"
    from institutional_migrations import ensure_schema
    ensure_schema()


def seed_contract(con, contract_id, source_id, market_id, title, rules, close, received):
    event_raw_id = f"raw-event-{contract_id}"
    market_raw_id = f"raw-market-{contract_id}"
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
    return event_raw_id, market_raw_id


def test_v6_exact_identity_matches_conservative_semantics_and_second_pass_is_incremental(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    from institutional_mapping_v6 import run_mapping_pass

    received = datetime(2026, 10, 14, 20, 0, tzinfo=timezone.utc)
    close = datetime(2027, 12, 31, tzinfo=timezone.utc)
    title = "Will China blockade Taiwan before 2028?"
    rules = "Resolves YES if China begins a blockade of Taiwan before 2028."
    con = db.connect()
    try:
        seed_contract(con, "sc-k", "kalshi", "KX-TW", title, rules, close, received)
        seed_contract(con, "sc-p", "polymarket", "PM-TW", title, rules, close, received)
    finally:
        con.close()

    first = run_mapping_pass()
    assert first["method_version"] == "mapping_rules_v6_indexed_history"
    assert first["candidate_history"] == "semantic_versioned_v1"
    assert first["contracts_total"] == 2
    assert first["contracts_changed"] == 2
    assert first["index_seed"]["seeded"] is True
    assert first["candidate_pairs_evaluated"] >= 1
    assert first["auto_exact_mappings"] == 1

    con = db.connect()
    try:
        mappings = con.execute("""
            SELECT source_contract_id,canonical_event_id FROM er_latest_contract_mappings
            WHERE source_contract_id IN ('sc-k','sc-p') ORDER BY source_contract_id
        """).fetchall()
        assert len(mappings) == 2
        assert len({row[1] for row in mappings}) == 1
        current = con.execute("""
            SELECT suggested_relationship,resolution_compatible,rules_available_both,
                   left_semantic_version_no,right_semantic_version_no,method_version,status
            FROM er_mapping_candidates WHERE method_version='mapping_rules_v6_indexed_history'
        """).fetchone()
        assert current == (
            "same_event", True, True, 1, 1, "mapping_rules_v6_indexed_history", "auto_accepted"
        )
        history = con.execute("""
            SELECT status,left_semantic_version_no,right_semantic_version_no
            FROM er_mapping_candidate_versions
            WHERE method_version='mapping_rules_v6_indexed_history'
        """).fetchall()
        assert history == [("auto_accepted", 1, 1)]
    finally:
        con.close()

    second = run_mapping_pass()
    assert second["contracts_changed"] == 0
    assert second["candidate_pairs_evaluated"] == 0
    assert second["candidates_generated"] == 0
    assert second["auto_exact_mappings"] == 0


def test_v6_method_upgrade_does_not_reprocess_current_v4_watermarks(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    received = datetime(2026, 10, 14, 20, 0, tzinfo=timezone.utc)
    close = datetime(2027, 12, 31, tzinfo=timezone.utc)
    con = db.connect()
    try:
        seed_contract(con, "sc-k", "kalshi", "KX-A", "Will US CPI be above 3%?", "YES if CPI is above 3 percent.", close, received)
        seed_contract(con, "sc-p", "polymarket", "PM-A", "Will US CPI be above 3%?", "YES if CPI is above 3 percent.", close, received)
        for contract_id in ["sc-k", "sc-p"]:
            con.execute("""
                INSERT INTO er_mapping_contract_state(
                  source_contract_id,semantic_version_no,semantic_received_at,method_version,processed_at,last_pass_id)
                VALUES (?,1,?,'mapping_rules_v4_batched',?,'legacy-v4-pass')
            """, [contract_id, received, received])
    finally:
        con.close()

    from institutional_mapping_v6 import run_mapping_pass
    result = run_mapping_pass()
    assert result["index_seed"]["seeded"] is True
    assert result["index_seed"]["contracts"] == 2
    assert result["contracts_changed"] == 0
    assert result["candidate_pairs_evaluated"] == 0
    assert result["candidates_generated"] == 0


def test_v6_semantic_change_reopens_current_review_and_preserves_prior_review_history(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    from institutional_mapping_v6 import review_candidate, run_mapping_pass

    received = datetime(2026, 10, 14, 20, 0, tzinfo=timezone.utc)
    close = datetime(2027, 12, 31, tzinfo=timezone.utc)
    title = "Will China blockade Taiwan before 2028?"
    con = db.connect()
    try:
        event_raw, market_raw = seed_contract(
            con, "sc-k", "kalshi", "KX-TW", title,
            "YES only if commercial shipping is physically prevented for 24 hours.", close, received,
        )
        seed_contract(
            con, "sc-p", "polymarket", "PM-TW", title,
            "YES if China formally announces a blockade, even before shipping is interrupted.", close, received,
        )
    finally:
        con.close()

    first = run_mapping_pass()
    assert first["contracts_changed"] == 2
    assert first["auto_exact_mappings"] == 0
    con = db.connect()
    try:
        candidate_id = con.execute("""
            SELECT candidate_id FROM er_mapping_candidates
            WHERE method_version='mapping_rules_v6_indexed_history'
        """).fetchone()[0]
    finally:
        con.close()
    review_candidate(candidate_id, "reject", "test-reviewer", "Old semantics were not identical")

    changed_at = received + timedelta(hours=1)
    con = db.connect()
    try:
        con.execute("""
            UPDATE er_contract_semantic_versions SET valid_to=?
            WHERE source_contract_id='sc-k' AND version_no=1
        """, [changed_at])
        con.execute("""
            INSERT INTO er_contract_semantic_versions(
              source_contract_id,version_no,source_id,source_market_id,title,description,outcome_type,status,
              opens_at,closes_at,resolves_at,resolution_rules,received_at,valid_from,
              event_raw_snapshot_id,market_raw_snapshot_id,metadata_completeness)
            SELECT source_contract_id,2,source_id,source_market_id,title,description,outcome_type,status,
                   opens_at,closes_at,resolves_at,'YES only if shipping is prevented for 48 hours.',?, ?,
                   ?,?,'full_v2'
            FROM er_contract_semantic_versions
            WHERE source_contract_id='sc-k' AND version_no=1
        """, [changed_at, changed_at, event_raw, market_raw])
        con.execute("""
            UPDATE er_source_contracts
            SET current_resolution_rules='YES only if shipping is prevented for 48 hours.'
            WHERE source_contract_id='sc-k'
        """)
    finally:
        con.close()

    second = run_mapping_pass()
    assert second["contracts_changed"] == 1
    assert second["candidate_pairs_evaluated"] >= 1
    con = db.connect()
    try:
        state = con.execute("""
            SELECT semantic_version_no,method_version FROM er_mapping_contract_state
            WHERE source_contract_id='sc-k'
        """).fetchone()
        assert state == (2, "mapping_rules_v6_indexed_history")
        current = con.execute("""
            SELECT status,review_action,left_semantic_version_no,right_semantic_version_no
            FROM er_mapping_candidates WHERE candidate_id=?
        """, [candidate_id]).fetchone()
        assert current[0:2] == ("pending", None)
        assert sorted([current[2], current[3]]) == [1, 2]
        history = con.execute("""
            SELECT status,review_action,reviewed_by,left_semantic_version_no,right_semantic_version_no,
                   superseded_at IS NOT NULL
            FROM er_mapping_candidate_versions
            WHERE candidate_id=? ORDER BY generated_at,candidate_version_id
        """, [candidate_id]).fetchall()
        assert len(history) == 2
        assert history[0][0:3] == ("reviewed", "reject", "test-reviewer")
        assert history[0][5] is True
        assert history[1][0:2] == ("pending", None)
        assert sorted([history[1][3], history[1][4]]) == [1, 2]
    finally:
        con.close()


def test_v6_recovers_stale_mapping_passes_as_abandoned(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    from institutional_mapping_v6 import run_mapping_pass
    import institutional_mapping_v5 as mapping_v5

    con = db.connect()
    try:
        con.execute("""
            INSERT INTO er_mapping_passes(pass_id,method_version,started_at,status)
            VALUES ('stale-pass','mapping_rules_v4_batched',now()-INTERVAL '2 hours','running')
        """)
        recovery = mapping_v5.recover_abandoned_mapping_passes(con)
        assert recovery["recovered"] == 1
        assert recovery["running"] == []
    finally:
        con.close()

    result = run_mapping_pass()
    assert result["method_version"] == "mapping_rules_v6_indexed_history"
    con = db.connect()
    try:
        row = con.execute("SELECT status,finished_at,details FROM er_mapping_passes WHERE pass_id='stale-pass'").fetchone()
        assert row[0] == "abandoned"
        assert row[1] is not None
        assert "mapping_pass_recovery_v1" in row[2]
    finally:
        con.close()
