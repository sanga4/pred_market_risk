import os
from datetime import datetime, timedelta, timezone

import db


def setup_database(monkeypatch, tmp_path):
    monkeypatch.setattr(db, "DB", str(tmp_path / "graph_v3.duckdb"))
    os.environ["DB_FORCE_READ_WRITE"] = "true"
    from institutional_migrations import ensure_schema
    ensure_schema()


def seed_interval(
    con,
    *,
    title="Will China blockade Taiwan before 2028?",
    resolution_rules="YES if China begins a blockade affecting Taiwan.",
):
    received = datetime(2026, 10, 14, 20, 0, tzinfo=timezone.utc)
    close = datetime(2027, 12, 31, tzinfo=timezone.utc)
    con.execute("""
        INSERT INTO er_sources(source_id,source_name,source_type,active)
        VALUES ('kalshi','Kalshi','prediction_market',TRUE)
        ON CONFLICT(source_id) DO NOTHING
    """)
    con.execute("""
        INSERT INTO er_raw_snapshots(
          raw_snapshot_id,source_id,source_object_type,source_object_id,source_observed_at,
          received_at,content_hash,payload_json,ingestion_run_id)
        VALUES
          ('raw-event','kalshi','event','evt-source',?,?,NULL,'{}','test-run'),
          ('raw-market','kalshi','market','mkt-source',?,?,NULL,'{}','test-run')
    """, [received,received,received,received])
    con.execute("""
        INSERT INTO er_source_contracts(
          source_contract_id,source_id,source_market_id,current_title,current_description,status,
          opens_at,closes_at,resolves_at,current_resolution_rules,first_seen_at,last_seen_at,active)
        VALUES ('sc-1','kalshi','KX-TW',?,?,'active',?,?,?,?,?,?,TRUE)
    """, [
        title, f"Description: {title}", received-timedelta(days=30), close, close,
        resolution_rules, received, received,
    ])
    con.execute("""
        INSERT INTO er_contract_semantic_versions(
          source_contract_id,version_no,source_id,source_market_id,title,description,outcome_type,status,
          opens_at,closes_at,resolves_at,resolution_rules,received_at,valid_from,
          event_raw_snapshot_id,market_raw_snapshot_id,metadata_completeness)
        VALUES ('sc-1',1,'kalshi','KX-TW',?,?,'binary','active',?,?,?,?,?,?,'raw-event','raw-market','full_v2')
    """, [
        title, f"Description: {title}", received-timedelta(days=30), close, close,
        resolution_rules, received, received,
    ])
    con.execute("""
        INSERT INTO er_event_families(event_family_id,canonical_name,category)
        VALUES ('fam-1','Taiwan blockade','geopolitics')
    """)
    con.execute("""
        INSERT INTO er_canonical_events(
          canonical_event_id,event_family_id,canonical_name,canonical_definition,event_class,status)
        VALUES ('ce-1','fam-1','Taiwan blockade before 2028','Test canonical definition','geopolitics','active')
    """)
    con.execute("""
        INSERT INTO er_event_mappings(
          mapping_id,source_contract_id,canonical_event_id,mapping_type,confidence,mapping_method,
          method_version,review_status,mapped_at,valid_from,created_by)
        VALUES ('map-1','sc-1','ce-1','source_native',1.0,'test','test','system',?,?, 'system')
    """, [received,received])
    return received


def test_graph_v3_preserves_v2_edge_identity_and_second_pass_is_zero_work(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    con = db.connect()
    try:
        seed_interval(con)
    finally:
        con.close()

    from institutional_graph_v3 import GRAPH_VERSION, run_graph_pass
    import institutional_graph as base

    first = run_graph_pass()
    assert first["graph_version"] == "graph_rules_v2_versioned_mentions"
    assert first["execution_version"] == "graph_execution_v3_incremental"
    assert first["semantic_intervals"] == 1
    assert first["intervals_processed"] == 1
    assert first["mention_edges_written"] >= 2
    assert first["evidence_records_written"] >= 4
    assert first["alias_changed"] is True

    china_edge = base.stable_id("edge", "map-1", "sc-1", 1, "mentions", "geo_china", GRAPH_VERSION)
    taiwan_edge = base.stable_id("edge", "map-1", "sc-1", 1, "mentions", "geo_taiwan", GRAPH_VERSION)
    con = db.connect()
    try:
        edges = con.execute("""
            SELECT edge_id,method_version,valid_to FROM er_graph_edges
            WHERE edge_id IN (?,?) ORDER BY edge_id
        """, [china_edge,taiwan_edge]).fetchall()
        assert len(edges) == 2
        assert {row[1] for row in edges} == {"graph_rules_v2_versioned_mentions"}
        assert all(row[2] is None for row in edges)
        assert con.execute("SELECT count(*) FROM er_graph_interval_state WHERE active=TRUE").fetchone()[0] == 1
        assert con.execute("SELECT count(*) FROM er_graph_edge_origins").fetchone()[0] >= 2
    finally:
        con.close()

    second = run_graph_pass()
    assert second["alias_changed"] is False
    assert second["semantic_intervals"] == 1
    assert second["intervals_processed"] == 0
    assert second["mention_edges_written"] == 0
    assert second["evidence_records_written"] == 0


def test_graph_v3_mapping_close_updates_edge_interval_without_new_identity(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    con = db.connect()
    try:
        received = seed_interval(con)
    finally:
        con.close()

    from institutional_graph_v3 import GRAPH_VERSION, run_graph_pass
    import institutional_graph as base
    run_graph_pass()
    closed_at = received + timedelta(hours=1)
    con = db.connect()
    try:
        con.execute("UPDATE er_event_mappings SET valid_to=? WHERE mapping_id='map-1'", [closed_at])
    finally:
        con.close()

    result = run_graph_pass()
    assert result["intervals_processed"] == 1
    edge_id = base.stable_id("edge", "map-1", "sc-1", 1, "mentions", "geo_china", GRAPH_VERSION)
    con = db.connect()
    try:
        row = con.execute("SELECT valid_to FROM er_graph_edges WHERE edge_id=?", [edge_id]).fetchone()
        assert row[0] == closed_at
        assert con.execute("SELECT count(*) FROM er_graph_edges WHERE edge_id=?", [edge_id]).fetchone()[0] == 1
    finally:
        con.close()


def test_graph_v3_zero_length_mapping_retires_stale_interval_precisely(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    con = db.connect()
    try:
        received = seed_interval(con)
    finally:
        con.close()

    from institutional_graph_v3 import GRAPH_VERSION, run_graph_pass
    import institutional_graph as base
    run_graph_pass()
    con = db.connect()
    try:
        con.execute("UPDATE er_event_mappings SET valid_to=valid_from WHERE mapping_id='map-1'")
    finally:
        con.close()

    result = run_graph_pass()
    assert result["semantic_intervals"] == 0
    assert result["intervals_processed"] == 0
    assert result["stale_intervals_retired"] == 1
    edge_id = base.stable_id("edge", "map-1", "sc-1", 1, "mentions", "geo_china", GRAPH_VERSION)
    con = db.connect()
    try:
        assert con.execute("SELECT valid_to FROM er_graph_edges WHERE edge_id=?", [edge_id]).fetchone()[0] == received
        assert con.execute("SELECT active FROM er_graph_interval_state WHERE mapping_id='map-1'").fetchone()[0] is False
    finally:
        con.close()


def test_graph_v3_alias_change_reprocesses_intervals_and_adds_new_identity_match(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    con = db.connect()
    try:
        seed_interval(
            con,
            title="Will Formosa face a blockade before 2028?",
            resolution_rules="YES if a blockade prevents commercial shipping for 24 hours.",
        )
    finally:
        con.close()

    from institutional_graph_v3 import GRAPH_VERSION, run_graph_pass
    import institutional_graph as base
    first = run_graph_pass()
    assert first["mention_edges_written"] == 0
    taiwan_edge = base.stable_id("edge", "map-1", "sc-1", 1, "mentions", "geo_taiwan", GRAPH_VERSION)
    con = db.connect()
    try:
        assert con.execute("SELECT count(*) FROM er_graph_edges WHERE edge_id=?", [taiwan_edge]).fetchone()[0] == 0
        con.execute("""
            INSERT INTO er_entity_identifiers(entity_id,identifier_type,identifier_value,source,is_primary)
            VALUES ('geo_taiwan','alias','formosa','test_alias_change',FALSE)
        """)
    finally:
        con.close()

    second = run_graph_pass()
    assert second["alias_changed"] is True
    assert second["intervals_processed"] == 1
    con = db.connect()
    try:
        assert con.execute("SELECT count(*) FROM er_graph_edges WHERE edge_id=?", [taiwan_edge]).fetchone()[0] == 1
    finally:
        con.close()


def test_graph_v3_recovers_stale_run_as_abandoned(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    from institutional_graph_v3 import recover_abandoned_graph_runs
    con = db.connect()
    try:
        con.execute("""
            INSERT INTO er_graph_runs(run_id,execution_version,graph_version,started_at,status)
            VALUES ('stale-graph','graph_execution_v3_incremental','graph_rules_v2_versioned_mentions',
                    now()-INTERVAL '2 hours','running')
        """)
        recovery = recover_abandoned_graph_runs(con)
        assert recovery["recovered"] == 1
        assert recovery["running"] == []
        row = con.execute("SELECT status,finished_at,details FROM er_graph_runs WHERE run_id='stale-graph'").fetchone()
        assert row[0] == "abandoned"
        assert row[1] is not None
        assert "graph_run_recovery_v1" in row[2]
    finally:
        con.close()
