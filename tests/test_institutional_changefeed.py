import os
from datetime import datetime, timezone

import db


def setup_database(monkeypatch, tmp_path):
    monkeypatch.setattr(db, "DB", str(tmp_path / "changefeed.duckdb"))
    os.environ["DB_FORCE_READ_WRITE"] = "true"
    from institutional_migrations import ensure_schema
    ensure_schema()


def seed(con):
    received = datetime(2026, 10, 14, 21, 0, tzinfo=timezone.utc)
    con.execute("""
        INSERT INTO er_sources(source_id,source_name,source_type,active)
        VALUES ('test','Test','prediction_market',TRUE)
    """)
    con.execute("""
        INSERT INTO er_event_families(event_family_id,canonical_name,category)
        VALUES ('fam_1','Test family','other')
    """)
    con.execute("""
        INSERT INTO er_canonical_events(
          canonical_event_id,event_family_id,canonical_name,canonical_definition,status
        ) VALUES ('evt_1','fam_1','Test event','Test event','active')
    """)
    for i, contract_id in enumerate(['sc_a','sc_b','sc_c'], start=1):
        market_id = f'm{i}'
        con.execute("""
            INSERT INTO er_source_contracts(
              source_contract_id,source_id,source_market_id,current_title,status,active,first_seen_at,last_seen_at
            ) VALUES (?,?,?,?,'active',TRUE,?,?)
        """, [contract_id, 'test', market_id, f'Contract {i}', received, received])
        con.execute("""
            INSERT INTO er_event_mappings(
              mapping_id,source_contract_id,canonical_event_id,mapping_type,confidence,
              mapping_method,method_version,review_status,valid_from,created_by
            ) VALUES (?,?,?,'source_native',1.0,'deterministic_identity','test','source_native',?,'system')
        """, [f'map_{i}', contract_id, 'evt_1', received])
        observed = received.replace(microsecond=i)
        con.execute("""
            INSERT INTO er_market_observations(
              source_id,source_contract_id,observed_at,received_at,processed_at,
              displayed_probability,market_status,methodology_version
            ) VALUES (?,?,?,?,?,?,?,?)
        """, ['test', contract_id, observed, received, received, 0.1 * i, 'active', 'source_native_v1'])
    return received


def test_changefeed_cursor_does_not_skip_same_received_timestamp(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    from institutional_changefeed_api import change_feed

    con = db.connect()
    try:
        received = seed(con)
    finally:
        con.close()

    first = change_feed(since=received.replace(microsecond=0), mapping_basis='as_known', limit=2)
    assert first['count'] == 2
    assert first['has_more_possible'] is True
    first_ids = [row['source_contract_id'] for row in first['observations']]
    assert first_ids == ['sc_a', 'sc_b']
    assert all(row['canonical_event_id'] == 'evt_1' for row in first['observations'])

    second = change_feed(cursor=first['cursor'], mapping_basis='as_known', limit=2)
    assert second['count'] == 1
    second_ids = [row['source_contract_id'] for row in second['observations']]
    assert second_ids == ['sc_c']

    all_ids = first_ids + second_ids
    assert all_ids == ['sc_a', 'sc_b', 'sc_c']
    assert len(set(all_ids)) == 3


def test_changefeed_current_taxonomy_is_explicit_opt_in(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    from institutional_changefeed_api import change_feed

    con = db.connect()
    try:
        received = seed(con)
    finally:
        con.close()

    result = change_feed(since=received, mapping_basis='current_taxonomy', limit=10)
    assert result['mapping_basis'] == 'current_taxonomy'
    assert result['count'] == 3
    assert all(row['canonical_event_id'] == 'evt_1' for row in result['observations'])
