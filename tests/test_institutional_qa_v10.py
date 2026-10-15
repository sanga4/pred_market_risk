from pathlib import Path

import duckdb

import institutional_qa_v10 as qa_v10


ROOT = Path(__file__).resolve().parents[1]


def _con():
    con = duckdb.connect(":memory:")
    con.execute("""
        CREATE TABLE er_event_mappings(
          mapping_id VARCHAR,
          source_contract_id VARCHAR,
          valid_from TIMESTAMPTZ,
          valid_to TIMESTAMPTZ
        )
    """)
    con.execute("""
        CREATE TABLE er_market_observations(
          source_contract_id VARCHAR,
          received_at TIMESTAMPTZ,
          methodology_version VARCHAR
        )
    """)
    return con


def _violations(con):
    rows = con.execute(qa_v10.POINT_IN_TIME_MAPPING_SQL).fetchall()
    return {row[0]: row[1] for row in rows}


def test_interval_mapping_qa_accepts_continuous_chain_and_ignores_legacy_prestart():
    con = _con()
    con.execute("""
        INSERT INTO er_event_mappings VALUES
          ('m1','good','2026-01-01 00:00:00+00','2026-02-01 00:00:00+00'),
          ('m2','good','2026-02-01 00:00:00+00',NULL)
    """)
    con.execute("""
        INSERT INTO er_market_observations VALUES
          ('good','2025-12-01 00:00:00+00','legacy_collector_v0.3'),
          ('good','2026-01-02 00:00:00+00','change_data_capture_v11_prefetched'),
          ('good','2026-03-01 00:00:00+00','change_data_capture_v11_prefetched')
    """)
    assert _violations(con) == {}


def test_interval_mapping_qa_detects_gap_overlap_and_uncovered_first_observation():
    con = _con()
    con.execute("""
        INSERT INTO er_event_mappings VALUES
          ('g1','gap','2026-01-01 00:00:00+00','2026-01-10 00:00:00+00'),
          ('g2','gap','2026-01-11 00:00:00+00',NULL),
          ('o1','overlap','2026-01-01 00:00:00+00','2026-01-20 00:00:00+00'),
          ('o2','overlap','2026-01-10 00:00:00+00',NULL),
          ('u1','uncovered','2026-01-05 00:00:00+00',NULL)
    """)
    con.execute("""
        INSERT INTO er_market_observations VALUES
          ('gap','2026-01-02 00:00:00+00','change_data_capture_v11_prefetched'),
          ('overlap','2026-01-02 00:00:00+00','change_data_capture_v11_prefetched'),
          ('uncovered','2026-01-01 00:00:00+00','change_data_capture_v11_prefetched')
    """)
    violations = _violations(con)
    assert set(violations) == {'gap', 'overlap', 'uncovered'}
    assert 'continuous/non-overlapping' in violations['gap']
    assert 'continuous/non-overlapping' in violations['overlap']
    assert 'First institutional observation' in violations['uncovered']


def test_interval_mapping_qa_requires_exactly_one_open_final_interval():
    con = _con()
    con.execute("""
        INSERT INTO er_event_mappings VALUES
          ('c1','closed_tail','2026-01-01 00:00:00+00','2026-02-01 00:00:00+00'),
          ('d1','double_open','2026-01-01 00:00:00+00',NULL),
          ('d2','double_open','2026-02-01 00:00:00+00',NULL)
    """)
    con.execute("""
        INSERT INTO er_market_observations VALUES
          ('closed_tail','2026-01-02 00:00:00+00','change_data_capture_v11_prefetched'),
          ('double_open','2026-01-02 00:00:00+00','change_data_capture_v11_prefetched')
    """)
    assert set(_violations(con)) == {'closed_tail', 'double_open'}


def test_qa_v10_is_new_generation_and_enrichment_v9_owns_it():
    qa = (ROOT / 'institutional_qa_v10.py').read_text()
    worker = (ROOT / 'institutional_enrichment_worker_v9.py').read_text()
    schema = (ROOT / 'institutional_schema_052.sql').read_text()
    migrations = (ROOT / 'institutional_migrations_v2.py').read_text()
    old_qa = (ROOT / 'institutional_qa_v9.py').read_text()

    assert 'institutional_qa_v10_interval_mapping_coverage' in qa
    assert 'mapping_interval_chain_plus_first_observation_coverage_v1' in qa
    assert 'FROM er_market_observations o' not in qa_v10.POINT_IN_TIME_MAPPING_SQL
    assert 'lead(valid_from)' in qa_v10.POINT_IN_TIME_MAPPING_SQL
    assert "methodology_version,'')<>'legacy_collector_v0.3'" in qa_v10.POINT_IN_TIME_MAPPING_SQL
    assert 'from institutional_qa_v10 import run_qa' in worker
    assert 'enrichment_path_v9_interval_qa' in worker
    assert 'institutional_qa_v10_interval_mapping_coverage' in schema
    assert '"institutional_schema_052.sql"' in migrations
    assert 'institutional_qa_v9_two_phase_read_snapshot' in old_qa
