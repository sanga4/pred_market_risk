import duckdb

import institutional_qa_v11 as qa_v11


def _con():
    con = duckdb.connect(":memory:")
    con.execute("""
        CREATE TABLE er_source_contracts(
          source_contract_id VARCHAR,
          active BOOLEAN
        )
    """)
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


def _pit_violations(con):
    return {row[0]: row[1] for row in con.execute(qa_v11.POINT_IN_TIME_MAPPING_SQL).fetchall()}


def _active_violations(con):
    return {row[0]: row[1] for row in con.execute(qa_v11.ACTIVE_MAPPING_SQL).fetchall()}


def test_active_cardinality_ignores_inactive_contract_without_live_mapping():
    con = _con()
    con.execute("INSERT INTO er_source_contracts VALUES ('active_missing',TRUE),('retired',FALSE)")
    con.execute("""
        INSERT INTO er_event_mappings VALUES
          ('old','retired','2026-01-01 00:00:00+00','2026-02-01 00:00:00+00')
    """)
    assert set(_active_violations(con)) == {'active_missing'}


def test_point_in_time_allows_unobserved_cutover_gap_and_closed_inactive_tail():
    con = _con()
    con.execute("""
        INSERT INTO er_event_mappings VALUES
          ('m1','cutover','2026-01-01 00:00:00+00','2026-01-10 00:00:00+00'),
          ('m2','cutover','2026-01-11 00:00:00+00',NULL),
          ('r1','retired','2026-01-01 00:00:00+00','2026-02-01 00:00:00+00')
    """)
    con.execute("""
        INSERT INTO er_market_observations VALUES
          ('cutover','2025-12-01 00:00:00+00','legacy_collector_v0.3'),
          ('cutover','2026-01-02 00:00:00+00','source_native_v1'),
          ('cutover','2026-01-12 00:00:00+00','source_native_v1'),
          ('retired','2026-01-15 00:00:00+00','source_native_v1')
    """)
    assert _pit_violations(con) == {}


def test_point_in_time_rejects_observed_gap_overlap_and_outside_coverage():
    con = _con()
    con.execute("""
        INSERT INTO er_event_mappings VALUES
          ('g1','gap','2026-01-01 00:00:00+00','2026-01-10 00:00:00+00'),
          ('g2','gap','2026-01-11 00:00:00+00',NULL),
          ('o1','overlap','2026-01-01 00:00:00+00','2026-01-20 00:00:00+00'),
          ('o2','overlap','2026-01-10 00:00:00+00',NULL),
          ('s1','early','2026-01-05 00:00:00+00',NULL),
          ('e1','late','2026-01-01 00:00:00+00','2026-01-20 00:00:00+00')
    """)
    con.execute("""
        INSERT INTO er_market_observations VALUES
          ('gap','2026-01-10 12:00:00+00','source_native_v1'),
          ('overlap','2026-01-15 00:00:00+00','source_native_v1'),
          ('early','2026-01-01 00:00:00+00','source_native_v1'),
          ('late','2026-01-21 00:00:00+00','source_native_v1')
    """)
    violations = _pit_violations(con)
    assert set(violations) == {'gap', 'overlap', 'early', 'late'}
    assert 'gap' in violations['gap'].lower()
    assert 'overlap' in violations['overlap'].lower()
    assert 'predates' in violations['early'].lower()
    assert 'after the final' in violations['late'].lower()


def test_point_in_time_rejects_nonfinal_open_interval():
    con = _con()
    con.execute("""
        INSERT INTO er_event_mappings VALUES
          ('m1','double_open','2026-01-01 00:00:00+00',NULL),
          ('m2','double_open','2026-02-01 00:00:00+00',NULL)
    """)
    con.execute("INSERT INTO er_market_observations VALUES ('double_open','2026-02-02 00:00:00+00','source_native_v1')")
    assert set(_pit_violations(con)) == {'double_open'}


def test_qa_v11_preserves_snapshot_engine_and_declares_lifecycle_policy():
    assert qa_v11.QA_VERSION == 'institutional_qa_v11_lifecycle_aware_mapping_coverage'
    assert qa_v11.POINT_IN_TIME_MAPPING_METHOD == 'observation_aware_interval_coverage_v2'
    assert 'WHERE c.active=TRUE' in qa_v11.ACTIVE_MAPPING_SQL
    assert "methodology_version,'')<>'legacy_collector_v0.3'" in qa_v11.POINT_IN_TIME_MAPPING_SQL
    assert 'institutional_qa_v10' in qa_v11.v10.QA_VERSION
