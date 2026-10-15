import os

import db


def setup_database(monkeypatch, tmp_path):
    monkeypatch.setattr(db, "DB", str(tmp_path / "presence.duckdb"))
    os.environ["DB_FORCE_READ_WRITE"] = "true"
    os.environ["PRESENCE_MISS_THRESHOLD"] = "3"
    from institutional_migrations import ensure_schema
    ensure_schema()


def seed_contract(con):
    con.execute("""
        INSERT INTO er_sources(source_id,source_name,source_type,active)
        VALUES ('test','Test','prediction_market',TRUE)
    """)
    con.execute("""
        INSERT INTO er_source_contracts(
          source_contract_id,source_id,source_market_id,current_title,status,active
        ) VALUES ('sc_1','test','m1','Test event','active',TRUE)
    """)


def state(con):
    return con.execute(
        "SELECT listing_state,consecutive_misses,last_seen_run_id,deactivated_at "
        "FROM er_contract_presence_state WHERE source_contract_id='sc_1'"
    ).fetchone()


def test_three_complete_misses_required_before_deactivation(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    import institutional_ingest_v3 as ingest

    con = db.connect()
    try:
        seed_contract(con)
        first = ingest.reconcile_presence(con, 'test', 'run-seen', {'sc_1'}, universe_complete=True)
        assert first['deactivated'] == 0
        assert state(con)[:2] == ('observed', 0)

        r1 = ingest.reconcile_presence(con, 'test', 'run-miss-1', set(), universe_complete=True)
        assert r1['deactivated'] == 0
        assert state(con)[:2] == ('missing_unconfirmed', 1)
        assert con.execute("SELECT active FROM er_source_contracts WHERE source_contract_id='sc_1'").fetchone()[0]

        r2 = ingest.reconcile_presence(con, 'test', 'run-miss-2', set(), universe_complete=True)
        assert r2['deactivated'] == 0
        assert state(con)[:2] == ('missing_unconfirmed', 2)
        assert con.execute("SELECT active FROM er_source_contracts WHERE source_contract_id='sc_1'").fetchone()[0]

        r3 = ingest.reconcile_presence(con, 'test', 'run-miss-3', set(), universe_complete=True)
        assert r3['deactivated'] == 1
        assert state(con)[:2] == ('missing_confirmed', 3)
        assert not con.execute("SELECT active FROM er_source_contracts WHERE source_contract_id='sc_1'").fetchone()[0]

        transitions = con.execute(
            "SELECT transition_type FROM er_contract_presence_transitions "
            "WHERE source_contract_id='sc_1' ORDER BY recorded_at,transition_id"
        ).fetchall()
        kinds = [r[0] for r in transitions]
        assert kinds.count('first_miss') == 1
        assert kinds.count('deactivated') == 1
    finally:
        con.close()


def test_incomplete_run_never_increments_misses(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    import institutional_ingest_v3 as ingest

    con = db.connect()
    try:
        seed_contract(con)
        ingest.reconcile_presence(con, 'test', 'run-seen', {'sc_1'}, universe_complete=True)
        before = state(con)
        result = ingest.reconcile_presence(con, 'test', 'run-partial', set(), universe_complete=False)
        after = state(con)
        assert result['universe_complete'] is False
        assert result['missed'] == 0
        assert result['deactivated'] == 0
        assert after[:2] == before[:2] == ('observed', 0)
        assert con.execute("SELECT active FROM er_source_contracts WHERE source_contract_id='sc_1'").fetchone()[0]
    finally:
        con.close()


def test_reappearance_resets_presence_and_reactivates(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    import institutional_ingest_v3 as ingest

    con = db.connect()
    try:
        seed_contract(con)
        ingest.reconcile_presence(con, 'test', 'run-seen', {'sc_1'}, universe_complete=True)
        for n in range(1, 4):
            ingest.reconcile_presence(con, 'test', f'run-miss-{n}', set(), universe_complete=True)
        assert not con.execute("SELECT active FROM er_source_contracts WHERE source_contract_id='sc_1'").fetchone()[0]

        # A real source observation is what reactivates source-contract serving state.
        con.execute("UPDATE er_source_contracts SET active=TRUE,status='active' WHERE source_contract_id='sc_1'")
        result = ingest.reconcile_presence(con, 'test', 'run-return', {'sc_1'}, universe_complete=True)
        assert result['reactivated'] == 1
        current = state(con)
        assert current[0] == 'observed'
        assert current[1] == 0
        assert current[2] == 'run-return'
        assert current[3] is None
        assert con.execute("SELECT active FROM er_source_contracts WHERE source_contract_id='sc_1'").fetchone()[0]
        reactivated = con.execute(
            "SELECT count(*) FROM er_contract_presence_transitions "
            "WHERE source_contract_id='sc_1' AND transition_type='reactivated'"
        ).fetchone()[0]
        assert reactivated == 1
    finally:
        con.close()
