import duckdb

import institutional_lifecycle_v2 as lifecycle


def test_changed_lifecycle_rows_matches_v1_semantics_and_skips_superseded():
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE er_canonical_events(canonical_event_id VARCHAR,status VARCHAR)")
    con.execute("CREATE TABLE er_event_mappings(source_contract_id VARCHAR,canonical_event_id VARCHAR,valid_to TIMESTAMP)")
    con.execute("CREATE TABLE er_source_contracts(source_contract_id VARCHAR,active BOOLEAN,status VARCHAR)")

    con.executemany(
        "INSERT INTO er_canonical_events VALUES (?,?)",
        [
            ("e_active_same", "active"),
            ("e_to_resolved", "active"),
            ("e_to_closed", "active"),
            ("e_active_wins", "closed"),
            ("e_superseded", "superseded"),
            ("e_unmapped", "active"),
        ],
    )
    con.executemany(
        "INSERT INTO er_source_contracts VALUES (?,?,?)",
        [
            ("c1", True, "active"),
            ("c2", False, "resolved"),
            ("c3", False, "closed"),
            ("c4", False, "resolved"),
            ("c5", True, "open"),
            ("c6", True, "active"),
            ("c7", False, "resolved"),
        ],
    )
    con.executemany(
        "INSERT INTO er_event_mappings VALUES (?,?,?)",
        [
            ("c1", "e_active_same", None),
            ("c2", "e_to_resolved", None),
            ("c3", "e_to_closed", None),
            ("c4", "e_active_wins", None),
            ("c5", "e_active_wins", None),
            ("c6", "e_superseded", None),
            ("c7", "e_to_closed", "2026-01-01"),
        ],
    )

    rows = lifecycle._changed_lifecycle_rows(con)
    assert rows == [
        ("e_active_wins", "closed", "active"),
        ("e_to_closed", "active", "closed"),
        ("e_to_resolved", "active", "resolved"),
    ]


def test_lifecycle_v2_uses_set_based_derivation_and_atomic_write_scope():
    body = open("institutional_lifecycle_v2.py").read()
    assert "GROUP BY m.canonical_event_id" in body
    assert "bool_or" in body
    assert "BEGIN TRANSACTION" in body
    assert "COMMIT" in body
    assert "ROLLBACK" in body
    assert "WHERE m.canonical_event_id=?" not in body
