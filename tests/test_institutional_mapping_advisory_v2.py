from datetime import datetime, timezone

import duckdb

import institutional_mapping_v2 as meaning
import institutional_mapping_v8 as mapping_v8
from institutional_mapping_advisory_v2 import _candidate_pair_rows_local


NOW = datetime(2026, 10, 15, tzinfo=timezone.utc)


def _setup():
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    con.execute("""
        CREATE TABLE er_mapping_contract_index(
          source_contract_id VARCHAR PRIMARY KEY,
          source_id VARCHAR,
          semantic_version_no BIGINT,
          normalized_title VARCHAR,
          normalized_rules VARCHAR,
          deadline_at TIMESTAMPTZ,
          deadline_bucket VARCHAR,
          meaningful_token_count INTEGER,
          active BOOLEAN,
          indexed_at TIMESTAMPTZ,
          method_version VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE er_mapping_token_index(
          source_contract_id VARCHAR,
          source_id VARCHAR,
          semantic_version_no BIGINT,
          token VARCHAR,
          method_version VARCHAR,
          indexed_at TIMESTAMPTZ,
          PRIMARY KEY(source_contract_id,token)
        )
    """)

    rows = [
        ("k1", "kalshi", 1, "china blockade taiwan before 2027", "official confirmation", "2027-12-31", "2027-12-31"),
        ("p1", "polymarket", 3, "china blockade taiwan before 2027", "official confirmation", "2027-12-31", "2027-12-31"),
        ("p2", "polymarket", 2, "china blockade of taiwan before 2028", "different source rules", "2028-12-31", "2028-12-31"),
        ("p3", "polymarket", 1, "us recession in 2027", "nber recession", "2027-12-31", "2027-12-31"),
        ("k2", "kalshi", 1, "oil price above 100 in 2027", "settlement price", "2027-12-31", "2027-12-31"),
    ]
    for contract_id, source_id, version, title, rules, deadline, bucket in rows:
        tokens = sorted(meaning.meaningful_tokens(title))
        con.execute(
            """INSERT INTO er_mapping_contract_index VALUES (?,?,?,?,?,?,?, ?,TRUE,?,?)""",
            [contract_id, source_id, version, title, rules, deadline, bucket, len(tokens), NOW, "test"],
        )
        for token in tokens:
            con.execute(
                "INSERT INTO er_mapping_token_index VALUES (?,?,?,?,?,?)",
                [contract_id, source_id, version, token, "test", NOW],
            )
    return con


def _signature(rows):
    return sorted(
        (row["left_id"], row["right_id"], row["shared_token_count"])
        for row in rows
    )


def test_local_advisory_pair_discovery_matches_v8_pair_semantics():
    changed = [{"source_contract_id": "k1"}]
    con_remote = _setup()
    con_local = _setup()
    try:
        expected, expected_stats = mapping_v8._candidate_pair_rows(con_remote, changed)
        actual, actual_stats = _candidate_pair_rows_local(con_local, changed)

        assert _signature(actual) == _signature(expected)
        assert actual_stats["pairs_blocked"] == expected_stats["pairs_blocked"]
        assert actual_stats["pairs_evaluated"] == expected_stats["pairs_evaluated"]
        assert actual_stats["changed_token_rows"] == expected_stats["changed_token_rows"]
        assert actual_stats["token_frequency_rows"] == expected_stats["token_frequency_rows"]
        assert actual_stats["execution_location"] == "enrichment_flight_local_python"
        assert actual_stats["frequency_scope"] == "changed_tokens_only_local"

        # The isolated execution must not reconstruct v8's remote pair materialization.
        count = con_local.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name='_mapping_pair_ids'"
        ).fetchone()[0]
        assert count == 0
    finally:
        con_remote.close()
        con_local.close()


def test_local_advisory_pair_discovery_empty_queue_is_noop():
    con = _setup()
    try:
        rows, stats = _candidate_pair_rows_local(con, [])
        assert rows == []
        assert stats["pairs_blocked"] == 0
        assert stats["pairs_evaluated"] == 0
        assert stats["execution_location"] == "enrichment_flight_local_python"
    finally:
        con.close()
