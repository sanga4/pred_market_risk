from datetime import datetime, timezone
from pathlib import Path

import duckdb

from institutional_mapping_advisory_v2 import (
    _candidate_pair_rows_local,
    _meaningful_tokens_from_index_title,
)


NOW = datetime(2026, 10, 15, tzinfo=timezone.utc)
ROOT = Path(__file__).resolve().parents[1]


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

    # These values intentionally represent the *persisted normalized mapping index*, not raw
    # source titles. The local advisory helper consumes this index and must not import historical
    # mapping generations merely to reinterpret it.
    rows = [
        ("k1", "kalshi", 1, "china blockade taiwan 2027", "official confirmation", "2027-12-31", "2027-12-31"),
        ("p1", "polymarket", 3, "china blockade taiwan 2027", "official confirmation", "2027-12-31", "2027-12-31"),
        ("p2", "polymarket", 2, "china blockade taiwan 2028", "different source rules", "2028-12-31", "2028-12-31"),
        ("p3", "polymarket", 1, "us recession 2027", "nber recession", "2027-12-31", "2027-12-31"),
        ("k2", "kalshi", 1, "oil price 100 2027", "settlement price", "2027-12-31", "2027-12-31"),
    ]
    for contract_id, source_id, version, title, rules, deadline, bucket in rows:
        tokens = sorted(_meaningful_tokens_from_index_title(title))
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


def test_local_advisory_pair_discovery_preserves_v8_blocking_semantics_without_remote_pairs():
    changed = [{"source_contract_id": "k1"}]
    con = _setup()
    try:
        actual, stats = _candidate_pair_rows_local(con, changed)

        # k1/p1 enters through exact title+deadline and shared tokens. k1/p2 enters through
        # rare-token blocking. k1/p3 shares only one token while both titles have >2 meaningful
        # tokens, so the v8 overlap threshold removes it. k2 is same-source and is excluded.
        assert _signature(actual) == [
            ("k1", "p1", 4),
            ("k1", "p2", 3),
        ]
        assert stats["pairs_blocked"] == 3
        assert stats["pairs_evaluated"] == 2
        assert stats["changed_token_rows"] == 4
        assert stats["token_frequency_rows"] == 4
        assert stats["execution_location"] == "enrichment_flight_local_python"
        assert stats["frequency_scope"] == "changed_tokens_only_local"

        # The isolated execution must not reconstruct v8's remote pair materialization.
        count = con.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name='_mapping_pair_ids'"
        ).fetchone()[0]
        assert count == 0
    finally:
        con.close()


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


def test_advisory_helper_has_no_top_level_mapping_generation_imports():
    body = (ROOT / "institutional_mapping_advisory_v2.py").read_text()
    prefix = body.split("def run_mapping_pass", 1)[0]
    for generation in [
        "institutional_mapping_v2",
        "institutional_mapping_v3",
        "institutional_mapping_v4",
        "institutional_mapping_v5",
        "institutional_mapping_v6",
        "institutional_mapping_v7",
        "institutional_mapping_v8",
    ]:
        assert f"import {generation}" not in prefix
