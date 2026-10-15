import os
from datetime import datetime, timezone

import db


def setup_database(monkeypatch, tmp_path):
    monkeypatch.setattr(db, "DB", str(tmp_path / "mapping-v8.duckdb"))
    os.environ["DB_FORCE_READ_WRITE"] = "true"
    from institutional_migrations_v2 import ensure_schema
    ensure_schema()


def row(contract_id, source_id, title, rules, close, version=1):
    return {
        "source_contract_id": contract_id,
        "source_id": source_id,
        "current_title": title,
        "current_resolution_rules": rules,
        "closes_at": close,
        "resolves_at": close,
        "semantic_version_no": version,
        "semantic_received_at": close,
    }


def test_v8_changed_token_frequency_matches_v7_pair_semantics(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    import institutional_mapping_v5 as v5
    import institutional_mapping_v7 as v7
    import institutional_mapping_v8 as v8

    close = datetime(2027, 12, 31, tzinfo=timezone.utc)
    all_rows = [
        row("k-a", "kalshi", "Will China blockade Taiwan before 2028?", "YES if blockade begins.", close),
        row("p-a", "polymarket", "Will China blockade Taiwan before 2028?", "YES if blockade begins.", close),
        row("p-b", "polymarket", "Will China blockade Taiwan shipping before 2028?", "YES if shipping stops.", close),
        row("k-b", "kalshi", "Will US CPI exceed 3 percent in 2027?", "YES if CPI exceeds 3 percent.", close),
        row("p-c", "polymarket", "Will US CPI exceed 4 percent in 2027?", "YES if CPI exceeds 4 percent.", close),
        # Unchanged contracts intentionally add global frequency to tokens used by changed rows.
        row("p-x", "polymarket", "Will China blockade another shipping route?", "YES if blockade begins.", close),
        row("k-x", "kalshi", "Will Taiwan shipping volumes rise?", "YES if volumes rise.", close),
    ]
    changed_rows = all_rows[:3]

    con = db.connect()
    try:
        v5._upsert_index_batch(con, all_rows)
        original_pairs, original_stats = v7._candidate_pair_rows(con, changed_rows)
        optimized_pairs, optimized_stats = v8._candidate_pair_rows(con, changed_rows)
    finally:
        con.close()

    original = {(r["left_id"], r["right_id"]) for r in original_pairs}
    optimized = {(r["left_id"], r["right_id"]) for r in optimized_pairs}
    assert optimized == original
    assert optimized_stats["pairs_blocked"] == original_stats["pairs_blocked"]
    assert optimized_stats["pairs_evaluated"] == original_stats["pairs_evaluated"]
    assert optimized_stats["frequency_scope"] == "changed_tokens_only"
    assert optimized_stats["changed_token_rows"] == optimized_stats["token_frequency_rows"]


def test_v8_no_changed_rows_avoids_frequency_scan(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    import institutional_mapping_v8 as v8

    con = db.connect()
    try:
        pairs, stats = v8._candidate_pair_rows(con, [])
    finally:
        con.close()

    assert pairs == []
    assert stats["pairs_blocked"] == 0
    assert stats["pairs_evaluated"] == 0
    assert stats["token_frequency_rows"] == 0
    assert stats["changed_token_rows"] == 0
    assert stats["frequency_scope"] == "changed_tokens_only"


def test_v8_methodology_registered(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    con = db.connect()
    try:
        assert con.execute(
            "SELECT count(*) FROM er_methodology_registry WHERE methodology_id='event_mapping' AND methodology_version='mapping_rules_v8_changed_token_frequency'"
        ).fetchone()[0] == 1
        assert con.execute(
            "SELECT count(*) FROM er_schema_migrations WHERE migration_name='institutional_schema_030.sql'"
        ).fetchone()[0] == 1
    finally:
        con.close()
