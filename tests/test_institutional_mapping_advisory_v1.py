import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import db

ROOT = Path(__file__).resolve().parents[1]


def setup_database(monkeypatch, tmp_path):
    path = tmp_path / "mapping_advisory.duckdb"
    monkeypatch.setattr(db, "DB", str(path))
    os.environ["DB_FORCE_READ_WRITE"] = "true"
    from institutional_migrations import ensure_schema
    ensure_schema()
    return path


def seed_contract(con, contract_id, source_id, market_id, title, rules, close, received):
    event_raw_id = f"raw_event_{contract_id}"
    market_raw_id = f"raw_market_{contract_id}"
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


def test_advisory_pass_discovers_candidate_without_creating_identity(monkeypatch, tmp_path):
    db_path = setup_database(monkeypatch, tmp_path)
    received = datetime(2026, 10, 15, 12, 0, tzinfo=timezone.utc)
    close = datetime(2027, 12, 31, tzinfo=timezone.utc)
    title = "Will China blockade Taiwan before 2028?"
    rules = "Resolves YES if China begins a blockade of Taiwan before 2028."

    con = db.connect()
    try:
        seed_contract(con, "sc-k", "kalshi", "KX-TW", title, rules, close, received)
        seed_contract(con, "sc-p", "polymarket", "PM-TW", title, rules, close, received)
        assert con.execute("SELECT count(*) FROM er_canonical_events").fetchone()[0] == 0
        assert con.execute("SELECT count(*) FROM er_event_mappings").fetchone()[0] == 0
    finally:
        con.close()

    # Mapping generations historically use module-level compatibility hooks. Run the advisory lane
    # in a child process just like the dedicated enrichment execution environment so this behavioral
    # test cannot contaminate historical-generation tests in the pytest process.
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT)
    env["DB"] = str(db_path)
    env["DB_FORCE_READ_WRITE"] = "true"
    code = (
        "import json; from institutional_mapping_advisory_v1 import run_mapping_pass; "
        "print(json.dumps(run_mapping_pass(), default=str, sort_keys=True))"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code], cwd=ROOT, env=env,
        check=True, capture_output=True, text=True,
    )
    result = json.loads(proc.stdout.strip().splitlines()[-1])
    assert result["identity_mutations_disabled"] is True
    assert result["native_events_created"] == 0
    assert result["auto_exact_mappings"] == 0
    assert result["candidates_generated"] >= 1

    con = db.connect()
    try:
        assert con.execute("SELECT count(*) FROM er_canonical_events").fetchone()[0] == 0
        assert con.execute("SELECT count(*) FROM er_event_mappings").fetchone()[0] == 0
        assert con.execute("SELECT count(*) FROM er_mapping_candidates").fetchone()[0] >= 1
        assert con.execute("SELECT count(*) FROM er_mapping_candidate_versions").fetchone()[0] >= 1
    finally:
        con.close()


def test_advisory_runtime_is_versioned_and_owned_by_enrichment_v6():
    advisory = (ROOT / "institutional_mapping_advisory_v1.py").read_text()
    worker = (ROOT / "institutional_enrichment_worker_v6.py").read_text()
    schema = (ROOT / "institutional_schema_049.sql").read_text()
    migrations = (ROOT / "institutional_migrations_v2.py").read_text()
    assert "mapping_advisory_v1_candidate_only" in advisory
    assert "no_identity_bootstrap" in advisory
    assert "no_auto_identity_merge" in advisory
    assert "identity_mutations_disabled" in worker
    assert "enrichment_path_v6_advisory_only_snapshot_qa" in worker
    assert "mapping_advisory_v1_candidate_only" in schema
    assert '"institutional_schema_049.sql"' in migrations
