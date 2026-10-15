from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def text(name: str) -> str:
    return (ROOT / name).read_text()


def test_extension_migration_ledger_registers_every_schema_through_049():
    body = text("institutional_migrations_v2.py")
    for migration in range(26, 50):
        assert f'"institutional_schema_{migration:03d}.sql"' in body
        assert (ROOT / f"institutional_schema_{migration:03d}.sql").exists()


def test_readiness_api_serves_transaction_clock_safe_v6():
    api = text("institutional_readiness_api.py")
    body = text("institutional_readiness_v6.py")
    assert "from institutional_readiness_v6 import" in api
    assert "ORDER BY started_at DESC, run_id DESC" in body
    assert "ORDER BY finished_at DESC" not in body
    assert "transaction_stable_finished_at_safe" in body


def test_hot_worker_v5_is_singleflight_and_uses_readiness_v6():
    body = text("institutional_worker_v5.py")
    schema46 = text("institutional_schema_046.sql")
    schema47 = text("institutional_schema_047.sql")
    assert "from institutional_ingest_v11 import run_once" in body
    assert "from institutional_mapping_identity_v1 import run_identity_pass" in body
    assert "from institutional_readiness_v6 import" in body
    assert "acquire_runtime_lease" in body and "release_runtime_lease" in body
    assert "hot_path_v5_v11_identity_singleflight_readiness6" in body
    assert "institutional_readiness_v6_started_run_basis" in schema46
    assert "hot_path_v5_v11_identity_singleflight_readiness6" in schema47


def test_enrichment_worker_v5_uses_snapshot_safe_qa_v8():
    worker = text("institutional_enrichment_worker_v5.py")
    qa = text("institutional_qa_v8.py")
    schema48 = text("institutional_schema_048.sql")
    assert "from institutional_qa_v8 import run_qa" in worker
    assert "ENRICHMENT_CORE_STABILITY_WAIT_SECONDS" in worker
    assert "enrichment_path_v5_snapshot_guarded_qa" in worker
    assert "identity_snapshot_state" in qa
    assert "check_latest_completed_full_universe_runs" in qa
    assert "institutional_qa_v8_snapshot_guard_completed_runs" in schema48


def test_enrichment_worker_v6_is_advisory_only_for_relationship_discovery():
    worker = text("institutional_enrichment_worker_v6.py")
    advisory = text("institutional_mapping_advisory_v1.py")
    schema49 = text("institutional_schema_049.sql")
    assert "from institutional_mapping_advisory_v1 import run_mapping_pass" in worker
    assert "identity_mutations_disabled" in worker
    assert "native_events_created" in worker and "auto_exact_mappings" in worker
    assert "no_identity_bootstrap" in advisory
    assert "no_auto_identity_merge" in advisory
    assert "mapping_advisory_v1_candidate_only" in schema49
    assert "enrichment_path_v6_advisory_only_snapshot_qa" in schema49
