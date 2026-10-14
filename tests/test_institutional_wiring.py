from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def text(name: str) -> str:
    return (ROOT / name).read_text()


def test_all_runtime_migration_paths_use_shared_ledger():
    migration_runner = text("institutional_migrations.py")
    assert '"institutional_schema_013.sql"' in migration_runner
    assert "er_schema_migrations" in migration_runner

    for name in ["institutional_service.py", "institutional_worker.py", "migrate_institutional.py"]:
        body = text(name)
        assert "from institutional_migrations import ensure_schema" in body, (
            f"{name} must use the shared restart-safe migration ledger"
        )


def test_runtime_collectors_use_verified_archive_full_semantics_and_versioned_graph():
    for name in ["institutional_service.py", "institutional_worker.py"]:
        body = text(name)
        assert "from institutional_archive_v2 import archive_pending" in body
        assert "from institutional_ingest_v3 import run_once" in body
        assert "from institutional_mapping_v3 import run_mapping_pass" in body
        assert "from institutional_mapping_drift import persist_mapping_drift" in body
        assert "from institutional_graph_v2 import run_graph_pass" in body
        assert "from institutional_reference_versions import sync_reference_versions" in body
        assert "from institutional_qa_v2 import run_qa" in body
        assert "RAW_ARCHIVE_MAX_ROWS" in body
        assert "RAW_ARCHIVE_BATCH_SIZE" in body


def test_service_exposes_curves_and_readiness():
    body = text("institutional_service.py")
    assert "institutional_curve_api" in body
    assert "institutional_readiness_api" in body
    assert "app.include_router(curve_router)" in body
    assert "app.include_router(readiness_router)" in body


def test_admin_release_path_uses_v3_point_in_time_generator():
    admin = text("institutional_admin_api.py")
    body = text("institutional_exports_v3.py")
    assert "from institutional_exports_v3 import generate_release" in admin
    assert "from institutional_readiness_v2 import evaluate_readiness" in body
    assert '"market_observations":' in body and "WHERE received_at<=?" in body
    assert "legacy_partial" in body
    assert "full_contract_semantic_versions" in body
    assert "manifest_hash" in body
    assert "later closure fields masked" in body


def test_latest_schema_contains_point_in_time_and_semantic_provenance():
    schema9 = text("institutional_schema_009.sql")
    for value in [
        "er_event_curve_as_known",
        "er_event_curve_current_taxonomy",
        "er_served_canonical_events",
        "er_event_relationship_evidence",
        "manifest_hash",
    ]:
        assert value in schema9

    schema11 = text("institutional_schema_011.sql")
    for value in ["er_source_versions", "er_entity_versions", "er_instrument_versions"]:
        assert value in schema11

    schema12 = text("institutional_schema_012.sql")
    for value in [
        "er_contract_semantic_versions",
        "event_raw_snapshot_id",
        "market_raw_snapshot_id",
        "metadata_completeness",
    ]:
        assert value in schema12


def test_raw_bundle_schema_exists():
    body = text("institutional_schema_008.sql")
    for field in ["archive_member", "archive_bundle_hash", "bundle_byte_size", "ingestion_run_id"]:
        assert field in body
