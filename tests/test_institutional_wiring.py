from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def text(name: str) -> str:
    return (ROOT / name).read_text()


def test_all_runtime_migration_paths_use_shared_ledger():
    migration_runner = text("institutional_migrations.py")
    assert '"institutional_schema_025.sql"' in migration_runner
    assert "er_schema_migrations" in migration_runner

    for name in ["institutional_service.py", "institutional_worker.py", "migrate_institutional.py"]:
        body = text(name)
        assert "from institutional_migrations import ensure_schema" in body, (
            f"{name} must use the shared restart-safe migration ledger"
        )


def test_runtime_collectors_use_promoted_incremental_pipeline():
    for name in ["institutional_service.py", "institutional_worker.py"]:
        body = text(name)
        assert "from institutional_archive_v2 import archive_pending" in body
        assert "from institutional_ingest_v6 import run_once" in body
        assert "from institutional_mapping_v6 import run_mapping_pass" in body
        assert "from institutional_mapping_drift import persist_mapping_drift" in body
        assert "from institutional_lifecycle import sync_event_lifecycle" in body
        assert "from institutional_ontology_v2 import run_ontology_pass" in body
        assert "from institutional_graph_v3 import run_graph_pass" in body
        assert "from institutional_reference_versions_v2 import sync_reference_versions" in body
        assert "from institutional_qa_v3 import run_qa" in body
        assert "from institutional_run_recovery import recover_abandoned_runs" in body
        assert "RAW_ARCHIVE_MAX_ROWS" in body
        assert "RAW_ARCHIVE_BATCH_SIZE" in body
        assert '"timing_seconds"' in body


def test_ops_review_uses_current_mapping_semantics():
    body = text("institutional_ops_api.py")
    assert "from institutional_mapping_v6 import DIRECTIONS, RELATIONSHIP_TYPES, review_candidate" in body


def test_service_exposes_curves_readiness_and_lossless_changefeed():
    body = text("institutional_service.py")
    assert "institutional_curve_api" in body
    assert "institutional_changefeed_api" in body
    assert "institutional_readiness_api" in body
    assert 'X-EventRisk-Schema-Version"] = "v2.5"' in body
    assert "app.include_router(curve_router)" in body
    assert "app.include_router(changefeed_router)" in body
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


def test_latest_schema_contains_point_in_time_semantic_presence_mapping_and_ops_provenance():
    schema9 = text("institutional_schema_009.sql")
    for value in [
        "er_event_curve_as_known", "er_event_curve_current_taxonomy", "er_served_canonical_events",
        "er_event_relationship_evidence", "manifest_hash",
    ]:
        assert value in schema9

    schema11 = text("institutional_schema_011.sql")
    for value in ["er_source_versions", "er_entity_versions", "er_instrument_versions"]:
        assert value in schema11

    schema12 = text("institutional_schema_012.sql")
    for value in [
        "er_contract_semantic_versions", "event_raw_snapshot_id", "market_raw_snapshot_id", "metadata_completeness",
    ]:
        assert value in schema12

    schema14 = text("institutional_schema_014.sql")
    for value in [
        "er_contract_presence_state", "er_contract_presence_transitions", "universe_complete", "collection_scope",
    ]:
        assert value in schema14

    schema16 = text("institutional_schema_016.sql")
    for value in ["set_based_ingest_v4", "compound_cursor_v2_1", "compound_cursor_changefeed_v1"]:
        assert value in schema16

    schema17 = text("institutional_schema_017.sql")
    assert "ingestion_run_recovery_v1" in schema17

    schema18 = text("institutional_schema_018.sql")
    for value in ["er_mapping_passes", "er_mapping_contract_state", "mapping_rules_v4_batched"]:
        assert value in schema18

    schema19 = text("institutional_schema_019.sql")
    assert "parallel_fetch_serial_write_v5" in schema19

    schema20 = text("institutional_schema_020.sql")
    for value in ["change_data_capture_v6", "state_change_points_v1"]:
        assert value in schema20

    schema21 = text("institutional_schema_021.sql")
    for value in ["er_mapping_contract_index", "er_mapping_token_index", "mapping_rules_v5_indexed"]:
        assert value in schema21

    schema22 = text("institutional_schema_022.sql")
    for value in ["er_ontology_runs", "er_ontology_event_state", "event_ontology_v2_batched"]:
        assert value in schema22

    schema23 = text("institutional_schema_023.sql")
    for value in ["er_mapping_candidate_versions", "er_latest_mapping_candidate_versions"]:
        assert value in schema23

    schema24 = text("institutional_schema_024.sql")
    assert "mapping_rules_v6_indexed_history" in schema24

    schema25 = text("institutional_schema_025.sql")
    for value in ["er_graph_runs", "er_graph_interval_state", "graph_execution_v3_incremental"]:
        assert value in schema25


def test_raw_bundle_schema_exists():
    body = text("institutional_schema_008.sql")
    for field in ["archive_member", "archive_bundle_hash", "bundle_byte_size", "ingestion_run_id"]:
        assert field in body
