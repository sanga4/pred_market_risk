from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def text(name: str) -> str:
    return (ROOT / name).read_text()


def test_all_runtime_migration_paths_use_shared_ledger():
    base_runner = text("institutional_migrations.py")
    extension_runner = text("institutional_migrations_v2.py")
    assert '"institutional_schema_025.sql"' in base_runner
    for migration in range(26, 32):
        assert f'"institutional_schema_{migration:03d}.sql"' in extension_runner
    assert "er_schema_migrations" in base_runner
    assert "er_schema_migrations" in extension_runner
    assert "ensure_base_schema" in extension_runner

    for name in ["institutional_service.py", "institutional_worker.py", "migrate_institutional.py"]:
        body = text(name)
        assert "from institutional_migrations_v2 import ensure_schema" in body, (
            f"{name} must use the shared restart-safe migration ledger extension"
        )


def test_runtime_collectors_use_promoted_incremental_pipeline():
    for name in ["institutional_service.py", "institutional_worker.py"]:
        body = text(name)
        assert "from institutional_archive_v2 import archive_pending" in body
        assert "from institutional_ingest_v8 import run_once" in body
        assert "from institutional_mapping_drift_v2 import persist_mapping_drift" in body
        assert "from institutional_lifecycle import sync_event_lifecycle" in body
        assert "from institutional_ontology_v2 import run_ontology_pass" in body
        assert "from institutional_graph_v3 import run_graph_pass" in body
        assert "from institutional_reference_versions_v2 import sync_reference_versions" in body
        assert "from institutional_qa_v4 import run_qa" in body
        assert "from institutional_run_recovery import recover_abandoned_runs" in body
        assert "RAW_ARCHIVE_MAX_ROWS" in body
        assert "RAW_ARCHIVE_BATCH_SIZE" in body
        assert '"timing_seconds"' in body

    worker = text("institutional_worker.py")
    service = text("institutional_service.py")
    assert "from institutional_mapping_v7 import run_mapping_pass" in worker
    assert "from institutional_mapping_v6 import run_mapping_pass" in service


def test_mapping_v7_is_registered_and_staged_worker_only_for_acceptance():
    body = text("institutional_mapping_v7.py")
    schema29 = text("institutional_schema_029.sql")
    assert "mapping_rules_v7_python_overlap" in body
    assert "mapping_rules_v7_python_overlap" in schema29
    assert "python_meaningful_tokens_v1" in body
    assert "from institutional_mapping_v7 import run_mapping_pass" in text("institutional_worker.py")
    assert "from institutional_mapping_v6 import run_mapping_pass" in text("institutional_service.py")


def test_mapping_v8_is_registered_but_not_promoted_before_acceptance():
    body = text("institutional_mapping_v8.py")
    schema30 = text("institutional_schema_030.sql")
    assert "mapping_rules_v8_changed_token_frequency" in body
    assert "mapping_rules_v8_changed_token_frequency" in schema30
    assert "frequency_scope" in body and "changed_tokens_only" in body
    assert "from institutional_mapping_v8 import run_mapping_pass" not in text("institutional_worker.py")
    assert "from institutional_mapping_v8 import run_mapping_pass" not in text("institutional_service.py")


def test_v8_runtime_uses_direct_temp_persistence_and_set_based_presence():
    body = text("institutional_ingest_v8.py")
    assert "from institutional_persist_registered import persist_registered_batch" in body
    assert "from institutional_presence_v2 import reconcile_presence" in body
    assert "pa.default_memory_pool().release_unused()" in body
    assert "rss_max_mb" in body
    assert "BEGIN TRANSACTION" in body and "ROLLBACK" in body


def test_ingest_v9_is_registered_but_not_promoted_before_acceptance():
    body = text("institutional_ingest_v9.py")
    presence = text("institutional_presence_v3.py")
    schema31 = text("institutional_schema_031.sql")
    assert "change_data_capture_v9_streamed" in body
    assert "presence_reconciliation_v3_staged_seen" in presence
    assert "change_data_capture_v9_streamed" in schema31
    assert "presence_reconciliation_v3_staged_seen" in schema31
    assert "itertools.islice" in body
    assert "stage_seen_from_registered_batch" in body
    assert "collect_seen_ids=False" in body
    assert "from institutional_ingest_v9 import run_once" not in text("institutional_worker.py")
    assert "from institutional_ingest_v9 import run_once" not in text("institutional_service.py")


def test_ops_review_uses_current_serving_mapping_semantics():
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

    schema26 = text("institutional_schema_026.sql")
    for value in ["reference_versions_v2_set_based", "mapping_drift_v2_set_based", "institutional_qa_v4_set_based"]:
        assert value in schema26

    schema27 = text("institutional_schema_027.sql")
    assert "change_data_capture_v7_bounded" in schema27

    schema28 = text("institutional_schema_028.sql")
    for value in ["change_data_capture_v8_direct_temp", "presence_reconciliation_v2_set_based"]:
        assert value in schema28

    schema29 = text("institutional_schema_029.sql")
    assert "mapping_rules_v7_python_overlap" in schema29

    schema30 = text("institutional_schema_030.sql")
    assert "mapping_rules_v8_changed_token_frequency" in schema30

    schema31 = text("institutional_schema_031.sql")
    for value in ["change_data_capture_v9_streamed", "presence_reconciliation_v3_staged_seen"]:
        assert value in schema31


def test_raw_bundle_schema_exists():
    body = text("institutional_schema_008.sql")
    for field in ["archive_member", "archive_bundle_hash", "bundle_byte_size", "ingestion_run_id"]:
        assert field in body
