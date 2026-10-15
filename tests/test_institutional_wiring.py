from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def text(name: str) -> str:
    return (ROOT / name).read_text()


def test_all_runtime_migration_paths_use_shared_ledger():
    base_runner = text("institutional_migrations.py")
    extension_runner = text("institutional_migrations_v2.py")
    assert '"institutional_schema_025.sql"' in base_runner
    for migration in range(26, 39):
        assert f'"institutional_schema_{migration:03d}.sql"' in extension_runner
    assert "er_schema_migrations" in base_runner
    assert "er_schema_migrations" in extension_runner
    assert "ensure_base_schema" in extension_runner

    for name in [
        "institutional_service.py", "institutional_worker.py",
        "institutional_enrichment_worker.py", "migrate_institutional.py",
    ]:
        body = text(name)
        assert "from institutional_migrations_v2 import ensure_schema" in body, (
            f"{name} must use the shared restart-safe migration ledger extension"
        )


def test_hot_runtime_uses_promoted_v11_identity_pipeline_only():
    for name in ["institutional_service.py", "institutional_worker.py"]:
        body = text(name)
        assert "from institutional_archive_v2 import archive_pending" in body
        assert "from institutional_ingest_v11 import run_once" in body
        assert "from institutional_mapping_identity_v1 import run_identity_pass" in body
        assert "evaluate_observed_readiness" in body
        assert "evaluate_identity_readiness" in body
        assert "from institutional_run_recovery import recover_abandoned_runs" in body
        assert "RAW_ARCHIVE_MAX_ROWS" in body
        assert "RAW_ARCHIVE_BATCH_SIZE" in body
        assert '"timing_seconds"' in body
        assert "hot_path_v2_v11_identity" in body
        for forbidden in [
            "from institutional_ingest_v10 import run_once",
            "institutional_mapping_v8", "institutional_mapping_v7", "institutional_mapping_v6",
            "institutional_ontology_v2", "institutional_graph_v3",
            "institutional_mapping_drift_v2", "institutional_reference_versions_v2",
            "institutional_qa_v4", "sync_event_lifecycle",
        ]:
            assert forbidden not in body


def test_enrichment_runtime_owns_advisory_normalization_and_full_qa():
    body = text("institutional_enrichment_worker.py")
    for required in [
        "from institutional_mapping_v8 import run_mapping_pass",
        "from institutional_lifecycle_v2 import sync_event_lifecycle",
        "from institutional_ontology_v2 import run_ontology_pass",
        "from institutional_graph_v3 import run_graph_pass",
        "from institutional_reference_versions_v2 import sync_reference_versions",
        "from institutional_mapping_drift_v2 import persist_mapping_drift",
        "from institutional_qa_v4 import run_qa",
        "evaluate_enrichment_readiness",
        "enrichment_path_v1_async",
    ]:
        assert required in body
    assert "from institutional_lifecycle import sync_event_lifecycle" not in body
    assert "institutional_ingest_v10" not in body
    assert "institutional_ingest_v11" not in body
    assert "institutional_mapping_identity_v1" not in body


def test_mapping_v7_remains_historical_and_not_promoted():
    body = text("institutional_mapping_v7.py")
    schema29 = text("institutional_schema_029.sql")
    assert "mapping_rules_v7_python_overlap" in body
    assert "mapping_rules_v7_python_overlap" in schema29
    assert "python_meaningful_tokens_v1" in body
    assert "from institutional_mapping_v7 import run_mapping_pass" not in text("institutional_worker.py")
    assert "from institutional_mapping_v7 import run_mapping_pass" not in text("institutional_service.py")
    assert "from institutional_mapping_v7 import run_mapping_pass" not in text("institutional_enrichment_worker.py")


def test_mapping_v8_is_promoted_only_to_advisory_enrichment():
    body = text("institutional_mapping_v8.py")
    schema30 = text("institutional_schema_030.sql")
    assert "mapping_rules_v8_changed_token_frequency" in body
    assert "mapping_rules_v8_changed_token_frequency" in schema30
    assert "frequency_scope" in body and "changed_tokens_only" in body
    assert "from institutional_mapping_v8 import run_mapping_pass" in text("institutional_enrichment_worker.py")
    assert "from institutional_mapping_v8 import run_mapping_pass" not in text("institutional_worker.py")
    assert "from institutional_mapping_v8 import run_mapping_pass" not in text("institutional_service.py")


def test_v8_runtime_uses_direct_temp_persistence_and_set_based_presence():
    body = text("institutional_ingest_v8.py")
    assert "from institutional_persist_registered import persist_registered_batch" in body
    assert "from institutional_presence_v2 import reconcile_presence" in body
    assert "pa.default_memory_pool().release_unused()" in body
    assert "rss_max_mb" in body
    assert "BEGIN TRANSACTION" in body and "ROLLBACK" in body


def test_ingest_v9_remains_historical_after_v11_promotion():
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


def test_identity_mapping_lane_is_promoted_to_hot_runtime_and_excludes_fuzzy():
    body = text("institutional_mapping_identity_v1.py")
    schema32 = text("institutional_schema_032.sql")
    for value in ["identity_mapping_v1_strict_exact", "er_mapping_identity_state", "er_mapping_identity_work_queue"]:
        assert value in body or value in schema32
    assert "fuzzy_review_candidates_excluded" in body
    assert "normalized_title=l.normalized_title" in body
    assert "normalized_rules=l.normalized_rules" in body
    assert "from institutional_mapping_identity_v1 import run_identity_pass" in text("institutional_worker.py")
    assert "from institutional_mapping_identity_v1 import run_identity_pass" in text("institutional_service.py")
    assert "institutional_mapping_identity_v1" not in text("institutional_enrichment_worker.py")


def test_ingest_v10_current_state_pointers_remain_v11_base_not_runtime_entrypoint():
    body = text("institutional_ingest_v10.py")
    schema33 = text("institutional_schema_033.sql")
    for value in ["er_raw_current_snapshots", "er_market_current_state", "change_data_capture_v10_stateful"]:
        assert value in schema33 or value in body
    assert "LEFT JOIN er_raw_current_snapshots" in body
    assert "LEFT JOIN er_market_current_state" in body
    assert "er_latest_market_observations" not in body
    assert "collect_seen_ids=False" in body
    assert "from institutional_ingest_v10 import run_once" not in text("institutional_worker.py")
    assert "from institutional_ingest_v10 import run_once" not in text("institutional_service.py")
    assert "institutional_ingest_v10" not in text("institutional_enrichment_worker.py")
    assert "import institutional_ingest_v10 as v10" in text("institutional_ingest_v11.py")


def test_ingest_v11_bounded_prefetch_is_promoted_to_hot_runtime():
    body = text("institutional_ingest_v11.py")
    schema36 = text("institutional_schema_036.sql")
    for value in ["change_data_capture_v11_prefetched", "queue.Queue(maxsize=1)", "streamed_state_pointer_cdc_prefetch1"]:
        assert value in body or value in schema36
    assert "from institutional_ingest_v11 import run_once" in text("institutional_worker.py")
    assert "from institutional_ingest_v11 import run_once" in text("institutional_service.py")
    assert "institutional_ingest_v11" not in text("institutional_enrichment_worker.py")


def test_lifecycle_v2_is_set_based_and_promoted_only_to_enrichment():
    body = text("institutional_lifecycle_v2.py")
    schema38 = text("institutional_schema_038.sql")
    assert "canonical_lifecycle_v2_set_based" in body
    assert "canonical_lifecycle_v2_set_based" in schema38
    assert "GROUP BY m.canonical_event_id" in body
    assert "BEGIN TRANSACTION" in body and "ROLLBACK" in body
    assert "from institutional_lifecycle_v2 import sync_event_lifecycle" in text("institutional_enrichment_worker.py")
    assert "institutional_lifecycle_v2" not in text("institutional_worker.py")
    assert "institutional_lifecycle_v2" not in text("institutional_service.py")


def test_layered_readiness_is_registered_and_api_exposes_each_lane():
    body = text("institutional_readiness_v4.py")
    api = text("institutional_readiness_api.py")
    schema34 = text("institutional_schema_034.sql")
    assert "institutional_readiness_v4_layered" in body
    assert "institutional_readiness_v4_layered" in schema34
    assert "evaluate_observed_readiness" in api
    assert "evaluate_identity_readiness" in api
    assert "evaluate_enrichment_readiness" in api
    for path in ["/v1/readiness/observed", "/v1/readiness/identity", "/v1/readiness/enrichment"]:
        assert path in api


def test_layered_runtime_topology_is_registered_and_v11_promoted():
    schema35 = text("institutional_schema_035.sql")
    for value in ["hot_path_v1_v10_identity", "enrichment_path_v1_async", "institutional_enrichment_worker.py"]:
        assert value in schema35
    schema37 = text("institutional_schema_037.sql")
    assert "hot_path_v2_v11_identity" in schema37


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

    schema32 = text("institutional_schema_032.sql")
    for value in ["er_mapping_identity_state", "er_mapping_identity_work_queue", "identity_mapping_v1_strict_exact"]:
        assert value in schema32

    schema33 = text("institutional_schema_033.sql")
    for value in ["er_raw_current_snapshots", "er_market_current_state", "change_data_capture_v10_stateful"]:
        assert value in schema33

    schema34 = text("institutional_schema_034.sql")
    assert "institutional_readiness_v4_layered" in schema34

    schema35 = text("institutional_schema_035.sql")
    for value in ["hot_path_v1_v10_identity", "enrichment_path_v1_async"]:
        assert value in schema35

    schema36 = text("institutional_schema_036.sql")
    assert "change_data_capture_v11_prefetched" in schema36

    schema37 = text("institutional_schema_037.sql")
    assert "hot_path_v2_v11_identity" in schema37

    schema38 = text("institutional_schema_038.sql")
    assert "canonical_lifecycle_v2_set_based" in schema38


def test_raw_bundle_schema_exists():
    body = text("institutional_schema_008.sql")
    for field in ["archive_member", "archive_bundle_hash", "bundle_byte_size", "ingestion_run_id"]:
        assert field in body
