"""Restart-safe schema migration runner for the institutional data plane."""

from __future__ import annotations

import hashlib
from pathlib import Path

from db import connect

MIGRATION_FILES = [
    "institutional_schema.sql",
    "institutional_schema_002.sql",
    "institutional_schema_003.sql",
    "institutional_schema_004.sql",
    "institutional_schema_005.sql",
    "institutional_schema_006.sql",
    "institutional_schema_007.sql",
    "institutional_schema_008.sql",
    "institutional_schema_009.sql",
    "institutional_schema_010.sql",
    "institutional_schema_011.sql",
    "institutional_schema_012.sql",
    "institutional_schema_013.sql",
    "institutional_schema_014.sql",
    "institutional_schema_015.sql",
    "institutional_schema_016.sql",
    "institutional_schema_017.sql",
    "institutional_schema_018.sql",
    "institutional_schema_019.sql",
    "institutional_schema_020.sql",
    "institutional_schema_021.sql",
    "institutional_schema_022.sql",
    "institutional_schema_023.sql",
]


def _table_exists(con, name: str) -> bool:
    return bool(con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name=?", [name]).fetchone()[0])


def _view_exists(con, name: str) -> bool:
    return bool(con.execute("SELECT count(*) FROM information_schema.views WHERE table_name=?", [name]).fetchone()[0])


def _column_exists(con, relation: str, column: str) -> bool:
    return bool(con.execute(
        "SELECT count(*) FROM information_schema.columns WHERE table_name=? AND column_name=?",
        [relation, column],
    ).fetchone()[0])


def _method_exists(con, methodology_id: str, version: str) -> bool:
    if not _table_exists(con, "er_methodology_registry"):
        return False
    return bool(con.execute(
        "SELECT count(*) FROM er_methodology_registry WHERE methodology_id=? AND methodology_version=?",
        [methodology_id, version],
    ).fetchone()[0])


def _migration_appears_applied(con, name: str) -> bool:
    if name == "institutional_schema.sql":
        return all(_table_exists(con, table) for table in (
            "er_raw_snapshots", "er_source_contracts", "er_market_observations", "er_canonical_events",
        ))
    if name == "institutional_schema_002.sql":
        return _table_exists(con, "er_mapping_candidates") and _table_exists(con, "er_source_health") and _view_exists(con, "er_event_curve")
    if name == "institutional_schema_003.sql":
        return _table_exists(con, "er_raw_archive_objects") and _view_exists(con, "er_unarchived_raw_snapshots")
    if name == "institutional_schema_004.sql":
        return _table_exists(con, "er_contract_version_evidence") and _table_exists(con, "er_data_releases") and _view_exists(con, "er_contract_lineage")
    if name == "institutional_schema_005.sql":
        return _table_exists(con, "er_canonical_event_versions") and _table_exists(con, "er_methodology_registry") and _view_exists(con, "er_latest_canonical_event_versions")
    if name == "institutional_schema_006.sql":
        return (
            _column_exists(con, "er_mapping_candidates", "resolution_similarity")
            and _column_exists(con, "er_mapping_candidates", "resolution_compatible")
            and _column_exists(con, "er_mapping_candidates", "rules_available_both")
            and _view_exists(con, "er_pending_mapping_review")
        )
    if name == "institutional_schema_007.sql":
        return _column_exists(con, "er_raw_snapshots", "payload_storage") and _column_exists(con, "er_raw_snapshots", "payload_compacted_at")
    if name == "institutional_schema_008.sql":
        return (
            _column_exists(con, "er_raw_archive_objects", "archive_member")
            and _column_exists(con, "er_raw_archive_objects", "archive_bundle_hash")
            and _column_exists(con, "er_raw_archive_objects", "ingestion_run_id")
            and _column_exists(con, "er_raw_lineage_status", "archive_member")
        )
    if name == "institutional_schema_009.sql":
        return (
            _table_exists(con, "er_event_relationship_evidence")
            and _column_exists(con, "er_data_releases", "manifest_hash")
            and _column_exists(con, "er_mapping_candidates", "review_relationship_type")
            and _view_exists(con, "er_event_curve_as_known")
            and _view_exists(con, "er_served_canonical_events")
        )
    if name == "institutional_schema_010.sql":
        return _method_exists(con, "event_mapping", "mapping_rules_v3_reviewable")
    if name == "institutional_schema_011.sql":
        return (
            _table_exists(con, "er_source_versions")
            and _table_exists(con, "er_entity_versions")
            and _table_exists(con, "er_instrument_versions")
            and _view_exists(con, "er_latest_entity_versions")
        )
    if name == "institutional_schema_012.sql":
        return _table_exists(con, "er_contract_semantic_versions") and _view_exists(con, "er_latest_contract_semantic_versions")
    if name == "institutional_schema_013.sql":
        return _method_exists(con, "bulk_release", "institutional_exports_v3")
    if name == "institutional_schema_014.sql":
        return (
            _table_exists(con, "er_contract_presence_state")
            and _table_exists(con, "er_contract_presence_transitions")
            and _column_exists(con, "er_ingestion_runs", "collection_scope")
            and _view_exists(con, "er_contract_presence_status")
        )
    if name == "institutional_schema_015.sql":
        return _method_exists(con, "data_quality", "institutional_qa_v3") and _method_exists(
            con, "distribution_readiness", "institutional_readiness_v3"
        )
    if name == "institutional_schema_016.sql":
        return _method_exists(con, "source_ingestion", "set_based_ingest_v4") and _method_exists(
            con, "observation_stream", "compound_cursor_v2_1"
        )
    if name == "institutional_schema_017.sql":
        return _method_exists(con, "ingestion_run_recovery", "ingestion_run_recovery_v1")
    if name == "institutional_schema_018.sql":
        return (
            _table_exists(con, "er_mapping_passes")
            and _table_exists(con, "er_mapping_contract_state")
            and _view_exists(con, "er_mapping_work_queue")
            and _method_exists(con, "event_mapping", "mapping_rules_v4_batched")
        )
    if name == "institutional_schema_019.sql":
        return _method_exists(con, "source_ingestion", "parallel_fetch_serial_write_v5")
    if name == "institutional_schema_020.sql":
        return _method_exists(con, "source_ingestion", "change_data_capture_v6") and _method_exists(
            con, "observation_sampling", "state_change_points_v1"
        )
    if name == "institutional_schema_021.sql":
        return (
            _table_exists(con, "er_mapping_contract_index")
            and _table_exists(con, "er_mapping_token_index")
            and _view_exists(con, "er_mapping_work_queue_v5")
            and _column_exists(con, "er_mapping_candidates", "left_semantic_version_no")
            and _column_exists(con, "er_mapping_candidates", "right_semantic_version_no")
            and _method_exists(con, "event_mapping", "mapping_rules_v5_indexed")
            and _method_exists(con, "mapping_pass_recovery", "mapping_pass_recovery_v1")
        )
    if name == "institutional_schema_022.sql":
        return (
            _table_exists(con, "er_ontology_runs")
            and _table_exists(con, "er_ontology_event_state")
            and _view_exists(con, "er_ontology_work_queue_v2")
            and _method_exists(con, "event_ontology", "event_ontology_v2_batched")
            and _method_exists(con, "ontology_run_recovery", "ontology_run_recovery_v1")
        )
    if name == "institutional_schema_023.sql":
        return (
            _table_exists(con, "er_mapping_candidate_versions")
            and _view_exists(con, "er_latest_mapping_candidate_versions")
            and _method_exists(con, "mapping_candidate_history", "mapping_candidate_versions_v1")
        )
    return False


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def ensure_schema() -> dict:
    root = Path(__file__).parent
    con = connect()
    applied_now: list[str] = []
    bootstrapped: list[str] = []
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS er_schema_migrations (
              migration_name VARCHAR PRIMARY KEY,
              content_sha256 VARCHAR NOT NULL,
              applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              bootstrapped BOOLEAN NOT NULL DEFAULT FALSE
            )
        """)
        if con.execute("SELECT count(*) FROM er_schema_migrations").fetchone()[0] == 0:
            for name in MIGRATION_FILES:
                sql = (root / name).read_text()
                if _migration_appears_applied(con, name):
                    con.execute("""
                        INSERT INTO er_schema_migrations(migration_name,content_sha256,bootstrapped)
                        VALUES (?, ?, TRUE) ON CONFLICT(migration_name) DO NOTHING
                    """, [name, _sha256(sql)])
                    bootstrapped.append(name)

        for name in MIGRATION_FILES:
            sql = (root / name).read_text()
            checksum = _sha256(sql)
            row = con.execute("SELECT content_sha256 FROM er_schema_migrations WHERE migration_name=?", [name]).fetchone()
            if row is not None:
                if row[0] != checksum:
                    raise RuntimeError(
                        f"Institutional migration checksum drift for {name}: applied SQL differs from repository SQL. "
                        "Add a new migration instead of editing an applied migration."
                    )
                continue
            con.execute("BEGIN TRANSACTION")
            try:
                con.execute(sql)
                con.execute("""
                    INSERT INTO er_schema_migrations(migration_name,content_sha256,bootstrapped)
                    VALUES (?, ?, FALSE)
                """, [name, checksum])
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise
            applied_now.append(name)
        total = con.execute("SELECT count(*) FROM er_schema_migrations").fetchone()[0]
        return {"migration_count": int(total), "applied_now": applied_now, "bootstrapped": bootstrapped}
    finally:
        con.close()
