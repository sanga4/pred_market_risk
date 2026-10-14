from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def text(name: str) -> str:
    return (ROOT / name).read_text()


def test_all_runtime_migration_paths_use_shared_ledger():
    migration_runner = text("institutional_migrations.py")
    assert '"institutional_schema_008.sql"' in migration_runner
    assert "er_schema_migrations" in migration_runner

    for name in ["institutional_service.py", "institutional_worker.py", "migrate_institutional.py"]:
        body = text(name)
        assert "from institutional_migrations import ensure_schema" in body, (
            f"{name} must use the shared restart-safe migration ledger"
        )


def test_runtime_collectors_use_verified_bundle_archive():
    for name in ["institutional_service.py", "institutional_worker.py"]:
        body = text(name)
        assert "from institutional_archive_v2 import archive_pending" in body
        assert "RAW_ARCHIVE_MAX_ROWS" in body
        assert "RAW_ARCHIVE_BATCH_SIZE" in body


def test_raw_bundle_schema_exists():
    body = text("institutional_schema_008.sql")
    for field in ["archive_member", "archive_bundle_hash", "bundle_byte_size", "ingestion_run_id"]:
        assert field in body
