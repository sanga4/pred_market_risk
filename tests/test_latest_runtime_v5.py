from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def text(name: str) -> str:
    return (ROOT / name).read_text()


def test_extension_migration_ledger_registers_every_schema_through_047():
    body = text("institutional_migrations_v2.py")
    for migration in range(26, 48):
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
