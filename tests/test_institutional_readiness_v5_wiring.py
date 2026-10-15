from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def text(name: str) -> str:
    return (ROOT / name).read_text()


def test_readiness_v5_registered_and_wired_everywhere():
    schema = text("institutional_schema_039.sql")
    migrations = text("institutional_migrations_v2.py")
    assert "institutional_readiness_v5_completed_run_basis" in schema
    assert '"institutional_schema_039.sql"' in migrations

    assert "from institutional_readiness_v5 import evaluate_identity_readiness, evaluate_observed_readiness" in text("institutional_worker.py")
    assert "from institutional_readiness_v5 import evaluate_identity_readiness, evaluate_observed_readiness" in text("institutional_service.py")
    assert "from institutional_readiness_v5 import evaluate_enrichment_readiness" in text("institutional_enrichment_worker.py")
    assert "from institutional_readiness_v5 import (" in text("institutional_readiness_api.py")

    assert "from institutional_readiness_v4 import evaluate_identity_readiness, evaluate_observed_readiness" not in text("institutional_worker.py")
    assert "from institutional_readiness_v4 import evaluate_identity_readiness, evaluate_observed_readiness" not in text("institutional_service.py")
