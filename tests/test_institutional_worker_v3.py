from pathlib import Path

import institutional_worker_v3 as worker

ROOT = Path(__file__).resolve().parents[1]


def text(name: str) -> str:
    return (ROOT / name).read_text()


def test_pre_readiness_recovery_runs_after_identity_before_readiness(monkeypatch):
    calls = []

    monkeypatch.setattr(worker, "run_once", lambda: calls.append("ingest") or [{"status": "success"}])
    monkeypatch.setattr(worker, "archive_pending", lambda **kwargs: calls.append("archive") or {"configured": False, "failed": 0})
    monkeypatch.setattr(worker, "run_identity_pass", lambda: calls.append("identity") or {"status": "success"})
    monkeypatch.setattr(worker, "recover_abandoned_runs", lambda: calls.append("recovery") or {"recovered": 1})
    monkeypatch.setattr(worker, "evaluate_observed_readiness", lambda **kwargs: calls.append("observed") or {"ready": True, "failed_controls": []})
    monkeypatch.setattr(worker, "evaluate_identity_readiness", lambda: calls.append("identity_readiness") or {"ready": True, "failed_controls": []})

    result = worker.cycle()

    assert calls == ["ingest", "archive", "identity", "recovery", "observed", "identity_readiness"]
    assert result["pre_readiness_recovery"] == {"recovered": 1}
    assert result["runtime_topology"] == "hot_path_v3_v11_identity_recovery"


def test_worker_v3_is_versioned_without_rewriting_prior_worker():
    body = text("institutional_worker_v3.py")
    schema = text("institutional_schema_041.sql")
    migrations = text("institutional_migrations_v2.py")

    assert "hot_path_v3_v11_identity_recovery" in body
    assert "pre_readiness_recovery" in body
    assert "hot_path_v3_v11_identity_recovery" in schema
    assert '"institutional_schema_041.sql"' in migrations
    assert "pre_readiness_recovery" not in text("institutional_worker.py")
