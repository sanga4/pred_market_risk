from __future__ import annotations

import pytest

import institutional_worker_v7 as worker


class FakeLease:
    acquired = True

    def as_dict(self):
        return {"acquired": True, "lease_name": "hot_ingestion"}


def test_write_cycle_waits_for_parallel_ingest_before_identity(monkeypatch):
    order = []

    monkeypatch.setattr(
        worker,
        "run_once",
        lambda: order.append("sources_terminal") or [
            {"source_id": "kalshi", "status": "success"},
            {"source_id": "polymarket", "status": "success"},
        ],
    )
    monkeypatch.setattr(worker, "archive_pending", lambda **kwargs: order.append("archive") or {"configured": False, "failed": 0})
    monkeypatch.setattr(worker, "run_identity_pass", lambda: order.append("identity") or {"status": "success"})
    monkeypatch.setattr(worker, "recover_abandoned_runs", lambda: order.append("recovery") or {"recovered": 0})

    result = worker.write_cycle()
    assert order == ["sources_terminal", "archive", "identity", "recovery"]
    assert result["identity"]["status"] == "success"


def test_write_cycle_fails_before_identity_when_any_source_failed(monkeypatch):
    called = []
    monkeypatch.setattr(
        worker,
        "run_once",
        lambda: [
            {"source_id": "kalshi", "status": "success"},
            {"source_id": "polymarket", "status": "failed", "error": "conflict"},
        ],
    )
    monkeypatch.setattr(worker, "run_identity_pass", lambda: called.append("identity"))

    with pytest.raises(RuntimeError, match="source ingestions failed"):
        worker.write_cycle()
    assert called == []


def test_worker_releases_global_lease_before_readiness(monkeypatch):
    order = []
    monkeypatch.setattr(worker, "ensure_schema", lambda: {"ok": True})
    monkeypatch.setattr(worker, "recover_abandoned_runs", lambda: {"recovered": 0})
    monkeypatch.setattr(worker, "acquire_runtime_lease", lambda *a, **k: FakeLease())
    monkeypatch.setattr(worker, "release_runtime_lease", lambda lease: order.append("release") or {"released": True})
    monkeypatch.setattr(worker, "write_cycle", lambda: order.append("write") or {"timing_seconds": {}})
    monkeypatch.setattr(worker, "readiness_cycle", lambda: order.append("readiness") or {
        "observed_readiness": {"ready": True},
        "identity_readiness": {"ready": True},
        "timing_seconds": {},
    })

    worker.main()
    assert order == ["write", "release", "readiness"]


def test_worker_releases_lease_when_parallel_write_cycle_raises(monkeypatch):
    order = []
    monkeypatch.setattr(worker, "ensure_schema", lambda: {"ok": True})
    monkeypatch.setattr(worker, "recover_abandoned_runs", lambda: {"recovered": 0})
    monkeypatch.setattr(worker, "acquire_runtime_lease", lambda *a, **k: FakeLease())
    monkeypatch.setattr(worker, "release_runtime_lease", lambda lease: order.append("release") or {"released": True})

    def fail_write():
        order.append("write")
        raise RuntimeError("parallel write failed")

    monkeypatch.setattr(worker, "write_cycle", fail_write)
    monkeypatch.setattr(worker, "readiness_cycle", lambda: order.append("readiness"))

    with pytest.raises(RuntimeError, match="parallel write failed"):
        worker.main()
    assert order == ["write", "release"]
