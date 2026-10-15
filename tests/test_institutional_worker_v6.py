from __future__ import annotations

import pytest

import institutional_worker_v6 as worker


class FakeLease:
    acquired = True

    def as_dict(self):
        return {"acquired": True, "lease_name": "hot_ingestion"}


def test_writer_lease_releases_before_readiness(monkeypatch):
    order = []

    monkeypatch.setattr(worker, "ensure_schema", lambda: {"ok": True})
    monkeypatch.setattr(worker, "recover_abandoned_runs", lambda: {"recovered": 0})
    monkeypatch.setattr(worker, "acquire_runtime_lease", lambda *a, **k: FakeLease())
    monkeypatch.setattr(worker, "release_runtime_lease", lambda lease: order.append("release") or {"released": True})
    monkeypatch.setattr(worker, "write_cycle", lambda: order.append("write") or {"timing_seconds": {"writer_critical_path_total": 1.0}})
    monkeypatch.setattr(worker, "readiness_cycle", lambda: order.append("readiness") or {
        "observed_readiness": {"ready": True},
        "identity_readiness": {"ready": True},
        "timing_seconds": {"readiness_total": 1.0},
    })

    worker.main()
    assert order == ["write", "release", "readiness"]


def test_writer_failure_still_releases_lease_and_skips_readiness(monkeypatch):
    order = []

    monkeypatch.setattr(worker, "ensure_schema", lambda: {"ok": True})
    monkeypatch.setattr(worker, "recover_abandoned_runs", lambda: {"recovered": 0})
    monkeypatch.setattr(worker, "acquire_runtime_lease", lambda *a, **k: FakeLease())
    monkeypatch.setattr(worker, "release_runtime_lease", lambda lease: order.append("release") or {"released": True})

    def fail_write():
        order.append("write")
        raise RuntimeError("write failed")

    monkeypatch.setattr(worker, "write_cycle", fail_write)
    monkeypatch.setattr(worker, "readiness_cycle", lambda: order.append("readiness"))

    with pytest.raises(RuntimeError, match="write failed"):
        worker.main()
    assert order == ["write", "release"]


def test_readiness_failure_happens_after_writer_lease_release(monkeypatch):
    order = []

    monkeypatch.setattr(worker, "ensure_schema", lambda: {"ok": True})
    monkeypatch.setattr(worker, "recover_abandoned_runs", lambda: {"recovered": 0})
    monkeypatch.setattr(worker, "acquire_runtime_lease", lambda *a, **k: FakeLease())
    monkeypatch.setattr(worker, "release_runtime_lease", lambda lease: order.append("release") or {"released": True})
    monkeypatch.setattr(worker, "write_cycle", lambda: order.append("write") or {"timing_seconds": {}})

    def fail_readiness():
        order.append("readiness")
        raise RuntimeError("readiness failed")

    monkeypatch.setattr(worker, "readiness_cycle", fail_readiness)

    with pytest.raises(RuntimeError, match="readiness failed"):
        worker.main()
    assert order == ["write", "release", "readiness"]
