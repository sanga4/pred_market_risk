from __future__ import annotations

import threading
import time

import institutional_ingest_v13 as v13


def test_v13_installs_thread_local_http_transport():
    assert v13.base.get_json is v13.thread_safe_get_json


def test_v13_runs_sources_concurrently_and_preserves_result_order(monkeypatch):
    barrier = threading.Barrier(2)
    entered: list[str] = []
    lock = threading.Lock()

    monkeypatch.setattr(v13.v11, "ADAPTERS", {"kalshi": object, "polymarket": object})

    def fake_ingest(source_id):
        with lock:
            entered.append(source_id)
        # If orchestration accidentally becomes sequential this barrier breaks the test instead of
        # silently passing with a slower implementation.
        barrier.wait(timeout=2.0)
        if source_id == "kalshi":
            time.sleep(0.03)
        return {"source_id": source_id, "status": "success", "records_written": 1}

    monkeypatch.setattr(v13.v11, "ingest_source", fake_ingest)
    results = v13.run_once(["kalshi", "polymarket"])

    assert sorted(entered) == ["kalshi", "polymarket"]
    assert [row["source_id"] for row in results] == ["kalshi", "polymarket"]
    assert all(row["status"] == "success" for row in results)
    assert all(row["orchestration"]["parallel_source_transaction"] for row in results)
    assert all(row["orchestration"]["method_version"] == v13.ORCHESTRATION_VERSION for row in results)


def test_v13_one_source_failure_is_explicit_and_does_not_hide_other_success(monkeypatch):
    barrier = threading.Barrier(2)
    monkeypatch.setattr(v13.v11, "ADAPTERS", {"kalshi": object, "polymarket": object})

    def fake_ingest(source_id):
        barrier.wait(timeout=2.0)
        if source_id == "polymarket":
            raise RuntimeError("transaction conflict")
        return {"source_id": source_id, "status": "success", "records_written": 1}

    monkeypatch.setattr(v13.v11, "ingest_source", fake_ingest)
    results = v13.run_once(["kalshi", "polymarket"])

    assert results[0]["source_id"] == "kalshi" and results[0]["status"] == "success"
    assert results[1]["source_id"] == "polymarket" and results[1]["status"] == "failed"
    assert results[1]["phase"] == "source"
    assert "transaction conflict" in results[1]["error"]


def test_v13_rejects_unknown_source_before_starting_threads(monkeypatch):
    monkeypatch.setattr(v13.v11, "ADAPTERS", {"kalshi": object})
    try:
        v13.run_once(["kalshi", "unknown"])
    except ValueError as exc:
        assert "unknown" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("unknown source must fail before orchestration")
