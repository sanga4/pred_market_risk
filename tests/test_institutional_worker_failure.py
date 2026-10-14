import os

import pytest


def test_run_once_worker_propagates_cycle_failure(monkeypatch):
    import institutional_worker as worker

    monkeypatch.setenv("RUN_ONCE", "true")
    monkeypatch.setattr(worker, "ensure_schema", lambda: {"migration_count": 18})
    monkeypatch.setattr(worker, "recover_abandoned_runs", lambda: {"recovered": 0})

    def boom():
        raise RuntimeError("mapping failed")

    monkeypatch.setattr(worker, "cycle", boom)
    with pytest.raises(RuntimeError, match="mapping failed"):
        worker.main()


def test_cycle_rejects_partial_source_ingestion(monkeypatch):
    import institutional_worker as worker

    monkeypatch.setattr(worker, "run_once", lambda: [
        {"source_id": "kalshi", "status": "success"},
        {"source_id": "polymarket", "status": "failed", "error": "source timeout"},
    ])
    with pytest.raises(RuntimeError, match="source ingestions failed"):
        worker.cycle()


def test_cycle_rejects_critical_qa(monkeypatch):
    import institutional_worker as worker

    monkeypatch.setattr(worker, "run_once", lambda: [{"source_id": "kalshi", "status": "success"}])
    monkeypatch.setattr(worker, "archive_pending", lambda **kwargs: {"configured": False, "failed": 0})
    monkeypatch.setattr(worker, "run_mapping_pass", lambda: {})
    monkeypatch.setattr(worker, "sync_event_lifecycle", lambda: {})
    monkeypatch.setattr(worker, "run_ontology_pass", lambda: {})
    monkeypatch.setattr(worker, "run_graph_pass", lambda: {})
    monkeypatch.setattr(worker, "sync_reference_versions", lambda: {})
    monkeypatch.setattr(worker, "persist_mapping_drift", lambda: {})
    monkeypatch.setattr(worker, "run_qa", lambda **kwargs: {"critical_issues": 1})
    with pytest.raises(RuntimeError, match="open critical"):
        worker.cycle()
