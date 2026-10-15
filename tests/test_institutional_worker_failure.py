import pytest


def test_run_once_worker_propagates_cycle_failure(monkeypatch):
    import institutional_worker as worker

    monkeypatch.setenv("RUN_ONCE", "true")
    monkeypatch.setattr(worker, "ensure_schema", lambda: {"migration_count": 35})
    monkeypatch.setattr(worker, "recover_abandoned_runs", lambda: {"recovered": 0})

    def boom():
        raise RuntimeError("hot path failed")

    monkeypatch.setattr(worker, "cycle", boom)
    with pytest.raises(RuntimeError, match="hot path failed"):
        worker.main()


def test_cycle_rejects_partial_source_ingestion(monkeypatch):
    import institutional_worker as worker

    monkeypatch.setattr(worker, "run_once", lambda: [
        {"source_id": "kalshi", "status": "success"},
        {"source_id": "polymarket", "status": "failed", "error": "source timeout"},
    ])
    with pytest.raises(RuntimeError, match="source ingestions failed"):
        worker.cycle()


def test_hot_cycle_rejects_failed_observed_readiness(monkeypatch):
    import institutional_worker as worker

    monkeypatch.setattr(worker, "run_once", lambda: [{"source_id": "kalshi", "status": "success"}])
    monkeypatch.setattr(worker, "archive_pending", lambda **kwargs: {"configured": False, "failed": 0})
    monkeypatch.setattr(worker, "run_identity_pass", lambda: {"contracts_changed": 0})
    monkeypatch.setattr(
        worker,
        "evaluate_observed_readiness",
        lambda **kwargs: {"ready": False, "failed_controls": ["raw_current_pointers_valid"]},
    )
    monkeypatch.setattr(worker, "evaluate_identity_readiness", lambda: {"ready": True, "failed_controls": []})
    with pytest.raises(RuntimeError, match="Observed-data readiness failed"):
        worker.cycle()


def test_hot_cycle_rejects_failed_identity_readiness(monkeypatch):
    import institutional_worker as worker

    monkeypatch.setattr(worker, "run_once", lambda: [{"source_id": "kalshi", "status": "success"}])
    monkeypatch.setattr(worker, "archive_pending", lambda **kwargs: {"configured": False, "failed": 0})
    monkeypatch.setattr(worker, "run_identity_pass", lambda: {"contracts_changed": 0})
    monkeypatch.setattr(worker, "evaluate_observed_readiness", lambda **kwargs: {"ready": True, "failed_controls": []})
    monkeypatch.setattr(
        worker,
        "evaluate_identity_readiness",
        lambda: {"ready": False, "failed_controls": ["identity_semantic_queue_caught_up"]},
    )
    with pytest.raises(RuntimeError, match="Deterministic identity readiness failed"):
        worker.cycle()


def test_enrichment_cycle_rejects_critical_qa(monkeypatch):
    import institutional_enrichment_worker as worker

    monkeypatch.setattr(worker, "run_mapping_pass", lambda: {})
    monkeypatch.setattr(worker, "sync_event_lifecycle", lambda: {})
    monkeypatch.setattr(worker, "run_ontology_pass", lambda: {})
    monkeypatch.setattr(worker, "run_graph_pass", lambda: {})
    monkeypatch.setattr(worker, "sync_reference_versions", lambda: {})
    monkeypatch.setattr(worker, "persist_mapping_drift", lambda: {})
    monkeypatch.setattr(worker, "run_qa", lambda **kwargs: {"critical_issues": 1})
    with pytest.raises(RuntimeError, match="open critical"):
        worker.cycle()
