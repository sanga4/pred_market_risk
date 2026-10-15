from pathlib import Path

import institutional_enrichment_worker_v10 as worker


ROOT = Path(__file__).resolve().parents[1]


def _identity(ready, failed=None):
    return {
        "layer": "identity",
        "ready": ready,
        "failed_controls": failed or ([] if ready else ["identity_semantic_queue_caught_up"]),
    }


def _pass_payload(n):
    return {
        "advisory_mapping": {
            "identity_mutations_disabled": True,
            "native_events_created": 0,
            "auto_exact_mappings": 0,
            "pair_execution_location": "enrichment_flight_local_python",
            "attempt": n,
        },
        "lifecycle": {}, "ontology": {}, "graph": {},
        "reference_versions": {}, "mapping_drift": {},
    }


def test_worker_waits_for_identity_before_starting_enrichment(monkeypatch):
    identities = iter([_identity(False), _identity(True)])
    pass_calls = []
    monkeypatch.setattr(worker, "_identity_ready", lambda: next(identities))
    monkeypatch.setattr(worker.time, "sleep", lambda _: None)
    monkeypatch.setattr(
        worker, "_run_enrichment_pass",
        lambda timings, attempt: pass_calls.append(attempt) or _pass_payload(attempt),
    )
    monkeypatch.setattr(worker, "run_qa", lambda **_: {"deferred": False, "critical_issues": 0})
    monkeypatch.setattr(worker, "evaluate_enrichment_readiness", lambda: {"ready": True, "failed_controls": []})

    result = worker.cycle()

    assert result["status"] == "success"
    assert result["attempts"] == 1
    assert pass_calls == [1]


def test_worker_reruns_full_enrichment_when_new_core_work_arrives(monkeypatch):
    pass_calls = []
    readiness = iter([
        {"ready": False, "failed_controls": ["ontology_caught_up"]},
        {"ready": True, "failed_controls": []},
    ])
    monkeypatch.setattr(worker, "_identity_ready", lambda: _identity(True))
    monkeypatch.setattr(worker.time, "sleep", lambda _: None)
    monkeypatch.setattr(
        worker, "_run_enrichment_pass",
        lambda timings, attempt: pass_calls.append(attempt) or _pass_payload(attempt),
    )
    monkeypatch.setattr(worker, "run_qa", lambda **_: {"deferred": False, "critical_issues": 0})
    monkeypatch.setattr(worker, "evaluate_enrichment_readiness", lambda: next(readiness))

    result = worker.cycle()

    assert result["status"] == "success"
    assert result["attempts"] == 2
    assert pass_calls == [1, 2]
    assert result["enrichment_readiness"]["ready"] is True


def test_worker_reruns_full_enrichment_after_qa_snapshot_defers(monkeypatch):
    pass_calls = []
    qa_results = iter([
        {"deferred": True, "deferred_reason": "core_fingerprint_changed_during_read_evaluation"},
        {"deferred": False, "critical_issues": 0},
    ])
    monkeypatch.setattr(worker, "_identity_ready", lambda: _identity(True))
    monkeypatch.setattr(worker.time, "sleep", lambda _: None)
    monkeypatch.setattr(
        worker, "_run_enrichment_pass",
        lambda timings, attempt: pass_calls.append(attempt) or _pass_payload(attempt),
    )
    monkeypatch.setattr(worker, "run_qa", lambda **_: next(qa_results))
    monkeypatch.setattr(worker, "evaluate_enrichment_readiness", lambda: {"ready": True, "failed_controls": []})

    result = worker.cycle()

    assert result["status"] == "success"
    assert result["attempts"] == 2
    assert pass_calls == [1, 2]


def test_worker_v10_registered_without_rewriting_prior_generations():
    body = (ROOT / "institutional_enrichment_worker_v10.py").read_text()
    schema = (ROOT / "institutional_schema_053.sql").read_text()
    migrations = (ROOT / "institutional_migrations_v2.py").read_text()
    qa9 = (ROOT / "institutional_enrichment_worker_v9.py").read_text()

    assert "enrichment_path_v10_stable_catchup_interval_qa" in body
    assert "from institutional_qa_v10 import run_qa" in body
    assert "evaluate_identity_readiness" in body
    assert "enrichment_path_v10_stable_catchup_interval_qa" in schema
    assert '"institutional_schema_053.sql"' in migrations
    assert "enrichment_path_v9_interval_qa" in qa9
