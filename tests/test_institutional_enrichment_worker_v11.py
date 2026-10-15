from pathlib import Path

import pytest

import institutional_enrichment_worker_v11 as worker

ROOT = Path(__file__).resolve().parents[1]


def _mapping():
    return {
        "identity_mutations_disabled": True,
        "native_events_created": 0,
        "auto_exact_mappings": 0,
        "pair_execution_location": "enrichment_flight_local_python",
    }


def test_enrichment_v11_requires_graph_v4_execution(monkeypatch):
    monkeypatch.setattr(worker, "_run_advisory_mapping", _mapping)
    monkeypatch.setattr(worker, "sync_event_lifecycle", lambda: {})
    monkeypatch.setattr(worker, "run_ontology_pass", lambda: {})
    monkeypatch.setattr(worker, "run_graph_pass", lambda: {
        "execution_version": "graph_execution_v4_no_issuer_inference",
        "identity_policy": "read_reference_master_only_no_issuer_inference",
    })
    monkeypatch.setattr(worker, "sync_reference_versions", lambda: {})
    monkeypatch.setattr(worker, "persist_mapping_drift", lambda: {})

    result = worker._run_enrichment_pass({}, 1)
    assert result["graph"]["execution_version"] == "graph_execution_v4_no_issuer_inference"

    monkeypatch.setattr(worker, "run_graph_pass", lambda: {"execution_version": "graph_execution_v3_incremental"})
    with pytest.raises(RuntimeError, match="approved non-inferential graph"):
        worker._run_enrichment_pass({}, 2)


def test_enrichment_v11_and_graph_v4_are_registered_additively():
    worker_body = (ROOT / "institutional_enrichment_worker_v11.py").read_text()
    graph_body = (ROOT / "institutional_graph_v4.py").read_text()
    schema57 = (ROOT / "institutional_schema_057.sql").read_text()
    schema58 = (ROOT / "institutional_schema_058.sql").read_text()
    migrations = (ROOT / "institutional_migrations_v2.py").read_text()
    old_worker = (ROOT / "institutional_enrichment_worker_v10.py").read_text()

    assert "from institutional_graph_v4 import run_graph_pass" in worker_body
    assert "enrichment_path_v11_no_issuer_inference" in worker_body
    assert "graph_execution_v4_no_issuer_inference" in graph_body
    assert "graph_execution_v4_no_issuer_inference" in schema57
    assert "enrichment_path_v11_no_issuer_inference" in schema58
    assert '"institutional_schema_057.sql"' in migrations
    assert '"institutional_schema_058.sql"' in migrations
    assert "from institutional_graph_v3 import run_graph_pass" in old_worker
