from __future__ import annotations

import institutional_ingest_v12 as v12


def test_v12_installs_thread_local_http_client():
    assert v12.base.get_json is v12.thread_safe_get_json


def test_prime_source_is_bounded_and_preserves_remainder(monkeypatch):
    class Adapter:
        def iter_snapshots(self):
            yield from range(7)

    monkeypatch.setitem(v12.v11.ADAPTERS, "demo", Adapter)
    primed = v12._prime_source("demo", chunk_rows=3)
    assert primed.source_id == "demo"
    assert primed.first_chunk == [0, 1, 2]
    assert list(primed.remainder) == [3, 4, 5, 6]
    assert primed.elapsed_seconds >= 0


def test_run_once_primes_next_source_without_changing_order(monkeypatch):
    seen = {}

    class A:
        def iter_snapshots(self):
            yield from ["a1", "a2", "a3"]

    class B:
        def iter_snapshots(self):
            yield from ["b1", "b2", "b3", "b4"]

    monkeypatch.setattr(v12.v11, "CHUNK_ROWS", 2)
    monkeypatch.setattr(v12.v11, "ADAPTERS", {"a": A, "b": B})

    def fake_ingest(source_id):
        rows = list(v12.v11.ADAPTERS[source_id]().iter_snapshots())
        seen[source_id] = rows
        return {"source_id": source_id, "status": "success", "records_written": len(rows)}

    monkeypatch.setattr(v12.v11, "ingest_source", fake_ingest)
    results = v12.run_once(["a", "b"])

    assert [r["source_id"] for r in results] == ["a", "b"]
    assert seen["a"] == ["a1", "a2", "a3"]
    assert seen["b"] == ["b1", "b2", "b3", "b4"]
    assert results[0]["orchestration"]["cross_source_primed"] is False
    assert results[1]["orchestration"]["cross_source_primed"] is True
    assert results[1]["orchestration"]["primed_rows"] == 2


def test_prime_failure_falls_back_to_authoritative_v11_ingestion(monkeypatch):
    class A:
        def iter_snapshots(self):
            yield "a1"

    class Flaky:
        instances = 0

        def __init__(self):
            type(self).instances += 1
            self.instance_no = type(self).instances

        def iter_snapshots(self):
            if self.instance_no == 1:
                raise RuntimeError("prime transport failed")
            yield from ["b1", "b2"]

    monkeypatch.setattr(v12.v11, "CHUNK_ROWS", 2)
    monkeypatch.setattr(v12.v11, "ADAPTERS", {"a": A, "b": Flaky})

    def fake_ingest(source_id):
        rows = list(v12.v11.ADAPTERS[source_id]().iter_snapshots())
        return {"source_id": source_id, "status": "success", "records_written": len(rows)}

    monkeypatch.setattr(v12.v11, "ingest_source", fake_ingest)
    results = v12.run_once(["a", "b"])

    assert results[0]["status"] == "success"
    assert results[1]["status"] == "success"
    assert results[1]["records_written"] == 2
    assert results[1]["orchestration"]["prime_fallback"] is True
    assert "prime transport failed" in results[1]["orchestration"]["prime_error"]


def test_real_source_failure_after_prime_fallback_still_returns_source_failure(monkeypatch):
    class A:
        def iter_snapshots(self):
            yield "a1"

    class Broken:
        def iter_snapshots(self):
            raise RuntimeError("source page failed")
            yield  # pragma: no cover

    monkeypatch.setattr(v12.v11, "CHUNK_ROWS", 2)
    monkeypatch.setattr(v12.v11, "ADAPTERS", {"a": A, "broken": Broken})

    def fake_ingest(source_id):
        rows = list(v12.v11.ADAPTERS[source_id]().iter_snapshots())
        return {"source_id": source_id, "status": "success", "records_written": len(rows)}

    monkeypatch.setattr(v12.v11, "ingest_source", fake_ingest)
    results = v12.run_once(["a", "broken"])

    assert results[0]["status"] == "success"
    assert results[1]["status"] == "failed"
    assert results[1]["phase"] == "source"
    assert "source page failed" in results[1]["error"]
