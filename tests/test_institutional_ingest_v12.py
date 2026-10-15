from __future__ import annotations

import institutional_ingest_v12 as v12


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


def test_run_once_fails_closed_when_next_source_prime_fails(monkeypatch):
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
    assert results[1]["phase"] == "cross_source_prime"
    assert "source page failed" in results[1]["error"]
