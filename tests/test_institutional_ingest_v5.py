import threading


def test_v5_fetches_sources_concurrently_but_persists_serially(monkeypatch):
    import institutional_ingest_v5 as ingest

    barrier = threading.Barrier(2, timeout=2)
    fetch_threads = set()
    write_active = 0
    max_write_active = 0
    write_order = []
    lock = threading.Lock()

    monkeypatch.setattr(
        ingest,
        "_prepare_run",
        lambda source_id: ingest.PreparedRun(source_id, f"run-{source_id}", {"scope": "full"}),
    )

    def fake_fetch(source_id):
        fetch_threads.add(threading.current_thread().name)
        barrier.wait()
        return ingest.FetchResult(source_id, [source_id], 0.1)

    def fake_persist(prepared, fetched):
        nonlocal write_active, max_write_active
        with lock:
            write_active += 1
            max_write_active = max(max_write_active, write_active)
        try:
            write_order.append(prepared.source_id)
            assert fetched.source_id == prepared.source_id
            return {"source_id": prepared.source_id, "status": "success"}
        finally:
            with lock:
                write_active -= 1

    monkeypatch.setattr(ingest, "_fetch_source", fake_fetch)
    monkeypatch.setattr(ingest, "_persist_source", fake_persist)

    result = ingest.run_once(["kalshi", "polymarket"])
    assert [r["source_id"] for r in result] == ["kalshi", "polymarket"]
    assert all(r["status"] == "success" for r in result)
    assert len(fetch_threads) == 2
    assert max_write_active == 1
    assert write_order == ["kalshi", "polymarket"]


def test_v5_fetch_failure_is_isolated_and_persistable_source_continues(monkeypatch):
    import institutional_ingest_v5 as ingest

    monkeypatch.setattr(
        ingest,
        "_prepare_run",
        lambda source_id: ingest.PreparedRun(source_id, f"run-{source_id}", {"scope": "full"}),
    )

    def fake_fetch(source_id):
        if source_id == "polymarket":
            raise RuntimeError("source unavailable")
        return ingest.FetchResult(source_id, [], 0.1)

    failed = []
    persisted = []
    monkeypatch.setattr(ingest, "_fetch_source", fake_fetch)
    monkeypatch.setattr(
        ingest,
        "_fail_run",
        lambda prepared, exc, **kwargs: failed.append(prepared.source_id) or {
            "source_id": prepared.source_id, "status": "failed", "phase": kwargs.get("phase")
        },
    )
    monkeypatch.setattr(
        ingest,
        "_persist_source",
        lambda prepared, fetched: persisted.append(prepared.source_id) or {
            "source_id": prepared.source_id, "status": "success"
        },
    )

    result = ingest.run_once(["kalshi", "polymarket"])
    assert persisted == ["kalshi"]
    assert failed == ["polymarket"]
    assert result == [
        {"source_id": "kalshi", "status": "success"},
        {"source_id": "polymarket", "status": "failed", "phase": "fetch"},
    ]
