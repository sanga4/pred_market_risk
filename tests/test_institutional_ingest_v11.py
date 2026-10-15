from pathlib import Path

import pytest

import institutional_ingest_v11 as v11

ROOT = Path(__file__).resolve().parents[1]


def text(name: str) -> str:
    return (ROOT / name).read_text()


def test_v11_is_registered_bounded_and_not_promoted_before_acceptance():
    body = text("institutional_ingest_v11.py")
    schema = text("institutional_schema_036.sql")
    migrations = text("institutional_migrations_v2.py")
    assert "change_data_capture_v11_prefetched" in body
    assert "change_data_capture_v11_prefetched" in schema
    assert '"institutional_schema_036.sql"' in migrations
    assert "queue.Queue(maxsize=1)" in body
    assert "threading.Thread" in body
    assert "BEGIN TRANSACTION" in body and "ROLLBACK" in body
    assert "collect_seen_ids=False" in body
    assert "streamed_state_pointer_cdc_prefetch1" in body
    assert "from institutional_ingest_v11 import run_once" not in text("institutional_worker.py")
    assert "from institutional_ingest_v11 import run_once" not in text("institutional_service.py")


def test_prefetch_preserves_order_and_chunk_boundaries():
    rows = iter(range(11))
    chunks = list(v11._prefetched_chunks(rows, chunk_rows=4))
    assert [chunk for chunk, _ in chunks] == [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9, 10]]
    assert all(fetch_seconds >= 0 for _, fetch_seconds in chunks)


def test_prefetch_propagates_source_iterator_error():
    def broken():
        yield 1
        yield 2
        raise RuntimeError("source page failed")

    with pytest.raises(RuntimeError, match="source page failed"):
        list(v11._prefetched_chunks(iter(broken()), chunk_rows=2))
