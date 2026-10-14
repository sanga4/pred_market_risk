import os
from pathlib import Path

import db


def setup_database(tmp_path):
    db.DB = str(tmp_path / "archive_test.duckdb")
    os.environ["DB_FORCE_READ_WRITE"] = "true"
    root = Path(__file__).resolve().parents[1]
    con = db.connect()
    try:
        for name in [
            "institutional_schema.sql",
            "institutional_schema_002.sql",
            "institutional_schema_003.sql",
            "institutional_schema_004.sql",
            "institutional_schema_005.sql",
            "institutional_schema_006.sql",
            "institutional_schema_007.sql",
        ]:
            con.execute((root / name).read_text())
    finally:
        con.close()


class FakeS3:
    def __init__(self):
        self.objects = {}

    def put_object(self, Bucket, Key, Body, ContentType=None, ContentEncoding=None, Metadata=None):
        self.objects[(Bucket, Key)] = {"Body": Body, "Metadata": dict(Metadata or {})}
        return {"ETag": "fake"}

    def head_object(self, Bucket, Key):
        return {"Metadata": self.objects[(Bucket, Key)]["Metadata"]}


def test_verified_archive_can_compact_database_payload(tmp_path, monkeypatch):
    setup_database(tmp_path)
    import institutional_archive as archive
    import institutional_ingest_v2 as ingest
    from datetime import datetime, timezone

    monkeypatch.setenv("RAW_ARCHIVE_ENDPOINT", "https://archive.invalid")
    monkeypatch.setenv("RAW_ARCHIVE_BUCKET", "test-bucket")
    monkeypatch.setenv("RAW_ARCHIVE_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("RAW_ARCHIVE_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("COMPACT_ARCHIVED_RAW", "true")
    fake = FakeS3()
    monkeypatch.setattr(archive, "s3_client", lambda: fake)

    con = db.connect()
    try:
        ingest.ensure_sources(con)
        run_id = "archive-run"
        con.execute(
            "INSERT INTO er_ingestion_runs(run_id,source_id,started_at,status) VALUES (?, 'kalshi', now(), 'running')",
            [run_id],
        )
        raw_id = ingest.insert_raw(
            con,
            "kalshi",
            "market",
            "MKT-1",
            {"ticker": "MKT-1", "yes_bid": 20, "yes_ask": 22},
            datetime(2026, 10, 14, 18, 0, tzinfo=timezone.utc),
            run_id,
        )
        before = con.execute(
            "SELECT payload_json,content_hash FROM er_raw_snapshots WHERE raw_snapshot_id=?", [raw_id]
        ).fetchone()
        assert before[0] is not None
        expected_hash = before[1]
    finally:
        con.close()

    result = archive.archive_pending()
    assert result["archived"] == 1
    assert result["failed"] == 0
    assert result["compacted"] == 1

    con = db.connect()
    try:
        row = con.execute(
            """SELECT r.payload_json,r.payload_storage,r.content_hash,a.status,a.content_hash,a.archive_key
               FROM er_raw_snapshots r JOIN er_raw_archive_objects a USING(raw_snapshot_id)
               WHERE r.raw_snapshot_id=?""",
            [raw_id],
        ).fetchone()
        assert row[0] is None
        assert row[1] == "object_archive"
        assert row[2] == expected_hash
        assert row[3] == "archived"
        assert row[4] == expected_hash
        assert row[5].endswith(f"{raw_id}.json.gz")

        lineage = con.execute(
            "SELECT lineage_state FROM er_raw_lineage_status WHERE raw_snapshot_id=?", [raw_id]
        ).fetchone()[0]
        assert lineage == "archive_only_verified"
    finally:
        con.close()
