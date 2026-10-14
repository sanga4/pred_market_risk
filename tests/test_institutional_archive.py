import os
from pathlib import Path

import db


MIGRATIONS = [
    "institutional_schema.sql",
    "institutional_schema_002.sql",
    "institutional_schema_003.sql",
    "institutional_schema_004.sql",
    "institutional_schema_005.sql",
    "institutional_schema_006.sql",
    "institutional_schema_007.sql",
    "institutional_schema_008.sql",
]


def setup_database(tmp_path):
    db.DB = str(tmp_path / "archive_test.duckdb")
    os.environ["DB_FORCE_READ_WRITE"] = "true"
    root = Path(__file__).resolve().parents[1]
    con = db.connect()
    try:
        for name in MIGRATIONS:
            con.execute((root / name).read_text())
    finally:
        con.close()


class FakeS3:
    def __init__(self):
        self.objects = {}

    def upload_file(self, Filename, Bucket, Key, ExtraArgs=None):
        body = Path(Filename).read_bytes()
        self.objects[(Bucket, Key)] = {
            "Body": body,
            "Metadata": dict((ExtraArgs or {}).get("Metadata") or {}),
        }

    def head_object(self, Bucket, Key):
        return {"Metadata": self.objects[(Bucket, Key)]["Metadata"]}

    def download_file(self, Bucket, Key, Filename):
        Path(Filename).write_bytes(self.objects[(Bucket, Key)]["Body"])


def test_verified_bundle_archive_can_compact_and_restore_database_payload(tmp_path, monkeypatch):
    setup_database(tmp_path)
    import institutional_archive_v2 as archive
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
        original_payload = before[0].encode("utf-8")
        expected_hash = before[1]
    finally:
        con.close()

    result = archive.archive_pending(max_rows=100, bundle_rows=100)
    assert result["archived"] == 1
    assert result["failed"] == 0
    assert result["compacted"] == 1
    assert result["bundles"] == 1

    con = db.connect()
    try:
        row = con.execute(
            """SELECT r.payload_json,r.payload_storage,r.content_hash,a.status,a.content_hash,
                      a.archive_key,a.archive_member,a.archive_bundle_hash,a.ingestion_run_id
               FROM er_raw_snapshots r JOIN er_raw_archive_objects a USING(raw_snapshot_id)
               WHERE r.raw_snapshot_id=?""",
            [raw_id],
        ).fetchone()
        assert row[0] is None
        assert row[1] == "object_archive"
        assert row[2] == expected_hash
        assert row[3] == "archived"
        assert row[4] == expected_hash
        assert row[5].endswith("part-0001.tar.gz")
        assert row[6] == f"{raw_id}.json"
        assert row[7]
        assert row[8] == run_id

        lineage = con.execute(
            "SELECT lineage_state FROM er_raw_lineage_status WHERE raw_snapshot_id=?", [raw_id]
        ).fetchone()[0]
        assert lineage == "archive_only_verified"
    finally:
        con.close()

    restored = archive.restore_payload(raw_id)
    assert restored == original_payload


def test_archive_failure_never_compacts_source_payload(tmp_path, monkeypatch):
    setup_database(tmp_path)
    import institutional_archive_v2 as archive
    import institutional_ingest_v2 as ingest
    from datetime import datetime, timezone

    monkeypatch.setenv("RAW_ARCHIVE_ENDPOINT", "https://archive.invalid")
    monkeypatch.setenv("RAW_ARCHIVE_BUCKET", "test-bucket")
    monkeypatch.setenv("RAW_ARCHIVE_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("RAW_ARCHIVE_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("COMPACT_ARCHIVED_RAW", "true")

    class BrokenS3(FakeS3):
        def head_object(self, Bucket, Key):
            return {"Metadata": {"sha256": "wrong"}}

    monkeypatch.setattr(archive, "s3_client", lambda: BrokenS3())

    con = db.connect()
    try:
        ingest.ensure_sources(con)
        run_id = "broken-archive-run"
        con.execute(
            "INSERT INTO er_ingestion_runs(run_id,source_id,started_at,status) VALUES (?, 'kalshi', now(), 'running')",
            [run_id],
        )
        raw_id = ingest.insert_raw(
            con, "kalshi", "market", "MKT-FAIL", {"id": "MKT-FAIL"},
            datetime(2026, 10, 14, 18, 0, tzinfo=timezone.utc), run_id,
        )
    finally:
        con.close()

    result = archive.archive_pending(max_rows=100, bundle_rows=100)
    assert result["archived"] == 0
    assert result["failed"] == 1
    assert result["compacted"] == 0

    con = db.connect()
    try:
        payload, status = con.execute(
            """SELECT r.payload_json,a.status FROM er_raw_snapshots r
               LEFT JOIN er_raw_archive_objects a USING(raw_snapshot_id)
               WHERE r.raw_snapshot_id=?""",
            [raw_id],
        ).fetchone()
        assert payload is not None
        assert status == "failed"
    finally:
        con.close()
