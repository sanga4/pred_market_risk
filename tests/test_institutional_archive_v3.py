import os
from datetime import datetime, timezone
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
    db.DB = str(tmp_path / "archive_v3_test.duckdb")
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
        self.objects[(Bucket, Key)] = {
            "Body": Path(Filename).read_bytes(),
            "Metadata": dict((ExtraArgs or {}).get("Metadata") or {}),
        }

    def head_object(self, Bucket, Key):
        return {"Metadata": self.objects[(Bucket, Key)]["Metadata"]}

    def download_file(self, Bucket, Key, Filename):
        Path(Filename).write_bytes(self.objects[(Bucket, Key)]["Body"])


def _configure(monkeypatch, archive, fake):
    monkeypatch.setenv("RAW_ARCHIVE_ENDPOINT", "https://archive.invalid")
    monkeypatch.setenv("RAW_ARCHIVE_BUCKET", "test-bucket")
    monkeypatch.setenv("RAW_ARCHIVE_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("RAW_ARCHIVE_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("COMPACT_ARCHIVED_RAW", "true")
    monkeypatch.setenv("RAW_ARCHIVE_COMPRESSION_LEVEL", "1")
    monkeypatch.setattr(archive, "s3_client", lambda: fake)


def test_archive_v3_uses_bounded_storage_scan_without_unarchived_view():
    import institutional_archive_v3 as archive

    assert "er_unarchived_raw_snapshots" not in archive.CANDIDATE_SQL
    assert "ORDER BY" not in archive.CANDIDATE_SQL.upper()
    assert "payload_storage" in archive.CANDIDATE_SQL
    assert "LIMIT ?" in archive.CANDIDATE_SQL


def test_successive_cron_batches_for_same_ingestion_run_never_reuse_object_key(tmp_path, monkeypatch):
    setup_database(tmp_path)
    import institutional_archive_v3 as archive
    import institutional_ingest_v2 as ingest

    fake = FakeS3()
    _configure(monkeypatch, archive, fake)

    con = db.connect()
    try:
        ingest.ensure_sources(con)
        run_id = "one-big-run"
        con.execute(
            "INSERT INTO er_ingestion_runs(run_id,source_id,started_at,status) VALUES (?, 'kalshi', now(), 'running')",
            [run_id],
        )
        raw_ids = [
            ingest.insert_raw(
                con, "kalshi", "market", f"MKT-{i}", {"id": f"MKT-{i}"},
                datetime(2026, 10, 14, 18, i, tzinfo=timezone.utc), run_id,
            )
            for i in range(2)
        ]
    finally:
        con.close()

    first = archive.archive_pending(max_rows=1, bundle_rows=1)
    second = archive.archive_pending(max_rows=1, bundle_rows=1)
    assert first["archived"] == 1
    assert second["archived"] == 1

    con = db.connect()
    try:
        rows = con.execute(
            "SELECT raw_snapshot_id,archive_key,status FROM er_raw_archive_objects ORDER BY raw_snapshot_id"
        ).fetchall()
    finally:
        con.close()

    assert {row[0] for row in rows} == set(raw_ids)
    assert all(row[2] == "archived" for row in rows)
    keys = {row[1] for row in rows}
    assert len(keys) == 2
    assert all("bundle-" in key and key.endswith(".tar.gz") for key in keys)
    assert all("part-0001" not in key for key in keys)
    assert len(fake.objects) == 2


def test_verified_v3_archive_compacts_set_wise_and_restores(tmp_path, monkeypatch):
    setup_database(tmp_path)
    import institutional_archive_v3 as archive
    import institutional_ingest_v2 as ingest

    fake = FakeS3()
    _configure(monkeypatch, archive, fake)

    con = db.connect()
    try:
        ingest.ensure_sources(con)
        run_id = "archive-v3-run"
        con.execute(
            "INSERT INTO er_ingestion_runs(run_id,source_id,started_at,status) VALUES (?, 'kalshi', now(), 'running')",
            [run_id],
        )
        raw_ids = []
        original = {}
        for i in range(3):
            raw_id = ingest.insert_raw(
                con, "kalshi", "market", f"BATCH-{i}", {"id": f"BATCH-{i}", "n": i},
                datetime(2026, 10, 14, 19, i, tzinfo=timezone.utc), run_id,
            )
            raw_ids.append(raw_id)
            original[raw_id] = con.execute(
                "SELECT payload_json FROM er_raw_snapshots WHERE raw_snapshot_id=?", [raw_id]
            ).fetchone()[0].encode("utf-8")
    finally:
        con.close()

    result = archive.archive_pending(max_rows=10, bundle_rows=10)
    assert result["archived"] == 3
    assert result["compacted"] == 3
    assert result["failed"] == 0
    assert result["bundles"] == 1
    assert result["selection"] == "bounded_raw_ledger_storage_scan"
    assert result["persistence"] == "set_wise_arrow_per_bundle"

    con = db.connect()
    try:
        states = con.execute(
            "SELECT raw_snapshot_id,payload_json,payload_storage FROM er_raw_snapshots ORDER BY raw_snapshot_id"
        ).fetchall()
    finally:
        con.close()
    assert all(row[1] is None and row[2] == "object_archive" for row in states)

    for raw_id in raw_ids:
        assert archive.restore_payload(raw_id) == original[raw_id]


def test_archive_failure_does_not_compact_payload(tmp_path, monkeypatch):
    setup_database(tmp_path)
    import institutional_archive_v3 as archive
    import institutional_ingest_v2 as ingest

    class BrokenS3(FakeS3):
        def head_object(self, Bucket, Key):
            return {"Metadata": {"sha256": "wrong"}}

    _configure(monkeypatch, archive, BrokenS3())
    con = db.connect()
    try:
        ingest.ensure_sources(con)
        run_id = "broken-v3-run"
        con.execute(
            "INSERT INTO er_ingestion_runs(run_id,source_id,started_at,status) VALUES (?, 'kalshi', now(), 'running')",
            [run_id],
        )
        raw_id = ingest.insert_raw(
            con, "kalshi", "market", "BROKEN", {"id": "BROKEN"},
            datetime(2026, 10, 14, 20, 0, tzinfo=timezone.utc), run_id,
        )
    finally:
        con.close()

    result = archive.archive_pending(max_rows=10, bundle_rows=10)
    assert result["failed"] == 1
    assert result["compacted"] == 0
    con = db.connect()
    try:
        payload, storage, status = con.execute(
            """SELECT r.payload_json,r.payload_storage,a.status
               FROM er_raw_snapshots r LEFT JOIN er_raw_archive_objects a USING(raw_snapshot_id)
               WHERE r.raw_snapshot_id=?""",
            [raw_id],
        ).fetchone()
    finally:
        con.close()
    assert payload is not None
    assert storage == "database"
    assert status == "failed"
