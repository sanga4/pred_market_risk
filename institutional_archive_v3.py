"""High-throughput immutable raw archive bundles for EventRisk.

v3 fixes the two production scaling hazards in the v2 archive worker:

* candidate selection reads directly from the raw ledger's storage state with a bounded LIMIT;
  it does not anti-join and globally sort the entire unarchived backlog before returning work;
* bundle object keys are content-addressed from their member raw IDs, so independent cron runs
  can never reuse ``part-0001.tar.gz`` for different members of the same ingestion run.

MotherDuck connections are held only while selecting or publishing metadata. Compression and S3
upload/HEAD verification happen with no database connection open. Archive pointers and raw-payload
compaction are persisted set-wise with Arrow staging in one short transaction per verified bundle.
A failed object-store operation never compacts the database payload.
"""

from __future__ import annotations

import hashlib
import io
import os
import tarfile
import tempfile
from collections import defaultdict
from datetime import timezone
from pathlib import Path

import boto3
import pyarrow as pa

from db import connect


CANDIDATE_SQL = """
SELECT raw_snapshot_id,source_id,source_object_type,source_object_id,
       received_at,content_hash,payload_json,ingestion_run_id
FROM er_raw_snapshots
WHERE payload_json IS NOT NULL
  AND coalesce(payload_storage,'database')='database'
LIMIT ?
"""


def configured() -> bool:
    return all(os.getenv(k) for k in [
        "RAW_ARCHIVE_ENDPOINT",
        "RAW_ARCHIVE_BUCKET",
        "RAW_ARCHIVE_ACCESS_KEY_ID",
        "RAW_ARCHIVE_SECRET_ACCESS_KEY",
    ])


def compact_enabled() -> bool:
    return os.getenv("COMPACT_ARCHIVED_RAW", "true").lower() in {"1", "true", "yes"}


def compression_level() -> int:
    return max(1, min(9, int(os.getenv("RAW_ARCHIVE_COMPRESSION_LEVEL", "3"))))


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["RAW_ARCHIVE_ENDPOINT"],
        aws_access_key_id=os.environ["RAW_ARCHIVE_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["RAW_ARCHIVE_SECRET_ACCESS_KEY"],
        region_name=os.getenv("RAW_ARCHIVE_REGION") or "auto",
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _candidate_rows(max_rows: int):
    con = connect(read_only=True)
    try:
        return con.execute(CANDIDATE_SQL, [max_rows]).fetchall()
    finally:
        con.close()


def _bundle_fingerprint(rows) -> str:
    digest = hashlib.sha256()
    for raw_id in sorted(str(row[0]) for row in rows):
        digest.update(raw_id.encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def bundle_key(source_id: str, run_id: str, rows) -> str:
    first_received = min(row[4] for row in rows).astimezone(timezone.utc)
    fingerprint = _bundle_fingerprint(rows)[:24]
    safe_run = str(run_id or "unknown-run").replace("/", "_")
    return (
        f"raw-bundles/source={source_id}/year={first_received:%Y}/month={first_received:%m}/"
        f"day={first_received:%d}/run={safe_run}/bundle-{fingerprint}.tar.gz"
    )


def _persist_success(pointer_rows: list[dict], *, compact: bool) -> int:
    if not pointer_rows:
        return 0
    table = pa.Table.from_pylist(pointer_rows)
    con = connect()
    try:
        con.execute("BEGIN TRANSACTION")
        con.register("_archive_success_arrow", table)
        try:
            con.execute("""
                INSERT INTO er_raw_archive_objects(
                  raw_snapshot_id,archive_provider,archive_bucket,archive_key,content_hash,
                  byte_size,compression,status,error_message,archive_member,archive_bundle_hash,
                  bundle_byte_size,ingestion_run_id)
                SELECT raw_snapshot_id,archive_provider,archive_bucket,archive_key,content_hash,
                       byte_size,compression,'archived',NULL,archive_member,archive_bundle_hash,
                       bundle_byte_size,ingestion_run_id
                FROM _archive_success_arrow
                ON CONFLICT(raw_snapshot_id) DO UPDATE SET
                  archive_provider=excluded.archive_provider,
                  archive_bucket=excluded.archive_bucket,
                  archive_key=excluded.archive_key,
                  content_hash=excluded.content_hash,
                  byte_size=excluded.byte_size,
                  compression=excluded.compression,
                  status='archived',error_message=NULL,
                  archive_member=excluded.archive_member,
                  archive_bundle_hash=excluded.archive_bundle_hash,
                  bundle_byte_size=excluded.bundle_byte_size,
                  ingestion_run_id=excluded.ingestion_run_id,
                  archived_at=now()
            """)
            if compact:
                con.execute("""
                    UPDATE er_raw_snapshots r
                    SET payload_json=NULL,payload_storage='object_archive',payload_compacted_at=now()
                    FROM _archive_success_arrow a
                    WHERE r.raw_snapshot_id=a.raw_snapshot_id
                """)
            else:
                con.execute("""
                    UPDATE er_raw_snapshots r
                    SET payload_storage='database+object_archive'
                    FROM _archive_success_arrow a
                    WHERE r.raw_snapshot_id=a.raw_snapshot_id
                """)
        finally:
            con.unregister("_archive_success_arrow")
        con.execute("COMMIT")
        return len(pointer_rows) if compact else 0
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()


def _persist_failure(rows, *, bucket: str, object_key: str, error: str) -> None:
    if not rows:
        return
    staged = [
        {
            "raw_snapshot_id": row[0],
            "archive_provider": "s3_compatible",
            "archive_bucket": bucket,
            "archive_key": object_key,
            "content_hash": row[5],
            "compression": "tar+gzip",
            "error_message": error[:2000],
            "archive_member": f"{row[0]}.json",
            "ingestion_run_id": row[7] or "unknown-run",
        }
        for row in rows
    ]
    table = pa.Table.from_pylist(staged)
    con = connect()
    try:
        con.execute("BEGIN TRANSACTION")
        con.register("_archive_failure_arrow", table)
        try:
            # Never downgrade a previously verified archive pointer because a retry failed.
            con.execute("""
                UPDATE er_raw_archive_objects a SET
                  archive_provider=f.archive_provider,
                  archive_bucket=f.archive_bucket,
                  archive_key=f.archive_key,
                  content_hash=f.content_hash,
                  compression=f.compression,
                  status='failed',error_message=f.error_message,
                  archive_member=f.archive_member,
                  ingestion_run_id=f.ingestion_run_id
                FROM _archive_failure_arrow f
                WHERE a.raw_snapshot_id=f.raw_snapshot_id AND a.status<>'archived'
            """)
            con.execute("""
                INSERT INTO er_raw_archive_objects(
                  raw_snapshot_id,archive_provider,archive_bucket,archive_key,content_hash,
                  compression,status,error_message,archive_member,ingestion_run_id)
                SELECT f.raw_snapshot_id,f.archive_provider,f.archive_bucket,f.archive_key,f.content_hash,
                       f.compression,'failed',f.error_message,f.archive_member,f.ingestion_run_id
                FROM _archive_failure_arrow f
                WHERE NOT EXISTS (
                  SELECT 1 FROM er_raw_archive_objects a WHERE a.raw_snapshot_id=f.raw_snapshot_id
                )
            """)
        finally:
            con.unregister("_archive_failure_arrow")
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()


def _archive_bundle(client, bucket: str, source_id: str, run_id: str, rows) -> tuple[list[dict], str]:
    object_key = bundle_key(source_id, run_id, rows)
    with tempfile.NamedTemporaryFile(prefix="eventrisk-raw-", suffix=".tar.gz", delete=False) as tmp:
        tmp_path = Path(tmp.name)
    try:
        members = []
        with tarfile.open(tmp_path, mode="w:gz", compresslevel=compression_level()) as tar:
            for raw_id, _, _, _, received_at, content_hash, payload_json, _ in rows:
                payload_bytes = payload_json.encode("utf-8")
                raw_digest = hashlib.sha256(payload_bytes).hexdigest()
                if content_hash and raw_digest != content_hash:
                    raise RuntimeError(f"raw hash mismatch before archive for {raw_id}")
                member_name = f"{raw_id}.json"
                info = tarfile.TarInfo(name=member_name)
                info.size = len(payload_bytes)
                info.mtime = int(received_at.timestamp())
                info.mode = 0o444
                tar.addfile(info, io.BytesIO(payload_bytes))
                members.append((raw_id, member_name, raw_digest, len(payload_bytes)))

        bundle_hash = sha256_file(tmp_path)
        bundle_size = tmp_path.stat().st_size
        client.upload_file(
            str(tmp_path),
            bucket,
            object_key,
            ExtraArgs={
                "ContentType": "application/gzip",
                "Metadata": {
                    "sha256": bundle_hash,
                    "ingestion_run_id": str(run_id or "unknown-run")[:100],
                    "source_id": str(source_id)[:100],
                    "member_fingerprint": _bundle_fingerprint(rows),
                },
            },
        )
        head = client.head_object(Bucket=bucket, Key=object_key)
        remote_hash = (head.get("Metadata") or {}).get("sha256")
        if remote_hash != bundle_hash:
            raise RuntimeError(f"archive bundle verification failed for {object_key}")

        pointer_rows = [
            {
                "raw_snapshot_id": raw_id,
                "archive_provider": "s3_compatible",
                "archive_bucket": bucket,
                "archive_key": object_key,
                "content_hash": raw_digest,
                "byte_size": raw_size,
                "compression": "tar+gzip",
                "archive_member": member_name,
                "archive_bundle_hash": bundle_hash,
                "bundle_byte_size": bundle_size,
                "ingestion_run_id": run_id or "unknown-run",
            }
            for raw_id, member_name, raw_digest, raw_size in members
        ]
        return pointer_rows, object_key
    finally:
        tmp_path.unlink(missing_ok=True)


def archive_pending(max_rows: int = 20000, bundle_rows: int = 5000) -> dict:
    if not configured():
        return {"configured": False, "archived": 0, "failed": 0, "compacted": 0, "bundles": 0}

    max_rows = max(1, int(max_rows))
    bundle_rows = max(1, int(bundle_rows))
    rows = _candidate_rows(max_rows)
    if not rows:
        return {
            "configured": True, "archived": 0, "failed": 0, "compacted": 0, "bundles": 0,
            "candidates": 0, "bucket": os.environ["RAW_ARCHIVE_BUCKET"],
        }

    groups = defaultdict(list)
    for row in rows:
        groups[(row[1], row[7] or "unknown-run")].append(row)

    client = s3_client()
    bucket = os.environ["RAW_ARCHIVE_BUCKET"]
    archived = failed = compacted = bundles = 0
    for (source_id, run_id), group_rows in groups.items():
        for start in range(0, len(group_rows), bundle_rows):
            batch = group_rows[start:start + bundle_rows]
            object_key = bundle_key(source_id, run_id, batch)
            try:
                pointer_rows, object_key = _archive_bundle(client, bucket, source_id, run_id, batch)
                compacted += _persist_success(pointer_rows, compact=compact_enabled())
                archived += len(pointer_rows)
                bundles += 1
            except Exception as exc:
                failed += len(batch)
                _persist_failure(batch, bucket=bucket, object_key=object_key, error=str(exc))

    return {
        "configured": True,
        "candidates": len(rows),
        "archived": archived,
        "failed": failed,
        "compacted": compacted,
        "bundles": bundles,
        "bucket": bucket,
        "compression_level": compression_level(),
        "selection": "bounded_raw_ledger_storage_scan",
        "bundle_keying": "content_addressed_member_fingerprint_v1",
        "persistence": "set_wise_arrow_per_bundle",
    }


def restore_payload(raw_snapshot_id: str) -> bytes:
    """Retrieve and verify one archived raw payload for audit/replay tooling."""
    if not configured():
        raise RuntimeError("raw archive is not configured")
    con = connect(read_only=True)
    try:
        row = con.execute(
            """SELECT archive_bucket,archive_key,archive_member,content_hash,archive_bundle_hash,status
               FROM er_raw_archive_objects WHERE raw_snapshot_id=?""",
            [raw_snapshot_id],
        ).fetchone()
    finally:
        con.close()
    if not row or row[5] != "archived":
        raise KeyError(raw_snapshot_id)
    bucket, key, member, raw_hash, expected_bundle_hash, _ = row
    client = s3_client()
    with tempfile.NamedTemporaryFile(prefix="eventrisk-restore-", suffix=".tar.gz", delete=False) as tmp:
        tmp_path = Path(tmp.name)
    try:
        client.download_file(bucket, key, str(tmp_path))
        if sha256_file(tmp_path) != expected_bundle_hash:
            raise RuntimeError("restored archive bundle hash mismatch")
        with tarfile.open(tmp_path, mode="r:gz") as tar:
            extracted = tar.extractfile(member)
            if extracted is None:
                raise RuntimeError("archive member missing")
            payload = extracted.read()
        if hashlib.sha256(payload).hexdigest() != raw_hash:
            raise RuntimeError("restored raw payload hash mismatch")
        return payload
    finally:
        tmp_path.unlink(missing_ok=True)


if __name__ == "__main__":
    print(archive_pending())
