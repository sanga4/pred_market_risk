"""Efficient immutable raw archive bundles for EventRisk.

A collection run can contain thousands of raw source objects. Uploading and HEAD-verifying
each object separately is safe but operationally expensive. This module writes a tar.gz
bundle per ingestion run (split only if a run exceeds the configured row cap), stores each
raw snapshot as `<raw_snapshot_id>.json`, verifies the bundle SHA-256 in object metadata,
then records the member path for every raw snapshot before optional DB compaction.
"""

from __future__ import annotations

import hashlib
import io
import os
import tarfile
import tempfile
from datetime import timezone
from pathlib import Path

import boto3

from db import connect


def configured() -> bool:
    return all(os.getenv(k) for k in [
        "RAW_ARCHIVE_ENDPOINT",
        "RAW_ARCHIVE_BUCKET",
        "RAW_ARCHIVE_ACCESS_KEY_ID",
        "RAW_ARCHIVE_SECRET_ACCESS_KEY",
    ])


def compact_enabled() -> bool:
    return os.getenv("COMPACT_ARCHIVED_RAW", "true").lower() in {"1", "true", "yes"}


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


def bundle_key(source_id: str, run_id: str, received_at, part: int) -> str:
    ts = received_at.astimezone(timezone.utc)
    return (
        f"raw-bundles/source={source_id}/year={ts:%Y}/month={ts:%m}/day={ts:%d}/"
        f"run={run_id}/part-{part:04d}.tar.gz"
    )


def _pending_groups(con, max_rows: int):
    rows = con.execute(
        """SELECT r.raw_snapshot_id,r.source_id,r.source_object_type,r.source_object_id,
                  r.received_at,r.content_hash,r.payload_json,r.ingestion_run_id
           FROM er_unarchived_raw_snapshots r
           WHERE r.payload_json IS NOT NULL
           ORDER BY r.ingestion_run_id,r.received_at,r.raw_snapshot_id
           LIMIT ?""",
        [max_rows],
    ).fetchall()
    groups = {}
    for row in rows:
        run_id = row[7] or "unknown-run"
        groups.setdefault((row[1], run_id), []).append(row)
    return groups


def archive_pending(max_rows: int = 20000, bundle_rows: int = 5000) -> dict:
    if not configured():
        return {"configured": False, "archived": 0, "failed": 0, "compacted": 0, "bundles": 0}

    client = s3_client()
    bucket = os.environ["RAW_ARCHIVE_BUCKET"]
    con = connect()
    archived = failed = compacted = bundles = 0
    try:
        groups = _pending_groups(con, max_rows)
        for (source_id, run_id), rows in groups.items():
            for part_index, start in enumerate(range(0, len(rows), bundle_rows), start=1):
                batch = rows[start:start + bundle_rows]
                first_received = batch[0][4]
                object_key = bundle_key(source_id, run_id, first_received, part_index)

                with tempfile.NamedTemporaryFile(prefix="eventrisk-raw-", suffix=".tar.gz", delete=False) as tmp:
                    tmp_path = Path(tmp.name)
                try:
                    members = []
                    with tarfile.open(tmp_path, mode="w:gz", compresslevel=6) as tar:
                        for raw_id, _, object_type, object_id, received_at, content_hash, payload_json, _ in batch:
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
                                "ingestion_run_id": run_id[:100],
                                "source_id": source_id[:100],
                            },
                        },
                    )
                    head = client.head_object(Bucket=bucket, Key=object_key)
                    remote_hash = (head.get("Metadata") or {}).get("sha256")
                    if remote_hash != bundle_hash:
                        raise RuntimeError(f"archive bundle verification failed for {object_key}")

                    # Commit lineage pointers only after the independently stored bundle verifies.
                    for raw_id, member_name, raw_digest, raw_size in members:
                        con.execute(
                            """INSERT INTO er_raw_archive_objects(
                                 raw_snapshot_id,archive_provider,archive_bucket,archive_key,content_hash,
                                 byte_size,compression,status,error_message,archive_member,archive_bundle_hash,
                                 bundle_byte_size,ingestion_run_id)
                               VALUES (?,?,?,?,?,?,?,'archived',NULL,?,?,?,?)
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
                                 archived_at=now()""",
                            [raw_id, "s3_compatible", bucket, object_key, raw_digest, raw_size, "tar+gzip",
                             member_name, bundle_hash, bundle_size, run_id],
                        )
                        archived += 1

                    if compact_enabled():
                        raw_ids = [m[0] for m in members]
                        placeholders = ",".join(["?"] * len(raw_ids))
                        con.execute(
                            f"""UPDATE er_raw_snapshots
                                SET payload_json=NULL,payload_storage='object_archive',payload_compacted_at=now()
                                WHERE raw_snapshot_id IN ({placeholders})""",
                            raw_ids,
                        )
                        compacted += len(raw_ids)
                    else:
                        raw_ids = [m[0] for m in members]
                        placeholders = ",".join(["?"] * len(raw_ids))
                        con.execute(
                            f"UPDATE er_raw_snapshots SET payload_storage='database+object_archive' WHERE raw_snapshot_id IN ({placeholders})",
                            raw_ids,
                        )
                    bundles += 1
                except Exception as exc:
                    failed += len(batch)
                    # We deliberately leave the DB payloads intact when archiving fails.
                    for row in batch:
                        raw_id = row[0]
                        con.execute(
                            """INSERT INTO er_raw_archive_objects(
                                 raw_snapshot_id,archive_provider,archive_bucket,archive_key,content_hash,
                                 compression,status,error_message,archive_member,ingestion_run_id)
                               VALUES (?,?,?,?,?,?,'failed',?,?,?)
                               ON CONFLICT(raw_snapshot_id) DO UPDATE SET
                                 status='failed',error_message=excluded.error_message,
                                 archive_key=excluded.archive_key,archive_member=excluded.archive_member,
                                 ingestion_run_id=excluded.ingestion_run_id""",
                            [raw_id, "s3_compatible", bucket, object_key, row[5], "tar+gzip",
                             str(exc)[:2000], f"{raw_id}.json", run_id],
                        )
                finally:
                    try:
                        tmp_path.unlink(missing_ok=True)
                    except Exception:
                        pass

        return {
            "configured": True,
            "archived": archived,
            "failed": failed,
            "compacted": compacted,
            "bundles": bundles,
            "bucket": bucket,
        }
    finally:
        con.close()


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
