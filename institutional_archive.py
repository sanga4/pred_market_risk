"""Archive raw institutional source snapshots to S3-compatible object storage.

Raw JSON first lands in the analytical database so normalization and lineage are atomic.
This module then writes a gzip-compressed independent copy to object storage, verifies the
SHA-256 metadata, and can compact the duplicate database JSON body. IDs, timestamps,
hashes and archive pointers remain queryable after compaction.
"""

from __future__ import annotations

import gzip
import hashlib
import os
from datetime import timezone

import boto3

from db import connect


def configured() -> bool:
    required = [
        "RAW_ARCHIVE_ENDPOINT",
        "RAW_ARCHIVE_BUCKET",
        "RAW_ARCHIVE_ACCESS_KEY_ID",
        "RAW_ARCHIVE_SECRET_ACCESS_KEY",
    ]
    return all(os.getenv(k) for k in required)


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


def archive_key(source_id, object_type, object_id, received_at, raw_snapshot_id):
    ts = received_at.astimezone(timezone.utc)
    safe_object = str(object_id).replace("/", "_")[:160]
    return (
        f"raw/source={source_id}/year={ts:%Y}/month={ts:%m}/day={ts:%d}/"
        f"type={object_type}/{safe_object}/{ts:%H%M%S}_{raw_snapshot_id}.json.gz"
    )


def archive_pending(limit: int = 5000) -> dict:
    if not configured():
        return {"configured": False, "archived": 0, "failed": 0, "compacted": 0}
    client = s3_client()
    bucket = os.environ["RAW_ARCHIVE_BUCKET"]
    con = connect()
    archived = failed = compacted = 0
    try:
        rows = con.execute(
            """SELECT raw_snapshot_id,source_id,source_object_type,source_object_id,
                      received_at,content_hash,payload_json
               FROM er_unarchived_raw_snapshots
               WHERE payload_json IS NOT NULL
               ORDER BY received_at
               LIMIT ?""",
            [limit],
        ).fetchall()
        for snapshot_id, source_id, object_type, object_id, received_at, content_hash, payload_json in rows:
            key = archive_key(source_id, object_type, object_id, received_at, snapshot_id)
            body = gzip.compress(payload_json.encode("utf-8"), compresslevel=6)
            digest = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
            if content_hash and digest != content_hash:
                failed += 1
                con.execute(
                    """INSERT INTO er_raw_archive_objects(
                         raw_snapshot_id,archive_provider,archive_bucket,archive_key,content_hash,
                         byte_size,compression,status,error_message)
                       VALUES (?,?,?,?,?,?,?,'failed',?)
                       ON CONFLICT(raw_snapshot_id) DO UPDATE SET
                         status='failed',error_message=excluded.error_message""",
                    [snapshot_id, "s3_compatible", bucket, key, digest, len(body), "gzip",
                     "Content hash mismatch before archive"],
                )
                continue
            try:
                client.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=body,
                    ContentType="application/json",
                    ContentEncoding="gzip",
                    Metadata={"sha256": digest, "raw_snapshot_id": snapshot_id},
                )
                # HEAD verification catches failed/misdirected writes without requiring a
                # second transfer of the entire object.
                head = client.head_object(Bucket=bucket, Key=key)
                remote_digest = (head.get("Metadata") or {}).get("sha256")
                if remote_digest != digest:
                    raise RuntimeError("archive metadata hash verification failed")
                con.execute(
                    """INSERT INTO er_raw_archive_objects(
                         raw_snapshot_id,archive_provider,archive_bucket,archive_key,content_hash,
                         byte_size,compression,status,error_message)
                       VALUES (?,?,?,?,?,?,?,'archived',NULL)
                       ON CONFLICT(raw_snapshot_id) DO UPDATE SET
                         archive_key=excluded.archive_key,content_hash=excluded.content_hash,
                         byte_size=excluded.byte_size,status='archived',error_message=NULL,archived_at=now()""",
                    [snapshot_id, "s3_compatible", bucket, key, digest, len(body), "gzip"],
                )
                archived += 1
                if compact_enabled():
                    con.execute(
                        """UPDATE er_raw_snapshots
                           SET payload_json=NULL,payload_storage='object_archive',payload_compacted_at=now()
                           WHERE raw_snapshot_id=?""",
                        [snapshot_id],
                    )
                    compacted += 1
                else:
                    con.execute(
                        "UPDATE er_raw_snapshots SET payload_storage='database+object_archive' WHERE raw_snapshot_id=?",
                        [snapshot_id],
                    )
            except Exception as exc:
                failed += 1
                con.execute(
                    """INSERT INTO er_raw_archive_objects(
                         raw_snapshot_id,archive_provider,archive_bucket,archive_key,content_hash,
                         byte_size,compression,status,error_message)
                       VALUES (?,?,?,?,?,?,?,'failed',?)
                       ON CONFLICT(raw_snapshot_id) DO UPDATE SET
                         status='failed',error_message=excluded.error_message""",
                    [snapshot_id, "s3_compatible", bucket, key, digest, len(body), "gzip", str(exc)[:2000]],
                )
        return {
            "configured": True,
            "archived": archived,
            "failed": failed,
            "compacted": compacted,
            "bucket": bucket,
        }
    finally:
        con.close()


if __name__ == "__main__":
    print(archive_pending())
