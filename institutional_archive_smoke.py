"""Non-destructive S3-compatible archive connectivity smoke check.

Uploads a tiny marker object, verifies its SHA-256 metadata with HEAD, then deletes it.
No credentials or secret values are printed.
"""
from __future__ import annotations

import hashlib
import json
import os
import uuid

from institutional_archive_v2 import configured, s3_client


def main() -> None:
    result = {
        "configured": configured(),
        "endpoint_present": bool(os.getenv("RAW_ARCHIVE_ENDPOINT")),
        "bucket_present": bool(os.getenv("RAW_ARCHIVE_BUCKET")),
        "access_key_present": bool(os.getenv("RAW_ARCHIVE_ACCESS_KEY_ID")),
        "secret_key_present": bool(os.getenv("RAW_ARCHIVE_SECRET_ACCESS_KEY")),
    }
    print(json.dumps({"stage": "config", **result}), flush=True)
    if not result["configured"]:
        raise RuntimeError("raw archive variables are not fully configured")

    client = s3_client()
    bucket = os.environ["RAW_ARCHIVE_BUCKET"]
    key = f"health/archive-smoke-{uuid.uuid4().hex}.txt"
    payload = b"eventrisk-archive-smoke-v1\n"
    digest = hashlib.sha256(payload).hexdigest()
    print(json.dumps({"stage": "put", "bucket_present": True}), flush=True)
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=payload,
        ContentType="text/plain",
        Metadata={"sha256": digest, "purpose": "archive-smoke"},
    )
    try:
        head = client.head_object(Bucket=bucket, Key=key)
        remote_digest = (head.get("Metadata") or {}).get("sha256")
        if remote_digest != digest:
            raise RuntimeError("archive smoke metadata verification failed")
        print(json.dumps({"stage": "verified", "ok": True}), flush=True)
    finally:
        client.delete_object(Bucket=bucket, Key=key)
        print(json.dumps({"stage": "cleanup", "ok": True}), flush=True)


if __name__ == "__main__":
    main()
