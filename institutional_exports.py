"""Generate point-in-time institutional Parquet releases.

Each release records its `as_of` timestamp, schema/mapping versions, per-dataset row counts,
SHA-256 hashes and object-storage locations. Releases are descriptive event data only.
"""

from __future__ import annotations

import hashlib
import os
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

import boto3

from db import connect

SCHEMA_VERSION = "v1"
MAPPING_VERSION = "mapping_rules_v2_blocked"
TAXONOMY_VERSION = "event_ontology_v1"

DATASETS = {
    "event_families": "SELECT * FROM er_event_families WHERE created_at <= ?",
    "canonical_events": "SELECT * FROM er_canonical_events WHERE created_at <= ?",
    "source_contracts": "SELECT * FROM er_source_contracts WHERE first_seen_at <= ?",
    "contract_versions": "SELECT * FROM er_source_contract_versions WHERE valid_from <= ?",
    "contract_version_evidence": """
        SELECT ev.* FROM er_contract_version_evidence ev
        JOIN er_source_contract_versions v
          ON v.source_contract_id=ev.source_contract_id AND v.version_no=ev.version_no
        WHERE v.valid_from <= ?
    """,
    "event_mappings": "SELECT * FROM er_event_mappings WHERE valid_from <= ?",
    "market_observations": "SELECT * FROM er_market_observations WHERE received_at <= ?",
    "entities": "SELECT * FROM er_entities WHERE created_at <= ?",
    "entity_identifiers": "SELECT * FROM er_entity_identifiers WHERE valid_from IS NULL OR valid_from <= ?",
    "instruments": "SELECT * FROM er_instruments WHERE created_at <= ?",
    "instrument_identifiers": "SELECT * FROM er_instrument_identifiers WHERE valid_from IS NULL OR valid_from <= ?",
    "graph_edges": "SELECT * FROM er_graph_edges WHERE valid_from <= ?",
    "edge_evidence": """
        SELECT ee.* FROM er_edge_evidence ee
        JOIN er_graph_edges ge USING(edge_id)
        WHERE ge.valid_from <= ?
    """,
    "event_relationships": "SELECT * FROM er_event_relationships WHERE valid_from <= ?",
    "quality_issues": "SELECT * FROM er_quality_issues WHERE detected_at <= ?",
}


def _configured() -> bool:
    return all(os.getenv(k) for k in [
        "RAW_ARCHIVE_ENDPOINT",
        "RAW_ARCHIVE_BUCKET",
        "RAW_ARCHIVE_ACCESS_KEY_ID",
        "RAW_ARCHIVE_SECRET_ACCESS_KEY",
    ])


def _client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["RAW_ARCHIVE_ENDPOINT"],
        aws_access_key_id=os.environ["RAW_ARCHIVE_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["RAW_ARCHIVE_SECRET_ACCESS_KEY"],
        region_name=os.getenv("RAW_ARCHIVE_REGION") or "auto",
    )


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def generate_release(as_of: datetime | None = None, notes: str | None = None) -> dict:
    if not _configured():
        raise RuntimeError("Raw/archive object storage must be configured before generating a bulk release")
    as_of = as_of or datetime.now(timezone.utc)
    if as_of.tzinfo is None:
        as_of = as_of.replace(tzinfo=timezone.utc)
    release_id = f"rel_{as_of:%Y%m%dT%H%M%SZ}_{uuid.uuid4().hex[:8]}"
    bucket = os.environ["RAW_ARCHIVE_BUCKET"]
    client = _client()
    con = connect()
    manifests = []
    try:
        con.execute(
            """INSERT INTO er_data_releases(
                 release_id,schema_version,taxonomy_version,mapping_method_version,as_of,status,notes)
               VALUES (?,?,?,?,?,'building',?)""",
            [release_id, SCHEMA_VERSION, TAXONOMY_VERSION, MAPPING_VERSION, as_of, notes],
        )

        with tempfile.TemporaryDirectory(prefix="eventrisk-release-") as tmp:
            tmpdir = Path(tmp)
            for name, sql in DATASETS.items():
                out = tmpdir / f"{name}.parquet"
                table = con.execute(sql, [as_of]).fetch_arrow_table()
                row_count = table.num_rows
                con.register("release_dataset", table)
                escaped_out = str(out).replace("'", "''")
                try:
                    con.execute(
                        f"COPY release_dataset TO '{escaped_out}' (FORMAT PARQUET, COMPRESSION ZSTD)"
                    )
                finally:
                    con.unregister("release_dataset")

                digest = _sha256(out)
                byte_size = out.stat().st_size
                object_key = f"releases/{release_id}/{name}.parquet"
                client.upload_file(
                    str(out),
                    bucket,
                    object_key,
                    ExtraArgs={
                        "ContentType": "application/vnd.apache.parquet",
                        "Metadata": {"sha256": digest, "release_id": release_id, "dataset": name},
                    },
                )
                head = client.head_object(Bucket=bucket, Key=object_key)
                if (head.get("Metadata") or {}).get("sha256") != digest:
                    raise RuntimeError(f"Object-store verification failed for {name}")

                time_row = None
                if name == "market_observations":
                    time_row = con.execute(
                        "SELECT min(observed_at),max(observed_at) FROM er_market_observations WHERE received_at<=?",
                        [as_of],
                    ).fetchone()
                min_ts, max_ts = time_row if time_row else (None, None)
                object_uri = f"s3://{bucket}/{object_key}"
                con.execute(
                    """INSERT INTO er_export_manifests(
                         release_id,dataset_name,format,object_uri,row_count,min_observed_at,max_observed_at,
                         content_hash,byte_size)
                       VALUES (?,?,?,?,?,?,?,?,?)""",
                    [release_id, name, "parquet", object_uri, row_count, min_ts, max_ts, digest, byte_size],
                )
                manifests.append({
                    "dataset_name": name,
                    "format": "parquet",
                    "object_uri": object_uri,
                    "row_count": row_count,
                    "content_hash": digest,
                    "byte_size": byte_size,
                })

        con.execute("UPDATE er_data_releases SET status='ready' WHERE release_id=?", [release_id])
        return {"release_id": release_id, "as_of": as_of.isoformat(), "status": "ready", "datasets": manifests}
    except Exception:
        con.execute("UPDATE er_data_releases SET status='failed' WHERE release_id=?", [release_id])
        raise
    finally:
        con.close()


def list_releases(limit: int = 50) -> list[dict]:
    con = connect(read_only=True)
    try:
        cur = con.execute("SELECT * FROM er_data_releases ORDER BY generated_at DESC LIMIT ?", [limit])
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        con.close()


def release_manifest(release_id: str) -> list[dict]:
    con = connect(read_only=True)
    try:
        cur = con.execute(
            "SELECT * FROM er_export_manifests WHERE release_id=? ORDER BY dataset_name",
            [release_id],
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        con.close()
