"""Reproducible institutional bulk releases with point-in-time-safe selection.

A release is an `as-known` research artifact: rows are included only if EventRisk had
received/recorded them by the release cutoff. Validity dates alone are not used as the
knowledge cutoff because later corrections can be back-effective. Every Parquet object
is SHA-256 verified and a canonical manifest is itself hashed and archived.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

import boto3

from db import connect
from institutional_readiness_v2 import evaluate_readiness

SCHEMA_VERSION = "v1.7"
MAPPING_VERSION = "mapping_rules_v3_reviewable"
TAXONOMY_VERSION = "event_ontology_v1"
GENERATOR_VERSION = "institutional_exports_v2"

DATASETS = {
    "sources": "SELECT * FROM er_sources WHERE created_at<=?",
    "ingestion_runs": "SELECT * FROM er_ingestion_runs WHERE started_at<=?",
    "event_families": "SELECT * FROM er_event_families WHERE created_at<=?",
    "event_family_versions": "SELECT * FROM er_event_family_versions WHERE created_at<=?",
    "canonical_events": "SELECT * FROM er_canonical_events WHERE created_at<=?",
    "canonical_event_versions": "SELECT * FROM er_canonical_event_versions WHERE created_at<=?",
    "source_contracts": "SELECT * FROM er_source_contracts WHERE first_seen_at<=?",
    "contract_versions": "SELECT * FROM er_source_contract_versions WHERE received_at<=?",
    "contract_version_evidence": "SELECT * FROM er_contract_version_evidence WHERE added_at<=?",
    "event_mappings": "SELECT * FROM er_event_mappings WHERE mapped_at<=?",
    "market_observations": "SELECT * FROM er_market_observations WHERE received_at<=?",
    "entities": "SELECT * FROM er_entities WHERE created_at<=?",
    "entity_identifiers": "SELECT * FROM er_entity_identifiers WHERE created_at<=?",
    "instruments": "SELECT * FROM er_instruments WHERE created_at<=?",
    "instrument_identifiers": "SELECT * FROM er_instrument_identifiers WHERE created_at<=?",
    "graph_edges": "SELECT * FROM er_graph_edges WHERE created_at<=?",
    "edge_evidence": "SELECT * FROM er_edge_evidence WHERE added_at<=?",
    "event_relationships": "SELECT * FROM er_event_relationships WHERE created_at<=?",
    "event_relationship_evidence": "SELECT * FROM er_event_relationship_evidence WHERE added_at<=?",
    "quality_issues": "SELECT * FROM er_quality_issues WHERE detected_at<=?",
    "revisions": "SELECT * FROM er_revisions WHERE recorded_at<=?",
    "methodology_registry": "SELECT * FROM er_methodology_registry WHERE created_at<=?",
    "schema_migrations": "SELECT * FROM er_schema_migrations WHERE applied_at<=?",
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


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _canonical_json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")


def generate_release(
    as_of: datetime | None = None,
    notes: str | None = None,
    *,
    require_ready: bool = True,
) -> dict:
    if not _configured():
        raise RuntimeError("Raw/archive object storage must be configured before generating a bulk release")

    readiness = evaluate_readiness()
    if require_ready and not readiness.get("ready"):
        failed = ", ".join(readiness.get("failed_controls") or [])
        raise RuntimeError(f"Institutional dataset is not distribution-ready: {failed or 'readiness gate failed'}")

    as_of = as_of or datetime.now(timezone.utc)
    if as_of.tzinfo is None:
        as_of = as_of.replace(tzinfo=timezone.utc)
    release_id = f"rel_{as_of:%Y%m%dT%H%M%SZ}_{uuid.uuid4().hex[:8]}"
    bucket = os.environ["RAW_ARCHIVE_BUCKET"]
    client = _client()
    con = connect()
    manifests: list[dict] = []
    readiness_json = json.dumps(readiness, default=str, sort_keys=True)
    try:
        con.execute(
            """INSERT INTO er_data_releases(
                 release_id,schema_version,taxonomy_version,mapping_method_version,as_of,status,notes,
                 generator_version,readiness_snapshot)
               VALUES (?,?,?,?,?,'building',?,?,?)""",
            [
                release_id,
                SCHEMA_VERSION,
                TAXONOMY_VERSION,
                MAPPING_VERSION,
                as_of,
                notes,
                GENERATOR_VERSION,
                readiness_json,
            ],
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

                min_ts = max_ts = None
                if name == "market_observations":
                    min_ts, max_ts = con.execute(
                        "SELECT min(observed_at),max(observed_at) FROM er_market_observations WHERE received_at<=?",
                        [as_of],
                    ).fetchone()
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
                    "min_observed_at": min_ts,
                    "max_observed_at": max_ts,
                    "content_hash": digest,
                    "byte_size": byte_size,
                })

            manifest = {
                "release_id": release_id,
                "as_of": as_of.isoformat(),
                "schema_version": SCHEMA_VERSION,
                "taxonomy_version": TAXONOMY_VERSION,
                "mapping_method_version": MAPPING_VERSION,
                "generator_version": GENERATOR_VERSION,
                "point_in_time_semantics": "as_known_by_eventrisk_at_or_before_as_of",
                "readiness": readiness,
                "datasets": sorted(manifests, key=lambda x: x["dataset_name"]),
            }
            manifest_bytes = _canonical_json(manifest)
            manifest_hash = _sha256_bytes(manifest_bytes)
            manifest_key = f"releases/{release_id}/manifest.json"
            client.put_object(
                Bucket=bucket,
                Key=manifest_key,
                Body=manifest_bytes,
                ContentType="application/json",
                Metadata={"sha256": manifest_hash, "release_id": release_id},
            )
            head = client.head_object(Bucket=bucket, Key=manifest_key)
            if (head.get("Metadata") or {}).get("sha256") != manifest_hash:
                raise RuntimeError("Object-store verification failed for release manifest")

        manifest_uri = f"s3://{bucket}/{manifest_key}"
        con.execute(
            """UPDATE er_data_releases
               SET status='ready',manifest_hash=?,manifest_object_uri=?,dataset_count=?
               WHERE release_id=?""",
            [manifest_hash, manifest_uri, len(manifests), release_id],
        )
        return {
            "release_id": release_id,
            "as_of": as_of.isoformat(),
            "status": "ready",
            "manifest_hash": manifest_hash,
            "manifest_object_uri": manifest_uri,
            "dataset_count": len(manifests),
            "datasets": manifests,
        }
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


def release_manifest(release_id: str) -> dict | None:
    con = connect(read_only=True)
    try:
        release_cur = con.execute("SELECT * FROM er_data_releases WHERE release_id=?", [release_id])
        cols = [d[0] for d in release_cur.description]
        row = release_cur.fetchone()
        if not row:
            return None
        release = dict(zip(cols, row))
        cur = con.execute(
            "SELECT * FROM er_export_manifests WHERE release_id=? ORDER BY dataset_name",
            [release_id],
        )
        mcols = [d[0] for d in cur.description]
        datasets = [dict(zip(mcols, r)) for r in cur.fetchall()]
        return {"release": release, "datasets": datasets}
    finally:
        con.close()
