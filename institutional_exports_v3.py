"""Point-in-time-safe institutional bulk releases v3.

Historical releases are assembled from immutable observations and version ledgers, never
from mutable current-state masters. Fields that were only populated by later closure or
resolution are masked at the requested cutoff. The manifest explicitly reports coverage
boundaries for prospective full-semantic/reference data.
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

SCHEMA_VERSION = "v2.0"
MAPPING_VERSION = "mapping_rules_v3_reviewable"
TAXONOMY_VERSION = "event_ontology_v1"
GENERATOR_VERSION = "institutional_exports_v3"

DATASETS = {
    "source_versions": """
        SELECT * EXCLUDE(valid_to),
               CASE WHEN valid_to IS NOT NULL AND valid_to<=? THEN valid_to ELSE NULL END AS valid_to
        FROM er_source_versions WHERE recorded_at<=? ORDER BY source_id,version_no
    """,
    "sources_as_of": """
        WITH eligible AS (
          SELECT *,row_number() OVER(PARTITION BY source_id ORDER BY version_no DESC) rn
          FROM er_source_versions WHERE recorded_at<=?
        )
        SELECT * EXCLUDE(rn,valid_to),NULL::TIMESTAMPTZ AS valid_to
        FROM eligible WHERE rn=1 ORDER BY source_id
    """,
    "ingestion_runs_completed": """
        SELECT * FROM er_ingestion_runs
        WHERE finished_at IS NOT NULL AND finished_at<=? ORDER BY started_at
    """,
    "event_family_versions": """
        SELECT * EXCLUDE(valid_to),
               CASE WHEN valid_to IS NOT NULL AND valid_to<=? THEN valid_to ELSE NULL END AS valid_to
        FROM er_event_family_versions WHERE created_at<=? ORDER BY event_family_id,version_no
    """,
    "event_families_as_of": """
        WITH eligible AS (
          SELECT *,row_number() OVER(PARTITION BY event_family_id ORDER BY version_no DESC) rn
          FROM er_event_family_versions
          WHERE created_at<=? AND valid_from<=? AND (valid_to IS NULL OR valid_to>?)
        )
        SELECT * EXCLUDE(rn,valid_to),NULL::TIMESTAMPTZ AS valid_to
        FROM eligible WHERE rn=1 ORDER BY event_family_id
    """,
    "canonical_event_versions": """
        SELECT * EXCLUDE(valid_to),
               CASE WHEN valid_to IS NOT NULL AND valid_to<=? THEN valid_to ELSE NULL END AS valid_to
        FROM er_canonical_event_versions WHERE created_at<=? ORDER BY canonical_event_id,version_no
    """,
    "canonical_events_as_of": """
        WITH eligible AS (
          SELECT *,row_number() OVER(PARTITION BY canonical_event_id ORDER BY version_no DESC) rn
          FROM er_canonical_event_versions
          WHERE created_at<=? AND valid_from<=? AND (valid_to IS NULL OR valid_to>?)
        )
        SELECT * EXCLUDE(rn,valid_to),NULL::TIMESTAMPTZ AS valid_to
        FROM eligible WHERE rn=1 ORDER BY canonical_event_id
    """,
    "legacy_contract_versions": """
        SELECT * EXCLUDE(valid_to),
               'legacy_partial' AS normalization_tier,
               CASE WHEN valid_to IS NOT NULL AND valid_to<=? THEN valid_to ELSE NULL END AS valid_to
        FROM er_source_contract_versions WHERE received_at<=? ORDER BY source_contract_id,version_no
    """,
    "full_contract_semantic_versions": """
        SELECT * EXCLUDE(valid_to),
               CASE WHEN valid_to IS NOT NULL AND valid_to<=? THEN valid_to ELSE NULL END AS valid_to
        FROM er_contract_semantic_versions WHERE received_at<=? ORDER BY source_contract_id,version_no
    """,
    "contracts_as_of": """
        WITH eligible AS (
          SELECT *,row_number() OVER(PARTITION BY source_contract_id ORDER BY version_no DESC) rn
          FROM er_contract_semantic_versions
          WHERE received_at<=? AND valid_from<=? AND (valid_to IS NULL OR valid_to>?)
        )
        SELECT * EXCLUDE(rn,valid_to),NULL::TIMESTAMPTZ AS valid_to
        FROM eligible WHERE rn=1 ORDER BY source_contract_id
    """,
    "contract_version_evidence": """
        SELECT * FROM er_contract_version_evidence
        WHERE added_at<=? ORDER BY source_contract_id,version_no,evidence_role
    """,
    "event_mappings": """
        SELECT * EXCLUDE(valid_to),
               CASE WHEN valid_to IS NOT NULL AND valid_to<=? THEN valid_to ELSE NULL END AS valid_to
        FROM er_event_mappings WHERE mapped_at<=? ORDER BY source_contract_id,valid_from,mapped_at
    """,
    "market_observations": """
        SELECT * FROM er_market_observations
        WHERE received_at<=? ORDER BY received_at,source_contract_id
    """,
    "entity_versions": """
        SELECT * EXCLUDE(valid_to),
               CASE WHEN valid_to IS NOT NULL AND valid_to<=? THEN valid_to ELSE NULL END AS valid_to
        FROM er_entity_versions WHERE recorded_at<=? ORDER BY entity_id,version_no
    """,
    "entities_as_of": """
        WITH eligible AS (
          SELECT *,row_number() OVER(PARTITION BY entity_id ORDER BY version_no DESC) rn
          FROM er_entity_versions WHERE recorded_at<=?
        )
        SELECT * EXCLUDE(rn,valid_to),NULL::TIMESTAMPTZ AS valid_to
        FROM eligible WHERE rn=1 ORDER BY entity_id
    """,
    "entity_identifiers": """
        SELECT * EXCLUDE(valid_to),
               CASE WHEN valid_to IS NOT NULL AND valid_to<=? THEN valid_to ELSE NULL END AS valid_to
        FROM er_entity_identifiers WHERE created_at<=? ORDER BY entity_id,identifier_type,identifier_value
    """,
    "instrument_versions": """
        SELECT * EXCLUDE(valid_to),
               CASE WHEN valid_to IS NOT NULL AND valid_to<=? THEN valid_to ELSE NULL END AS valid_to
        FROM er_instrument_versions WHERE recorded_at<=? ORDER BY instrument_id,version_no
    """,
    "instruments_as_of": """
        WITH eligible AS (
          SELECT *,row_number() OVER(PARTITION BY instrument_id ORDER BY version_no DESC) rn
          FROM er_instrument_versions WHERE recorded_at<=?
        )
        SELECT * EXCLUDE(rn,valid_to),NULL::TIMESTAMPTZ AS valid_to
        FROM eligible WHERE rn=1 ORDER BY instrument_id
    """,
    "instrument_identifiers": """
        SELECT * EXCLUDE(valid_to),
               CASE WHEN valid_to IS NOT NULL AND valid_to<=? THEN valid_to ELSE NULL END AS valid_to
        FROM er_instrument_identifiers WHERE created_at<=? ORDER BY instrument_id,identifier_type,identifier_value
    """,
    "graph_edges": """
        SELECT * EXCLUDE(valid_to),
               CASE WHEN valid_to IS NOT NULL AND valid_to<=? THEN valid_to ELSE NULL END AS valid_to
        FROM er_graph_edges WHERE created_at<=? ORDER BY created_at,edge_id
    """,
    "edge_evidence": """
        SELECT * FROM er_edge_evidence WHERE added_at<=? ORDER BY added_at,edge_id,evidence_id
    """,
    "event_relationships": """
        SELECT * EXCLUDE(valid_to),
               CASE WHEN valid_to IS NOT NULL AND valid_to<=? THEN valid_to ELSE NULL END AS valid_to
        FROM er_event_relationships WHERE created_at<=? ORDER BY created_at,relationship_id
    """,
    "event_relationship_evidence": """
        SELECT * FROM er_event_relationship_evidence
        WHERE added_at<=? ORDER BY added_at,relationship_id,evidence_id
    """,
    "quality_issues": """
        SELECT * EXCLUDE(resolved_at,resolution_notes),
               CASE WHEN resolved_at IS NOT NULL AND resolved_at<=? THEN resolved_at ELSE NULL END AS resolved_at,
               CASE WHEN resolved_at IS NOT NULL AND resolved_at<=? THEN resolution_notes ELSE NULL END AS resolution_notes
        FROM er_quality_issues WHERE detected_at<=? ORDER BY detected_at,quality_issue_id
    """,
    "revisions": """
        SELECT * FROM er_revisions WHERE recorded_at<=? ORDER BY recorded_at,revision_id
    """,
    "methodology_registry": """
        SELECT * EXCLUDE(effective_to),
               CASE WHEN effective_to IS NOT NULL AND effective_to<=? THEN effective_to ELSE NULL END AS effective_to
        FROM er_methodology_registry WHERE created_at<=? ORDER BY methodology_id,methodology_version
    """,
    "schema_migrations": """
        SELECT * FROM er_schema_migrations WHERE applied_at<=? ORDER BY applied_at,migration_name
    """,
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


def _canonical_json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")


def _params(sql: str, as_of: datetime) -> list[datetime]:
    return [as_of] * sql.count("?")


def _coverage(con, as_of: datetime) -> dict:
    market = con.execute(
        "SELECT min(received_at),max(received_at),count(*) FROM er_market_observations WHERE received_at<=?",
        [as_of],
    ).fetchone()
    semantic = con.execute(
        "SELECT min(received_at),max(received_at),count(*) FROM er_contract_semantic_versions WHERE received_at<=?",
        [as_of],
    ).fetchone()
    entity_ref = con.execute(
        "SELECT min(recorded_at),max(recorded_at),count(*) FROM er_entity_versions WHERE recorded_at<=?",
        [as_of],
    ).fetchone()
    return {
        "market_observations": {
            "first_received_at": market[0],
            "last_received_at": market[1],
            "rows": market[2],
        },
        "full_contract_semantics": {
            "first_received_at": semantic[0],
            "last_received_at": semantic[1],
            "rows": semantic[2],
            "coverage_note": (
                "Prospective full semantic normalization starts with institutional_ingest_v3; "
                "earlier contract versions remain legacy_partial."
            ),
        },
        "entity_reference_versions": {
            "first_recorded_at": entity_ref[0],
            "last_recorded_at": entity_ref[1],
            "rows": entity_ref[2],
            "coverage_note": (
                "Reference-master point-in-time versioning begins at institutional_schema_011; "
                "earlier current metadata is not backdated as known-at-the-time."
            ),
        },
    }


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
        raise RuntimeError(
            f"Institutional dataset is not distribution-ready: {failed or 'readiness gate failed'}"
        )

    as_of = as_of or datetime.now(timezone.utc)
    if as_of.tzinfo is None:
        as_of = as_of.replace(tzinfo=timezone.utc)
    release_id = f"rel_{as_of:%Y%m%dT%H%M%SZ}_{uuid.uuid4().hex[:8]}"
    bucket = os.environ["RAW_ARCHIVE_BUCKET"]
    client = _client()
    con = connect()
    manifests: list[dict] = []
    readiness_json = json.dumps(readiness, default=str, sort_keys=True)
    coverage = _coverage(con, as_of)
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
                table = con.execute(sql, _params(sql, as_of)).fetch_arrow_table()
                row_count = table.num_rows
                con.register("release_dataset", table)
                escaped = str(out).replace("'", "''")
                try:
                    con.execute(
                        f"COPY release_dataset TO '{escaped}' (FORMAT PARQUET, COMPRESSION ZSTD)"
                    )
                finally:
                    con.unregister("release_dataset")

                digest = _sha256(out)
                byte_size = out.stat().st_size
                key = f"releases/{release_id}/{name}.parquet"
                client.upload_file(
                    str(out),
                    bucket,
                    key,
                    ExtraArgs={
                        "ContentType": "application/vnd.apache.parquet",
                        "Metadata": {
                            "sha256": digest,
                            "release_id": release_id,
                            "dataset": name,
                        },
                    },
                )
                head = client.head_object(Bucket=bucket, Key=key)
                if (head.get("Metadata") or {}).get("sha256") != digest:
                    raise RuntimeError(f"Object-store verification failed for {name}")

                min_ts = max_ts = None
                if name == "market_observations":
                    min_ts, max_ts = con.execute(
                        "SELECT min(observed_at),max(observed_at) FROM er_market_observations WHERE received_at<=?",
                        [as_of],
                    ).fetchone()
                object_uri = f"s3://{bucket}/{key}"
                con.execute(
                    """INSERT INTO er_export_manifests(
                         release_id,dataset_name,format,object_uri,row_count,min_observed_at,max_observed_at,
                         content_hash,byte_size)
                       VALUES (?,?,?,?,?,?,?,?,?)""",
                    [
                        release_id,
                        name,
                        "parquet",
                        object_uri,
                        row_count,
                        min_ts,
                        max_ts,
                        digest,
                        byte_size,
                    ],
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
                "point_in_time_semantics": (
                    "only_information_recorded_by_eventrisk_at_or_before_as_of; "
                    "later closure fields masked"
                ),
                "normalization_tiers": {
                    "observed": "immutable source/market observations and source payload lineage",
                    "legacy_partial": (
                        "pre-v2.0 normalized contract history missing some semantic fields; "
                        "no retroactive completion"
                    ),
                    "full_v2": (
                        "prospective full source-contract semantic ledger with event+market raw evidence"
                    ),
                },
                "coverage": coverage,
                "readiness": readiness,
                "datasets": sorted(manifests, key=lambda x: x["dataset_name"]),
            }
            manifest_bytes = _canonical_json(manifest)
            manifest_hash = hashlib.sha256(manifest_bytes).hexdigest()
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
            "coverage": coverage,
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
        cur = con.execute(
            "SELECT * FROM er_data_releases ORDER BY generated_at DESC LIMIT ?", [limit]
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        con.close()


def release_manifest(release_id: str) -> dict | None:
    con = connect(read_only=True)
    try:
        cur = con.execute("SELECT * FROM er_data_releases WHERE release_id=?", [release_id])
        cols = [d[0] for d in cur.description]
        row = cur.fetchone()
        if not row:
            return None
        release = dict(zip(cols, row))
        mcur = con.execute(
            "SELECT * FROM er_export_manifests WHERE release_id=? ORDER BY dataset_name",
            [release_id],
        )
        mcols = [d[0] for d in mcur.description]
        datasets = [dict(zip(mcols, r)) for r in mcur.fetchall()]
        return {"release": release, "datasets": datasets}
    finally:
        con.close()
