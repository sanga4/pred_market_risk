"""Point-in-time institutional bulk releases v4 with instrument reference attributes.

v4 preserves v3's as-known release semantics and immutable manifest verification. It adds the
source-described instrument reference attribute ledger (sector/industry/country/exchange label)
without backdating those attributes to source timestamps. Source timestamps remain metadata;
EventRisk recorded_at/valid_from controls historical availability.
"""

from __future__ import annotations

import hashlib
import json
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

import institutional_exports_v3 as v3
from db import connect
from institutional_readiness_v2 import evaluate_readiness

SCHEMA_VERSION = "v2.1"
MAPPING_VERSION = v3.MAPPING_VERSION
TAXONOMY_VERSION = v3.TAXONOMY_VERSION
GENERATOR_VERSION = "institutional_exports_v4_instrument_reference"

DATASETS = dict(v3.DATASETS)
DATASETS.update({
    "instrument_reference_attribute_versions": """
        SELECT * EXCLUDE(valid_to),
               CASE WHEN valid_to IS NOT NULL AND valid_to<=? THEN valid_to ELSE NULL END AS valid_to
        FROM er_instrument_reference_attribute_versions
        WHERE recorded_at<=?
        ORDER BY instrument_id,version_no
    """,
    "instrument_reference_attributes_as_of": """
        WITH eligible AS (
          SELECT *,row_number() OVER(PARTITION BY instrument_id ORDER BY version_no DESC) rn
          FROM er_instrument_reference_attribute_versions
          WHERE recorded_at<=? AND valid_from<=? AND (valid_to IS NULL OR valid_to>?)
        )
        SELECT * EXCLUDE(rn,valid_to),NULL::TIMESTAMPTZ AS valid_to
        FROM eligible WHERE rn=1 ORDER BY instrument_id
    """,
})


def _coverage(con, as_of: datetime) -> dict:
    coverage = v3._coverage(con, as_of)
    instrument_ref = con.execute(
        """SELECT min(recorded_at),max(recorded_at),count(*)
           FROM er_instrument_reference_attribute_versions WHERE recorded_at<=?""",
        [as_of],
    ).fetchone()
    coverage["instrument_reference_versions"] = {
        "first_recorded_at": instrument_ref[0],
        "last_recorded_at": instrument_ref[1],
        "rows": instrument_ref[2],
        "coverage_note": (
            "Source-described instrument attributes are prospective from EventRisk record time; "
            "source_updated_at is retained as metadata and is not used to backdate as-known availability."
        ),
    }
    return coverage


def generate_release(
    as_of: datetime | None = None,
    notes: str | None = None,
    *,
    require_ready: bool = True,
) -> dict:
    if not v3._configured():
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
    import os
    bucket = os.environ["RAW_ARCHIVE_BUCKET"]
    client = v3._client()
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
                release_id,SCHEMA_VERSION,TAXONOMY_VERSION,MAPPING_VERSION,as_of,notes,
                GENERATOR_VERSION,readiness_json,
            ],
        )

        with tempfile.TemporaryDirectory(prefix="eventrisk-release-") as tmp:
            tmpdir = Path(tmp)
            for name, sql in DATASETS.items():
                out = tmpdir / f"{name}.parquet"
                table = con.execute(sql, v3._params(sql, as_of)).fetch_arrow_table()
                row_count = table.num_rows
                con.register("release_dataset", table)
                escaped = str(out).replace("'", "''")
                try:
                    con.execute(f"COPY release_dataset TO '{escaped}' (FORMAT PARQUET, COMPRESSION ZSTD)")
                finally:
                    con.unregister("release_dataset")

                digest = v3._sha256(out)
                byte_size = out.stat().st_size
                key = f"releases/{release_id}/{name}.parquet"
                client.upload_file(
                    str(out),bucket,key,
                    ExtraArgs={
                        "ContentType":"application/vnd.apache.parquet",
                        "Metadata":{"sha256":digest,"release_id":release_id,"dataset":name},
                    },
                )
                head = client.head_object(Bucket=bucket, Key=key)
                if (head.get("Metadata") or {}).get("sha256") != digest:
                    raise RuntimeError(f"Object-store verification failed for {name}")

                min_ts = max_ts = None
                if name == "market_observations":
                    min_ts,max_ts = con.execute(
                        "SELECT min(observed_at),max(observed_at) FROM er_market_observations WHERE received_at<=?",
                        [as_of],
                    ).fetchone()
                object_uri = f"s3://{bucket}/{key}"
                con.execute(
                    """INSERT INTO er_export_manifests(
                         release_id,dataset_name,format,object_uri,row_count,min_observed_at,max_observed_at,
                         content_hash,byte_size)
                       VALUES (?,?,?,?,?,?,?,?,?)""",
                    [release_id,name,"parquet",object_uri,row_count,min_ts,max_ts,digest,byte_size],
                )
                manifests.append({
                    "dataset_name":name,"format":"parquet","object_uri":object_uri,
                    "row_count":row_count,"min_observed_at":min_ts,"max_observed_at":max_ts,
                    "content_hash":digest,"byte_size":byte_size,
                })

            manifest = {
                "release_id":release_id,
                "as_of":as_of.isoformat(),
                "schema_version":SCHEMA_VERSION,
                "taxonomy_version":TAXONOMY_VERSION,
                "mapping_method_version":MAPPING_VERSION,
                "generator_version":GENERATOR_VERSION,
                "point_in_time_semantics":(
                    "only_information_recorded_by_eventrisk_at_or_before_as_of; later closure fields masked"
                ),
                "normalization_tiers": {
                    "observed":"immutable source/market observations and source payload lineage",
                    "legacy_partial":"pre-v2.0 normalized contract history missing some semantic fields; no retroactive completion",
                    "full_v2":"prospective full source-contract semantic ledger with event+market raw evidence",
                    "instrument_reference":"prospective source-described instrument attributes recorded by EventRisk without issuer inference",
                },
                "coverage":coverage,
                "readiness":readiness,
                "datasets":sorted(manifests,key=lambda x:x["dataset_name"]),
            }
            manifest_bytes = v3._canonical_json(manifest)
            manifest_hash = hashlib.sha256(manifest_bytes).hexdigest()
            manifest_key = f"releases/{release_id}/manifest.json"
            client.put_object(
                Bucket=bucket,Key=manifest_key,Body=manifest_bytes,ContentType="application/json",
                Metadata={"sha256":manifest_hash,"release_id":release_id},
            )
            head = client.head_object(Bucket=bucket, Key=manifest_key)
            if (head.get("Metadata") or {}).get("sha256") != manifest_hash:
                raise RuntimeError("Object-store verification failed for release manifest")

        manifest_uri = f"s3://{bucket}/{manifest_key}"
        con.execute(
            """UPDATE er_data_releases
               SET status='ready',manifest_hash=?,manifest_object_uri=?,dataset_count=?
               WHERE release_id=?""",
            [manifest_hash,manifest_uri,len(manifests),release_id],
        )
        return {
            "release_id":release_id,"as_of":as_of.isoformat(),"status":"ready",
            "manifest_hash":manifest_hash,"manifest_object_uri":manifest_uri,
            "dataset_count":len(manifests),"coverage":coverage,"datasets":manifests,
        }
    except Exception:
        con.execute("UPDATE er_data_releases SET status='failed' WHERE release_id=?", [release_id])
        raise
    finally:
        con.close()


def list_releases(limit: int = 50) -> list[dict]:
    return v3.list_releases(limit)


def release_manifest(release_id: str) -> dict | None:
    return v3.release_manifest(release_id)
