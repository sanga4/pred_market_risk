"""Set-based point-in-time versioning for mutable institutional reference masters.

Semantics match `institutional_reference_versions.py`, but all current-vs-latest comparisons,
interval closure and next-version inserts happen set-wise inside one transaction. This removes
one remote warehouse round trip per source/entity/instrument.
"""

from __future__ import annotations

from db import connect

METHOD_VERSION = "reference_versions_v2_set_based"


SPECS = [
    {
        "name": "sources",
        "current": "er_sources",
        "version": "er_source_versions",
        "latest": "er_latest_source_versions",
        "id": "source_id",
        "fields": [
            "source_name", "source_type", "base_url", "redistribution_status",
            "attribution_required", "active",
        ],
    },
    {
        "name": "entities",
        "current": "er_entities",
        "version": "er_entity_versions",
        "latest": "er_latest_entity_versions",
        "id": "entity_id",
        "fields": ["entity_type", "canonical_name", "legal_name", "country_code", "active"],
    },
    {
        "name": "instruments",
        "current": "er_instruments",
        "version": "er_instrument_versions",
        "latest": "er_latest_instrument_versions",
        "id": "instrument_id",
        "fields": [
            "issuer_entity_id", "instrument_type", "canonical_name", "currency",
            "exchange_mic", "active",
        ],
    },
]


def _sync_spec(con, spec: dict) -> int:
    current = spec["current"]
    version = spec["version"]
    latest = spec["latest"]
    id_col = spec["id"]
    fields = spec["fields"]
    temp = f"_reference_changes_{spec['name']}"
    comparisons = " OR ".join(f"c.{field} IS DISTINCT FROM v.{field}" for field in fields)
    selected = ",".join(f"c.{field}" for field in fields)
    latest_selected = ",".join(f"v.{field} AS old_{field}" for field in fields)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE {temp} AS
        SELECT c.{id_col},{selected},c.created_at,
               v.version_no AS previous_version_no,{latest_selected}
        FROM {current} c
        LEFT JOIN {latest} v USING({id_col})
        WHERE v.{id_col} IS NULL OR {comparisons}
    """)
    changed = int(con.execute(f"SELECT count(*) FROM {temp}").fetchone()[0])
    if not changed:
        return 0

    con.execute(f"""
        UPDATE {version} hist SET valid_to=now()
        FROM {temp} d
        WHERE hist.{id_col}=d.{id_col}
          AND hist.valid_to IS NULL
          AND d.previous_version_no IS NOT NULL
    """)
    insert_cols = ",".join([id_col, "version_no", *fields, "change_type", "valid_from", "recorded_at"])
    insert_values = ",".join([f"d.{id_col}", "coalesce(d.previous_version_no,0)+1", *[f"d.{f}" for f in fields]])
    con.execute(f"""
        INSERT INTO {version}({insert_cols})
        SELECT {insert_values},
               CASE WHEN d.previous_version_no IS NULL THEN 'initial_record' ELSE 'metadata_change' END,
               CASE WHEN d.previous_version_no IS NULL THEN coalesce(d.created_at,now()) ELSE now() END,
               now()
        FROM {temp} d
    """)
    return changed


def sync_reference_versions() -> dict:
    con = connect()
    try:
        con.execute("BEGIN TRANSACTION")
        try:
            results = {spec["name"]: _sync_spec(con, spec) for spec in SPECS}
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        return {
            "source_versions_created": results["sources"],
            "entity_versions_created": results["entities"],
            "instrument_versions_created": results["instruments"],
            "method_version": METHOD_VERSION,
        }
    finally:
        con.close()


if __name__ == "__main__":
    print(sync_reference_versions())
