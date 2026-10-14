"""Version mutable institutional reference masters without rewriting history."""

from __future__ import annotations

from datetime import datetime, timezone

from db import connect


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _sync_table(con, *, current_table: str, version_table: str, id_col: str, fields: list[str]) -> int:
    rows = con.execute(
        f"SELECT {id_col},{','.join(fields)},created_at FROM {current_table} ORDER BY {id_col}"
    ).fetchall()
    created = 0
    for row in rows:
        object_id = row[0]
        values = tuple(row[1:1 + len(fields)])
        created_at = row[-1]
        latest = con.execute(
            f"SELECT version_no,{','.join(fields)} FROM {version_table} WHERE {id_col}=? ORDER BY version_no DESC LIMIT 1",
            [object_id],
        ).fetchone()
        if latest is None:
            changed_at = now_utc()
            cols = ",".join([id_col, "version_no", *fields, "change_type", "valid_from", "recorded_at"])
            marks = ",".join("?" for _ in range(2 + len(fields) + 3))
            con.execute(
                f"INSERT INTO {version_table}({cols}) VALUES ({marks})",
                [object_id, 1, *values, "initial_record", created_at or changed_at, changed_at],
            )
            created += 1
            continue
        if tuple(latest[1:]) == values:
            continue
        changed_at = now_utc()
        con.execute(
            f"UPDATE {version_table} SET valid_to=? WHERE {id_col}=? AND valid_to IS NULL",
            [changed_at, object_id],
        )
        next_version = int(latest[0]) + 1
        cols = ",".join([id_col, "version_no", *fields, "change_type", "valid_from", "recorded_at"])
        marks = ",".join("?" for _ in range(2 + len(fields) + 3))
        con.execute(
            f"INSERT INTO {version_table}({cols}) VALUES ({marks})",
            [object_id, next_version, *values, "metadata_change", changed_at, changed_at],
        )
        created += 1
    return created


def sync_reference_versions() -> dict:
    con = connect()
    try:
        sources = _sync_table(
            con,
            current_table="er_sources",
            version_table="er_source_versions",
            id_col="source_id",
            fields=[
                "source_name", "source_type", "base_url", "redistribution_status",
                "attribution_required", "active",
            ],
        )
        entities = _sync_table(
            con,
            current_table="er_entities",
            version_table="er_entity_versions",
            id_col="entity_id",
            fields=["entity_type", "canonical_name", "legal_name", "country_code", "active"],
        )
        instruments = _sync_table(
            con,
            current_table="er_instruments",
            version_table="er_instrument_versions",
            id_col="instrument_id",
            fields=[
                "issuer_entity_id", "instrument_type", "canonical_name", "currency",
                "exchange_mic", "active",
            ],
        )
        return {
            "source_versions_created": sources,
            "entity_versions_created": entities,
            "instrument_versions_created": instruments,
        }
    finally:
        con.close()


if __name__ == "__main__":
    print(sync_reference_versions())
