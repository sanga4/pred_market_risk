"""Conservative bridge from EventRisk's existing security universe into institutional instruments.

This module intentionally stops at instrument identity. It does not infer legal issuers from a
security name, does not interpret exchange labels as ISO MICs, and does not create event-to-security
impact edges. The existing EventRisk security master is treated as a source-described reference
universe whose exact fields and source timestamps are preserved with point-in-time versions.
"""

from __future__ import annotations

import json
import os
import re
import time
import uuid

from db import connect
from institutional_reference_versions_v2 import sync_reference_versions

METHOD_VERSION = "eventrisk_security_master_bridge_v1"
SOURCE_SYSTEM = "event_risk.main.securities"
SOURCE_TABLE = os.getenv("EVENTRISK_SECURITY_MASTER_TABLE", SOURCE_SYSTEM).strip()

_TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*){0,2}$")


def _validated_source_table() -> str:
    if not _TABLE_RE.fullmatch(SOURCE_TABLE):
        raise RuntimeError(f"Unsafe EVENTRISK_SECURITY_MASTER_TABLE={SOURCE_TABLE!r}")
    return SOURCE_TABLE


def _start_run() -> tuple[str, float]:
    run_id = f"refsync_{uuid.uuid4().hex}"
    started = time.monotonic()
    con = connect()
    try:
        con.execute(
            """
            INSERT INTO er_reference_sync_runs(
              sync_run_id,source_system,method_version,started_at,status
            ) VALUES (?,?,?,now(),'running')
            """,
            [run_id, SOURCE_SYSTEM, METHOD_VERSION],
        )
    finally:
        con.close()
    return run_id, started


def _finish_run(run_id: str, *, status: str, stats: dict, error: str | None = None) -> None:
    con = connect()
    try:
        details = dict(stats)
        if error:
            details["error"] = error[:4000]
        con.execute(
            """
            UPDATE er_reference_sync_runs
            SET finished_at=now(),status=?,records_received=?,instruments_upserted=?,
                instruments_deactivated=?,attribute_versions_created=?,details=?
            WHERE sync_run_id=?
            """,
            [
                status,
                int(stats.get("records_received", 0)),
                int(stats.get("instruments_upserted", 0)),
                int(stats.get("instruments_deactivated", 0)),
                int(stats.get("attribute_versions_created", 0)),
                json.dumps(details, sort_keys=True, default=str),
                run_id,
            ],
        )
    finally:
        con.close()


def sync_security_master() -> dict:
    """Synchronize source-described instruments and reference attributes set-wise.

    Instrument IDs are deterministic only within this bridge and are keyed from the source's own
    ticker primary key. That does *not* assert continuity across ticker changes. Issuer entity IDs,
    currencies and MICs are deliberately left untouched/null unless another authoritative reference
    process supplies them.
    """

    source_table = _validated_source_table()
    run_id, started = _start_run()
    stats: dict[str, object] = {
        "sync_run_id": run_id,
        "source_system": SOURCE_SYSTEM,
        "source_table": source_table,
        "method_version": METHOD_VERSION,
        "records_received": 0,
        "instruments_upserted": 0,
        "instruments_deactivated": 0,
        "attribute_versions_created": 0,
    }

    con = connect()
    try:
        con.execute("SET TimeZone='UTC'")
        con.execute("BEGIN TRANSACTION")
        try:
            con.execute(
                f"""
                CREATE OR REPLACE TEMP TABLE _security_master_source AS
                SELECT trim(ticker) AS ticker,
                       nullif(trim(name),'') AS name,
                       nullif(trim(exchange),'') AS exchange_label,
                       nullif(trim(quote_type),'') AS quote_type,
                       coalesce(nullif(trim(asset_class),''),'security') AS asset_class,
                       nullif(trim(sector),'') AS sector,
                       nullif(trim(industry),'') AS industry,
                       nullif(trim(country),'') AS country,
                       nullif(trim(source),'') AS source_record_source,
                       updated_at AS source_updated_at
                FROM {source_table}
                WHERE ticker IS NOT NULL AND trim(ticker)<>''
                """
            )
            records_received = int(con.execute("SELECT count(*) FROM _security_master_source").fetchone()[0])
            if records_received == 0:
                raise RuntimeError("Security master returned zero rows; refusing to deactivate institutional instruments")
            stats["records_received"] = records_received

            con.execute(
                """
                CREATE OR REPLACE TEMP TABLE _security_master_normalized AS
                SELECT 'instr_eventrisk_' || substr(sha256(lower(ticker)),1,24) AS instrument_id,
                       ticker,name,exchange_label,quote_type,asset_class,sector,industry,country,
                       source_record_source,source_updated_at
                FROM _security_master_source
                """
            )

            # Determine attribute changes before mutating current state. Source update time is
            # metadata only; version valid_from is the time EventRisk actually records this state.
            con.execute(
                """
                CREATE OR REPLACE TEMP TABLE _security_master_attr_changes AS
                SELECT s.*,
                       v.version_no AS previous_version_no
                FROM _security_master_normalized s
                LEFT JOIN er_latest_instrument_reference_attribute_versions v
                  ON v.instrument_id=s.instrument_id
                WHERE v.instrument_id IS NULL
                   OR v.source_system IS DISTINCT FROM ?
                   OR v.source_ticker IS DISTINCT FROM s.ticker
                   OR v.source_name IS DISTINCT FROM s.name
                   OR v.exchange_label IS DISTINCT FROM s.exchange_label
                   OR v.asset_class IS DISTINCT FROM s.asset_class
                   OR v.sector IS DISTINCT FROM s.sector
                   OR v.industry IS DISTINCT FROM s.industry
                   OR v.country IS DISTINCT FROM s.country
                   OR v.source_record_source IS DISTINCT FROM s.source_record_source
                   OR v.source_updated_at IS DISTINCT FROM s.source_updated_at
                """,
                [SOURCE_SYSTEM],
            )
            attr_changes = int(con.execute("SELECT count(*) FROM _security_master_attr_changes").fetchone()[0])
            stats["attribute_versions_created"] = attr_changes

            con.execute(
                """
                INSERT INTO er_instruments(
                  instrument_id,issuer_entity_id,instrument_type,canonical_name,currency,exchange_mic,
                  active,created_at,updated_at
                )
                SELECT instrument_id,NULL,asset_class,coalesce(name,ticker),NULL,NULL,TRUE,now(),now()
                FROM _security_master_normalized
                ON CONFLICT(instrument_id) DO UPDATE SET
                  instrument_type=excluded.instrument_type,
                  canonical_name=excluded.canonical_name,
                  active=TRUE,
                  updated_at=now()
                """
            )
            stats["instruments_upserted"] = records_received

            # The source ticker is an interoperability identifier, not a claim that it is globally
            # unique outside EventRisk's own source universe.
            con.execute(
                """
                INSERT INTO er_instrument_identifiers(
                  instrument_id,identifier_type,identifier_value,source,valid_from,valid_to
                )
                SELECT instrument_id,'ticker_eventrisk',ticker,?,now(),NULL
                FROM _security_master_normalized
                ON CONFLICT(instrument_id,identifier_type,identifier_value) DO UPDATE SET
                  source=excluded.source
                """,
                [SOURCE_SYSTEM],
            )

            con.execute(
                """
                INSERT INTO er_instrument_reference_attributes(
                  instrument_id,source_system,source_ticker,source_name,exchange_label,asset_class,
                  sector,industry,country,source_record_source,source_updated_at,recorded_at,updated_at
                )
                SELECT instrument_id,?,ticker,name,exchange_label,asset_class,sector,industry,country,
                       source_record_source,source_updated_at,now(),now()
                FROM _security_master_normalized
                ON CONFLICT(instrument_id) DO UPDATE SET
                  source_system=excluded.source_system,
                  source_ticker=excluded.source_ticker,
                  source_name=excluded.source_name,
                  exchange_label=excluded.exchange_label,
                  asset_class=excluded.asset_class,
                  sector=excluded.sector,
                  industry=excluded.industry,
                  country=excluded.country,
                  source_record_source=excluded.source_record_source,
                  source_updated_at=excluded.source_updated_at,
                  updated_at=now()
                """,
                [SOURCE_SYSTEM],
            )

            if attr_changes:
                con.execute(
                    """
                    UPDATE er_instrument_reference_attribute_versions hist
                    SET valid_to=now()
                    FROM _security_master_attr_changes d
                    WHERE hist.instrument_id=d.instrument_id
                      AND hist.valid_to IS NULL
                      AND d.previous_version_no IS NOT NULL
                    """
                )
                con.execute(
                    """
                    INSERT INTO er_instrument_reference_attribute_versions(
                      instrument_id,version_no,source_system,source_ticker,source_name,exchange_label,
                      asset_class,sector,industry,country,source_record_source,source_updated_at,
                      change_type,valid_from,recorded_at
                    )
                    SELECT instrument_id,coalesce(previous_version_no,0)+1,?,ticker,name,exchange_label,
                           asset_class,sector,industry,country,source_record_source,source_updated_at,
                           CASE WHEN previous_version_no IS NULL THEN 'initial_record' ELSE 'metadata_change' END,
                           now(),now()
                    FROM _security_master_attr_changes
                    """,
                    [SOURCE_SYSTEM],
                )

            # Only deactivate instruments that this bridge itself previously sourced and that are
            # absent from a non-empty complete source snapshot. Do not touch instruments owned by
            # other reference providers.
            deactivated = int(
                con.execute(
                    """
                    SELECT count(*)
                    FROM er_instruments i
                    JOIN er_instrument_reference_attributes a USING(instrument_id)
                    WHERE a.source_system=? AND i.active=TRUE
                      AND NOT EXISTS (
                        SELECT 1 FROM _security_master_normalized s
                        WHERE s.instrument_id=i.instrument_id
                      )
                    """,
                    [SOURCE_SYSTEM],
                ).fetchone()[0]
            )
            stats["instruments_deactivated"] = deactivated
            if deactivated:
                con.execute(
                    """
                    UPDATE er_instruments i SET active=FALSE,updated_at=now()
                    FROM er_instrument_reference_attributes a
                    WHERE i.instrument_id=a.instrument_id AND a.source_system=? AND i.active=TRUE
                      AND NOT EXISTS (
                        SELECT 1 FROM _security_master_normalized s
                        WHERE s.instrument_id=i.instrument_id
                      )
                    """,
                    [SOURCE_SYSTEM],
                )

            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
    except Exception as exc:
        con.close()
        stats["elapsed_seconds"] = round(time.monotonic() - started, 6)
        _finish_run(run_id, status="failed", stats=stats, error=str(exc))
        raise
    else:
        con.close()

    # Existing set-based reference versioning records the mutable institutional instrument master
    # (including later activation/deactivation) after the source-specific reference transaction.
    ref_versions = sync_reference_versions()
    stats["instrument_versions_created"] = int(ref_versions.get("instrument_versions_created", 0))
    stats["elapsed_seconds"] = round(time.monotonic() - started, 6)
    _finish_run(run_id, status="success", stats=stats)
    return stats


if __name__ == "__main__":
    print(json.dumps(sync_security_master(), indent=2, default=str))
