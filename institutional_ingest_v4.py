"""Set-based institutional ingestion for remote MotherDuck warehouses.

v3 remains the reference row-oriented implementation used by local tests. v4 preserves the
same source/raw/contract/semantic/observation semantics, but stages one complete source pull
as Arrow relations and performs bounded set-based SQL. This avoids thousands of network
round trips when DB is `md:`.
"""

from __future__ import annotations

import hashlib
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

import pyarrow as pa

import institutional_ingest as base
import institutional_ingest_v3 as v3
from db import connect
from institutional_universe_adapters import ADAPTERS, collection_policy

log = logging.getLogger("eventrisk.institutional_ingest_v4")
METHOD_VERSION = "set_based_ingest_v4"


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _raw_id() -> str:
    return f"raw_{uuid.uuid4().hex}"


def _canonical(payload: object) -> tuple[str, str]:
    text = base.canonical_json(payload)
    return text, hashlib.sha256(text.encode("utf-8")).hexdigest()


def _table(rows: list[dict], schema: pa.Schema) -> pa.Table:
    if not rows:
        return pa.Table.from_pylist([], schema=schema)
    return pa.Table.from_pylist(rows, schema=schema)


RAW_SCHEMA = pa.schema([
    ("raw_snapshot_id", pa.string()), ("source_id", pa.string()),
    ("source_object_type", pa.string()), ("source_object_id", pa.string()),
    ("source_observed_at", pa.timestamp("us", tz="UTC")),
    ("received_at", pa.timestamp("us", tz="UTC")), ("content_hash", pa.string()),
    ("payload_json", pa.string()), ("ingestion_run_id", pa.string()),
])

CONTRACT_SCHEMA = pa.schema([
    ("source_contract_id", pa.string()), ("source_id", pa.string()),
    ("source_market_id", pa.string()), ("source_event_id", pa.string()),
    ("title", pa.string()), ("description", pa.string()), ("outcome_type", pa.string()),
    ("status", pa.string()), ("opens_at", pa.timestamp("us", tz="UTC")),
    ("closes_at", pa.timestamp("us", tz="UTC")), ("resolves_at", pa.timestamp("us", tz="UTC")),
    ("resolution_authority", pa.string()), ("resolution_rules", pa.string()),
    ("source_url", pa.string()), ("observed_at", pa.timestamp("us", tz="UTC")),
    ("received_at", pa.timestamp("us", tz="UTC")), ("event_raw_id", pa.string()),
    ("market_raw_id", pa.string()), ("active", pa.bool_()),
])

OBS_SCHEMA = pa.schema([
    ("source_id", pa.string()), ("source_contract_id", pa.string()),
    ("observed_at", pa.timestamp("us", tz="UTC")), ("received_at", pa.timestamp("us", tz="UTC")),
    ("processed_at", pa.timestamp("us", tz="UTC")), ("yes_bid", pa.float64()),
    ("yes_ask", pa.float64()), ("no_bid", pa.float64()), ("no_ask", pa.float64()),
    ("midpoint", pa.float64()), ("last_trade", pa.float64()),
    ("displayed_probability", pa.float64()), ("volume", pa.float64()),
    ("volume_24h", pa.float64()), ("open_interest", pa.float64()),
    ("liquidity", pa.float64()), ("orderbook_depth", pa.float64()), ("spread", pa.float64()),
    ("market_status", pa.string()), ("raw_snapshot_id", pa.string()),
    ("methodology_version", pa.string()),
])


def _stage_snapshots(source_id: str, run_id: str, snapshots: list) -> tuple[pa.Table, pa.Table, pa.Table]:
    raw_rows: list[dict] = []
    contracts: list[dict] = []
    observations: list[dict] = []
    event_raw: dict[str, str] = {}
    received = now_utc()

    for snap in snapshots:
        event_object_id = snap.source_event_id or snap.source_market_id
        if event_object_id not in event_raw:
            event_id = _raw_id()
            event_text, event_hash = _canonical(snap.raw_event)
            raw_rows.append({
                "raw_snapshot_id": event_id, "source_id": source_id,
                "source_object_type": "event", "source_object_id": event_object_id,
                "source_observed_at": snap.observed_at, "received_at": received,
                "content_hash": event_hash, "payload_json": event_text, "ingestion_run_id": run_id,
            })
            event_raw[event_object_id] = event_id

        market_raw_id = _raw_id()
        market_text, market_hash = _canonical(snap.raw_market)
        raw_rows.append({
            "raw_snapshot_id": market_raw_id, "source_id": source_id,
            "source_object_type": "market", "source_object_id": snap.source_market_id,
            "source_observed_at": snap.observed_at, "received_at": received,
            "content_hash": market_hash, "payload_json": market_text, "ingestion_run_id": run_id,
        })
        contract_id = base.stable_id("sc", source_id, snap.source_market_id)
        active = (snap.status or "").lower() in {"active", "open"}
        contracts.append({
            "source_contract_id": contract_id, "source_id": source_id,
            "source_market_id": snap.source_market_id, "source_event_id": snap.source_event_id,
            "title": snap.title, "description": snap.description, "outcome_type": "binary",
            "status": snap.status, "opens_at": snap.opens_at, "closes_at": snap.closes_at,
            "resolves_at": snap.resolves_at, "resolution_authority": snap.resolution_authority,
            "resolution_rules": snap.resolution_rules, "source_url": snap.source_url,
            "observed_at": snap.observed_at, "received_at": received,
            "event_raw_id": event_raw[event_object_id], "market_raw_id": market_raw_id,
            "active": active,
        })
        midpoint = None
        spread = None
        if snap.yes_bid is not None and snap.yes_ask is not None:
            spread = snap.yes_ask - snap.yes_bid
            if snap.yes_bid <= snap.yes_ask:
                midpoint = (snap.yes_bid + snap.yes_ask) / 2
        observations.append({
            "source_id": source_id, "source_contract_id": contract_id,
            "observed_at": snap.observed_at, "received_at": received, "processed_at": received,
            "yes_bid": snap.yes_bid, "yes_ask": snap.yes_ask, "no_bid": snap.no_bid,
            "no_ask": snap.no_ask, "midpoint": midpoint, "last_trade": snap.last_trade,
            "displayed_probability": snap.displayed_probability, "volume": snap.volume,
            "volume_24h": snap.volume_24h, "open_interest": snap.open_interest,
            "liquidity": snap.liquidity, "orderbook_depth": snap.orderbook_depth,
            "spread": spread, "market_status": snap.status, "raw_snapshot_id": market_raw_id,
            "methodology_version": "source_native_v1",
        })
    return _table(raw_rows, RAW_SCHEMA), _table(contracts, CONTRACT_SCHEMA), _table(observations, OBS_SCHEMA)


def _register_batch(con, raw: pa.Table, contracts: pa.Table, observations: pa.Table) -> None:
    con.register("_arrow_raw", raw)
    con.register("_arrow_contracts", contracts)
    con.register("_arrow_observations", observations)
    con.execute("CREATE OR REPLACE TEMP TABLE _batch_raw AS SELECT * FROM _arrow_raw")
    con.execute("CREATE OR REPLACE TEMP TABLE _batch_contracts AS SELECT * FROM _arrow_contracts")
    con.execute("CREATE OR REPLACE TEMP TABLE _batch_observations AS SELECT * FROM _arrow_observations")
    con.unregister("_arrow_raw")
    con.unregister("_arrow_contracts")
    con.unregister("_arrow_observations")


def _persist_batch(con, source_id: str, run_id: str, raw: pa.Table, contracts: pa.Table, observations: pa.Table) -> dict:
    _register_batch(con, raw, contracts, observations)
    con.execute("SET TimeZone='UTC'")

    # Capture change sets before mutating current serving tables.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _contract_changes AS
        SELECT b.*,
               c.source_contract_id IS NULL AS is_new,
               coalesce((SELECT max(v.version_no) FROM er_source_contract_versions v
                         WHERE v.source_contract_id=b.source_contract_id),0)+1 AS next_version
        FROM _batch_contracts b
        LEFT JOIN er_source_contracts c USING(source_contract_id)
        WHERE c.source_contract_id IS NULL
           OR c.current_title IS DISTINCT FROM b.title
           OR c.current_description IS DISTINCT FROM b.description
           OR c.status IS DISTINCT FROM b.status
           OR c.opens_at IS DISTINCT FROM b.opens_at
           OR c.closes_at IS DISTINCT FROM b.closes_at
           OR c.resolves_at IS DISTINCT FROM b.resolves_at
           OR c.resolution_authority IS DISTINCT FROM b.resolution_authority
           OR c.current_resolution_rules IS DISTINCT FROM b.resolution_rules
           OR c.source_url IS DISTINCT FROM b.source_url
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _semantic_changes AS
        SELECT b.*,
               s.source_contract_id IS NULL AS is_new_semantic,
               coalesce((SELECT max(v.version_no) FROM er_contract_semantic_versions v
                         WHERE v.source_contract_id=b.source_contract_id),0)+1 AS next_semantic_version
        FROM _batch_contracts b
        LEFT JOIN er_latest_contract_semantic_versions s USING(source_contract_id)
        WHERE s.source_contract_id IS NULL
           OR s.source_id IS DISTINCT FROM b.source_id
           OR s.source_market_id IS DISTINCT FROM b.source_market_id
           OR s.source_event_id IS DISTINCT FROM b.source_event_id
           OR s.title IS DISTINCT FROM b.title
           OR s.description IS DISTINCT FROM b.description
           OR s.outcome_type IS DISTINCT FROM b.outcome_type
           OR s.status IS DISTINCT FROM b.status
           OR s.opens_at IS DISTINCT FROM b.opens_at
           OR s.closes_at IS DISTINCT FROM b.closes_at
           OR s.resolves_at IS DISTINCT FROM b.resolves_at
           OR s.resolution_authority IS DISTINCT FROM b.resolution_authority
           OR s.resolution_rules IS DISTINCT FROM b.resolution_rules
           OR s.source_url IS DISTINCT FROM b.source_url
    """)

    con.execute("""
        INSERT INTO er_raw_snapshots(
          raw_snapshot_id,source_id,source_object_type,source_object_id,source_observed_at,
          received_at,content_hash,payload_json,ingestion_run_id)
        SELECT raw_snapshot_id,source_id,source_object_type,source_object_id,source_observed_at,
               received_at,content_hash,payload_json,ingestion_run_id FROM _batch_raw
    """)

    con.execute("""
        UPDATE er_source_contract_versions v SET valid_to=b.received_at
        FROM _contract_changes b
        WHERE v.source_contract_id=b.source_contract_id AND v.valid_to IS NULL AND b.is_new=FALSE
    """)
    con.execute("""
        INSERT INTO er_source_contract_versions(
          source_contract_id,version_no,title,description,status,resolution_rules,opens_at,closes_at,
          resolves_at,source_observed_at,received_at,valid_from,raw_snapshot_id)
        SELECT source_contract_id,next_version,title,description,status,resolution_rules,opens_at,closes_at,
               resolves_at,observed_at,received_at,received_at,market_raw_id
        FROM _contract_changes
    """)
    con.execute("""
        INSERT INTO er_contract_version_evidence(source_contract_id,version_no,evidence_role,raw_snapshot_id)
        SELECT source_contract_id,next_version,'event_payload',event_raw_id FROM _contract_changes
        UNION ALL
        SELECT source_contract_id,next_version,'market_payload',market_raw_id FROM _contract_changes
    """)

    con.execute("""
        UPDATE er_contract_semantic_versions v SET valid_to=b.received_at
        FROM _semantic_changes b
        WHERE v.source_contract_id=b.source_contract_id AND v.valid_to IS NULL AND b.is_new_semantic=FALSE
    """)
    con.execute("""
        INSERT INTO er_contract_semantic_versions(
          source_contract_id,version_no,source_id,source_market_id,source_event_id,title,description,
          outcome_type,status,opens_at,closes_at,resolves_at,resolution_authority,resolution_rules,
          source_url,source_observed_at,received_at,valid_from,event_raw_snapshot_id,
          market_raw_snapshot_id,metadata_completeness)
        SELECT source_contract_id,next_semantic_version,source_id,source_market_id,source_event_id,title,description,
               outcome_type,status,opens_at,closes_at,resolves_at,resolution_authority,resolution_rules,
               source_url,observed_at,received_at,received_at,event_raw_id,market_raw_id,'full_v2'
        FROM _semantic_changes
    """)

    con.execute("""
        INSERT INTO er_source_contracts(
          source_contract_id,source_id,source_market_id,source_event_id,current_title,current_description,
          outcome_type,status,opens_at,closes_at,resolves_at,resolution_authority,current_resolution_rules,
          source_url,first_seen_at,last_seen_at,active)
        SELECT source_contract_id,source_id,source_market_id,source_event_id,title,description,outcome_type,
               status,opens_at,closes_at,resolves_at,resolution_authority,resolution_rules,source_url,
               received_at,received_at,active
        FROM _batch_contracts
        ON CONFLICT(source_contract_id) DO UPDATE SET
          source_event_id=excluded.source_event_id,current_title=excluded.current_title,
          current_description=excluded.current_description,outcome_type=excluded.outcome_type,
          status=excluded.status,opens_at=excluded.opens_at,closes_at=excluded.closes_at,
          resolves_at=excluded.resolves_at,resolution_authority=excluded.resolution_authority,
          current_resolution_rules=excluded.current_resolution_rules,source_url=excluded.source_url,
          last_seen_at=excluded.last_seen_at,active=excluded.active
    """)

    con.execute("""
        INSERT INTO er_market_observations(
          source_id,source_contract_id,observed_at,received_at,processed_at,yes_bid,yes_ask,no_bid,no_ask,
          midpoint,last_trade,displayed_probability,volume,volume_24h,open_interest,liquidity,orderbook_depth,
          spread,market_status,raw_snapshot_id,methodology_version)
        SELECT * FROM _batch_observations
    """)

    # Revision records are descriptive audit records; deterministic md5 IDs make retries idempotent.
    con.execute("""
        INSERT INTO er_revisions(
          revision_id,object_type,object_id,revision_type,reason,previous_version,new_version,
          effective_at,recorded_at,actor)
        SELECT 'rev_' || md5(source_contract_id || '|contract|' || cast(next_version AS varchar) || '|' || cast(epoch_us(received_at) AS varchar)),
               'source_contract',source_contract_id,
               CASE WHEN is_new THEN 'initial_capture' ELSE 'metadata_change' END,
               'Set-based source contract capture',
               CASE WHEN is_new THEN NULL ELSE cast(next_version-1 AS varchar) END,
               cast(next_version AS varchar),received_at,received_at,?
        FROM _contract_changes
        ON CONFLICT(revision_id) DO NOTHING
    """, [METHOD_VERSION])
    con.execute("""
        INSERT INTO er_revisions(
          revision_id,object_type,object_id,revision_type,reason,previous_version,new_version,
          effective_at,recorded_at,actor)
        SELECT 'rev_' || md5(source_contract_id || '|semantic|' || cast(next_semantic_version AS varchar) || '|' || cast(epoch_us(received_at) AS varchar)),
               'source_contract_semantics',source_contract_id,
               CASE WHEN is_new_semantic THEN 'initial_full_semantic_capture' ELSE 'semantic_metadata_change' END,
               'Prospective full source-contract semantic ledger',
               CASE WHEN is_new_semantic THEN NULL ELSE cast(next_semantic_version-1 AS varchar) END,
               cast(next_semantic_version AS varchar),received_at,received_at,?
        FROM _semantic_changes
        ON CONFLICT(revision_id) DO NOTHING
    """, [METHOD_VERSION])

    changed = con.execute("SELECT count(*) FROM _contract_changes").fetchone()[0]
    semantic_changed = con.execute("SELECT count(*) FROM _semantic_changes").fetchone()[0]
    seen_ids = {r[0] for r in con.execute("SELECT source_contract_id FROM _batch_contracts").fetchall()}
    return {
        "contract_versions_created": int(changed),
        "semantic_versions_created": int(semantic_changed),
        "seen_contract_ids": seen_ids,
    }


def ingest_source(source_id: str) -> dict:
    if source_id not in ADAPTERS:
        raise ValueError(f"Unknown source {source_id}")
    run_id = f"ing_{uuid.uuid4().hex}"
    started = now_utc()
    policy = collection_policy()
    con = connect()
    try:
        v3.ensure_sources(con)
        con.execute("""
            INSERT INTO er_ingestion_runs(
              run_id,source_id,started_at,status,collection_scope,universe_complete,collection_policy)
            VALUES (?,?,?,'running',?,FALSE,?)
        """, [run_id, source_id, started, policy["scope"], json.dumps(policy, sort_keys=True)])
    finally:
        con.close()

    try:
        snapshots = list(ADAPTERS[source_id]().iter_snapshots())
        raw, contracts, observations = _stage_snapshots(source_id, run_id, snapshots)
        con = connect()
        try:
            con.execute("BEGIN TRANSACTION")
            try:
                persisted = _persist_batch(con, source_id, run_id, raw, contracts, observations)
                universe_complete = policy["scope"] == "full"
                presence = v3.reconcile_presence(
                    con, source_id, run_id, persisted["seen_contract_ids"],
                    universe_complete=universe_complete,
                )
                details = {
                    "writer": METHOD_VERSION,
                    "collection_policy": policy,
                    "raw_rows": raw.num_rows,
                    "contract_rows": contracts.num_rows,
                    "contract_versions_created": persisted["contract_versions_created"],
                    "semantic_versions_created": persisted["semantic_versions_created"],
                    "presence": presence,
                }
                con.execute("""
                    UPDATE er_ingestion_runs SET finished_at=?,status='success',records_received=?,
                      records_written=?,error_count=0,details=?,universe_complete=?,collection_scope=?,collection_policy=?
                    WHERE run_id=?
                """, [
                    now_utc(), contracts.num_rows, contracts.num_rows,
                    json.dumps(details, sort_keys=True, default=str), universe_complete,
                    policy["scope"], json.dumps(policy, sort_keys=True), run_id,
                ])
                v3.update_health(con, source_id, run_id, "success", contracts.num_rows, None)
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise
        finally:
            con.close()
        return {
            "source_id": source_id, "run_id": run_id, "status": "success",
            "records_written": contracts.num_rows, "raw_rows_written": raw.num_rows,
            "contract_versions_created": persisted["contract_versions_created"],
            "semantic_versions_created": persisted["semantic_versions_created"],
            "collection_policy": policy, "presence": presence, "writer": METHOD_VERSION,
        }
    except Exception as exc:
        log.exception("Set-based institutional ingestion failed for %s", source_id)
        con = connect()
        try:
            con.execute("""
                UPDATE er_ingestion_runs SET finished_at=?,status='failed',error_count=1,details=?,
                  universe_complete=FALSE WHERE run_id=?
            """, [now_utc(), json.dumps({"error": str(exc)[:1800], "writer": METHOD_VERSION}), run_id])
            v3.update_health(con, source_id, run_id, "failed", 0, str(exc)[:2000])
        finally:
            con.close()
        raise


def run_once(source_ids: Optional[list[str]] = None) -> list[dict]:
    results = []
    for source_id in source_ids or list(ADAPTERS):
        try:
            results.append(ingest_source(source_id))
        except Exception as exc:
            results.append({"source_id": source_id, "status": "failed", "error": str(exc), "writer": METHOD_VERSION})
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(json.dumps(run_once(), indent=2, default=str))
