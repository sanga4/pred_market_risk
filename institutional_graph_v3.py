"""Incremental, provenance-preserving graph execution v3.

Graph semantics and edge IDs remain exactly `graph_rules_v2_versioned_mentions`. The change is
operational: source semantic/mapping intervals are hashed and checkpointed, explicit entity
mentions are detected in memory, and edges/evidence/origins/state are persisted set-wise.
The first run builds interval state; subsequent runs process only changed intervals unless the
active identity-alias fingerprint changes.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
import uuid
from datetime import datetime, timedelta, timezone

import pyarrow as pa

import institutional_graph as base
from db import connect

GRAPH_VERSION = "graph_rules_v2_versioned_mentions"
EXECUTION_VERSION = "graph_execution_v3_incremental"
RECOVERY_VERSION = "graph_run_recovery_v1"
RECOVERY_MINUTES = max(10, int(os.getenv("GRAPH_RUN_RECOVERY_MINUTES", "30")))


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _rows_dict(cur) -> list[dict]:
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def recover_abandoned_graph_runs(con) -> dict:
    cutoff = now_utc() - timedelta(minutes=RECOVERY_MINUTES)
    stale = _rows_dict(con.execute("""
        SELECT run_id,execution_version,started_at
        FROM er_graph_runs
        WHERE status='running' AND started_at<?
        ORDER BY started_at
    """, [cutoff]))
    for row in stale:
        con.execute("""
            UPDATE er_graph_runs SET status='abandoned',finished_at=now(),details=?
            WHERE run_id=? AND status='running'
        """, [json.dumps({
            "recovery": {
                "method_version": RECOVERY_VERSION,
                "reason": "graph run remained running beyond threshold after prior worker/process termination",
                "threshold_minutes": RECOVERY_MINUTES,
                "recovered_at": now_utc().isoformat(),
            }
        }, sort_keys=True), row["run_id"]])
    running = _rows_dict(con.execute("""
        SELECT run_id,execution_version,started_at FROM er_graph_runs
        WHERE status='running' ORDER BY started_at
    """))
    return {"recovered": len(stale), "stale": stale, "running": running, "threshold_minutes": RECOVERY_MINUTES}


def _seed_identity_aliases(con) -> int:
    expected = sum(len(item[4]) for item in base.SEED_ENTITIES)
    existing = int(con.execute("""
        SELECT count(*) FROM er_entity_identifiers
        WHERE identifier_type='alias' AND source='eventrisk_seed'
    """).fetchone()[0])
    if existing < expected:
        base.seed_entities(con)
    return expected


def _bulk_security_master(con) -> dict:
    try:
        rows = con.execute(
            "SELECT ticker,name,exchange,quote_type,asset_class,sector,industry,country FROM securities"
        ).fetchall()
    except Exception:
        return {"entities": 0, "instruments": 0, "security_master_available": False}
    entity_rows = []
    entity_id_rows = []
    instrument_rows = []
    instrument_id_rows = []
    seen_entities = set()
    seen_instruments = set()
    for ticker, name, exchange, quote_type, asset_class, sector, industry, country in rows:
        if not ticker:
            continue
        legal_name = name or ticker
        entity_id = base.stable_id("ent", (legal_name or ticker).lower(), country or "")
        instrument_id = base.stable_id("ins", ticker, exchange or "")
        if entity_id not in seen_entities:
            entity_rows.append({
                "entity_id": entity_id, "entity_type": "legal_entity",
                "canonical_name": legal_name, "legal_name": legal_name, "country_code": country,
            })
            seen_entities.add(entity_id)
        entity_id_rows.append({
            "entity_id": entity_id, "identifier_type": "legacy_ticker_owner",
            "identifier_value": ticker, "source": "legacy_security_master", "is_primary": True,
        })
        if instrument_id not in seen_instruments:
            instrument_rows.append({
                "instrument_id": instrument_id, "issuer_entity_id": entity_id,
                "instrument_type": asset_class or quote_type or "equity",
                "canonical_name": f"{ticker} {legal_name}",
            })
            seen_instruments.add(instrument_id)
        instrument_id_rows.append({
            "instrument_id": instrument_id, "identifier_type": "ticker",
            "identifier_value": ticker, "source": "legacy_security_master",
        })
    tables = {
        "_graph_entities_arrow": pa.Table.from_pylist(entity_rows) if entity_rows else None,
        "_graph_entity_ids_arrow": pa.Table.from_pylist(entity_id_rows) if entity_id_rows else None,
        "_graph_instruments_arrow": pa.Table.from_pylist(instrument_rows) if instrument_rows else None,
        "_graph_instrument_ids_arrow": pa.Table.from_pylist(instrument_id_rows) if instrument_id_rows else None,
    }
    try:
        for name, table in tables.items():
            if table is not None:
                con.register(name, table)
        if entity_rows:
            con.execute("""
                INSERT INTO er_entities(entity_id,entity_type,canonical_name,legal_name,country_code)
                SELECT entity_id,entity_type,canonical_name,legal_name,country_code FROM _graph_entities_arrow
                ON CONFLICT(entity_id) DO NOTHING
            """)
        if entity_id_rows:
            con.execute("""
                INSERT INTO er_entity_identifiers(entity_id,identifier_type,identifier_value,source,is_primary)
                SELECT entity_id,identifier_type,identifier_value,source,is_primary FROM _graph_entity_ids_arrow
                ON CONFLICT(entity_id,identifier_type,identifier_value) DO NOTHING
            """)
        if instrument_rows:
            con.execute("""
                INSERT INTO er_instruments(instrument_id,issuer_entity_id,instrument_type,canonical_name)
                SELECT instrument_id,issuer_entity_id,instrument_type,canonical_name FROM _graph_instruments_arrow
                ON CONFLICT(instrument_id) DO NOTHING
            """)
        if instrument_id_rows:
            con.execute("""
                INSERT INTO er_instrument_identifiers(instrument_id,identifier_type,identifier_value,source)
                SELECT instrument_id,identifier_type,identifier_value,source FROM _graph_instrument_ids_arrow
                ON CONFLICT(instrument_id,identifier_type,identifier_value) DO NOTHING
            """)
    finally:
        for name, table in tables.items():
            if table is not None:
                con.unregister(name)
    return {
        "entities": len(seen_entities), "instruments": len(seen_instruments),
        "security_master_available": True,
    }


def _aliases(con):
    rows = _rows_dict(con.execute("""
        SELECT i.entity_id,i.identifier_value,e.canonical_name,e.entity_type
        FROM er_entity_identifiers i
        JOIN er_entities e USING(entity_id)
        WHERE i.identifier_type='alias' AND e.active=TRUE
          AND (i.valid_to IS NULL OR i.valid_to>now())
        ORDER BY i.entity_id,lower(i.identifier_value),i.identifier_value
    """))
    raw = "\x1f".join(
        f"{r['entity_id']}\x1e{r['identifier_value']}\x1e{r['canonical_name']}\x1e{r['entity_type']}"
        for r in rows
    )
    fingerprint = hashlib.sha256(raw.encode()).hexdigest()
    alias_map: dict[str, list[tuple[str, str, str]]] = {}
    for row in rows:
        alias_map.setdefault((row["identifier_value"] or "").lower(), []).append(
            (row["entity_id"], row["canonical_name"], row["entity_type"])
        )
    aliases = sorted((a for a in alias_map if a), key=lambda value: (-len(value), value))
    pattern = None
    if aliases:
        pattern = re.compile(
            r"(?<![a-z0-9])(?:" + "|".join(re.escape(alias) for alias in aliases) + r")(?![a-z0-9])",
            re.IGNORECASE,
        )
    return rows, alias_map, pattern, fingerprint


def _retire_v1_edges(con) -> int:
    count = int(con.execute("""
        SELECT count(*) FROM er_graph_edges
        WHERE edge_class='source_asserted' AND method_version='graph_rules_v1' AND valid_to IS NULL
    """).fetchone()[0])
    if count:
        con.execute("""
            UPDATE er_graph_edges SET valid_to=now()
            WHERE edge_class='source_asserted' AND method_version='graph_rules_v1' AND valid_to IS NULL
        """)
    return count


def _load_work(con, alias_changed: bool):
    relation = "er_graph_current_intervals_v3" if alias_changed else "er_graph_interval_work_queue_v3"
    return con.execute(f"SELECT * FROM {relation} ORDER BY canonical_event_id,source_contract_id,semantic_version_no").fetch_arrow_table()


def _mention_rows(work_table, alias_map, pattern, alias_fingerprint: str, run_id: str):
    edge_rows = []
    evidence_rows = []
    origin_rows = []
    state_rows = []
    intervals = 0
    alias_hits = 0
    for batch in work_table.to_batches(max_chunksize=20000):
        for row in batch.to_pylist():
            intervals += 1
            state_rows.append({
                "mapping_id": row["mapping_id"],
                "source_contract_id": row["source_contract_id"],
                "semantic_version_no": row["semantic_version_no"],
                "interval_hash": row["interval_hash"],
                "alias_fingerprint": alias_fingerprint,
                "active": True,
                "processed_at": now_utc(),
                "last_run_id": run_id,
            })
            if pattern is None:
                continue
            text = "\n".join(x for x in [row.get("title"), row.get("description"), row.get("resolution_rules")] if x)
            if not text:
                continue
            matched_entities: dict[str, tuple[str, str]] = {}
            for match in pattern.finditer(text):
                alias = match.group(0).lower()
                for entity_id, canonical_name, entity_type in alias_map.get(alias, []):
                    matched_entities[entity_id] = (canonical_name, entity_type)
            alias_hits += len(matched_entities)
            for entity_id, (canonical_name, entity_type) in matched_entities.items():
                edge_id = base.stable_id(
                    "edge", row["mapping_id"], row["source_contract_id"], row["semantic_version_no"],
                    "mentions", entity_id, GRAPH_VERSION,
                )
                edge_rows.append({
                    "edge_id": edge_id,
                    "source_node_id": row["canonical_event_id"],
                    "source_node_type": "canonical_event",
                    "relationship_type": "mentions",
                    "target_node_id": entity_id,
                    "target_node_type": entity_type,
                    "edge_class": "source_asserted",
                    "confidence": 1.0,
                    "assertion_text": f"Full semantic source-contract version explicitly mentions {canonical_name}",
                    "inference_method": None,
                    "method_version": GRAPH_VERSION,
                    "human_reviewed": False,
                    "review_status": "source_asserted",
                    "observed_at": row.get("source_observed_at"),
                    "valid_from": row.get("interval_valid_from"),
                    "valid_to": row.get("interval_valid_to"),
                })
                origin_rows.append({
                    "edge_id": edge_id,
                    "mapping_id": row["mapping_id"],
                    "source_contract_id": row["source_contract_id"],
                    "semantic_version_no": row["semantic_version_no"],
                    "graph_version": GRAPH_VERSION,
                })
                for role, raw_snapshot_id in (
                    ("event_payload", row.get("event_raw_snapshot_id")),
                    ("market_payload", row.get("market_raw_snapshot_id")),
                ):
                    evidence_rows.append({
                        "edge_id": edge_id,
                        "evidence_id": base.stable_id("evd", edge_id, role, raw_snapshot_id),
                        "evidence_type": f"semantic_contract_{role}",
                        "source_id": row.get("source_id"),
                        "source_record_id": raw_snapshot_id,
                        "excerpt": (row.get("title") or "")[:1000],
                    })
    return edge_rows, evidence_rows, origin_rows, state_rows, {
        "intervals": intervals, "entity_mentions": alias_hits,
    }


def _persist_graph_batch(con, edge_rows, evidence_rows, origin_rows, state_rows, alias_changed: bool):
    names = []
    try:
        if state_rows:
            con.register("_graph_state_arrow", pa.Table.from_pylist(state_rows)); names.append("_graph_state_arrow")
            con.execute("CREATE OR REPLACE TEMP TABLE _graph_work_keys AS SELECT mapping_id,source_contract_id,semantic_version_no FROM _graph_state_arrow")
        else:
            con.execute("CREATE OR REPLACE TEMP TABLE _graph_work_keys(mapping_id VARCHAR,source_contract_id VARCHAR,semantic_version_no INTEGER)")
        if edge_rows:
            con.register("_graph_edges_arrow", pa.Table.from_pylist(edge_rows)); names.append("_graph_edges_arrow")
            con.execute("CREATE OR REPLACE TEMP TABLE _graph_desired_edge_ids AS SELECT edge_id FROM _graph_edges_arrow")
        else:
            con.execute("CREATE OR REPLACE TEMP TABLE _graph_desired_edge_ids(edge_id VARCHAR)")

        # If aliases changed, a formerly recognized identity alias may have been retired.
        # Retire only edges whose logical interval was reprocessed and is no longer desired.
        if alias_changed:
            con.execute("""
                UPDATE er_graph_edges e SET valid_to=coalesce(e.valid_to,now())
                FROM er_graph_edge_origins o
                JOIN _graph_work_keys w
                  ON w.mapping_id=o.mapping_id
                 AND w.source_contract_id=o.source_contract_id
                 AND w.semantic_version_no=o.semantic_version_no
                WHERE e.edge_id=o.edge_id AND o.graph_version=?
                  AND NOT EXISTS (SELECT 1 FROM _graph_desired_edge_ids d WHERE d.edge_id=e.edge_id)
            """, [GRAPH_VERSION])

        if edge_rows:
            con.execute("""
                INSERT INTO er_graph_edges(
                  edge_id,source_node_id,source_node_type,relationship_type,target_node_id,target_node_type,
                  edge_class,confidence,assertion_text,inference_method,method_version,human_reviewed,
                  review_status,observed_at,valid_from,valid_to)
                SELECT edge_id,source_node_id,source_node_type,relationship_type,target_node_id,target_node_type,
                       edge_class,confidence,assertion_text,inference_method,method_version,human_reviewed,
                       review_status,observed_at,valid_from,valid_to
                FROM _graph_edges_arrow
                ON CONFLICT(edge_id) DO UPDATE SET
                  source_node_id=excluded.source_node_id,
                  target_node_id=excluded.target_node_id,
                  target_node_type=excluded.target_node_type,
                  assertion_text=excluded.assertion_text,
                  observed_at=excluded.observed_at,
                  valid_from=excluded.valid_from,
                  valid_to=excluded.valid_to,
                  review_status=excluded.review_status
            """)
        if origin_rows:
            con.register("_graph_origins_arrow", pa.Table.from_pylist(origin_rows)); names.append("_graph_origins_arrow")
            con.execute("""
                INSERT INTO er_graph_edge_origins(edge_id,mapping_id,source_contract_id,semantic_version_no,graph_version)
                SELECT edge_id,mapping_id,source_contract_id,semantic_version_no,graph_version
                FROM _graph_origins_arrow
                ON CONFLICT(edge_id) DO UPDATE SET
                  mapping_id=excluded.mapping_id,
                  source_contract_id=excluded.source_contract_id,
                  semantic_version_no=excluded.semantic_version_no,
                  graph_version=excluded.graph_version
            """)
        if evidence_rows:
            con.register("_graph_evidence_arrow", pa.Table.from_pylist(evidence_rows)); names.append("_graph_evidence_arrow")
            con.execute("""
                INSERT INTO er_edge_evidence(edge_id,evidence_id,evidence_type,source_id,source_record_id,excerpt)
                SELECT edge_id,evidence_id,evidence_type,source_id,source_record_id,excerpt
                FROM _graph_evidence_arrow
                ON CONFLICT(edge_id,evidence_id) DO NOTHING
            """)
        if state_rows:
            con.execute("""
                INSERT INTO er_graph_interval_state(
                  mapping_id,source_contract_id,semantic_version_no,interval_hash,alias_fingerprint,
                  active,processed_at,last_run_id)
                SELECT mapping_id,source_contract_id,semantic_version_no,interval_hash,alias_fingerprint,
                       active,processed_at,last_run_id
                FROM _graph_state_arrow
                ON CONFLICT(mapping_id,source_contract_id,semantic_version_no) DO UPDATE SET
                  interval_hash=excluded.interval_hash,
                  alias_fingerprint=excluded.alias_fingerprint,
                  active=TRUE,
                  processed_at=excluded.processed_at,
                  last_run_id=excluded.last_run_id
            """)
    finally:
        for name in names:
            con.unregister(name)


def _retire_stale_intervals(con, run_id: str) -> int:
    count = int(con.execute("SELECT count(*) FROM er_graph_stale_intervals_v3").fetchone()[0])
    if not count:
        return 0
    con.execute("""
        UPDATE er_graph_edges e SET valid_to=coalesce(e.valid_to,
          CASE
            WHEN m.valid_to IS NULL AND v.valid_to IS NULL THEN now()
            WHEN m.valid_to IS NULL THEN v.valid_to
            WHEN v.valid_to IS NULL THEN m.valid_to
            ELSE least(m.valid_to,v.valid_to)
          END)
        FROM er_graph_edge_origins o
        JOIN er_graph_stale_intervals_v3 s
          ON s.mapping_id=o.mapping_id
         AND s.source_contract_id=o.source_contract_id
         AND s.semantic_version_no=o.semantic_version_no
        JOIN er_event_mappings m ON m.mapping_id=s.mapping_id
        JOIN er_contract_semantic_versions v
          ON v.source_contract_id=s.source_contract_id AND v.version_no=s.semantic_version_no
        WHERE e.edge_id=o.edge_id AND o.graph_version=?
    """, [GRAPH_VERSION])
    con.execute("""
        UPDATE er_graph_interval_state s SET active=FALSE,processed_at=now(),last_run_id=?
        WHERE s.active=TRUE
          AND NOT EXISTS (
            SELECT 1 FROM er_graph_current_intervals_v3 i
            WHERE i.mapping_id=s.mapping_id
              AND i.source_contract_id=s.source_contract_id
              AND i.semantic_version_no=s.semantic_version_no
          )
    """, [run_id])
    return count


def run_graph_pass() -> dict:
    run_id = f"gr_{uuid.uuid4().hex}"
    con = connect()
    try:
        recovery = recover_abandoned_graph_runs(con)
        if recovery["running"]:
            raise RuntimeError(f"another graph run is still active: {recovery['running']}")
        con.execute("""
            INSERT INTO er_graph_runs(run_id,execution_version,graph_version,started_at,status)
            VALUES (?,?,?,now(),'running')
        """, [run_id, EXECUTION_VERSION, GRAPH_VERSION])
        started = time.monotonic()
        try:
            t0 = time.monotonic()
            seed_aliases = _seed_identity_aliases(con)
            identity = _bulk_security_master(con)
            aliases, alias_map, pattern, alias_fingerprint = _aliases(con)
            previous_fp_row = con.execute(
                "SELECT state_value FROM er_graph_state WHERE state_key='active_alias_fingerprint'"
            ).fetchone()
            alias_changed = previous_fp_row is None or previous_fp_row[0] != alias_fingerprint
            identity_seconds = time.monotonic() - t0

            t0 = time.monotonic()
            retired_v1 = _retire_v1_edges(con)
            work_table = _load_work(con, alias_changed)
            intervals_total = int(con.execute("SELECT count(*) FROM er_graph_current_intervals_v3").fetchone()[0])
            load_seconds = time.monotonic() - t0

            t0 = time.monotonic()
            edge_rows,evidence_rows,origin_rows,state_rows,mention_stats = _mention_rows(
                work_table,alias_map,pattern,alias_fingerprint,run_id
            )
            detect_seconds = time.monotonic() - t0

            t0 = time.monotonic()
            _persist_graph_batch(con,edge_rows,evidence_rows,origin_rows,state_rows,alias_changed)
            stale_retired = _retire_stale_intervals(con,run_id)
            con.execute("""
                INSERT INTO er_graph_state(state_key,state_value,updated_at)
                VALUES ('active_alias_fingerprint',?,now())
                ON CONFLICT(state_key) DO UPDATE SET state_value=excluded.state_value,updated_at=excluded.updated_at
            """, [alias_fingerprint])
            persist_seconds = time.monotonic() - t0
            elapsed = time.monotonic() - started
            details = {
                "incremental": True,
                "edge_semantics_unchanged": GRAPH_VERSION,
                "alias_fingerprint": alias_fingerprint,
                "alias_changed": alias_changed,
                "active_aliases": len(aliases),
                "seed_aliases_expected": seed_aliases,
                "legacy_v1_edges_retired": retired_v1,
                "stale_intervals_retired": stale_retired,
                "entity_mentions": mention_stats["entity_mentions"],
                "identity": identity,
                "recovery": recovery,
                "timing": {
                    "identity_seconds": round(identity_seconds,6),
                    "load_seconds": round(load_seconds,6),
                    "detect_seconds": round(detect_seconds,6),
                    "persist_seconds": round(persist_seconds,6),
                    "total_seconds": round(elapsed,6),
                },
            }
            con.execute("""
                UPDATE er_graph_runs SET finished_at=now(),status='success',intervals_total=?,
                  intervals_processed=?,edges_written=?,evidence_written=?,details=? WHERE run_id=?
            """, [
                intervals_total,mention_stats["intervals"],len(edge_rows),len(evidence_rows),
                json.dumps(details,sort_keys=True,default=str),run_id,
            ])
            return {
                "run_id": run_id,
                **identity,
                "semantic_intervals": intervals_total,
                "intervals_processed": mention_stats["intervals"],
                "mention_edges_written": len(edge_rows),
                "evidence_records_written": len(evidence_rows),
                "stale_intervals_retired": stale_retired,
                "legacy_v1_edges_retired": retired_v1,
                "alias_changed": alias_changed,
                "graph_version": GRAPH_VERSION,
                "execution_version": EXECUTION_VERSION,
                "timing": details["timing"],
            }
        except Exception as exc:
            con.execute("""
                UPDATE er_graph_runs SET finished_at=now(),status='failed',details=? WHERE run_id=?
            """, [json.dumps({"error":str(exc)[:4000]},sort_keys=True),run_id])
            raise
    finally:
        con.close()


if __name__ == "__main__":
    print(run_graph_pass())
