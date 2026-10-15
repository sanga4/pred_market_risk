"""Institutional graph execution v4: reference-aware without issuer inference.

v4 preserves graph v3's interval hashing, provenance, evidence, alias matching and `mentions`
semantics. It removes the dormant legacy behavior that could synthesize legal entities and issuer
links from a local `securities` table. The graph may inspect the institutional instrument master for
coverage/status, but entity/issuer relationships must arrive from explicit reference data or
reviewed/inferred relationship pipelines with their own provenance.
"""

from __future__ import annotations

import json
import time
import uuid

import institutional_graph_v3 as v3
from db import connect

GRAPH_VERSION = v3.GRAPH_VERSION
EXECUTION_VERSION = "graph_execution_v4_no_issuer_inference"


def _reference_master_status(con) -> dict:
    instruments = int(con.execute("SELECT count(*) FROM er_instruments WHERE active=TRUE").fetchone()[0])
    entities = int(con.execute("SELECT count(*) FROM er_entities WHERE active=TRUE").fetchone()[0])
    ticker_ids = int(con.execute("""
        SELECT count(*) FROM er_instrument_identifiers
        WHERE identifier_type='ticker_eventrisk' AND (valid_to IS NULL OR valid_to>now())
    """).fetchone()[0])
    issuer_links = int(con.execute("""
        SELECT count(*) FROM er_instruments
        WHERE active=TRUE AND issuer_entity_id IS NOT NULL
    """).fetchone()[0])
    return {
        "entities": entities,
        "instruments": instruments,
        "ticker_identifiers": ticker_ids,
        "issuer_links": issuer_links,
        "security_master_available": instruments > 0,
        "identity_policy": "read_reference_master_only_no_issuer_inference",
    }


def run_graph_pass() -> dict:
    run_id = f"gr_{uuid.uuid4().hex}"
    con = connect()
    try:
        recovery = v3.recover_abandoned_graph_runs(con)
        if recovery["running"]:
            raise RuntimeError(f"another graph run is still active: {recovery['running']}")
        con.execute("""
            INSERT INTO er_graph_runs(run_id,execution_version,graph_version,started_at,status)
            VALUES (?,?,?,now(),'running')
        """, [run_id, EXECUTION_VERSION, GRAPH_VERSION])
        started = time.monotonic()
        try:
            t0 = time.monotonic()
            seed_aliases = v3._seed_identity_aliases(con)
            identity = _reference_master_status(con)
            aliases, alias_map, pattern, alias_fingerprint = v3._aliases(con)
            previous_fp_row = con.execute(
                "SELECT state_value FROM er_graph_state WHERE state_key='active_alias_fingerprint'"
            ).fetchone()
            alias_changed = previous_fp_row is None or previous_fp_row[0] != alias_fingerprint
            identity_seconds = time.monotonic() - t0

            t0 = time.monotonic()
            retired_v1 = v3._retire_v1_edges(con)
            work_table = v3._load_work(con, alias_changed)
            intervals_total = int(con.execute("SELECT count(*) FROM er_graph_current_intervals_v3").fetchone()[0])
            load_seconds = time.monotonic() - t0

            t0 = time.monotonic()
            edge_rows,evidence_rows,origin_rows,state_rows,mention_stats = v3._mention_rows(
                work_table,alias_map,pattern,alias_fingerprint,run_id
            )
            detect_seconds = time.monotonic() - t0

            t0 = time.monotonic()
            v3._persist_graph_batch(con,edge_rows,evidence_rows,origin_rows,state_rows,alias_changed)
            stale_retired = v3._retire_stale_intervals(con,run_id)
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
                "identity_policy": "reference_master_read_only_no_synthetic_issuer_entities",
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
