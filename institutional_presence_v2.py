"""Set-based full-universe presence reconciliation.

This preserves the v1 presence model and miss threshold while replacing per-contract
transition inserts with bulk SQL. Absence is never rewritten into a source-reported status;
only repeated misses across successful complete-universe runs can deactivate serving rows.
"""

from __future__ import annotations

import institutional_ingest_v3 as v3

METHOD_VERSION = "presence_reconciliation_v2_set_based"


def reconcile_presence(con, source_id: str, run_id: str, seen_contract_ids: set[str], *, universe_complete: bool) -> dict:
    seen = sorted(seen_contract_ids)
    con.execute("DROP TABLE IF EXISTS _er_seen_contracts")
    con.execute("CREATE TEMP TABLE _er_seen_contracts(source_contract_id VARCHAR PRIMARY KEY)")
    if seen:
        con.execute("INSERT INTO _er_seen_contracts SELECT unnest(?)", [seen])

    con.execute("""
        CREATE OR REPLACE TEMP TABLE _presence_reappeared AS
        SELECT p.source_contract_id,p.listing_state previous_state,p.consecutive_misses previous_misses
        FROM er_contract_presence_state p
        JOIN _er_seen_contracts s USING(source_contract_id)
        WHERE p.listing_state<>'observed'
    """)
    reappeared = int(con.execute("SELECT count(*) FROM _presence_reappeared").fetchone()[0])

    con.execute("""
        INSERT INTO er_contract_presence_state(
          source_contract_id,source_id,listing_state,consecutive_misses,last_seen_run_id,
          last_seen_at,last_evaluated_run_id,deactivated_at,updated_at)
        SELECT c.source_contract_id,c.source_id,'observed',0,?,now(),?,NULL,now()
        FROM er_source_contracts c
        JOIN _er_seen_contracts s USING(source_contract_id)
        WHERE c.source_id=?
        ON CONFLICT(source_contract_id) DO UPDATE SET
          source_id=excluded.source_id,listing_state='observed',consecutive_misses=0,
          last_seen_run_id=excluded.last_seen_run_id,last_seen_at=excluded.last_seen_at,
          last_evaluated_run_id=excluded.last_evaluated_run_id,deactivated_at=NULL,updated_at=now()
    """, [run_id, run_id, source_id])
    con.execute("""
        INSERT INTO er_contract_presence_transitions(
          transition_id,source_contract_id,source_id,run_id,transition_type,
          previous_state,new_state,consecutive_misses,reason)
        SELECT
          'prs_' || md5(source_contract_id || '|' || ? || '|reactivated'),
          source_contract_id,? ,? ,'reactivated',previous_state,'observed',0,
          'Contract reappeared in source open universe after ' || cast(previous_misses AS varchar) || ' consecutive miss(es)'
        FROM _presence_reappeared
        ON CONFLICT(transition_id) DO NOTHING
    """, [run_id, source_id, run_id])

    if not universe_complete:
        con.execute("DROP TABLE IF EXISTS _er_seen_contracts")
        con.execute("DROP TABLE IF EXISTS _presence_reappeared")
        return {
            "universe_complete": False,
            "seen": len(seen),
            "reactivated": reappeared,
            "missed": 0,
            "deactivated": 0,
            "miss_threshold": v3.PRESENCE_MISS_THRESHOLD,
            "method_version": METHOD_VERSION,
        }

    con.execute("""
        CREATE OR REPLACE TEMP TABLE _presence_missing_before AS
        SELECT c.source_contract_id,p.listing_state previous_state,
               coalesce(p.consecutive_misses,0) previous_misses
        FROM er_source_contracts c
        LEFT JOIN er_contract_presence_state p USING(source_contract_id)
        LEFT JOIN _er_seen_contracts s USING(source_contract_id)
        WHERE c.source_id=? AND c.active=TRUE AND s.source_contract_id IS NULL
    """, [source_id])

    con.execute("""
        INSERT INTO er_contract_presence_state(
          source_contract_id,source_id,listing_state,consecutive_misses,last_evaluated_run_id,updated_at)
        SELECT c.source_contract_id,c.source_id,'missing_unconfirmed',1,?,now()
        FROM er_source_contracts c
        LEFT JOIN _er_seen_contracts s USING(source_contract_id)
        WHERE c.source_id=? AND c.active=TRUE AND s.source_contract_id IS NULL
        ON CONFLICT(source_contract_id) DO UPDATE SET
          source_id=excluded.source_id,
          consecutive_misses=er_contract_presence_state.consecutive_misses+1,
          listing_state=CASE
            WHEN er_contract_presence_state.consecutive_misses+1>=? THEN 'missing_confirmed'
            ELSE 'missing_unconfirmed'
          END,
          last_evaluated_run_id=excluded.last_evaluated_run_id,
          deactivated_at=CASE
            WHEN er_contract_presence_state.consecutive_misses+1>=?
              AND er_contract_presence_state.listing_state<>'missing_confirmed'
            THEN now() ELSE er_contract_presence_state.deactivated_at END,
          updated_at=now()
    """, [run_id, source_id, v3.PRESENCE_MISS_THRESHOLD, v3.PRESENCE_MISS_THRESHOLD])

    con.execute("""
        CREATE OR REPLACE TEMP TABLE _presence_missing_after AS
        SELECT p.source_contract_id,p.listing_state new_state,p.consecutive_misses,
               b.previous_state,b.previous_misses
        FROM er_contract_presence_state p
        JOIN _presence_missing_before b USING(source_contract_id)
        WHERE p.source_id=? AND p.last_evaluated_run_id=?
    """, [source_id, run_id])
    missed = int(con.execute("SELECT count(*) FROM _presence_missing_after").fetchone()[0])
    deactivated = int(con.execute(
        "SELECT count(*) FROM _presence_missing_after WHERE new_state='missing_confirmed'"
    ).fetchone()[0])

    con.execute("""
        INSERT INTO er_contract_presence_transitions(
          transition_id,source_contract_id,source_id,run_id,transition_type,
          previous_state,new_state,consecutive_misses,reason)
        SELECT
          'prs_' || md5(source_contract_id || '|' || ? || '|' ||
                         CASE WHEN new_state='missing_confirmed' THEN 'deactivated' ELSE 'first_miss' END),
          source_contract_id,? ,? ,
          CASE WHEN new_state='missing_confirmed' THEN 'deactivated' ELSE 'first_miss' END,
          previous_state,new_state,consecutive_misses,
          'Contract absent from complete full-universe source run; source status itself was not rewritten'
        FROM _presence_missing_after
        WHERE previous_state IS DISTINCT FROM new_state
        ON CONFLICT(transition_id) DO NOTHING
    """, [run_id, source_id, run_id])

    if deactivated:
        con.execute("""
            UPDATE er_source_contracts SET active=FALSE,last_seen_at=last_seen_at
            WHERE source_id=? AND source_contract_id IN (
              SELECT source_contract_id FROM _presence_missing_after
              WHERE new_state='missing_confirmed'
            )
        """, [source_id])

    for name in [
        "_er_seen_contracts", "_presence_reappeared", "_presence_missing_before", "_presence_missing_after",
    ]:
        con.execute(f"DROP TABLE IF EXISTS {name}")
    return {
        "universe_complete": True,
        "seen": len(seen),
        "reactivated": reappeared,
        "missed": missed,
        "deactivated": deactivated,
        "miss_threshold": v3.PRESENCE_MISS_THRESHOLD,
        "method_version": METHOD_VERSION,
    }
