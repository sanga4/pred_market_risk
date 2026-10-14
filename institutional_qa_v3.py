"""Institutional QA v3: full-universe completeness, presence coherence and raw durability.

This layer extends v2. It does not modify observations or infer source states. Controls only
open/resolve explicit quality issues so distribution readiness can fail closed.
"""

from __future__ import annotations

import os

import institutional_qa as base
import institutional_qa_v2 as v2
from db import connect

QA_VERSION = "institutional_qa_v3"


def check_latest_full_universe_runs(con) -> int:
    rows = con.execute("""
        WITH ranked AS (
          SELECT r.*,
                 row_number() OVER(PARTITION BY source_id ORDER BY started_at DESC,run_id DESC) rn
          FROM er_ingestion_runs r
        )
        SELECT s.source_id,r.run_id,r.status,r.collection_scope,r.universe_complete,r.started_at,r.finished_at
        FROM er_sources s
        LEFT JOIN ranked r ON r.source_id=s.source_id AND r.rn=1
        WHERE s.active=TRUE
          AND (r.run_id IS NULL OR r.status<>'success' OR coalesce(r.collection_scope,'')<>'full'
               OR r.universe_complete IS DISTINCT FROM TRUE)
    """).fetchall()
    bad = {r[0] for r in rows}
    for source_id, run_id, status, scope, complete, started, finished in rows:
        base.open_issue(
            con, "source", source_id, "latest_run_not_complete_full_universe", "high",
            f"run_id={run_id}; status={status}; scope={scope}; universe_complete={complete}; "
            f"started={started}; finished={finished}",
        )
    for (source_id,) in con.execute("SELECT source_id FROM er_sources WHERE active=TRUE").fetchall():
        if source_id not in bad:
            base.resolve_issue(con, "source", source_id, "latest_run_not_complete_full_universe")
    return len(rows)


def check_presence_coherence(con) -> int:
    threshold = max(2, int(os.getenv("PRESENCE_MISS_THRESHOLD", "3")))
    rows = con.execute("""
        SELECT c.source_contract_id,c.active,p.listing_state,p.consecutive_misses,p.deactivated_at
        FROM er_source_contracts c
        JOIN er_contract_presence_state p USING(source_contract_id)
        WHERE (p.listing_state='missing_confirmed' AND c.active=TRUE)
           OR (p.listing_state='missing_confirmed' AND p.consecutive_misses<?)
           OR (p.listing_state='observed' AND p.consecutive_misses<>0)
           OR (p.listing_state='observed' AND p.deactivated_at IS NOT NULL)
    """, [threshold]).fetchall()
    bad = {r[0] for r in rows}
    for contract_id, active, state, misses, deactivated_at in rows:
        base.open_issue(
            con, "source_contract", contract_id, "presence_state_incoherent", "critical",
            f"active={active}; listing_state={state}; consecutive_misses={misses}; "
            f"deactivated_at={deactivated_at}; miss_threshold={threshold}",
        )
    for (contract_id,) in con.execute("SELECT source_contract_id FROM er_source_contracts").fetchall():
        if contract_id not in bad:
            base.resolve_issue(con, "source_contract", contract_id, "presence_state_incoherent")
    return len(rows)


def check_raw_durability(con) -> int:
    rows = con.execute("""
        SELECT r.raw_snapshot_id,r.payload_storage,a.status,a.archive_key,a.archive_member,a.archive_bundle_hash
        FROM er_raw_snapshots r
        LEFT JOIN er_raw_archive_objects a USING(raw_snapshot_id)
        WHERE r.payload_json IS NULL
          AND NOT (
            a.status='archived' AND a.archive_key IS NOT NULL AND a.archive_member IS NOT NULL
            AND a.archive_bundle_hash IS NOT NULL AND a.content_hash=r.content_hash
          )
    """).fetchall()
    bad = {r[0] for r in rows}
    for raw_id, storage, archive_status, key, member, bundle_hash in rows:
        base.open_issue(
            con, "raw_snapshot", raw_id, "raw_payload_not_durable", "critical",
            f"payload_storage={storage}; archive_status={archive_status}; archive_key={key}; "
            f"archive_member={member}; bundle_hash={bundle_hash}",
        )
    # Resolving across the whole raw table can be expensive; only close existing issues whose
    # row is now demonstrably durable.
    con.execute("""
        UPDATE er_quality_issues q
        SET status='resolved',resolved_at=now(),resolution_notes='Raw evidence is durable again'
        WHERE q.status='open' AND q.rule_code='raw_payload_not_durable'
          AND NOT EXISTS (
            SELECT 1 FROM er_raw_snapshots r
            LEFT JOIN er_raw_archive_objects a USING(raw_snapshot_id)
            WHERE r.raw_snapshot_id=q.object_id AND r.payload_json IS NULL
              AND NOT (a.status='archived' AND a.archive_key IS NOT NULL AND a.archive_member IS NOT NULL
                       AND a.archive_bundle_hash IS NOT NULL AND a.content_hash=r.content_hash)
          )
    """)
    return len(rows)


def check_stuck_ingestion_runs(con, stale_minutes: int) -> int:
    threshold = max(30, stale_minutes * 2)
    rows = con.execute("""
        SELECT run_id,source_id,started_at,date_diff('minute',started_at,now()) age_minutes
        FROM er_ingestion_runs
        WHERE status='running' AND started_at < now() - (? * INTERVAL '1 minute')
    """, [threshold]).fetchall()
    bad = {r[0] for r in rows}
    for run_id, source_id, started_at, age in rows:
        base.open_issue(
            con, "ingestion_run", run_id, "ingestion_run_stuck", "high",
            f"source={source_id}; started_at={started_at}; age_minutes={age}; threshold={threshold}",
        )
    existing = con.execute(
        "SELECT object_id FROM er_quality_issues WHERE status='open' AND rule_code='ingestion_run_stuck'"
    ).fetchall()
    for (run_id,) in existing:
        if run_id not in bad:
            base.resolve_issue(con, "ingestion_run", run_id, "ingestion_run_stuck")
    return len(rows)


def run_qa(stale_minutes: int = 20) -> dict:
    result = v2.run_qa(stale_minutes=stale_minutes)
    con = connect()
    try:
        extra = {
            "latest_full_universe_runs": check_latest_full_universe_runs(con),
            "presence_coherence": check_presence_coherence(con),
            "raw_durability": check_raw_durability(con),
            "stuck_ingestion_runs": check_stuck_ingestion_runs(con, stale_minutes),
        }
        open_count = con.execute("SELECT count(*) FROM er_quality_issues WHERE status='open'").fetchone()[0]
        critical_count = con.execute(
            "SELECT count(*) FROM er_quality_issues WHERE status='open' AND severity='critical'"
        ).fetchone()[0]
    finally:
        con.close()
    result["checks"].update(extra)
    result["open_issues"] = open_count
    result["critical_issues"] = critical_count
    result["qa_version"] = QA_VERSION
    return result


if __name__ == "__main__":
    print(run_qa())
