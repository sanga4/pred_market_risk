"""Institutional data-quality checks for EventRisk.

The QA layer does not alter source data. It opens/resolves quality issues so downstream
clients can see freshness, completeness, mapping ambiguity and impossible market states.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone

from db import connect


def now_utc():
    return datetime.now(timezone.utc).replace(microsecond=0)


def issue_id(object_type: str, object_id: str, rule_code: str) -> str:
    raw = f"{object_type}\x1f{object_id}\x1f{rule_code}"
    return "qi_" + hashlib.sha256(raw.encode()).hexdigest()[:28]


def open_issue(con, object_type: str, object_id: str, rule_code: str, severity: str, details: str):
    qid = issue_id(object_type, object_id, rule_code)
    existing = con.execute("SELECT quality_issue_id FROM er_quality_issues WHERE quality_issue_id=?", [qid]).fetchone()
    if existing:
        con.execute(
            """UPDATE er_quality_issues SET severity=?,status='open',details=?,detected_at=now(),
                      resolved_at=NULL,resolution_notes=NULL WHERE quality_issue_id=?""",
            [severity, details[:4000], qid],
        )
    else:
        con.execute(
            """INSERT INTO er_quality_issues(
                 quality_issue_id,object_type,object_id,rule_code,severity,status,details)
               VALUES (?,?,?,?,?,'open',?)""",
            [qid, object_type, object_id, rule_code, severity, details[:4000]],
        )
    return qid


def resolve_issue(con, object_type: str, object_id: str, rule_code: str, note: str = "Condition no longer present"):
    qid = issue_id(object_type, object_id, rule_code)
    con.execute(
        """UPDATE er_quality_issues SET status='resolved',resolved_at=now(),resolution_notes=?
           WHERE quality_issue_id=? AND status='open'""",
        [note, qid],
    )


def check_probability_bounds(con) -> int:
    rows = con.execute(
        """SELECT source_contract_id,observed_at,displayed_probability,yes_bid,yes_ask
           FROM er_market_observations
           WHERE (displayed_probability IS NOT NULL AND (displayed_probability<0 OR displayed_probability>1))
              OR (yes_bid IS NOT NULL AND (yes_bid<0 OR yes_bid>1))
              OR (yes_ask IS NOT NULL AND (yes_ask<0 OR yes_ask>1))"""
    ).fetchall()
    for contract_id, observed_at, prob, bid, ask in rows:
        open_issue(con, "source_contract", contract_id, "probability_out_of_bounds", "critical",
                   f"Observed {observed_at}: probability={prob}, bid={bid}, ask={ask}")
    return len(rows)


def check_crossed_books(con) -> int:
    rows = con.execute(
        """SELECT DISTINCT source_contract_id FROM er_market_observations
           WHERE yes_bid IS NOT NULL AND yes_ask IS NOT NULL AND yes_bid>yes_ask"""
    ).fetchall()
    bad = {r[0] for r in rows}
    all_contracts = {r[0] for r in con.execute("SELECT source_contract_id FROM er_source_contracts WHERE active=TRUE").fetchall()}
    for contract_id in bad:
        open_issue(con, "source_contract", contract_id, "crossed_yes_book", "high", "yes_bid is greater than yes_ask")
    for contract_id in all_contracts - bad:
        resolve_issue(con, "source_contract", contract_id, "crossed_yes_book")
    return len(bad)


def check_missing_raw_lineage(con) -> int:
    # Legacy collector observations are intentionally exempt: their missing raw payloads are
    # disclosed once at dataset level by the migration. Every institutional-era observation
    # must trace to an immutable raw source snapshot.
    rows = con.execute(
        """SELECT DISTINCT o.source_contract_id
           FROM er_market_observations o
           LEFT JOIN er_raw_snapshots r ON r.raw_snapshot_id=o.raw_snapshot_id
           WHERE coalesce(o.methodology_version,'') <> 'legacy_collector_v0.3'
             AND (o.raw_snapshot_id IS NULL OR r.raw_snapshot_id IS NULL)"""
    ).fetchall()
    for (contract_id,) in rows:
        open_issue(con, "source_contract", contract_id, "observation_missing_raw_lineage", "critical",
                   "One or more institutional-era observations cannot be traced to an immutable raw snapshot")
    return len(rows)


def check_contract_versions(con) -> int:
    rows = con.execute(
        """SELECT c.source_contract_id
           FROM er_source_contracts c
           LEFT JOIN er_source_contract_versions v USING(source_contract_id)
           GROUP BY c.source_contract_id HAVING count(v.version_no)=0"""
    ).fetchall()
    for (contract_id,) in rows:
        open_issue(con, "source_contract", contract_id, "missing_contract_version", "high",
                   "Source contract has no versioned metadata record")
    return len(rows)


def check_active_mapping_cardinality(con) -> int:
    rows = con.execute(
        """SELECT source_contract_id,count(*) n
           FROM er_event_mappings WHERE valid_to IS NULL
           GROUP BY source_contract_id HAVING count(*)<>1"""
    ).fetchall()
    for contract_id, count in rows:
        open_issue(con, "source_contract", contract_id, "active_mapping_cardinality", "critical",
                   f"Expected exactly one active canonical mapping; found {count}")
    return len(rows)


def check_source_freshness(con, stale_minutes: int = 20) -> int:
    rows = con.execute(
        """SELECT s.source_id,h.last_success_at,h.last_observation_at,h.last_run_status,
                  date_diff('minute',coalesce(h.last_success_at,TIMESTAMPTZ '1970-01-01'),now()) AS mins
           FROM er_sources s LEFT JOIN er_source_health h USING(source_id)
           WHERE s.active=TRUE"""
    ).fetchall()
    bad = 0
    for source_id, last_success, last_observation, status, mins in rows:
        if last_success is None or mins is None or mins > stale_minutes:
            bad += 1
            open_issue(con, "source", source_id, "source_stale", "high",
                       f"Last success={last_success}; last observation={last_observation}; last status={status}; age_minutes={mins}")
            con.execute("UPDATE er_source_health SET health_status='stale',updated_at=now() WHERE source_id=?", [source_id])
        else:
            resolve_issue(con, "source", source_id, "source_stale")
            con.execute("UPDATE er_source_health SET health_status=CASE WHEN last_run_status='success' THEN 'healthy' ELSE 'degraded' END,updated_at=now() WHERE source_id=?", [source_id])
    return bad


def check_ingestion_volume_drop(con) -> int:
    sources = [r[0] for r in con.execute("SELECT source_id FROM er_sources WHERE active=TRUE").fetchall()]
    bad = 0
    for source_id in sources:
        rows = con.execute(
            """SELECT records_written,status,finished_at FROM er_ingestion_runs
               WHERE source_id=? AND status IN ('success','partial')
               ORDER BY finished_at DESC LIMIT 2""",
            [source_id],
        ).fetchall()
        if len(rows) < 2 or not rows[1][0]:
            continue
        latest, previous = rows[0][0], rows[1][0]
        if latest < max(5, previous * 0.5):
            bad += 1
            open_issue(con, "source", source_id, "ingestion_volume_drop", "medium",
                       f"Latest run wrote {latest} records versus {previous} previously")
        else:
            resolve_issue(con, "source", source_id, "ingestion_volume_drop")
    return bad


def check_mapping_ambiguity(con) -> int:
    rows = con.execute(
        """SELECT left_source_contract_id,count(*) n
           FROM er_mapping_candidates
           WHERE status='pending' AND suggested_relationship='same_event' AND overall_confidence>=0.90
           GROUP BY left_source_contract_id HAVING count(*)>1"""
    ).fetchall()
    for contract_id, count in rows:
        open_issue(con, "source_contract", contract_id, "ambiguous_high_confidence_mapping", "medium",
                   f"Contract has {count} unresolved high-confidence same-event candidates")
    return len(rows)


def check_inferred_edge_metadata(con) -> int:
    rows = con.execute(
        """SELECT edge_id FROM er_graph_edges
           WHERE edge_class='inferred' AND (inference_method IS NULL OR method_version IS NULL OR confidence IS NULL)"""
    ).fetchall()
    for (edge_id,) in rows:
        open_issue(con, "graph_edge", edge_id, "inferred_edge_missing_methodology", "critical",
                   "Inferred edge is missing confidence, inference method, or method version")
    return len(rows)


def run_qa(stale_minutes: int = 20) -> dict:
    con = connect()
    try:
        checks = {
            "probability_bounds": check_probability_bounds(con),
            "crossed_books": check_crossed_books(con),
            "missing_raw_lineage": check_missing_raw_lineage(con),
            "missing_contract_versions": check_contract_versions(con),
            "mapping_cardinality": check_active_mapping_cardinality(con),
            "source_freshness": check_source_freshness(con, stale_minutes),
            "ingestion_volume_drop": check_ingestion_volume_drop(con),
            "mapping_ambiguity": check_mapping_ambiguity(con),
            "inferred_edge_metadata": check_inferred_edge_metadata(con),
        }
        open_count = con.execute("SELECT count(*) FROM er_quality_issues WHERE status='open'").fetchone()[0]
        critical_count = con.execute("SELECT count(*) FROM er_quality_issues WHERE status='open' AND severity='critical'").fetchone()[0]
        return {"checks": checks, "open_issues": open_count, "critical_issues": critical_count}
    finally:
        con.close()


if __name__ == "__main__":
    print(run_qa())
