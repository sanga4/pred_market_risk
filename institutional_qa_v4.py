"""Institutional QA v4: set-based issue synchronization at full-universe scale.

Rule meanings, object types and severities are preserved from QA v1-v3. The execution model
changes: every check produces only its current violation set, then quality issues are bulk
upserted and previously-open issues absent from that set are resolved with one SQL statement.
No check resolves an entire contract/event universe one object at a time.
"""

from __future__ import annotations

import os
from typing import Any

import pyarrow as pa

import institutional_qa as qa_base
from db import connect
from institutional_migrations import MIGRATION_FILES
from institutional_migrations_v2 import EXTRA_MIGRATION_FILES

QA_VERSION = "institutional_qa_v4_set_based"


def _rows_dict(cur) -> list[dict]:
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _sync_rule(
    con,
    *,
    object_type: str,
    rule_code: str,
    severity: str,
    sql: str,
    params: list[Any] | None = None,
    resolution_note: str = "Condition no longer present",
) -> int:
    rows = _rows_dict(con.execute(sql, params or []))
    staged = []
    for row in rows:
        object_id = str(row["object_id"])
        staged.append({
            "quality_issue_id": qa_base.issue_id(object_type, object_id, rule_code),
            "object_id": object_id,
            "details": str(row.get("details") or "")[:12000],
        })
    temp = f"_qa_{rule_code}_{object_type}".replace("-", "_")
    if staged:
        con.register(temp + "_arrow", pa.Table.from_pylist(staged))
        try:
            con.execute(f"CREATE OR REPLACE TEMP TABLE {temp} AS SELECT * FROM {temp}_arrow")
        finally:
            con.unregister(temp + "_arrow")
        con.execute(f"""
            INSERT INTO er_quality_issues(
              quality_issue_id,object_type,object_id,rule_code,severity,status,details,detected_at,
              resolved_at,resolution_notes)
            SELECT quality_issue_id,?,?,?,?,'open',details,now(),NULL,NULL FROM {temp}
            ON CONFLICT(quality_issue_id) DO UPDATE SET
              severity=excluded.severity,status='open',details=excluded.details,detected_at=now(),
              resolved_at=NULL,resolution_notes=NULL
        """, [object_type, rule_code, severity])
        con.execute(f"""
            UPDATE er_quality_issues q
            SET status='resolved',resolved_at=now(),resolution_notes=?
            WHERE q.status='open' AND q.object_type=? AND q.rule_code=?
              AND NOT EXISTS (SELECT 1 FROM {temp} b WHERE b.object_id=q.object_id)
        """, [resolution_note, object_type, rule_code])
    else:
        con.execute("""
            UPDATE er_quality_issues
            SET status='resolved',resolved_at=now(),resolution_notes=?
            WHERE status='open' AND object_type=? AND rule_code=?
        """, [resolution_note, object_type, rule_code])
    return len(staged)


def check_probability_bounds(con) -> int:
    return _sync_rule(
        con, object_type="source_contract", rule_code="probability_out_of_bounds", severity="critical",
        sql="""
          SELECT source_contract_id AS object_id,
                 'out-of-bounds observations=' || count(*) || '; latest_received_at=' || cast(max(received_at) AS varchar) AS details
          FROM er_market_observations
          WHERE (displayed_probability IS NOT NULL AND (displayed_probability<0 OR displayed_probability>1))
             OR (yes_bid IS NOT NULL AND (yes_bid<0 OR yes_bid>1))
             OR (yes_ask IS NOT NULL AND (yes_ask<0 OR yes_ask>1))
          GROUP BY source_contract_id
        """,
    )


def check_crossed_books(con) -> int:
    return _sync_rule(
        con, object_type="source_contract", rule_code="crossed_yes_book", severity="high",
        sql="""
          SELECT source_contract_id AS object_id,
                 'yes_bid exceeded yes_ask in ' || count(*) || ' observation(s)' AS details
          FROM er_market_observations
          WHERE yes_bid IS NOT NULL AND yes_ask IS NOT NULL AND yes_bid>yes_ask
          GROUP BY source_contract_id
        """,
    )


def check_missing_raw_lineage(con) -> int:
    return _sync_rule(
        con, object_type="source_contract", rule_code="observation_missing_raw_lineage", severity="critical",
        sql="""
          SELECT o.source_contract_id AS object_id,
                 'institutional observations missing immutable raw lineage=' || count(*) AS details
          FROM er_market_observations o
          LEFT JOIN er_raw_snapshots r ON r.raw_snapshot_id=o.raw_snapshot_id
          WHERE coalesce(o.methodology_version,'')<>'legacy_collector_v0.3'
            AND (o.raw_snapshot_id IS NULL OR r.raw_snapshot_id IS NULL)
          GROUP BY o.source_contract_id
        """,
    )


def check_archive_integrity(con) -> int:
    return _sync_rule(
        con, object_type="raw_snapshot", rule_code="raw_archive_integrity", severity="critical",
        sql="""
          SELECT r.raw_snapshot_id AS object_id,
                 'storage=' || coalesce(r.payload_storage,'') || '; archive_status=' || coalesce(a.status,'') ||
                 '; archive_key=' || coalesce(a.archive_key,'') || '; member=' || coalesce(a.archive_member,'') ||
                 '; raw_hash=' || coalesce(r.content_hash,'') || '; archive_hash=' || coalesce(a.content_hash,'') AS details
          FROM er_raw_snapshots r
          LEFT JOIN er_raw_archive_objects a USING(raw_snapshot_id)
          WHERE (r.payload_json IS NULL AND
                 (a.status IS DISTINCT FROM 'archived' OR a.archive_key IS NULL OR
                  a.archive_member IS NULL OR a.archive_bundle_hash IS NULL))
             OR (a.status='archived' AND
                 (a.archive_key IS NULL OR a.archive_member IS NULL OR a.archive_bundle_hash IS NULL OR
                  a.content_hash IS NULL OR a.content_hash<>r.content_hash))
        """,
    )


def check_archive_backlog(con, grace_minutes: int = 20) -> int:
    configured = all(os.getenv(k) for k in [
        "RAW_ARCHIVE_ENDPOINT", "RAW_ARCHIVE_BUCKET",
        "RAW_ARCHIVE_ACCESS_KEY_ID", "RAW_ARCHIVE_SECRET_ACCESS_KEY",
    ])
    if not configured:
        return 0
    return _sync_rule(
        con, object_type="source", rule_code="raw_archive_backlog", severity="high",
        sql="""
          SELECT source_id AS object_id,
                 count(*) || ' raw snapshots older than ' || cast(? AS varchar) ||
                 ' minutes are not verified in object storage; oldest=' || cast(min(received_at) AS varchar) AS details
          FROM er_unarchived_raw_snapshots
          WHERE received_at < now() - (? * INTERVAL '1 minute')
          GROUP BY source_id
        """, params=[grace_minutes, grace_minutes],
    )


def check_contract_versions(con) -> int:
    missing = _sync_rule(
        con, object_type="source_contract", rule_code="missing_contract_version", severity="high",
        sql="""
          SELECT c.source_contract_id AS object_id,'Source contract has no versioned metadata record' AS details
          FROM er_source_contracts c
          LEFT JOIN er_source_contract_versions v USING(source_contract_id)
          GROUP BY c.source_contract_id HAVING count(v.version_no)=0
        """,
    )
    invalid = _sync_rule(
        con, object_type="source_contract", rule_code="contract_version_window_invalid", severity="critical",
        sql="""
          SELECT source_contract_id AS object_id,
                 'Expected one open version and positive intervals; open_versions=' ||
                 sum(CASE WHEN valid_to IS NULL THEN 1 ELSE 0 END) || '; invalid_windows=' ||
                 sum(CASE WHEN valid_to IS NOT NULL AND valid_to<=valid_from THEN 1 ELSE 0 END) AS details
          FROM er_source_contract_versions
          GROUP BY source_contract_id
          HAVING sum(CASE WHEN valid_to IS NULL THEN 1 ELSE 0 END)<>1
              OR sum(CASE WHEN valid_to IS NOT NULL AND valid_to<=valid_from THEN 1 ELSE 0 END)>0
        """,
    )
    return missing + invalid


def check_contract_version_evidence(con) -> int:
    return _sync_rule(
        con, object_type="source_contract", rule_code="contract_version_missing_evidence", severity="high",
        sql="""
          SELECT v.source_contract_id AS object_id,
                 'version(s) with raw pointer but no explicit evidence=' || string_agg(cast(v.version_no AS varchar),',') AS details
          FROM er_source_contract_versions v
          LEFT JOIN er_contract_version_evidence e
            ON e.source_contract_id=v.source_contract_id AND e.version_no=v.version_no
          WHERE v.raw_snapshot_id IS NOT NULL
          GROUP BY v.source_contract_id
          HAVING count(e.raw_snapshot_id) FILTER (WHERE e.raw_snapshot_id IS NOT NULL) <>
                 count(DISTINCT v.version_no)
        """,
    )


def check_canonical_version_windows(con) -> int:
    total = 0
    for table, id_col, object_type in [
        ("er_canonical_event_versions", "canonical_event_id", "canonical_event"),
        ("er_event_family_versions", "event_family_id", "event_family"),
    ]:
        total += _sync_rule(
            con, object_type=object_type, rule_code="canonical_version_window_invalid", severity="high",
            sql=f"""
              SELECT {id_col} AS object_id,
                     'open_versions=' || sum(CASE WHEN valid_to IS NULL THEN 1 ELSE 0 END) ||
                     '; invalid_windows=' || sum(CASE WHEN valid_to IS NOT NULL AND valid_to<=valid_from THEN 1 ELSE 0 END) AS details
              FROM {table}
              GROUP BY {id_col}
              HAVING sum(CASE WHEN valid_to IS NULL THEN 1 ELSE 0 END)<>1
                  OR sum(CASE WHEN valid_to IS NOT NULL AND valid_to<=valid_from THEN 1 ELSE 0 END)>0
            """,
        )
    return total


def check_active_mapping_cardinality(con) -> int:
    return _sync_rule(
        con, object_type="source_contract", rule_code="active_mapping_cardinality", severity="critical",
        sql="""
          SELECT c.source_contract_id AS object_id,
                 'Expected exactly one active canonical mapping; found ' || count(m.mapping_id) AS details
          FROM er_source_contracts c
          LEFT JOIN er_event_mappings m
            ON m.source_contract_id=c.source_contract_id AND m.valid_to IS NULL
          GROUP BY c.source_contract_id HAVING count(m.mapping_id)<>1
        """,
    )


def check_source_freshness(con, stale_minutes: int) -> int:
    bad = _sync_rule(
        con, object_type="source", rule_code="source_stale", severity="high",
        sql="""
          SELECT s.source_id AS object_id,
                 'Last success=' || coalesce(cast(h.last_success_at AS varchar),'NULL') ||
                 '; last observation=' || coalesce(cast(h.last_observation_at AS varchar),'NULL') ||
                 '; last status=' || coalesce(h.last_run_status,'NULL') ||
                 '; age_minutes=' || coalesce(cast(date_diff('minute',h.last_success_at,now()) AS varchar),'NULL') AS details
          FROM er_sources s LEFT JOIN er_source_health h USING(source_id)
          WHERE s.active=TRUE
            AND (h.last_success_at IS NULL OR date_diff('minute',h.last_success_at,now())>?)
        """, params=[stale_minutes],
    )
    con.execute("""
        UPDATE er_source_health h SET
          health_status=CASE
            WHEN h.last_success_at IS NULL OR date_diff('minute',h.last_success_at,now())>? THEN 'stale'
            WHEN h.last_run_status='success' THEN 'healthy' ELSE 'degraded' END,
          updated_at=now()
        FROM er_sources s
        WHERE s.source_id=h.source_id AND s.active=TRUE
    """, [stale_minutes])
    return bad


def check_ingestion_volume_drop(con) -> int:
    return _sync_rule(
        con, object_type="source", rule_code="ingestion_volume_drop", severity="medium",
        sql="""
          WITH ranked AS (
            SELECT source_id,records_written,finished_at,
                   row_number() OVER(PARTITION BY source_id ORDER BY finished_at DESC,run_id DESC) rn
            FROM er_ingestion_runs WHERE status IN ('success','partial')
          ), paired AS (
            SELECT source_id,
                   max(records_written) FILTER (WHERE rn=1) latest,
                   max(records_written) FILTER (WHERE rn=2) previous
            FROM ranked WHERE rn<=2 GROUP BY source_id
          )
          SELECT source_id AS object_id,
                 'Latest run wrote ' || latest || ' records versus ' || previous || ' previously' AS details
          FROM paired WHERE previous IS NOT NULL AND previous>0 AND latest<greatest(5,previous*0.5)
        """,
    )


def check_observation_clocks(con, future_tolerance_minutes: int = 5) -> int:
    return _sync_rule(
        con, object_type="source_contract", rule_code="source_clock_ahead", severity="medium",
        sql="""
          SELECT source_contract_id AS object_id,
                 'observations ahead of receive clock=' || count(*) || '; tolerance_minutes=' || cast(? AS varchar) AS details
          FROM er_market_observations
          WHERE observed_at > received_at + (? * INTERVAL '1 minute')
          GROUP BY source_contract_id
        """, params=[future_tolerance_minutes, future_tolerance_minutes],
    )


def check_mapping_ambiguity(con) -> int:
    return _sync_rule(
        con, object_type="source_contract", rule_code="ambiguous_high_confidence_mapping", severity="medium",
        sql="""
          SELECT left_source_contract_id AS object_id,
                 'Contract has ' || count(*) || ' unresolved high-confidence same-event candidates' AS details
          FROM er_mapping_candidates
          WHERE status='pending' AND suggested_relationship='same_event' AND overall_confidence>=0.90
          GROUP BY left_source_contract_id HAVING count(*)>1
        """,
    )


def check_inferred_edge_metadata(con) -> int:
    return _sync_rule(
        con, object_type="graph_edge", rule_code="inferred_edge_missing_methodology", severity="critical",
        sql="""
          SELECT edge_id AS object_id,
                 'Inferred edge is missing confidence, inference method, or method version' AS details
          FROM er_graph_edges
          WHERE edge_class='inferred' AND (inference_method IS NULL OR method_version IS NULL OR confidence IS NULL)
        """,
    )


def check_active_orphan_events(con) -> int:
    return _sync_rule(
        con, object_type="canonical_event", rule_code="active_event_without_source_mapping", severity="high",
        sql="""
          SELECT e.canonical_event_id AS object_id,
                 'Active canonical event has no active source contract mapping: ' || e.canonical_name AS details
          FROM er_canonical_events e
          LEFT JOIN er_event_mappings m
            ON m.canonical_event_id=e.canonical_event_id AND m.valid_to IS NULL
          WHERE e.status='active'
          GROUP BY e.canonical_event_id,e.canonical_name HAVING count(m.mapping_id)=0
        """,
    )


def check_mapping_targets_distributable_events(con) -> int:
    return _sync_rule(
        con, object_type="source_contract", rule_code="active_mapping_targets_nonserved_event", severity="critical",
        sql="""
          SELECT m.source_contract_id AS object_id,
                 'Active mapping points to canonical_event=' || m.canonical_event_id || '; status=' || e.status AS details
          FROM er_event_mappings m JOIN er_canonical_events e USING(canonical_event_id)
          WHERE m.valid_to IS NULL AND e.status IN ('superseded','inactive','retired')
        """,
    )


def check_point_in_time_mapping_cardinality(con) -> int:
    return _sync_rule(
        con, object_type="source_contract", rule_code="point_in_time_mapping_cardinality", severity="critical",
        sql="""
          WITH bad AS (
            SELECT o.source_contract_id,o.observed_at,o.received_at,count(m.mapping_id) mapping_count
            FROM er_market_observations o
            LEFT JOIN er_event_mappings m
              ON m.source_contract_id=o.source_contract_id
             AND m.valid_from<=o.received_at
             AND (m.valid_to IS NULL OR m.valid_to>o.received_at)
            GROUP BY o.source_contract_id,o.observed_at,o.received_at
            HAVING count(m.mapping_id)<>1
          )
          SELECT source_contract_id AS object_id,
                 'observations with non-one mapping interval=' || count(*) ||
                 '; latest_bad_received_at=' || cast(max(received_at) AS varchar) AS details
          FROM bad GROUP BY source_contract_id
        """,
    )


def check_reviewed_relationship_provenance(con) -> int:
    return _sync_rule(
        con, object_type="graph_edge", rule_code="reviewed_relationship_missing_provenance", severity="critical",
        sql="""
          SELECT e.edge_id AS object_id,
                 'Reviewed relationship missing explicit evidence rows or methodology metadata' AS details
          FROM er_graph_edges e
          LEFT JOIN er_event_relationship_evidence r USING(edge_id)
          WHERE e.human_reviewed=TRUE
          GROUP BY e.edge_id,e.inference_method,e.method_version
          HAVING count(r.evidence_id)=0 OR e.method_version IS NULL
        """,
    )


def check_active_contract_semantic_coverage(con) -> int:
    return _sync_rule(
        con, object_type="source_contract", rule_code="active_contract_missing_full_semantic_version", severity="high",
        sql="""
          SELECT c.source_contract_id AS object_id,
                 'Active source contract has no prospective full semantic version' AS details
          FROM er_source_contracts c
          LEFT JOIN er_latest_contract_semantic_versions v USING(source_contract_id)
          WHERE c.active=TRUE AND v.source_contract_id IS NULL
        """,
    )


def check_reference_master_versions(con) -> int:
    total = 0
    for current, latest, id_col, object_type in [
        ("er_sources", "er_latest_source_versions", "source_id", "source"),
        ("er_entities", "er_latest_entity_versions", "entity_id", "entity"),
        ("er_instruments", "er_latest_instrument_versions", "instrument_id", "instrument"),
    ]:
        total += _sync_rule(
            con, object_type=object_type, rule_code="reference_master_missing_version", severity="high",
            sql=f"""
              SELECT c.{id_col} AS object_id,
                     'Current {object_type} master row has no point-in-time version record' AS details
              FROM {current} c LEFT JOIN {latest} v USING({id_col}) WHERE v.{id_col} IS NULL
            """,
        )
    return total


def check_ready_release_manifests(con) -> int:
    return _sync_rule(
        con, object_type="data_release", rule_code="ready_release_manifest_incomplete", severity="critical",
        sql="""
          SELECT r.release_id AS object_id,
                 'dataset_count=' || coalesce(cast(r.dataset_count AS varchar),'NULL') ||
                 '; manifest_rows=' || count(m.dataset_name) ||
                 '; manifest_hash=' || coalesce(r.manifest_hash,'NULL') ||
                 '; manifest_uri=' || coalesce(r.manifest_object_uri,'NULL') AS details
          FROM er_data_releases r LEFT JOIN er_export_manifests m USING(release_id)
          WHERE r.status='ready'
          GROUP BY r.release_id,r.dataset_count,r.manifest_hash,r.manifest_object_uri
          HAVING r.manifest_hash IS NULL OR r.manifest_object_uri IS NULL
              OR r.dataset_count IS NULL OR r.dataset_count<>count(m.dataset_name)
        """,
    )


def check_migration_ledger(con) -> int:
    count = int(con.execute("SELECT count(*) FROM er_schema_migrations").fetchone()[0])
    expected = len(MIGRATION_FILES) + len(EXTRA_MIGRATION_FILES)
    rows = [{"object_id": "institutional_schema", "details": f"Applied migration count={count}; expected={expected}"}] if count != expected else []
    if rows:
        con.register("_qa_migration_ledger_arrow", pa.Table.from_pylist(rows))
        try:
            con.execute("CREATE OR REPLACE TEMP TABLE _qa_migration_ledger_source AS SELECT * FROM _qa_migration_ledger_arrow")
        finally:
            con.unregister("_qa_migration_ledger_arrow")
        return _sync_rule(
            con, object_type="dataset", rule_code="migration_ledger_incomplete", severity="critical",
            sql="SELECT object_id,details FROM _qa_migration_ledger_source",
        )
    return _sync_rule(
        con, object_type="dataset", rule_code="migration_ledger_incomplete", severity="critical",
        sql="SELECT 'never' AS object_id,'never' AS details WHERE FALSE",
    )


def check_latest_full_universe_runs(con) -> int:
    return _sync_rule(
        con, object_type="source", rule_code="latest_run_not_complete_full_universe", severity="high",
        sql="""
          WITH ranked AS (
            SELECT r.*,row_number() OVER(PARTITION BY source_id ORDER BY started_at DESC,run_id DESC) rn
            FROM er_ingestion_runs r
          )
          SELECT s.source_id AS object_id,
                 'run_id=' || coalesce(r.run_id,'NULL') || '; status=' || coalesce(r.status,'NULL') ||
                 '; scope=' || coalesce(r.collection_scope,'NULL') ||
                 '; universe_complete=' || coalesce(cast(r.universe_complete AS varchar),'NULL') AS details
          FROM er_sources s LEFT JOIN ranked r ON r.source_id=s.source_id AND r.rn=1
          WHERE s.active=TRUE
            AND (r.run_id IS NULL OR r.status<>'success' OR coalesce(r.collection_scope,'')<>'full'
                 OR r.universe_complete IS DISTINCT FROM TRUE)
        """,
    )


def check_presence_coherence(con) -> int:
    threshold = max(2, int(os.getenv("PRESENCE_MISS_THRESHOLD", "3")))
    return _sync_rule(
        con, object_type="source_contract", rule_code="presence_state_incoherent", severity="critical",
        sql="""
          SELECT c.source_contract_id AS object_id,
                 'active=' || cast(c.active AS varchar) || '; listing_state=' || p.listing_state ||
                 '; consecutive_misses=' || p.consecutive_misses || '; deactivated_at=' ||
                 coalesce(cast(p.deactivated_at AS varchar),'NULL') || '; miss_threshold=' || cast(? AS varchar) AS details
          FROM er_source_contracts c JOIN er_contract_presence_state p USING(source_contract_id)
          WHERE (p.listing_state='missing_confirmed' AND c.active=TRUE)
             OR (p.listing_state='missing_confirmed' AND p.consecutive_misses<?)
             OR (p.listing_state='observed' AND p.consecutive_misses<>0)
             OR (p.listing_state='observed' AND p.deactivated_at IS NOT NULL)
        """, params=[threshold, threshold],
    )


def check_raw_durability(con) -> int:
    return _sync_rule(
        con, object_type="raw_snapshot", rule_code="raw_payload_not_durable", severity="critical",
        sql="""
          SELECT r.raw_snapshot_id AS object_id,
                 'payload_storage=' || coalesce(r.payload_storage,'') || '; archive_status=' || coalesce(a.status,'') ||
                 '; archive_key=' || coalesce(a.archive_key,'') || '; archive_member=' || coalesce(a.archive_member,'') AS details
          FROM er_raw_snapshots r LEFT JOIN er_raw_archive_objects a USING(raw_snapshot_id)
          WHERE r.payload_json IS NULL
            AND NOT (a.status='archived' AND a.archive_key IS NOT NULL AND a.archive_member IS NOT NULL
                     AND a.archive_bundle_hash IS NOT NULL AND a.content_hash=r.content_hash)
        """, resolution_note="Raw evidence is durable again",
    )


def check_stuck_ingestion_runs(con, stale_minutes: int) -> int:
    threshold = max(30, stale_minutes * 2)
    return _sync_rule(
        con, object_type="ingestion_run", rule_code="ingestion_run_stuck", severity="high",
        sql="""
          SELECT run_id AS object_id,
                 'source=' || source_id || '; started_at=' || cast(started_at AS varchar) ||
                 '; age_minutes=' || date_diff('minute',started_at,now()) || '; threshold=' || cast(? AS varchar) AS details
          FROM er_ingestion_runs
          WHERE status='running' AND started_at < now() - (? * INTERVAL '1 minute')
        """, params=[threshold, threshold],
    )


def run_qa(stale_minutes: int = 20) -> dict:
    con = connect()
    try:
        con.execute("BEGIN TRANSACTION")
        try:
            checks = {
                "probability_bounds": check_probability_bounds(con),
                "crossed_books": check_crossed_books(con),
                "missing_raw_lineage": check_missing_raw_lineage(con),
                "archive_integrity": check_archive_integrity(con),
                "archive_backlog": check_archive_backlog(con),
                "contract_versions": check_contract_versions(con),
                "contract_version_evidence": check_contract_version_evidence(con),
                "canonical_version_windows": check_canonical_version_windows(con),
                "mapping_cardinality": check_active_mapping_cardinality(con),
                "source_freshness": check_source_freshness(con, stale_minutes),
                "ingestion_volume_drop": check_ingestion_volume_drop(con),
                "observation_clocks": check_observation_clocks(con),
                "mapping_ambiguity": check_mapping_ambiguity(con),
                "inferred_edge_metadata": check_inferred_edge_metadata(con),
                "active_orphan_events": check_active_orphan_events(con),
                "mapping_targets": check_mapping_targets_distributable_events(con),
                "point_in_time_mapping_cardinality": check_point_in_time_mapping_cardinality(con),
                "reviewed_relationship_provenance": check_reviewed_relationship_provenance(con),
                "active_contract_semantic_coverage": check_active_contract_semantic_coverage(con),
                "reference_master_versions": check_reference_master_versions(con),
                "ready_release_manifests": check_ready_release_manifests(con),
                "migration_ledger": check_migration_ledger(con),
                "latest_full_universe_runs": check_latest_full_universe_runs(con),
                "presence_coherence": check_presence_coherence(con),
                "raw_durability": check_raw_durability(con),
                "stuck_ingestion_runs": check_stuck_ingestion_runs(con, stale_minutes),
            }
            open_count = int(con.execute("SELECT count(*) FROM er_quality_issues WHERE status='open'").fetchone()[0])
            critical_count = int(con.execute(
                "SELECT count(*) FROM er_quality_issues WHERE status='open' AND severity='critical'"
            ).fetchone()[0])
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        return {
            "checks": checks,
            "open_issues": open_count,
            "critical_issues": critical_count,
            "qa_version": QA_VERSION,
        }
    finally:
        con.close()


if __name__ == "__main__":
    print(run_qa())
