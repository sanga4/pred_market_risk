"""Read-only reconciliation for the institutional staging production gate.

The validator treats each source's first institutional raw snapshot as the cutover.
Only retail observations strictly before that cutover are legacy. This matters when
the old retail collector continues running after institutional ingestion begins.

Environment:
  DB=md:event_risk_institutional
  LEGACY_SOURCE_DB=event_risk
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

from db import connect

_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _qualified(source_db: str | None, table: str) -> str:
    if not source_db:
        return table
    if not _NAME.fullmatch(source_db):
        raise ValueError("LEGACY_SOURCE_DB must be a simple MotherDuck database alias")
    return f'"{source_db}".main."{table}"'


def _scalar(con, sql: str) -> int:
    return int((con.execute(sql).fetchone() or [0])[0] or 0)


def _check(name: str, passed: bool, observed: Any, expected: Any, detail: str) -> dict[str, Any]:
    return {"name": name, "passed": bool(passed), "observed": observed, "expected": expected, "detail": detail}


def validate(*, source_db: str | None = None) -> dict[str, Any]:
    con = connect(read_only=True)
    try:
        migration_files = sorted(Path(__file__).resolve().parent.glob("institutional_schema*.sql"))
        expected_migrations = len(migration_files)
        applied_migrations = _scalar(con, "SELECT count(*) FROM er_schema_migrations")

        ticks = _qualified(source_db, "ticks")
        canonical = _qualified(source_db, "canonical_events")
        event_markets = _qualified(source_db, "event_markets")

        cutoff_cte = """
          WITH cutoffs AS (
            SELECT source_id,min(received_at) cutoff
            FROM er_raw_snapshots
            WHERE source_id IN ('kalshi','polymarket')
            GROUP BY source_id
          )
        """

        eligible_ticks = _scalar(con, cutoff_cte + f"""
          SELECT count(*)
          FROM {ticks} t LEFT JOIN cutoffs c ON c.source_id=t.venue
          WHERE t.venue IN ('kalshi','polymarket')
            AND t.market_id IS NOT NULL AND t.ts IS NOT NULL
            AND (c.cutoff IS NULL OR t.ts<c.cutoff)
        """)
        legacy_observations = _scalar(con, """
          SELECT count(*) FROM er_market_observations
          WHERE methodology_version='legacy_collector_v0.3'
        """)

        eligible_contracts = _scalar(con, cutoff_cte + f"""
          SELECT count(*) FROM (
            SELECT t.venue,t.market_id
            FROM {ticks} t LEFT JOIN cutoffs c ON c.source_id=t.venue
            WHERE t.venue IN ('kalshi','polymarket') AND t.market_id IS NOT NULL AND t.ts IS NOT NULL
              AND (c.cutoff IS NULL OR t.ts<c.cutoff)
            GROUP BY t.venue,t.market_id
          ) q
        """)
        legacy_contracts = _scalar(con, """
          SELECT count(DISTINCT source_contract_id)
          FROM er_market_observations
          WHERE methodology_version='legacy_collector_v0.3'
        """)

        eligible_events = _scalar(con, f"SELECT count(*) FROM {canonical}")
        migrated_events = _scalar(con, "SELECT count(*) FROM er_canonical_events WHERE taxonomy_version='legacy_rules_v1'")

        eligible_mappings = _scalar(con, cutoff_cte + f"""
          SELECT count(*) FROM (
            SELECT DISTINCT em.venue,em.market_id,em.canonical_event_id
            FROM {event_markets} em
            LEFT JOIN cutoffs c ON c.source_id=em.venue
            WHERE em.venue IN ('kalshi','polymarket')
              AND EXISTS (
                SELECT 1 FROM {ticks} t
                WHERE t.venue=em.venue AND t.market_id=em.market_id AND t.ts IS NOT NULL
                  AND (c.cutoff IS NULL OR t.ts<c.cutoff)
              )
          ) q
        """)
        migrated_mappings = _scalar(con, """
          SELECT count(*) FROM er_event_mappings
          WHERE mapping_type='legacy_existing_mapping' AND review_status='legacy_unreviewed'
        """)

        disclosure = _scalar(con, """
          SELECT count(*) FROM er_quality_issues
          WHERE quality_issue_id='quality:legacy_raw_payload_unavailable'
        """)
        overlap = _scalar(con, cutoff_cte + """
          SELECT count(*)
          FROM er_market_observations o JOIN cutoffs c ON c.source_id=o.source_id
          WHERE o.methodology_version='legacy_collector_v0.3' AND o.observed_at>=c.cutoff
        """)
        orphan_observations = _scalar(con, """
          SELECT count(*)
          FROM er_market_observations o LEFT JOIN er_source_contracts c USING(source_contract_id)
          WHERE o.methodology_version='legacy_collector_v0.3' AND c.source_contract_id IS NULL
        """)
        current_legacy_mappings = _scalar(con, """
          SELECT count(*) FROM er_event_mappings
          WHERE mapping_type='legacy_existing_mapping' AND valid_to IS NULL
        """)

        checks = [
            _check("all_schema_migrations_applied", applied_migrations == expected_migrations, applied_migrations, expected_migrations, "Every institutional_schema*.sql file is recorded."),
            _check("legacy_observation_count_reconciles", legacy_observations == eligible_ticks, legacy_observations, eligible_ticks, "Only pre-cutover retail ticks are retained as legacy observations."),
            _check("legacy_contract_count_reconciles", legacy_contracts == eligible_contracts, legacy_contracts, eligible_contracts, "Every distinct pre-cutover venue/market pair is represented."),
            _check("legacy_event_count_reconciles", migrated_events == eligible_events, migrated_events, eligible_events, "Legacy canonical events are preserved one-for-one."),
            _check("legacy_mapping_count_reconciles", migrated_mappings == eligible_mappings, migrated_mappings, eligible_mappings, "Only mappings backed by pre-cutover observations are retained."),
            _check("no_post_cutover_legacy_overlap", overlap == 0, overlap, 0, "No institutional-era retail observations remain labeled as legacy."),
            _check("legacy_observation_contracts_resolve", orphan_observations == 0, orphan_observations, 0, "Every retained legacy observation resolves to a stable source contract identity."),
            _check("legacy_mappings_are_historical", current_legacy_mappings == 0, current_legacy_mappings, 0, "Imported legacy mappings never compete with current institutional identity."),
            _check("legacy_raw_payload_limitation_disclosed", disclosure == 1, disclosure, 1, "The pre-institutional raw-payload limitation is explicitly present in the QA ledger."),
        ]
        return {
            "ready": all(c["passed"] for c in checks),
            "source_db": source_db or "current database",
            "checks": checks,
            "failed_checks": [c["name"] for c in checks if not c["passed"]],
        }
    finally:
        con.close()


def main() -> int:
    result = validate(source_db=os.getenv("LEGACY_SOURCE_DB") or None)
    print(json.dumps(result, indent=2, default=str))
    return 0 if result["ready"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
