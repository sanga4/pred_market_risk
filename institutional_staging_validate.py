"""Read-only reconciliation for the institutional staging production gate.

This validator compares the legacy EventRisk source database with the isolated
institutional staging database. It never mutates either database.

Environment:
  DB=md:event_risk_institutional
  LEGACY_SOURCE_DB=event_risk

Run after applying all institutional migrations and executing the legacy backfill
against staging. A non-zero exit status means the migration gate is still open.
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
    row = con.execute(sql).fetchone()
    return int(row[0] or 0)


def _check(name: str, passed: bool, observed: Any, expected: Any, detail: str) -> dict[str, Any]:
    return {
        "name": name,
        "passed": bool(passed),
        "observed": observed,
        "expected": expected,
        "detail": detail,
    }


def validate(*, source_db: str | None = None) -> dict[str, Any]:
    """Validate schema coverage and legacy reconciliation without writing data."""
    con = connect(read_only=True)
    try:
        migration_files = sorted(Path(__file__).resolve().parent.glob("institutional_schema*.sql"))
        expected_migrations = len(migration_files)
        applied_migrations = _scalar(con, "SELECT count(*) FROM er_schema_migrations")

        ticks = _qualified(source_db, "ticks")
        canonical = _qualified(source_db, "canonical_events")
        event_markets = _qualified(source_db, "event_markets")

        eligible_ticks = _scalar(
            con,
            f"""SELECT count(*) FROM {ticks}
                WHERE venue IN ('kalshi','polymarket')
                  AND market_id IS NOT NULL AND ts IS NOT NULL""",
        )
        legacy_observations = _scalar(
            con,
            """SELECT count(*) FROM er_market_observations
                WHERE methodology_version='legacy_collector_v0.3'""",
        )

        eligible_contracts = _scalar(
            con,
            f"""SELECT count(*) FROM (
                  SELECT venue, market_id FROM {ticks}
                  WHERE venue IN ('kalshi','polymarket') AND market_id IS NOT NULL
                  GROUP BY venue, market_id
                ) q""",
        )
        legacy_contracts = _scalar(
            con,
            """SELECT count(*) FROM er_source_contracts
                WHERE source_id IN ('kalshi','polymarket')
                  AND source_contract_id IN (
                    SELECT DISTINCT source_contract_id
                    FROM er_market_observations
                    WHERE methodology_version='legacy_collector_v0.3'
                  )""",
        )

        eligible_events = _scalar(con, f"SELECT count(*) FROM {canonical}")
        migrated_events = _scalar(
            con,
            "SELECT count(*) FROM er_canonical_events WHERE taxonomy_version='legacy_rules_v1'",
        )

        eligible_mappings = _scalar(
            con,
            f"""SELECT count(*) FROM {event_markets} em
                JOIN {ticks} t ON t.venue=em.venue AND t.market_id=em.market_id
                WHERE t.venue IN ('kalshi','polymarket')
                GROUP BY ALL""".replace("SELECT count(*)", "SELECT count(*) FROM (SELECT DISTINCT em.venue, em.market_id, em.canonical_event_id")
                + ") q",
        )
        migrated_mappings = _scalar(
            con,
            """SELECT count(*) FROM er_event_mappings
                WHERE mapping_type='legacy_existing_mapping'
                  AND review_status='legacy_unreviewed'""",
        )

        disclosure = _scalar(
            con,
            """SELECT count(*) FROM er_quality_issues
                WHERE quality_issue_id='quality:legacy_raw_payload_unavailable'""",
        )

        checks = [
            _check(
                "all_schema_migrations_applied",
                applied_migrations == expected_migrations,
                applied_migrations,
                expected_migrations,
                "Every institutional_schema*.sql file is recorded in er_schema_migrations.",
            ),
            _check(
                "legacy_observation_count_reconciles",
                legacy_observations == eligible_ticks,
                legacy_observations,
                eligible_ticks,
                "Every eligible legacy tick is represented as a legacy institutional observation.",
            ),
            _check(
                "legacy_contract_count_reconciles",
                legacy_contracts == eligible_contracts,
                legacy_contracts,
                eligible_contracts,
                "Every distinct eligible legacy venue/market pair is represented in the migrated legacy observation set.",
            ),
            _check(
                "legacy_event_count_reconciles",
                migrated_events == eligible_events,
                migrated_events,
                eligible_events,
                "Legacy canonical events are preserved one-for-one.",
            ),
            _check(
                "legacy_mapping_count_reconciles",
                migrated_mappings == eligible_mappings,
                migrated_mappings,
                eligible_mappings,
                "Distinct legacy event-market mappings are preserved and explicitly marked unreviewed.",
            ),
            _check(
                "legacy_raw_payload_limitation_disclosed",
                disclosure == 1,
                disclosure,
                1,
                "The pre-institutional raw-payload limitation is explicitly present in the QA ledger.",
            ),
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
