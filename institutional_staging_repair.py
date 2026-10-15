"""One-shot reconciliation for a staging database that already has live institutional data.

This exists for the migration case where the legacy retail collector continued running after
institutional ingestion began. It is deliberately guarded against the production retail database.

Usage:
    STAGING_DB=event_risk_institutional LEGACY_SOURCE_DB=event_risk \
      python institutional_staging_repair.py

The repair:
- discovers each source's institutional cutover from the first raw snapshot;
- rebinds imported observations from legacy placeholder IDs to existing institutional contracts;
- removes only legacy-labelled observations at/after the institutional cutover;
- retains imported mappings strictly as historical intervals ending at cutover;
- marks legacy canonical records as historical/currently inactive;
- deactivates legacy-only contracts that have no institutional current-state pointer.
"""
from __future__ import annotations

import os

import duckdb


STAGING_DB = os.getenv("STAGING_DB", "event_risk_institutional")
LEGACY_SOURCE_DB = os.getenv("LEGACY_SOURCE_DB", "event_risk")


def _safe_name(value: str) -> str:
    if not value.replace("_", "").isalnum():
        raise ValueError(f"unsafe database alias: {value!r}")
    return value


def main() -> None:
    staging = _safe_name(STAGING_DB)
    source = _safe_name(LEGACY_SOURCE_DB)
    if staging == source or staging == "event_risk":
        raise RuntimeError("refusing to run staging repair against the retail source/production database")

    con = duckdb.connect("md:")
    con.execute("SET TimeZone='UTC'")

    cutoffs = dict(con.execute(f"""
        SELECT source_id,min(received_at)
        FROM {staging}.main.er_raw_snapshots
        WHERE source_id IN ('kalshi','polymarket')
        GROUP BY source_id
    """).fetchall())
    missing = {"kalshi", "polymarket"} - set(cutoffs)
    if missing:
        raise RuntimeError(f"cannot establish institutional cutover for sources: {sorted(missing)}")

    # Rebind only imported legacy rows. Placeholder IDs use src:<source>:<source_market_id>.
    con.execute(f"""
        UPDATE {staging}.main.er_market_observations o
        SET source_contract_id=c.source_contract_id
        FROM {staging}.main.er_source_contracts c
        WHERE o.methodology_version='legacy_collector_v0.3'
          AND c.source_id=o.source_id
          AND c.source_market_id=substr(o.source_contract_id, length('src:' || o.source_id || ':') + 1)
          AND NOT EXISTS (
            SELECT 1 FROM {staging}.main.er_source_contracts direct
            WHERE direct.source_contract_id=o.source_contract_id
          )
    """)

    # Remove only the imported copy after the institutional source became authoritative.
    for source_id, cutoff in sorted(cutoffs.items()):
        con.execute(f"""
            DELETE FROM {staging}.main.er_market_observations
            WHERE methodology_version='legacy_collector_v0.3'
              AND source_id=? AND observed_at>=?
        """, [source_id, cutoff])

    # Mapping history is retained only for contracts that actually existed before cutover.
    con.execute(f"""
        DELETE FROM {staging}.main.er_event_mappings lm
        USING {staging}.main.er_source_contracts c
        WHERE lm.source_contract_id=c.source_contract_id
          AND lm.mapping_type='legacy_existing_mapping'
          AND NOT EXISTS (
            SELECT 1 FROM {source}.main.ticks t
            WHERE t.venue=c.source_id AND t.market_id=c.source_market_id
              AND t.ts IS NOT NULL
              AND t.ts < CASE c.source_id
                WHEN 'kalshi' THEN ?
                WHEN 'polymarket' THEN ?
                ELSE TIMESTAMPTZ '1970-01-01'
              END
          )
    """, [cutoffs["kalshi"], cutoffs["polymarket"]])

    con.execute(f"""
        UPDATE {staging}.main.er_event_mappings lm
        SET valid_from=x.first_ts,valid_to=x.cutoff
        FROM (
          SELECT c.source_contract_id,min(t.ts) first_ts,
                 CASE c.source_id WHEN 'kalshi' THEN ? WHEN 'polymarket' THEN ? END cutoff
          FROM {staging}.main.er_source_contracts c
          JOIN {source}.main.ticks t
            ON t.venue=c.source_id AND t.market_id=c.source_market_id
          WHERE c.source_id IN ('kalshi','polymarket') AND t.ts IS NOT NULL
            AND t.ts < CASE c.source_id WHEN 'kalshi' THEN ? WHEN 'polymarket' THEN ? END
          GROUP BY c.source_contract_id,c.source_id
        ) x
        WHERE lm.source_contract_id=x.source_contract_id
          AND lm.mapping_type='legacy_existing_mapping'
    """, [cutoffs["kalshi"], cutoffs["polymarket"], cutoffs["kalshi"], cutoffs["polymarket"]])

    con.execute(f"""
        UPDATE {staging}.main.er_canonical_events
        SET status='inactive'
        WHERE taxonomy_version='legacy_rules_v1'
    """)

    con.execute(f"""
        UPDATE {staging}.main.er_source_contracts c
        SET active=FALSE
        WHERE c.active=TRUE AND c.source_id IN ('kalshi','polymarket')
          AND NOT EXISTS (
            SELECT 1 FROM {staging}.main.er_market_current_state s
            WHERE s.source_contract_id=c.source_contract_id
          )
    """)

    result = con.execute(f"""
        SELECT
          (SELECT count(*) FROM {staging}.main.er_market_observations
           WHERE methodology_version='legacy_collector_v0.3') legacy_observations,
          (SELECT count(*) FROM {staging}.main.er_market_observations o
           LEFT JOIN {staging}.main.er_source_contracts c USING(source_contract_id)
           WHERE o.methodology_version='legacy_collector_v0.3' AND c.source_contract_id IS NULL) orphan_legacy_observations,
          (SELECT count(*) FROM {staging}.main.er_event_mappings
           WHERE mapping_type='legacy_existing_mapping' AND valid_to IS NULL) current_legacy_mappings
    """).fetchone()
    print({"legacy_observations": result[0], "orphan_legacy_observations": result[1], "current_legacy_mappings": result[2], "cutoffs": cutoffs})


if __name__ == "__main__":
    main()
