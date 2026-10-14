"""Backfill the institutional data layer from the existing EventRisk tables.

This migration intentionally does NOT fabricate raw payloads that the legacy collector
never stored. Historical observations are labeled `legacy_collector_v0.3` and their
received/processed timestamps are approximated with the legacy tick timestamp.
"""
from db import connect


SOURCES = [
    ("kalshi", "Kalshi", "prediction_market"),
    ("polymarket", "Polymarket", "prediction_market"),
]


def main():
    con = connect()
    try:
        for source_id, source_name, source_type in SOURCES:
            con.execute(
                """
                INSERT INTO er_sources(source_id, source_name, source_type, redistribution_status)
                VALUES (?, ?, ?, 'unknown')
                ON CONFLICT(source_id) DO UPDATE SET
                  source_name = excluded.source_name,
                  source_type = excluded.source_type,
                  updated_at = now()
                """,
                [source_id, source_name, source_type],
            )

        # One source-contract row per legacy market, using the most recent stored title/deadline.
        con.execute(
            """
            INSERT INTO er_source_contracts(
              source_contract_id, source_id, source_market_id, source_event_id,
              current_title, closes_at, first_seen_at, last_seen_at, active
            )
            SELECT
              'src:' || venue || ':' || market_id,
              venue,
              market_id,
              arg_max(event_id, ts),
              coalesce(arg_max(market_title, ts), arg_max(event_title, ts), market_id),
              arg_max(close_time, ts),
              min(ts),
              max(ts),
              coalesce(arg_max(close_time, ts), now() + INTERVAL '1 day') > now()
            FROM ticks
            WHERE venue IN ('kalshi', 'polymarket')
            GROUP BY venue, market_id
            ON CONFLICT(source_id, source_market_id) DO UPDATE SET
              source_event_id = excluded.source_event_id,
              current_title = excluded.current_title,
              closes_at = excluded.closes_at,
              last_seen_at = excluded.last_seen_at,
              active = excluded.active
            """
        )

        # Preserve the existing canonicalization as a legacy layer, without claiming it
        # satisfies the new ontology or review requirements.
        con.execute(
            """
            INSERT INTO er_event_families(
              event_family_id, canonical_name, category, description
            )
            SELECT DISTINCT
              'legacy_family:' || coalesce(theme, 'other'),
              'Legacy ' || coalesce(theme, 'other'),
              category,
              'Imported from the pre-institutional EventRisk theme taxonomy.'
            FROM canonical_events
            ON CONFLICT(event_family_id) DO NOTHING
            """
        )

        con.execute(
            """
            INSERT INTO er_canonical_events(
              canonical_event_id, event_family_id, canonical_name,
              canonical_definition, event_class, geography_entity_id,
              status, taxonomy_version, updated_at
            )
            SELECT
              canonical_event_id,
              'legacy_family:' || coalesce(theme, 'other'),
              title,
              title,
              category,
              NULL,
              CASE WHEN eligible THEN 'active' ELSE 'inactive' END,
              'legacy_rules_v1',
              updated_at
            FROM canonical_events
            ON CONFLICT(canonical_event_id) DO UPDATE SET
              canonical_name = excluded.canonical_name,
              canonical_definition = excluded.canonical_definition,
              status = excluded.status,
              updated_at = excluded.updated_at
            """
        )

        con.execute(
            """
            INSERT INTO er_event_mappings(
              mapping_id, source_contract_id, canonical_event_id,
              mapping_type, confidence, mapping_method, method_version,
              review_status, review_notes, created_by
            )
            SELECT
              'legacy-map:' || em.venue || ':' || em.market_id || ':' || em.canonical_event_id,
              'src:' || em.venue || ':' || em.market_id,
              em.canonical_event_id,
              'legacy_existing_mapping',
              NULL,
              'legacy_migration',
              'rules_v1',
              'legacy_unreviewed',
              'Imported from the existing event_markets table; requires review under institutional mapping policy.',
              'migration'
            FROM event_markets em
            JOIN er_source_contracts c
              ON c.source_contract_id = 'src:' || em.venue || ':' || em.market_id
            ON CONFLICT(mapping_id) DO NOTHING
            """
        )

        # Backfill the historical numerical observations we really have. Do not infer
        # order-book depth, open interest, liquidity, or missing raw payload fields.
        con.execute(
            """
            INSERT INTO er_market_observations(
              source_id, source_contract_id, observed_at, received_at, processed_at,
              yes_bid, yes_ask, midpoint, last_trade, displayed_probability,
              volume, volume_24h, spread, methodology_version
            )
            SELECT
              venue,
              'src:' || venue || ':' || market_id,
              ts,
              ts,
              ts,
              yes_bid,
              yes_ask,
              CASE
                WHEN yes_bid IS NOT NULL AND yes_ask IS NOT NULL
                  AND yes_ask >= yes_bid AND yes_ask - yes_bid <= 0.10
                THEN (yes_bid + yes_ask) / 2.0
                ELSE NULL
              END,
              last,
              prob,
              volume,
              volume_24h,
              CASE
                WHEN yes_bid IS NOT NULL AND yes_ask IS NOT NULL
                THEN yes_ask - yes_bid
                ELSE NULL
              END,
              'legacy_collector_v0.3'
            FROM ticks
            WHERE venue IN ('kalshi', 'polymarket')
              AND market_id IS NOT NULL
              AND ts IS NOT NULL
            ON CONFLICT(source_contract_id, observed_at, received_at) DO NOTHING
            """
        )

        con.execute(
            """
            INSERT INTO er_quality_issues(
              quality_issue_id, object_type, object_id, rule_code,
              severity, status, details
            )
            VALUES (
              'quality:legacy_raw_payload_unavailable',
              'dataset',
              'legacy_ticks',
              'legacy_raw_payload_unavailable',
              'info',
              'open',
              'Historical ticks predate the institutional raw ledger. Numerical market observations are preserved, but original full source payloads and exact receive/process times cannot be reconstructed.'
            )
            ON CONFLICT(quality_issue_id) DO NOTHING
            """
        )

        print("institutional legacy backfill complete")
    finally:
        con.close()


if __name__ == "__main__":
    main()
