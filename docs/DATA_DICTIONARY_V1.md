# EventRisk Institutional Data Dictionary — v1

## Product boundary

EventRisk Institutional v1 supplies observed and normalized event data. It does **not**
prescribe security impacts, portfolio P&L, hedges, trades, or investment decisions.

The product is deliberately separated into:

1. **Observed layer** — source contracts, raw payloads, market observations and source metadata.
2. **Normalized layer** — canonical events/families, cross-source mappings, taxonomy, entities,
   relationships, term structures and provenance.
3. **Optional downstream analytics** — not part of this v1 institutional data contract.

## Time semantics

EventRisk stores multiple clocks because collapsing them creates look-ahead bias.

| Field | Meaning |
|---|---|
| `source_observed_at` | Timestamp asserted by the upstream source, when provided. |
| `observed_at` | Market observation time used for the source observation. |
| `received_at` | When EventRisk received/persisted the observation. Uses microsecond precision for institutional-era data. |
| `processed_at` | When EventRisk completed normalization of the observation. |
| `valid_from` / `valid_to` | Interval during which EventRisk considered normalized metadata or a mapping valid. |
| `created_at` | When the EventRisk record/version was created. |

For backtests that must reproduce what EventRisk knew at a historical moment, use the
point-in-time endpoints (`/as-known`, `/history-point-in-time`) and `received_at`/validity
intervals. The ordinary `/history` endpoint applies today's active mapping to historical
market observations and is therefore a **restated** history.

## `er_sources`

One record per upstream source.

Important fields:
- `source_id`: stable EventRisk source identifier, e.g. `kalshi`, `polymarket`.
- `source_type`: source category.
- `redistribution_status`: rights status tracked separately from technical availability.
- `attribution_required`: whether downstream attribution is required under source terms.
- `active`: whether EventRisk currently attempts ingestion.

## `er_ingestion_runs`

Audit log for every source collection attempt.

Important fields:
- `run_id`: immutable ingestion run identifier.
- `source_id`: upstream source.
- `started_at`, `finished_at`.
- `status`: `running`, `success`, `partial`, or `failed`.
- `records_received`, `records_written`, `error_count`.
- `details`: structured/diagnostic run metadata.

## `er_raw_snapshots`

Append-only raw-source ledger. Institutional-era normalized observations should always be
traceable to one of these records.

Important fields:
- `raw_snapshot_id`: immutable EventRisk raw-object identifier.
- `source_object_type`: e.g. `event` or `market`.
- `source_object_id`: upstream object identifier.
- `source_observed_at`: upstream timestamp when available.
- `received_at`: EventRisk receipt timestamp.
- `content_hash`: SHA-256 of canonicalized raw JSON.
- `payload_json`: exact JSON payload retained in the analytical lineage store.
- `ingestion_run_id`: collection attempt that produced the record.

Raw payloads are also independently copied to object storage and tracked by
`er_raw_archive_objects`.

## `er_source_contracts`

Current normalized view of each source-native market/contract. A source contract is not a
canonical EventRisk event.

Stable identity is `(source_id, source_market_id)` and represented by
`source_contract_id`.

Important fields:
- `current_title`, `current_description`.
- `current_resolution_rules`, `resolution_authority`.
- `opens_at`, `closes_at`, `resolves_at`.
- `first_seen_at`, `last_seen_at`.
- `active`.

## `er_source_contract_versions`

Bitemporal-style metadata history for source contracts. A new version is created only when
normalized source-contract metadata changes; repeated price pulls do not create false
metadata versions.

`er_contract_version_evidence` links each normalized version to the exact raw source event
and raw source market payloads that supported it.

## `er_market_observations`

Append-only point-in-time market observations.

Important fields:
- `source_id`, `source_contract_id`.
- `observed_at`, `received_at`, `processed_at`.
- `yes_bid`, `yes_ask`, `no_bid`, `no_ask`.
- `midpoint`, `last_trade`, `displayed_probability`.
- `volume`, `volume_24h`, `open_interest`, `liquidity`, `orderbook_depth`, `spread`.
- `market_status`.
- `raw_snapshot_id`: raw market payload used for the observation.
- `methodology_version`: identifies source-native/legacy treatment.

`displayed_probability` is a descriptive source/normalization field; institutions can use
bid/ask/last instead and construct their own probability convention.

## Legacy observations

Historical observations collected before the institutional ledger are retained with
`methodology_version='legacy_collector_v0.3'`.

The pre-institutional collector did not retain full raw API payloads, so EventRisk does not
fabricate them. This limitation is recorded as the dataset-level quality disclosure
`legacy_raw_payload_unavailable`.

## `er_event_families`

Groups related horizons/outcomes without claiming those events are identical.

Examples:
- a recession family with 2026, 2027 and 2028 horizons;
- a China/Taiwan conflict family containing distinct blockade and invasion events.

Families are normalized organizational objects, not probabilities.

## `er_canonical_events`

Current normalized EventRisk definition of an event.

Important fields:
- `canonical_event_id`: permanent EventRisk identifier.
- `event_family_id`.
- `canonical_name`, `canonical_definition`.
- `event_class`.
- `predicate`, `threshold_value`, `threshold_unit` when deterministically extractable.
- `resolution_deadline`, `outcome_type`, `status`.
- `taxonomy_version`.

Changes to normalized canonical metadata are preserved in
`er_canonical_event_versions`; family metadata is preserved in `er_event_family_versions`.

## `er_event_mappings`

Versioned relationship between a source-native contract and a canonical event.

Important fields:
- `mapping_type`.
- `confidence`.
- `mapping_method`, `method_version`.
- `review_status`, `review_notes`.
- `valid_from`, `valid_to`.
- `supersedes_mapping_id`.

Mapping is intentionally conservative. Source-native identity is deterministic. Cross-source
merges require matching title/deadline semantics and compatible source resolution rules for
automatic acceptance; otherwise they remain review candidates.

## `er_mapping_candidates`

Human-review queue for possible cross-source relationships.

Important fields:
- left/right source contracts.
- `suggested_relationship`.
- `title_similarity`.
- `deadline_distance_hours`.
- `resolution_similarity`.
- `resolution_compatible`.
- `rules_available_both`.
- `overall_confidence`.
- `candidate_method`, `method_version`.
- review status/action/notes/reviewer/timestamp.

Similarity is candidate-generation evidence, not proof of equivalence.

## Relationship classes

EventRisk distinguishes relationship provenance rather than presenting every graph edge as
fact.

- **factual** — externally verifiable structural fact with provenance.
- **source_asserted** — explicitly asserted/mentioned by a source record.
- **inferred** — model/rule inference; must include confidence, method and method version.

The default graph APIs exclude inferred edges unless `include_inferred=true` is requested.

## `er_graph_edges`

Generic knowledge-graph edge store.

Important fields:
- source/target node IDs and node types.
- `relationship_type`.
- `edge_class`.
- `confidence`.
- `assertion_text`.
- `inference_method`, `method_version`.
- `human_reviewed`, `review_status`.
- `valid_from`, `valid_to`.

`er_edge_evidence` stores the underlying evidence record(s) for each edge.

The initial automated graph creates only conservative source-asserted `mentions` edges for
known entity aliases explicitly present in source contract text. It does not convert a
mention into an `affects`, positive/negative-return or portfolio-impact claim.

## Entities and instruments

`er_entities` represents real-world entities/organizations/geographies.
`er_instruments` represents investable instruments. These are intentionally separate.

Identifiers are stored in separate versionable tables so clients can join EventRisk IDs to
external identifier systems without making a ticker the permanent identity of a company.

## Quality records

`er_quality_issues` is the visible QA ledger. Rules currently cover, among other things:

- probabilities outside valid bounds;
- crossed bid/ask books;
- institutional observations missing raw lineage;
- missing contract versions;
- invalid active-mapping cardinality;
- stale sources;
- material ingestion-volume drops;
- ambiguous high-confidence mappings;
- inferred graph edges missing methodology metadata.

Quality issues are not silently deleted when resolved; status and resolution metadata are
preserved.

## Bulk releases

`er_data_releases` defines a reproducible point-in-time release.
`er_export_manifests` records each Parquet object's:

- dataset name and format;
- object-storage URI;
- row count;
- observation time range where applicable;
- SHA-256 content hash;
- byte size.

## Methodology registry

`er_methodology_registry` names and versions deterministic normalization/mapping/graph
methods so a client can distinguish a methodology change from a change in the underlying
world.

## API history choices

- `/v1/events/{id}/history` — historical observations restated through the current active
  mapping. Useful when applying today's taxonomy to historical source data.
- `/v1/events/{id}/history-point-in-time` — only observations assigned to that canonical
  event under the mapping that was valid when EventRisk received each observation.
- `/v1/events/{id}/as-known?at=...` — canonical metadata and latest source observations that
  were available under EventRisk's then-valid taxonomy/mapping.
- `/v1/events/{id}/versions` — complete canonical metadata version history.

Clients should explicitly choose the history definition appropriate to their research.
