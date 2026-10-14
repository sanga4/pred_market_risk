-- EventRisk institutional v1.7: served-event coverage, first-class curves, richer reviewed relationships,
-- and reproducible bulk-release manifests.

ALTER TABLE er_mapping_candidates ADD COLUMN IF NOT EXISTS review_relationship_type VARCHAR;
ALTER TABLE er_mapping_candidates ADD COLUMN IF NOT EXISTS review_direction VARCHAR;

ALTER TABLE er_event_relationships ADD COLUMN IF NOT EXISTS review_notes VARCHAR;
ALTER TABLE er_event_relationships ADD COLUMN IF NOT EXISTS source_candidate_id VARCHAR;
ALTER TABLE er_event_relationships ADD COLUMN IF NOT EXISTS created_by VARCHAR DEFAULT 'system';

CREATE TABLE IF NOT EXISTS er_event_relationship_evidence (
  relationship_id VARCHAR NOT NULL,
  evidence_id VARCHAR NOT NULL,
  evidence_type VARCHAR NOT NULL,
  source_id VARCHAR,
  source_record_id VARCHAR,
  excerpt VARCHAR,
  added_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (relationship_id, evidence_id)
);

ALTER TABLE er_data_releases ADD COLUMN IF NOT EXISTS manifest_hash VARCHAR;
ALTER TABLE er_data_releases ADD COLUMN IF NOT EXISTS manifest_object_uri VARCHAR;
ALTER TABLE er_data_releases ADD COLUMN IF NOT EXISTS dataset_count INTEGER;
ALTER TABLE er_data_releases ADD COLUMN IF NOT EXISTS generator_version VARCHAR;
ALTER TABLE er_data_releases ADD COLUMN IF NOT EXISTS readiness_snapshot VARCHAR;

ALTER TABLE er_entity_identifiers ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT now();
ALTER TABLE er_instrument_identifiers ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT now();

CREATE INDEX IF NOT EXISTS er_market_obs_received_idx
  ON er_market_observations(received_at, source_id);
CREATE INDEX IF NOT EXISTS er_market_obs_contract_received_idx
  ON er_market_observations(source_contract_id, received_at);
CREATE INDEX IF NOT EXISTS er_event_mapping_validity_idx
  ON er_event_mappings(source_contract_id, valid_from, valid_to);
CREATE INDEX IF NOT EXISTS er_event_mapping_event_idx
  ON er_event_mappings(canonical_event_id, valid_from, valid_to);

-- Current-taxonomy curve: restates historical observations through today's active mapping.
CREATE OR REPLACE VIEW er_event_curve_current_taxonomy AS
SELECT
  m.canonical_event_id,
  o.source_id,
  c.source_market_id,
  o.source_contract_id,
  o.observed_at,
  o.received_at,
  o.processed_at,
  o.yes_bid,
  o.yes_ask,
  o.no_bid,
  o.no_ask,
  o.midpoint,
  o.last_trade,
  o.displayed_probability,
  o.volume,
  o.volume_24h,
  o.open_interest,
  o.liquidity,
  o.orderbook_depth,
  o.spread,
  o.market_status,
  o.methodology_version,
  m.mapping_id,
  m.mapping_type,
  m.mapping_method,
  m.method_version AS mapping_method_version,
  m.review_status AS mapping_review_status,
  'current_taxonomy' AS mapping_basis
FROM er_market_observations o
JOIN er_source_contracts c USING(source_contract_id)
JOIN er_latest_contract_mappings m USING(source_contract_id);

-- Point-in-time curve: maps every observation using the mapping EventRisk knew at receipt time.
CREATE OR REPLACE VIEW er_event_curve_as_known AS
SELECT
  m.canonical_event_id,
  o.source_id,
  c.source_market_id,
  o.source_contract_id,
  o.observed_at,
  o.received_at,
  o.processed_at,
  o.yes_bid,
  o.yes_ask,
  o.no_bid,
  o.no_ask,
  o.midpoint,
  o.last_trade,
  o.displayed_probability,
  o.volume,
  o.volume_24h,
  o.open_interest,
  o.liquidity,
  o.orderbook_depth,
  o.spread,
  o.market_status,
  o.methodology_version,
  m.mapping_id,
  m.mapping_type,
  m.mapping_method,
  m.method_version AS mapping_method_version,
  m.review_status AS mapping_review_status,
  'as_known' AS mapping_basis
FROM er_market_observations o
JOIN er_source_contracts c USING(source_contract_id)
JOIN er_event_mappings m
  ON m.source_contract_id=o.source_contract_id
 AND m.valid_from<=o.received_at
 AND (m.valid_to IS NULL OR m.valid_to>o.received_at);

-- A canonical event is distributable by default only while at least one source contract maps to it.
CREATE OR REPLACE VIEW er_served_canonical_events AS
SELECT
  e.canonical_event_id,
  e.event_family_id,
  e.canonical_name,
  e.canonical_definition,
  e.subject_entity_id,
  e.predicate,
  e.object_entity_id,
  e.event_class,
  e.geography_entity_id,
  e.threshold_value,
  e.threshold_unit,
  e.measurement_name,
  e.observation_period_start,
  e.observation_period_end,
  e.resolution_deadline,
  e.outcome_type,
  e.status,
  e.taxonomy_version,
  e.created_at,
  e.updated_at,
  count(DISTINCT m.source_contract_id) AS active_contract_count,
  count(DISTINCT c.source_id) AS source_count,
  max(o.received_at) AS latest_received_at
FROM er_canonical_events e
JOIN er_latest_contract_mappings m ON m.canonical_event_id=e.canonical_event_id
JOIN er_source_contracts c USING(source_contract_id)
LEFT JOIN er_latest_market_observations o USING(source_contract_id)
GROUP BY ALL;

CREATE OR REPLACE VIEW er_active_event_relationships AS
SELECT *
FROM er_event_relationships
WHERE valid_to IS NULL;
