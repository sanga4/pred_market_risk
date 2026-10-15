-- EventRisk institutional data layer v1
-- Additive schema: does not modify the existing retail tables.
-- Design principles:
--   * raw/source observations are append-only
--   * source contracts are distinct from canonical events
--   * mappings and graph edges are versioned and provenance-aware
--   * inferred relationships are never indistinguishable from factual ones
--   * point-in-time reconstruction is supported with observed/received/valid timestamps

CREATE TABLE IF NOT EXISTS er_sources (
  source_id VARCHAR PRIMARY KEY,
  source_name VARCHAR NOT NULL,
  source_type VARCHAR NOT NULL,
  base_url VARCHAR,
  redistribution_status VARCHAR DEFAULT 'unknown',
  attribution_required BOOLEAN DEFAULT FALSE,
  active BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS er_ingestion_runs (
  run_id VARCHAR PRIMARY KEY,
  source_id VARCHAR NOT NULL,
  started_at TIMESTAMPTZ NOT NULL,
  finished_at TIMESTAMPTZ,
  status VARCHAR NOT NULL,
  records_received BIGINT DEFAULT 0,
  records_written BIGINT DEFAULT 0,
  error_count BIGINT DEFAULT 0,
  details VARCHAR
);

CREATE TABLE IF NOT EXISTS er_raw_snapshots (
  raw_snapshot_id VARCHAR PRIMARY KEY,
  source_id VARCHAR NOT NULL,
  source_object_type VARCHAR NOT NULL,
  source_object_id VARCHAR NOT NULL,
  source_observed_at TIMESTAMPTZ,
  received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  content_hash VARCHAR,
  payload_json VARCHAR NOT NULL,
  ingestion_run_id VARCHAR,
  UNIQUE (source_id, source_object_type, source_object_id, received_at)
);

CREATE TABLE IF NOT EXISTS er_source_contracts (
  source_contract_id VARCHAR PRIMARY KEY,
  source_id VARCHAR NOT NULL,
  source_market_id VARCHAR NOT NULL,
  source_event_id VARCHAR,
  current_title VARCHAR NOT NULL,
  current_description VARCHAR,
  outcome_type VARCHAR DEFAULT 'binary',
  status VARCHAR,
  opens_at TIMESTAMPTZ,
  closes_at TIMESTAMPTZ,
  resolves_at TIMESTAMPTZ,
  resolution_authority VARCHAR,
  current_resolution_rules VARCHAR,
  source_url VARCHAR,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  active BOOLEAN DEFAULT TRUE,
  UNIQUE (source_id, source_market_id)
);

CREATE TABLE IF NOT EXISTS er_source_contract_versions (
  source_contract_id VARCHAR NOT NULL,
  version_no INTEGER NOT NULL,
  title VARCHAR NOT NULL,
  description VARCHAR,
  status VARCHAR,
  resolution_rules VARCHAR,
  opens_at TIMESTAMPTZ,
  closes_at TIMESTAMPTZ,
  resolves_at TIMESTAMPTZ,
  source_observed_at TIMESTAMPTZ,
  received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  valid_from TIMESTAMPTZ NOT NULL DEFAULT now(),
  valid_to TIMESTAMPTZ,
  raw_snapshot_id VARCHAR,
  PRIMARY KEY (source_contract_id, version_no)
);

CREATE TABLE IF NOT EXISTS er_event_families (
  event_family_id VARCHAR PRIMARY KEY,
  canonical_name VARCHAR NOT NULL,
  category VARCHAR,
  subcategory VARCHAR,
  description VARCHAR,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS er_canonical_events (
  canonical_event_id VARCHAR PRIMARY KEY,
  event_family_id VARCHAR,
  canonical_name VARCHAR NOT NULL,
  canonical_definition VARCHAR NOT NULL,
  subject_entity_id VARCHAR,
  predicate VARCHAR,
  object_entity_id VARCHAR,
  event_class VARCHAR,
  geography_entity_id VARCHAR,
  threshold_value DOUBLE,
  threshold_unit VARCHAR,
  measurement_name VARCHAR,
  observation_period_start TIMESTAMPTZ,
  observation_period_end TIMESTAMPTZ,
  resolution_deadline TIMESTAMPTZ,
  outcome_type VARCHAR DEFAULT 'binary',
  status VARCHAR DEFAULT 'active',
  taxonomy_version VARCHAR DEFAULT 'event_ontology_v1',
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS er_event_mappings (
  mapping_id VARCHAR PRIMARY KEY,
  source_contract_id VARCHAR NOT NULL,
  canonical_event_id VARCHAR NOT NULL,
  mapping_type VARCHAR NOT NULL,
  confidence DOUBLE,
  mapping_method VARCHAR NOT NULL,
  method_version VARCHAR,
  review_status VARCHAR DEFAULT 'unreviewed',
  review_notes VARCHAR,
  mapped_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  valid_from TIMESTAMPTZ NOT NULL DEFAULT now(),
  valid_to TIMESTAMPTZ,
  supersedes_mapping_id VARCHAR,
  created_by VARCHAR DEFAULT 'system'
);

CREATE TABLE IF NOT EXISTS er_market_observations (
  source_id VARCHAR NOT NULL,
  source_contract_id VARCHAR NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL,
  received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  yes_bid DOUBLE,
  yes_ask DOUBLE,
  no_bid DOUBLE,
  no_ask DOUBLE,
  midpoint DOUBLE,
  last_trade DOUBLE,
  displayed_probability DOUBLE,
  volume DOUBLE,
  volume_24h DOUBLE,
  open_interest DOUBLE,
  liquidity DOUBLE,
  orderbook_depth DOUBLE,
  spread DOUBLE,
  market_status VARCHAR,
  source_sequence VARCHAR,
  raw_snapshot_id VARCHAR,
  methodology_version VARCHAR DEFAULT 'source_native_v1',
  PRIMARY KEY (source_contract_id, observed_at, received_at)
);

CREATE TABLE IF NOT EXISTS er_entities (
  entity_id VARCHAR PRIMARY KEY,
  entity_type VARCHAR NOT NULL,
  canonical_name VARCHAR NOT NULL,
  legal_name VARCHAR,
  country_code VARCHAR,
  active BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS er_entity_identifiers (
  entity_id VARCHAR NOT NULL,
  identifier_type VARCHAR NOT NULL,
  identifier_value VARCHAR NOT NULL,
  source VARCHAR,
  valid_from TIMESTAMPTZ,
  valid_to TIMESTAMPTZ,
  is_primary BOOLEAN DEFAULT FALSE,
  PRIMARY KEY (entity_id, identifier_type, identifier_value)
);

CREATE TABLE IF NOT EXISTS er_instruments (
  instrument_id VARCHAR PRIMARY KEY,
  issuer_entity_id VARCHAR,
  instrument_type VARCHAR NOT NULL,
  canonical_name VARCHAR,
  currency VARCHAR,
  exchange_mic VARCHAR,
  active BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS er_instrument_identifiers (
  instrument_id VARCHAR NOT NULL,
  identifier_type VARCHAR NOT NULL,
  identifier_value VARCHAR NOT NULL,
  source VARCHAR,
  valid_from TIMESTAMPTZ,
  valid_to TIMESTAMPTZ,
  PRIMARY KEY (instrument_id, identifier_type, identifier_value)
);

CREATE TABLE IF NOT EXISTS er_graph_edges (
  edge_id VARCHAR PRIMARY KEY,
  source_node_id VARCHAR NOT NULL,
  source_node_type VARCHAR NOT NULL,
  relationship_type VARCHAR NOT NULL,
  target_node_id VARCHAR NOT NULL,
  target_node_type VARCHAR NOT NULL,
  edge_class VARCHAR NOT NULL,
  confidence DOUBLE,
  assertion_text VARCHAR,
  inference_method VARCHAR,
  method_version VARCHAR,
  human_reviewed BOOLEAN DEFAULT FALSE,
  review_status VARCHAR DEFAULT 'unreviewed',
  observed_at TIMESTAMPTZ,
  valid_from TIMESTAMPTZ DEFAULT now(),
  valid_to TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now(),
  supersedes_edge_id VARCHAR
);

CREATE TABLE IF NOT EXISTS er_edge_evidence (
  edge_id VARCHAR NOT NULL,
  evidence_id VARCHAR NOT NULL,
  evidence_type VARCHAR NOT NULL,
  source_id VARCHAR,
  source_record_id VARCHAR,
  source_url VARCHAR,
  excerpt VARCHAR,
  observed_at TIMESTAMPTZ,
  added_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (edge_id, evidence_id)
);

CREATE TABLE IF NOT EXISTS er_event_relationships (
  relationship_id VARCHAR PRIMARY KEY,
  source_event_id VARCHAR NOT NULL,
  relationship_type VARCHAR NOT NULL,
  target_event_id VARCHAR NOT NULL,
  relationship_class VARCHAR NOT NULL,
  confidence DOUBLE,
  method VARCHAR NOT NULL,
  method_version VARCHAR,
  human_reviewed BOOLEAN DEFAULT FALSE,
  review_status VARCHAR DEFAULT 'unreviewed',
  valid_from TIMESTAMPTZ DEFAULT now(),
  valid_to TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS er_quality_issues (
  quality_issue_id VARCHAR PRIMARY KEY,
  object_type VARCHAR NOT NULL,
  object_id VARCHAR NOT NULL,
  rule_code VARCHAR NOT NULL,
  severity VARCHAR NOT NULL,
  status VARCHAR DEFAULT 'open',
  details VARCHAR,
  detected_at TIMESTAMPTZ DEFAULT now(),
  resolved_at TIMESTAMPTZ,
  resolution_notes VARCHAR
);

CREATE TABLE IF NOT EXISTS er_revisions (
  revision_id VARCHAR PRIMARY KEY,
  object_type VARCHAR NOT NULL,
  object_id VARCHAR NOT NULL,
  revision_type VARCHAR NOT NULL,
  reason VARCHAR,
  previous_version VARCHAR,
  new_version VARCHAR,
  effective_at TIMESTAMPTZ,
  recorded_at TIMESTAMPTZ DEFAULT now(),
  actor VARCHAR DEFAULT 'system'
);

CREATE OR REPLACE VIEW er_latest_contract_mappings AS
SELECT * EXCLUDE (rn)
FROM (
  SELECT *, row_number() OVER (
    PARTITION BY source_contract_id
    ORDER BY valid_from DESC, mapped_at DESC
  ) AS rn
  FROM er_event_mappings
  WHERE valid_to IS NULL
)
WHERE rn = 1;

CREATE OR REPLACE VIEW er_latest_market_observations AS
SELECT * EXCLUDE (rn)
FROM (
  SELECT *, row_number() OVER (
    PARTITION BY source_contract_id
    ORDER BY observed_at DESC, received_at DESC
  ) AS rn
  FROM er_market_observations
)
WHERE rn = 1;

CREATE OR REPLACE VIEW er_event_source_snapshot AS
SELECT
  e.canonical_event_id,
  e.event_family_id,
  e.canonical_name,
  e.canonical_definition,
  e.event_class,
  e.status AS event_status,
  m.mapping_type,
  m.confidence AS mapping_confidence,
  m.review_status AS mapping_review_status,
  c.source_contract_id,
  c.source_id,
  c.source_market_id,
  c.current_title AS source_title,
  c.closes_at,
  o.observed_at,
  o.received_at,
  o.yes_bid,
  o.yes_ask,
  o.midpoint,
  o.last_trade,
  o.displayed_probability,
  o.volume,
  o.volume_24h,
  o.open_interest,
  o.liquidity,
  o.spread,
  o.market_status,
  o.methodology_version
FROM er_canonical_events e
JOIN er_latest_contract_mappings m
  ON m.canonical_event_id = e.canonical_event_id
JOIN er_source_contracts c
  ON c.source_contract_id = m.source_contract_id
LEFT JOIN er_latest_market_observations o
  ON o.source_contract_id = c.source_contract_id;
