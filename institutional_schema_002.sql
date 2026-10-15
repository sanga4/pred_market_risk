-- EventRisk institutional data layer v1.1 additions
-- Additive migration executed after institutional_schema.sql.

CREATE TABLE IF NOT EXISTS er_mapping_candidates (
  candidate_id VARCHAR PRIMARY KEY,
  left_source_contract_id VARCHAR NOT NULL,
  right_source_contract_id VARCHAR NOT NULL,
  suggested_relationship VARCHAR NOT NULL,
  title_similarity DOUBLE,
  deadline_distance_hours DOUBLE,
  overall_confidence DOUBLE,
  candidate_method VARCHAR NOT NULL,
  method_version VARCHAR NOT NULL,
  status VARCHAR NOT NULL DEFAULT 'pending',
  review_action VARCHAR,
  review_notes VARCHAR,
  reviewed_by VARCHAR,
  reviewed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE (left_source_contract_id, right_source_contract_id, method_version)
);

CREATE TABLE IF NOT EXISTS er_source_health (
  source_id VARCHAR PRIMARY KEY,
  last_attempt_at TIMESTAMPTZ,
  last_success_at TIMESTAMPTZ,
  last_observation_at TIMESTAMPTZ,
  last_run_id VARCHAR,
  last_run_status VARCHAR,
  records_last_run BIGINT,
  consecutive_failures INTEGER DEFAULT 0,
  health_status VARCHAR DEFAULT 'unknown',
  details VARCHAR,
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS er_collection_state (
  state_key VARCHAR PRIMARY KEY,
  state_value VARCHAR,
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS er_admin_actions (
  action_id VARCHAR PRIMARY KEY,
  action_type VARCHAR NOT NULL,
  object_type VARCHAR NOT NULL,
  object_id VARCHAR NOT NULL,
  actor VARCHAR NOT NULL,
  details VARCHAR,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS er_api_clients (
  client_id VARCHAR PRIMARY KEY,
  client_name VARCHAR NOT NULL,
  key_hash VARCHAR NOT NULL,
  active BOOLEAN DEFAULT TRUE,
  scopes VARCHAR DEFAULT 'read',
  created_at TIMESTAMPTZ DEFAULT now(),
  last_used_at TIMESTAMPTZ
);

CREATE OR REPLACE VIEW er_event_curve AS
SELECT
  m.canonical_event_id,
  o.source_id,
  c.source_market_id,
  o.source_contract_id,
  o.observed_at,
  o.received_at,
  o.displayed_probability,
  o.yes_bid,
  o.yes_ask,
  o.midpoint,
  o.last_trade,
  o.volume,
  o.volume_24h,
  o.open_interest,
  o.liquidity,
  o.spread,
  o.market_status,
  m.mapping_type,
  m.review_status AS mapping_review_status
FROM er_market_observations o
JOIN er_source_contracts c USING (source_contract_id)
JOIN er_latest_contract_mappings m USING (source_contract_id);

CREATE OR REPLACE VIEW er_term_structure_latest AS
SELECT
  f.event_family_id,
  f.canonical_name AS event_family_name,
  e.canonical_event_id,
  e.canonical_name AS event_name,
  e.resolution_deadline,
  s.source_id,
  s.source_contract_id,
  s.observed_at,
  s.displayed_probability,
  s.yes_bid,
  s.yes_ask,
  s.volume,
  s.open_interest,
  s.liquidity,
  s.spread,
  s.mapping_review_status
FROM er_event_families f
JOIN er_canonical_events e USING (event_family_id)
JOIN er_event_source_snapshot s USING (canonical_event_id)
WHERE e.status = 'active';

CREATE OR REPLACE VIEW er_source_contract_current_versions AS
SELECT * EXCLUDE (rn)
FROM (
  SELECT v.*, row_number() OVER (
    PARTITION BY source_contract_id
    ORDER BY version_no DESC
  ) rn
  FROM er_source_contract_versions v
)
WHERE rn = 1;

CREATE OR REPLACE VIEW er_open_quality_issues AS
SELECT * FROM er_quality_issues
WHERE status = 'open';
