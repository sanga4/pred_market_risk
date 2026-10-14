-- EventRisk institutional v2.1: source-universe completeness and conservative contract-presence reconciliation.
--
-- Open-universe APIs often stop returning a contract once it closes. Absence is not treated as
-- a source assertion. Instead, EventRisk records run completeness and requires repeated misses
-- across successful full-universe runs before changing the contract's serving-active flag.

ALTER TABLE er_ingestion_runs ADD COLUMN IF NOT EXISTS collection_scope VARCHAR;
ALTER TABLE er_ingestion_runs ADD COLUMN IF NOT EXISTS universe_complete BOOLEAN;
ALTER TABLE er_ingestion_runs ADD COLUMN IF NOT EXISTS collection_policy VARCHAR;

CREATE TABLE IF NOT EXISTS er_contract_presence_state (
  source_contract_id VARCHAR PRIMARY KEY,
  source_id VARCHAR NOT NULL,
  listing_state VARCHAR NOT NULL DEFAULT 'observed',
  consecutive_misses INTEGER NOT NULL DEFAULT 0,
  last_seen_run_id VARCHAR,
  last_seen_at TIMESTAMPTZ,
  last_evaluated_run_id VARCHAR,
  deactivated_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS er_contract_presence_transitions (
  transition_id VARCHAR PRIMARY KEY,
  source_contract_id VARCHAR NOT NULL,
  source_id VARCHAR NOT NULL,
  run_id VARCHAR NOT NULL,
  transition_type VARCHAR NOT NULL,
  previous_state VARCHAR,
  new_state VARCHAR NOT NULL,
  consecutive_misses INTEGER NOT NULL,
  reason VARCHAR NOT NULL,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS er_presence_source_state_idx
  ON er_contract_presence_state(source_id, listing_state, consecutive_misses);
CREATE INDEX IF NOT EXISTS er_presence_transition_contract_idx
  ON er_contract_presence_transitions(source_contract_id, recorded_at);

CREATE OR REPLACE VIEW er_contract_presence_status AS
SELECT
  c.source_contract_id,
  c.source_id,
  c.source_market_id,
  c.current_title,
  c.status AS last_source_status,
  c.active AS serving_active,
  p.listing_state,
  p.consecutive_misses,
  p.last_seen_run_id,
  p.last_seen_at,
  p.last_evaluated_run_id,
  p.deactivated_at,
  p.updated_at
FROM er_source_contracts c
LEFT JOIN er_contract_presence_state p USING(source_contract_id);

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES (
  'contract_presence','presence_reconciliation_v1','normalization',
  'Conservative open-universe presence reconciliation v1',
  'Records whether each source contract is observed in complete full-universe collection runs. A missing contract is not treated as closed until it is absent from multiple consecutive successful full-universe runs; reappearance resets misses and reactivates serving state. Source-reported status is never fabricated from absence.',
  TRUE,'institutional_ingest_v3.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
