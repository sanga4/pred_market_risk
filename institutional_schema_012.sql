-- EventRisk institutional v2.0: prospective full semantic source-contract ledger.
-- This starts at the first upgraded ingestion after migration. Older contract metadata is
-- not fabricated from today's current row; lower-completeness historical versions remain
-- available in er_source_contract_versions.

CREATE TABLE IF NOT EXISTS er_contract_semantic_versions (
  source_contract_id VARCHAR NOT NULL,
  version_no INTEGER NOT NULL,
  source_id VARCHAR NOT NULL,
  source_market_id VARCHAR NOT NULL,
  source_event_id VARCHAR,
  title VARCHAR NOT NULL,
  description VARCHAR,
  outcome_type VARCHAR,
  status VARCHAR,
  opens_at TIMESTAMPTZ,
  closes_at TIMESTAMPTZ,
  resolves_at TIMESTAMPTZ,
  resolution_authority VARCHAR,
  resolution_rules VARCHAR,
  source_url VARCHAR,
  source_observed_at TIMESTAMPTZ,
  received_at TIMESTAMPTZ NOT NULL,
  valid_from TIMESTAMPTZ NOT NULL,
  valid_to TIMESTAMPTZ,
  event_raw_snapshot_id VARCHAR NOT NULL,
  market_raw_snapshot_id VARCHAR NOT NULL,
  metadata_completeness VARCHAR NOT NULL DEFAULT 'full_v2',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (source_contract_id, version_no)
);

CREATE INDEX IF NOT EXISTS er_contract_semantic_received_idx
  ON er_contract_semantic_versions(source_contract_id,received_at);

CREATE OR REPLACE VIEW er_latest_contract_semantic_versions AS
SELECT * EXCLUDE(rn) FROM (
  SELECT v.*,row_number() OVER(PARTITION BY source_contract_id ORDER BY version_no DESC) rn
  FROM er_contract_semantic_versions v
) WHERE rn=1;

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES
  ('source_contract_semantics','contract_semantics_v2','normalization','Full source-contract semantic ledger v2',
   'Prospectively versions source event identity, title, description, outcome type, status, dates, resolution authority, resolution rules and source URL, tied to exact immutable event and market raw snapshots.',TRUE,
   'institutional_ingest_v3.py'),
  ('graph_mentions','graph_rules_v2_versioned_mentions','knowledge_graph','Version-aware source mention graph v2',
   'Constructs source-asserted mentions over intersections of source-contract metadata versions and canonical-mapping versions; raw version evidence is retained and no investment-impact relationship is inferred.',TRUE,
   'institutional_graph_v2.py'),
  ('reference_master','reference_versions_v1','normalization','Point-in-time reference masters v1',
   'Versions mutable source, entity and instrument master metadata prospectively so historical releases do not silently inherit later names or classifications.',TRUE,
   'institutional_reference_versions.py')
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
