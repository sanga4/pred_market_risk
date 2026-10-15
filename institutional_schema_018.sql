-- EventRisk institutional v2.3: scalable incremental canonical mapping passes.
--
-- Mapping remains conservative. This migration adds operational state so unchanged source
-- contract semantics are not recomputed on every collection cycle. It does not change the
-- meaning of same_event or relationship review decisions.

CREATE TABLE IF NOT EXISTS er_mapping_passes (
  pass_id VARCHAR PRIMARY KEY,
  method_version VARCHAR NOT NULL,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  status VARCHAR NOT NULL DEFAULT 'running',
  contracts_total BIGINT DEFAULT 0,
  contracts_changed BIGINT DEFAULT 0,
  native_events_created BIGINT DEFAULT 0,
  candidate_pairs_evaluated BIGINT DEFAULT 0,
  candidates_inserted BIGINT DEFAULT 0,
  auto_exact_mappings BIGINT DEFAULT 0,
  details VARCHAR
);

CREATE TABLE IF NOT EXISTS er_mapping_contract_state (
  source_contract_id VARCHAR PRIMARY KEY,
  semantic_version_no INTEGER,
  semantic_received_at TIMESTAMPTZ,
  method_version VARCHAR NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_pass_id VARCHAR NOT NULL
);

CREATE INDEX IF NOT EXISTS er_mapping_contract_state_method_idx
  ON er_mapping_contract_state(method_version, processed_at);
CREATE INDEX IF NOT EXISTS er_mapping_pass_status_idx
  ON er_mapping_passes(status, started_at);

CREATE OR REPLACE VIEW er_mapping_work_queue AS
SELECT
  c.source_contract_id,
  c.source_id,
  c.current_title,
  c.current_resolution_rules,
  c.closes_at,
  c.resolves_at,
  s.version_no AS semantic_version_no,
  s.received_at AS semantic_received_at,
  st.semantic_version_no AS processed_semantic_version_no,
  st.method_version AS processed_method_version
FROM er_source_contracts c
LEFT JOIN er_latest_contract_semantic_versions s USING(source_contract_id)
LEFT JOIN er_mapping_contract_state st USING(source_contract_id)
WHERE c.active=TRUE
  AND (
    st.source_contract_id IS NULL
    OR st.method_version<>'mapping_rules_v4_batched'
    OR coalesce(st.semantic_version_no,-1)<>coalesce(s.version_no,-1)
  );

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES (
  'event_mapping','mapping_rules_v4_batched','normalization',
  'Incremental batched canonical mapping v4',
  'Creates source-native canonical identities in batches, compares only new or semantically changed contracts against the active cross-source universe through conservative token blocking, persists candidates in batches, and automatically merges only exact normalized title/deadline/resolution-rule identities. Similar or ambiguous contracts remain separate for review.',
  TRUE,'institutional_mapping_v4.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
