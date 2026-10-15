-- EventRisk institutional v3.7: deterministic identity mapping state separated from fuzzy review discovery.
--
-- Exact canonical identity is latency-sensitive and judgement-free under the existing strict rule.
-- Fuzzy/related-event discovery is advisory and may run independently. Separate watermarks prevent
-- slow review-candidate generation from blocking exact canonical identity or event curves.

CREATE TABLE IF NOT EXISTS er_mapping_identity_passes (
  pass_id VARCHAR PRIMARY KEY,
  method_version VARCHAR NOT NULL,
  started_at TIMESTAMPTZ NOT NULL,
  finished_at TIMESTAMPTZ,
  status VARCHAR NOT NULL,
  contracts_changed BIGINT,
  exact_pairs_evaluated BIGINT,
  exact_candidates_inserted BIGINT,
  exact_mappings_accepted BIGINT,
  details VARCHAR
);

CREATE TABLE IF NOT EXISTS er_mapping_identity_state (
  source_contract_id VARCHAR PRIMARY KEY,
  semantic_version_no BIGINT,
  semantic_received_at TIMESTAMPTZ,
  method_version VARCHAR NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL,
  last_pass_id VARCHAR
);

-- Existing contracts already processed by the prior conservative mapper have already received
-- source-native identity and strict exact-match consideration. Adopt those semantic watermarks so
-- the new identity lane begins with only genuinely unprocessed semantic changes.
INSERT INTO er_mapping_identity_state(
  source_contract_id,semantic_version_no,semantic_received_at,method_version,processed_at,last_pass_id
)
SELECT st.source_contract_id,st.semantic_version_no,st.semantic_received_at,
       'identity_bootstrap_from_mapping_state_v1',now(),st.last_pass_id
FROM er_mapping_contract_state st
JOIN er_latest_contract_semantic_versions s USING(source_contract_id)
WHERE coalesce(st.semantic_version_no,-1)=coalesce(s.version_no,-1)
ON CONFLICT(source_contract_id) DO NOTHING;

CREATE OR REPLACE VIEW er_mapping_identity_work_queue AS
SELECT c.source_contract_id,c.source_id,c.current_title,c.current_resolution_rules,
       c.closes_at,c.resolves_at,s.version_no AS semantic_version_no,
       s.received_at AS semantic_received_at,
       st.semantic_version_no AS processed_semantic_version_no
FROM er_source_contracts c
LEFT JOIN er_latest_contract_semantic_versions s USING(source_contract_id)
LEFT JOIN er_mapping_identity_state st USING(source_contract_id)
WHERE c.active=TRUE
  AND coalesce(st.semantic_version_no,-1)<>coalesce(s.version_no,-1);

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES (
  'event_identity_mapping','identity_mapping_v1_strict_exact','normalization',
  'Strict exact canonical identity mapping v1',
  'Separates deterministic canonical identity from fuzzy review-candidate discovery. New or semantically changed contracts receive source-native canonical identity immediately; cross-source auto-merges require exact normalized title, exact non-empty normalized resolution rules, and the existing <=24h-or-missing deadline compatibility rule. Fuzzy similarity is excluded from this lane.',
  TRUE,'institutional_mapping_identity_v1.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
