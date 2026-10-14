-- EventRisk institutional v2.6: persistent incremental mapping index.
--
-- Candidate semantics remain conservative and unchanged. This migration stores normalized
-- title/rule blocks and meaningful title tokens so only new or semantically changed contracts
-- need to be joined against the cross-source universe after the initial index build.

CREATE TABLE IF NOT EXISTS er_mapping_contract_index (
  source_contract_id VARCHAR PRIMARY KEY,
  source_id VARCHAR NOT NULL,
  semantic_version_no INTEGER,
  normalized_title VARCHAR NOT NULL,
  normalized_rules VARCHAR NOT NULL,
  deadline_at TIMESTAMPTZ,
  deadline_bucket VARCHAR NOT NULL,
  meaningful_token_count INTEGER NOT NULL DEFAULT 0,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  indexed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  method_version VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS er_mapping_token_index (
  source_contract_id VARCHAR NOT NULL,
  source_id VARCHAR NOT NULL,
  semantic_version_no INTEGER,
  token VARCHAR NOT NULL,
  method_version VARCHAR NOT NULL,
  indexed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY(source_contract_id, token)
);

CREATE INDEX IF NOT EXISTS er_mapping_contract_index_title_idx
  ON er_mapping_contract_index(normalized_title, deadline_bucket, source_id, active);
CREATE INDEX IF NOT EXISTS er_mapping_contract_index_source_idx
  ON er_mapping_contract_index(source_id, active, semantic_version_no);
CREATE INDEX IF NOT EXISTS er_mapping_token_index_token_idx
  ON er_mapping_token_index(token, source_id);

ALTER TABLE er_mapping_candidates ADD COLUMN IF NOT EXISTS left_semantic_version_no INTEGER;
ALTER TABLE er_mapping_candidates ADD COLUMN IF NOT EXISTS right_semantic_version_no INTEGER;

CREATE OR REPLACE VIEW er_mapping_work_queue_v5 AS
SELECT
  c.source_contract_id,
  c.source_id,
  c.current_title,
  c.current_resolution_rules,
  c.closes_at,
  c.resolves_at,
  s.version_no AS semantic_version_no,
  s.received_at AS semantic_received_at,
  i.semantic_version_no AS indexed_semantic_version_no,
  st.semantic_version_no AS processed_semantic_version_no,
  st.method_version AS processed_method_version
FROM er_source_contracts c
LEFT JOIN er_latest_contract_semantic_versions s USING(source_contract_id)
LEFT JOIN er_mapping_contract_index i USING(source_contract_id)
LEFT JOIN er_mapping_contract_state st USING(source_contract_id)
WHERE c.active=TRUE
  AND (
    i.source_contract_id IS NULL
    OR coalesce(i.semantic_version_no,-1)<>coalesce(s.version_no,-1)
    OR st.source_contract_id IS NULL
    OR coalesce(st.semantic_version_no,-1)<>coalesce(s.version_no,-1)
  );

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES (
  'event_mapping','mapping_rules_v5_indexed','normalization',
  'Persistent indexed incremental canonical mapping v5',
  'Preserves v4 canonicalization rules while maintaining a persistent normalized-title and meaningful-token index. New or semantically changed contracts are SQL-blocked against active opposite-source peers; only bounded candidate pairs are transferred to Python for the same title/deadline/resolution-rule scoring used by v4. Candidate provenance records both source semantic version numbers.',
  TRUE,'institutional_mapping_v5.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES (
  'mapping_pass_recovery','mapping_pass_recovery_v1','operations',
  'Abandoned mapping-pass recovery v1',
  'Marks mapping passes that remain running beyond a conservative timeout as abandoned on a later pass. Recovery never converts an interrupted pass to success and retains the original pass record for audit.',
  TRUE,'institutional_mapping_v5.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
