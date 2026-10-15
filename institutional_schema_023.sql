-- EventRisk institutional v2.8: immutable semantic-version history for mapping candidates.
--
-- `er_mapping_candidates` remains the backwards-compatible current review queue with its
-- one-row-per-contract-pair+method uniqueness contract. Every regenerated candidate is also
-- recorded here against the exact left/right contract semantic versions that produced it.

CREATE TABLE IF NOT EXISTS er_mapping_candidate_versions (
  candidate_version_id VARCHAR PRIMARY KEY,
  candidate_id VARCHAR NOT NULL,
  left_source_contract_id VARCHAR NOT NULL,
  right_source_contract_id VARCHAR NOT NULL,
  left_semantic_version_no INTEGER,
  right_semantic_version_no INTEGER,
  suggested_relationship VARCHAR NOT NULL,
  title_similarity DOUBLE,
  deadline_distance_hours DOUBLE,
  overall_confidence DOUBLE,
  candidate_method VARCHAR NOT NULL,
  method_version VARCHAR NOT NULL,
  resolution_similarity DOUBLE,
  resolution_compatible BOOLEAN,
  rules_available_both BOOLEAN,
  status VARCHAR NOT NULL DEFAULT 'pending',
  review_action VARCHAR,
  review_notes VARCHAR,
  reviewed_by VARCHAR,
  reviewed_at TIMESTAMPTZ,
  review_relationship_type VARCHAR,
  review_direction VARCHAR,
  generated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  superseded_at TIMESTAMPTZ,
  UNIQUE (
    left_source_contract_id,right_source_contract_id,method_version,
    left_semantic_version_no,right_semantic_version_no
  )
);

CREATE INDEX IF NOT EXISTS er_mapping_candidate_versions_candidate_idx
  ON er_mapping_candidate_versions(candidate_id,generated_at);
CREATE INDEX IF NOT EXISTS er_mapping_candidate_versions_pair_idx
  ON er_mapping_candidate_versions(left_source_contract_id,right_source_contract_id,method_version);

CREATE OR REPLACE VIEW er_latest_mapping_candidate_versions AS
SELECT * EXCLUDE(rn)
FROM (
  SELECT v.*,
         row_number() OVER(
           PARTITION BY left_source_contract_id,right_source_contract_id,method_version
           ORDER BY generated_at DESC,candidate_version_id DESC
         ) AS rn
  FROM er_mapping_candidate_versions v
)
WHERE rn=1;

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES (
  'mapping_candidate_history','mapping_candidate_versions_v1','provenance',
  'Semantic-versioned mapping candidate history v1',
  'Keeps the existing pair-level mapping review queue for compatibility while recording each generated candidate against the exact left and right contract semantic versions. If either contract semantics change, the current review item is reopened and prior review state is retained in immutable candidate history.',
  TRUE,'institutional_mapping_v5.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
