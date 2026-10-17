-- Independently versioned advisory semantic hierarchy. Core source/identity ledger is unchanged.
CREATE TABLE IF NOT EXISTS er_hierarchy_taxonomies (
  taxonomy_version VARCHAR PRIMARY KEY, content_sha256 VARCHAR NOT NULL,
  definition_json VARCHAR NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS er_hierarchy_themes (
  taxonomy_version VARCHAR NOT NULL, theme_id VARCHAR NOT NULL, canonical_name VARCHAR NOT NULL,
  PRIMARY KEY(taxonomy_version,theme_id)
);
CREATE TABLE IF NOT EXISTS er_hierarchy_subjects (
  taxonomy_version VARCHAR NOT NULL, subject_id VARCHAR NOT NULL, canonical_name VARCHAR NOT NULL,
  definition VARCHAR NOT NULL, PRIMARY KEY(taxonomy_version,subject_id)
);
CREATE TABLE IF NOT EXISTS er_hierarchy_subject_theme_mappings (
  taxonomy_version VARCHAR NOT NULL, subject_id VARCHAR NOT NULL, theme_id VARCHAR NOT NULL,
  relationship_type VARCHAR NOT NULL DEFAULT 'member_of', edge_class VARCHAR NOT NULL DEFAULT 'governed_taxonomy',
  definition_sha256 VARCHAR NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY(taxonomy_version,subject_id)
);
CREATE TABLE IF NOT EXISTS er_hierarchy_models (
  model_id VARCHAR PRIMARY KEY, taxonomy_version VARCHAR NOT NULL,
  manifest_sha256 VARCHAR NOT NULL, manifest_json VARCHAR NOT NULL,
  validation_status VARCHAR NOT NULL DEFAULT 'provisional_not_independently_validated',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS er_hierarchy_prediction_cache (
  cache_key VARCHAR PRIMARY KEY, model_id VARCHAR NOT NULL, input_hash VARCHAR NOT NULL,
  input_text VARCHAR NOT NULL, subject_id VARCHAR NOT NULL, theme_id VARCHAR NOT NULL,
  candidate_subject_id VARCHAR NOT NULL, similarity DOUBLE, margin DOUBLE,
  abstained BOOLEAN NOT NULL, reason VARCHAR NOT NULL, output_json VARCHAR NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), UNIQUE(model_id,input_hash)
);
CREATE TABLE IF NOT EXISTS er_hierarchy_event_subject_mappings (
  mapping_id VARCHAR PRIMARY KEY, canonical_event_id VARCHAR NOT NULL,
  version_no INTEGER NOT NULL, subject_id VARCHAR NOT NULL, taxonomy_version VARCHAR NOT NULL,
  model_id VARCHAR NOT NULL, input_hash VARCHAR NOT NULL, cache_key VARCHAR,
  assignment_status VARCHAR NOT NULL, review_status VARCHAR NOT NULL DEFAULT 'unreviewed',
  edge_class VARCHAR NOT NULL DEFAULT 'inferred', method_version VARCHAR NOT NULL,
  source_event_family_id VARCHAR, evidence_json VARCHAR NOT NULL,
  valid_from TIMESTAMPTZ NOT NULL, valid_to TIMESTAMPTZ, run_id VARCHAR NOT NULL,
  UNIQUE(canonical_event_id,version_no)
);
CREATE TABLE IF NOT EXISTS er_hierarchy_runs (
  run_id VARCHAR PRIMARY KEY, operation VARCHAR NOT NULL, model_id VARCHAR NOT NULL,
  partition_id INTEGER NOT NULL DEFAULT 0, partition_count INTEGER NOT NULL DEFAULT 1,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(), finished_at TIMESTAMPTZ,
  status VARCHAR NOT NULL, inputs_processed BIGINT DEFAULT 0,
  mappings_changed BIGINT DEFAULT 0, details_json VARCHAR
);
CREATE TABLE IF NOT EXISTS er_hierarchy_evaluation_sets (
  dataset_id VARCHAR PRIMARY KEY, dataset_kind VARCHAR NOT NULL,
  content_sha256 VARCHAR NOT NULL, description VARCHAR NOT NULL, protocol_json VARCHAR,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS er_hierarchy_labels (
  dataset_id VARCHAR NOT NULL, example_id VARCHAR NOT NULL, group_id VARCHAR NOT NULL,
  input_text VARCHAR NOT NULL, subject_id VARCHAR NOT NULL, split VARCHAR NOT NULL,
  reviewer VARCHAR, review_status VARCHAR NOT NULL, reviewed_at TIMESTAMPTZ, review_evidence_json VARCHAR,
  PRIMARY KEY(dataset_id,example_id)
);
CREATE TABLE IF NOT EXISTS er_hierarchy_evaluations (
  evaluation_id VARCHAR PRIMARY KEY, dataset_id VARCHAR NOT NULL, model_id VARCHAR NOT NULL,
  measured_at TIMESTAMPTZ NOT NULL DEFAULT now(), metrics_json VARCHAR NOT NULL,
  predictions_json VARCHAR NOT NULL, evaluation_kind VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS er_hierarchy_overrides (
  override_id VARCHAR PRIMARY KEY, canonical_event_id VARCHAR NOT NULL,
  input_hash VARCHAR NOT NULL, taxonomy_version VARCHAR NOT NULL, subject_id VARCHAR NOT NULL,
  reviewer VARCHAR NOT NULL, reason VARCHAR NOT NULL,
  valid_from TIMESTAMPTZ NOT NULL DEFAULT now(),valid_to TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS er_hierarchy_model_approvals (
  model_id VARCHAR NOT NULL, evaluation_id VARCHAR NOT NULL, reviewer VARCHAR NOT NULL,
  approved_at TIMESTAMPTZ NOT NULL DEFAULT now(), approval_notes VARCHAR NOT NULL,
  PRIMARY KEY(model_id,evaluation_id,reviewer)
);
CREATE INDEX IF NOT EXISTS er_hierarchy_mapping_event_idx ON er_hierarchy_event_subject_mappings(canonical_event_id,valid_to);
CREATE INDEX IF NOT EXISTS er_hierarchy_cache_model_idx ON er_hierarchy_prediction_cache(model_id,input_hash);

CREATE OR REPLACE VIEW er_hierarchy_current AS
SELECT m.*,s.canonical_name AS subject_name,t.theme_id,th.canonical_name AS theme_name,
       c.similarity,c.margin,c.abstained,c.reason,c.candidate_subject_id,c.output_json,
       mo.validation_status AS model_validation_status
FROM er_hierarchy_event_subject_mappings m
JOIN er_hierarchy_subjects s ON s.subject_id=m.subject_id AND s.taxonomy_version=m.taxonomy_version
JOIN er_hierarchy_subject_theme_mappings t ON t.subject_id=m.subject_id AND t.taxonomy_version=m.taxonomy_version
JOIN er_hierarchy_themes th ON th.theme_id=t.theme_id AND th.taxonomy_version=t.taxonomy_version
JOIN er_hierarchy_models mo ON mo.model_id=m.model_id
LEFT JOIN er_hierarchy_prediction_cache c ON c.cache_key=m.cache_key
WHERE m.valid_from<=now() AND m.valid_to IS NULL;

CREATE OR REPLACE VIEW er_hierarchy_review_queue AS
SELECT h.*,e.canonical_name,e.status AS event_status
FROM er_hierarchy_current h JOIN er_canonical_events e USING(canonical_event_id)
WHERE h.review_status<>'human_reviewed' AND e.status='active';
