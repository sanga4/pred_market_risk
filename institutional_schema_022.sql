-- EventRisk institutional v2.7: restart-safe set-based ontology normalization.
-- Taxonomy semantics remain event_ontology_v1; only execution and operational state change.

CREATE TABLE IF NOT EXISTS er_ontology_runs (
  run_id VARCHAR PRIMARY KEY,
  method_version VARCHAR NOT NULL,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  status VARCHAR NOT NULL DEFAULT 'running',
  events_total BIGINT DEFAULT 0,
  events_processed BIGINT DEFAULT 0,
  events_changed BIGINT DEFAULT 0,
  families_changed BIGINT DEFAULT 0,
  details VARCHAR
);

CREATE TABLE IF NOT EXISTS er_ontology_event_state (
  canonical_event_id VARCHAR PRIMARY KEY,
  input_hash VARCHAR NOT NULL,
  taxonomy_version VARCHAR NOT NULL,
  method_version VARCHAR NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_run_id VARCHAR NOT NULL
);

CREATE INDEX IF NOT EXISTS er_ontology_run_status_idx
  ON er_ontology_runs(status, started_at);
CREATE INDEX IF NOT EXISTS er_ontology_state_method_idx
  ON er_ontology_event_state(method_version, processed_at);

CREATE OR REPLACE VIEW er_ontology_work_queue_v2 AS
SELECT
  e.canonical_event_id,e.event_family_id,e.canonical_name,e.canonical_definition,
  e.event_class,e.predicate,e.threshold_value,e.threshold_unit,e.taxonomy_version,
  md5(
    coalesce(e.event_family_id,'') || chr(31) ||
    coalesce(e.canonical_name,'') || chr(31) ||
    coalesce(e.canonical_definition,'')
  ) AS input_hash,
  st.input_hash AS processed_input_hash,
  st.taxonomy_version AS processed_taxonomy_version
FROM er_canonical_events e
LEFT JOIN er_ontology_event_state st USING(canonical_event_id)
WHERE st.canonical_event_id IS NULL
   OR st.input_hash<>md5(
        coalesce(e.event_family_id,'') || chr(31) ||
        coalesce(e.canonical_name,'') || chr(31) ||
        coalesce(e.canonical_definition,'')
      )
   OR st.taxonomy_version<>'event_ontology_v1';

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES (
  'event_ontology','event_ontology_v2_batched','normalization',
  'Batched incremental event ontology execution v2',
  'Applies the unchanged event_ontology_v1 classification and threshold rules only to canonical events whose classification inputs changed. Desired classifications are computed in bounded Python batches and persisted/versioned with set-based SQL; affected event-family categories are recomputed set-wise.',
  TRUE,'institutional_ontology_v2.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES (
  'ontology_run_recovery','ontology_run_recovery_v1','operations',
  'Abandoned ontology-run recovery v1',
  'Marks ontology runs left running beyond a conservative timeout as abandoned on a later start. Partial prior work remains versioned and is safely reconciled by the input-hash work queue.',
  TRUE,'institutional_ontology_v2.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
