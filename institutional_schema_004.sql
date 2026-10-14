-- EventRisk institutional v1.2: stronger provenance and reproducible bulk releases.

CREATE TABLE IF NOT EXISTS er_contract_version_evidence (
  source_contract_id VARCHAR NOT NULL,
  version_no INTEGER NOT NULL,
  evidence_role VARCHAR NOT NULL,
  raw_snapshot_id VARCHAR NOT NULL,
  added_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (source_contract_id, version_no, evidence_role)
);

CREATE TABLE IF NOT EXISTS er_data_releases (
  release_id VARCHAR PRIMARY KEY,
  schema_version VARCHAR NOT NULL,
  taxonomy_version VARCHAR,
  mapping_method_version VARCHAR,
  generated_at TIMESTAMPTZ DEFAULT now(),
  as_of TIMESTAMPTZ NOT NULL,
  status VARCHAR NOT NULL DEFAULT 'ready',
  notes VARCHAR
);

CREATE TABLE IF NOT EXISTS er_export_manifests (
  release_id VARCHAR NOT NULL,
  dataset_name VARCHAR NOT NULL,
  format VARCHAR NOT NULL,
  object_uri VARCHAR,
  row_count BIGINT,
  min_observed_at TIMESTAMPTZ,
  max_observed_at TIMESTAMPTZ,
  content_hash VARCHAR,
  byte_size BIGINT,
  created_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (release_id, dataset_name, format)
);

CREATE OR REPLACE VIEW er_contract_lineage AS
SELECT
  v.source_contract_id,
  v.version_no,
  v.title,
  v.status,
  v.valid_from,
  v.valid_to,
  ev.evidence_role,
  ev.raw_snapshot_id,
  r.source_id,
  r.source_object_type,
  r.source_object_id,
  r.source_observed_at,
  r.received_at,
  r.content_hash
FROM er_source_contract_versions v
LEFT JOIN er_contract_version_evidence ev
  ON ev.source_contract_id=v.source_contract_id AND ev.version_no=v.version_no
LEFT JOIN er_raw_snapshots r USING(raw_snapshot_id);
