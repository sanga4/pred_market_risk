-- EventRisk institutional v1.3: point-in-time canonical metadata and methodology registry.

CREATE TABLE IF NOT EXISTS er_canonical_event_versions (
  canonical_event_id VARCHAR NOT NULL,
  version_no INTEGER NOT NULL,
  event_family_id VARCHAR,
  canonical_name VARCHAR NOT NULL,
  canonical_definition VARCHAR NOT NULL,
  subject_entity_id VARCHAR,
  predicate VARCHAR,
  object_entity_id VARCHAR,
  event_class VARCHAR,
  geography_entity_id VARCHAR,
  threshold_value DOUBLE,
  threshold_unit VARCHAR,
  measurement_name VARCHAR,
  observation_period_start TIMESTAMPTZ,
  observation_period_end TIMESTAMPTZ,
  resolution_deadline TIMESTAMPTZ,
  outcome_type VARCHAR,
  status VARCHAR,
  taxonomy_version VARCHAR,
  change_type VARCHAR NOT NULL,
  method VARCHAR NOT NULL,
  method_version VARCHAR,
  review_status VARCHAR DEFAULT 'system',
  valid_from TIMESTAMPTZ NOT NULL,
  valid_to TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (canonical_event_id, version_no)
);

CREATE TABLE IF NOT EXISTS er_event_family_versions (
  event_family_id VARCHAR NOT NULL,
  version_no INTEGER NOT NULL,
  canonical_name VARCHAR NOT NULL,
  category VARCHAR,
  subcategory VARCHAR,
  description VARCHAR,
  change_type VARCHAR NOT NULL,
  method VARCHAR NOT NULL,
  method_version VARCHAR,
  review_status VARCHAR DEFAULT 'system',
  valid_from TIMESTAMPTZ NOT NULL,
  valid_to TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (event_family_id, version_no)
);

CREATE TABLE IF NOT EXISTS er_methodology_registry (
  methodology_id VARCHAR NOT NULL,
  methodology_version VARCHAR NOT NULL,
  methodology_type VARCHAR NOT NULL,
  name VARCHAR NOT NULL,
  description VARCHAR NOT NULL,
  deterministic BOOLEAN NOT NULL,
  source_code_ref VARCHAR,
  effective_from TIMESTAMPTZ NOT NULL DEFAULT now(),
  effective_to TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (methodology_id, methodology_version)
);

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES
  ('event_ontology','event_ontology_v1','normalization','Event ontology v1',
   'Deterministic event-class and simple threshold normalization. Does not infer security or portfolio impact.',TRUE,
   'institutional_ontology.py'),
  ('event_mapping','mapping_rules_v2_blocked','mapping','Cross-source event mapping v2',
   'Token-blocked candidate generation; only strict exact semantic/deadline matches auto-merge. Other candidates require review.',TRUE,
   'institutional_mapping_v2.py'),
  ('graph_mentions','graph_rules_v1','knowledge_graph','Source-asserted mention graph v1',
   'Creates mentions edges only where source contract text explicitly names a known entity alias.',TRUE,
   'institutional_graph.py')
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;

CREATE OR REPLACE VIEW er_latest_canonical_event_versions AS
SELECT * EXCLUDE(rn)
FROM (
  SELECT v.*,row_number() OVER(PARTITION BY canonical_event_id ORDER BY version_no DESC) rn
  FROM er_canonical_event_versions v
)
WHERE rn=1;

CREATE OR REPLACE VIEW er_latest_event_family_versions AS
SELECT * EXCLUDE(rn)
FROM (
  SELECT v.*,row_number() OVER(PARTITION BY event_family_id ORDER BY version_no DESC) rn
  FROM er_event_family_versions v
)
WHERE rn=1;
