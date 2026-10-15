-- EventRisk institutional v1.8: methodology registry for v3 identity mapping,
-- point-in-time curves, semantic QA/readiness and reproducible bulk distribution.

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES
  ('event_mapping','mapping_rules_v3_reviewable','mapping','Cross-source event mapping v3',
   'Token-blocked deterministic candidate generation with exact identity merges only when normalized title, deadline and full resolution rules agree. Human review may create explicitly typed event relationships without merging canonical identity.',TRUE,
   'institutional_mapping_v3.py'),
  ('event_curve','event_curve_v1_7','normalization','Point-in-time event curves v1.7',
   'Exposes source-native observations under either the mapping known when EventRisk received each row or the current canonical taxonomy. Does not impose a consensus probability.',TRUE,
   'institutional_curve_api.py'),
  ('data_quality','institutional_qa_v2','quality','Institutional semantic and temporal QA v2',
   'Checks lineage, archive integrity, source freshness, version windows, point-in-time mapping cardinality, canonical event coverage and reviewed relationship provenance without silently repairing source data.',TRUE,
   'institutional_qa_v2.py'),
  ('distribution_readiness','institutional_readiness_v2','quality','Institutional distribution readiness v2',
   'Fail-closed distribution gate covering source freshness, raw archive coverage, point-in-time reconstructability, canonical mapping integrity and reviewed relationship provenance.',TRUE,
   'institutional_readiness_v2.py'),
  ('bulk_release','institutional_exports_v2','distribution','Point-in-time bulk releases v2',
   'Creates as-known Parquet releases using receipt/recording timestamps, verifies every dataset hash, and publishes a separately hashed canonical manifest.',TRUE,
   'institutional_exports_v2.py')
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
