-- EventRisk institutional v2.0 distribution methodology registration.

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES
  ('bulk_release','institutional_exports_v3','distribution','Point-in-time bulk releases v3',
   'Builds historical releases from immutable observations and version ledgers rather than mutable current-state masters; information recorded after the release cutoff is excluded and later closure fields are masked. Release manifests disclose prospective full-semantic and reference-master coverage boundaries.',TRUE,
   'institutional_exports_v3.py')
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
