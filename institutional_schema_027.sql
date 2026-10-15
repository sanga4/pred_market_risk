-- EventRisk institutional v3.2: bounded-memory full-universe change-data capture.

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES (
  'source_ingestion','change_data_capture_v7_bounded','ingestion',
  'Bounded-memory full-universe CDC v7',
  'Preserves v6 raw-evidence reuse, market-state change-point sampling, complete-universe presence reconciliation and transactional source writes while processing one source at a time in bounded chunks. Processed source objects, temporary relations and Arrow relations are released between chunks to prevent full-universe duplicate materialization from exceeding worker memory.',
  TRUE,'institutional_ingest_v7.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
