-- EventRisk institutional v2.2: operational ingestion-run recovery methodology.

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES (
  'ingestion_run_recovery','ingestion_run_recovery_v1','operations',
  'Abandoned ingestion-run recovery v1',
  'On worker/service startup, ingestion runs left in running state beyond a conservative threshold are marked abandoned, never success. Existing run details are preserved and augmented with the recovery reason, threshold and timestamp.',
  TRUE,'institutional_run_recovery.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
