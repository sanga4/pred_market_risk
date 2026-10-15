-- EventRisk institutional v3.6: streamed bounded source ingestion and staged presence IDs.

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES
(
  'source_ingestion','change_data_capture_v9_streamed','ingestion',
  'Streamed bounded full-universe CDC v9',
  'Preserves v8 immutable raw-evidence reuse, semantic versioning, state-change observations and whole-source transaction rollback while consuming source adapters in bounded chunks instead of materializing the full source universe in Python memory.',
  TRUE,'institutional_ingest_v9.py'
),
(
  'presence_reconciliation','presence_reconciliation_v3_staged_seen','ingestion',
  'Staged-set source presence reconciliation v3',
  'Preserves the v2 repeated-miss, deactivation and reactivation state machine while accumulating observed contract IDs directly in a MotherDuck TEMP table during ingestion rather than round-tripping the complete ID set through Python.',
  TRUE,'institutional_presence_v3.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
