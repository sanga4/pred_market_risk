-- EventRisk institutional v3.3: direct-temp CDC and set-based presence reconciliation.

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES
(
  'source_ingestion','change_data_capture_v8_direct_temp','ingestion',
  'Direct-temp bounded full-universe CDC v8',
  'Preserves complete-universe polling, raw evidence reuse, semantic versioning and state-change observations while staging each bounded chunk once, applying CDC directly to DuckDB TEMP relations and persisting those filtered relations without a second Arrow materialization. Native Arrow memory is released after each chunk and source writes remain atomic.',
  TRUE,'institutional_ingest_v8.py'
),
(
  'presence_reconciliation','presence_reconciliation_v2_set_based','ingestion',
  'Set-based source presence reconciliation v2',
  'Preserves the repeated-miss presence state machine and transition audit semantics while bulk-upserting observed/missing state and bulk-inserting reactivation, first-miss and deactivation transitions instead of issuing one remote insert per changed contract.',
  TRUE,'institutional_presence_v2.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
