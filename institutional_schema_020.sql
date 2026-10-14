-- EventRisk institutional v2.5: full-universe polling with change-data capture.

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES (
  'source_ingestion','change_data_capture_v6','ingestion',
  'Parallel full-universe polling with raw/state CDC v6',
  'Fetches every supported open source universe on each run, reconciles presence from the complete contract set, reuses prior immutable raw evidence when the exact payload hash for the same source object is unchanged, and appends market observations only when served market-state fields change. Source HTTP fetches may overlap; MotherDuck writes remain serialized and transactional.',
  TRUE,'institutional_ingest_v6.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES (
  'observation_sampling','state_change_points_v1','observation_sampling',
  'Market state change-point sampling v1',
  'Suppresses flat duplicate observations when bid/ask, midpoint, last trade, displayed probability, volume, open interest, liquidity, depth, spread and market status are unchanged. Point-in-time reconstruction uses the most recent prior observation while complete ingestion runs and presence state prove polling coverage.',
  TRUE,'institutional_ingest_v6.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
