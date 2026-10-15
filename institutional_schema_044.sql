-- EventRisk institutional v4.9: promote single-flight hot runtime.

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES
(
  'runtime_topology','hot_path_v4_v11_identity_singleflight','operations',
  'Layered institutional hot path v4',
  'Runs accepted v11 full-universe CDC, strict deterministic identity, pre-readiness run recovery and layered readiness under a transactional single-flight lease so overlapping schedule triggers cannot create concurrent hot writers.',
  TRUE,'institutional_worker_v4.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
