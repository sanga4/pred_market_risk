-- EventRisk institutional v5.2: promote hot runtime with single-flight + readiness v6.

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES
(
  'runtime_topology','hot_path_v5_v11_identity_singleflight_readiness6','operations',
  'Layered institutional hot path v5',
  'Runs accepted v11 full-universe CDC, strict deterministic identity and pre-readiness run recovery under the transactional single-flight lease, with readiness v6 selecting completed source runs by invocation start order to remain correct under transaction-stable database clocks.',
  TRUE,'institutional_worker_v5.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
