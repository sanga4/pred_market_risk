-- EventRisk institutional v4.6: hot-path pre-readiness ledger recovery.
--
-- A killed prior collector can leave a run just below the abandonment threshold at startup and
-- cross it during a valid refresh. The hot path now performs a second abandoned-run recovery
-- immediately before readiness so the stuck-run control evaluates the repaired operational ledger.

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES
(
  'runtime_topology','hot_path_v3_v11_identity_recovery','operations',
  'Layered institutional hot path v3',
  'Preserves v11 full-universe capture, strict deterministic identity and layered readiness while repairing stale abandoned ingestion-run ledger rows immediately before readiness. The stuck-run control remains fail-closed after recovery.',
  TRUE,'institutional_worker_v3.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
