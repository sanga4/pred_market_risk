-- EventRisk institutional v5.1: transaction-stable-clock-safe source-run readiness.
--
-- MotherDuck/DuckDB now() is transaction-stable. Readiness therefore orders completed source runs
-- by started_at/run_id rather than finished_at, so a later recovery timestamp on an older abandoned
-- run cannot eclipse a newer successful full-universe refresh.

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES
(
  'institutional_readiness','institutional_readiness_v6_started_run_basis','operations',
  'Institutional layered readiness v6',
  'Preserves layered readiness and completed-run distribution semantics while selecting the newest completed source invocation by started_at and using started_at for freshness, avoiding transaction-stable finished_at ordering errors after stale-run recovery.',
  TRUE,'institutional_readiness_v6.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
