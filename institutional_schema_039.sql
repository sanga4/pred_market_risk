-- EventRisk institutional v4.4: layered readiness v5 completed-run basis.
--
-- A normal in-progress refresh does not invalidate the last completed good dataset. Readiness
-- evaluates the latest completed source run per active source, while active refreshes are exposed
-- separately and stuck source runs remain a critical failure.

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES
(
  'institutional_readiness','institutional_readiness_v5_completed_run_basis','operations',
  'Layered institutional readiness v5',
  'Preserves v4 lineage, identity and enrichment controls while evaluating observed distribution fitness from the latest completed source run. In-progress refreshes are informational; stale or failed completed runs and stuck refreshes remain fail-closed controls.',
  TRUE,'institutional_readiness_v5.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
