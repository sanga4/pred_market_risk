-- EventRisk institutional v5.3: stable catch-up enrichment orchestration.
--
-- Enrichment remains advisory and non-blocking for core distribution. v10 waits for deterministic
-- identity readiness before starting, then re-runs the entire advisory enrichment pass if the hot
-- core advances before QA/readiness publication. No hot-path locks are acquired by enrichment.

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES
(
  'runtime_topology','enrichment_path_v10_stable_catchup_interval_qa','operations',
  'Asynchronous enrichment path v10 stable catch-up',
  'Waits for deterministic identity readiness, runs advisory mapping/lifecycle/ontology/graph/reference/drift, evaluates QA v10 on a stable snapshot, and repeats the full enrichment pass when newer core semantics arrive before readiness can be published. The hot observed/identity path is never paused or locked.',
  TRUE,'institutional_enrichment_worker_v10.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
