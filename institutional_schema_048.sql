-- EventRisk institutional v4.8: concurrency-safe enrichment QA.
--
-- QA v8 preserves all existing substantive controls but only evaluates a transaction snapshot
-- when deterministic identity is internally caught up. It also evaluates source completeness from
-- the latest completed source invocation so a normal in-progress hot refresh cannot generate a
-- false enrichment failure.

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES
(
  'institutional_qa','institutional_qa_v8_snapshot_guard_completed_runs','quality',
  'Snapshot-safe institutional QA v8',
  'Runs the full role-aware institutional QA suite only on an identity-consistent transaction snapshot and evaluates source completeness using the latest completed run by started_at. Mid-transition identity snapshots are deferred without mutating quality issues.',
  TRUE,'institutional_qa_v8.py'
),
(
  'runtime_topology','enrichment_path_v5_snapshot_guarded_qa','operations',
  'Asynchronous enrichment path v5',
  'Preserves fuzzy mapping, lifecycle, ontology, graph, reference-version, drift and full QA stages while adding bounded retry around QA snapshots that overlap the independently scheduled observed/identity hot path.',
  TRUE,'institutional_enrichment_worker_v5.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
