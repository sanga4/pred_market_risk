-- EventRisk institutional v4.2: promote bounded-prefetch ingestion v11 to the core hot path.
--
-- The core path remains observed source capture -> optional archive -> strict deterministic
-- identity -> observed/identity readiness. Advisory fuzzy/ontology/graph work remains isolated.
-- v11 changes only source execution: one bounded next-chunk prefetch overlaps network latency
-- with current-chunk staging/persistence while preserving whole-source transaction semantics.

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES
(
  'runtime_topology','hot_path_v2_v11_identity','operations',
  'Layered institutional hot path v2',
  'Promoted frequent path: bounded-prefetch state-pointer full-universe CDC v11, optional raw archive flush, strict deterministic identity normalization, and layer-specific observed/identity readiness gates. Advisory fuzzy discovery, ontology and graph remain excluded from the cadence-critical path.',
  TRUE,'institutional_worker.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
