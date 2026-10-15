-- EventRisk institutional v4.0: promote layered runtime topology after live acceptance.
--
-- Core distribution runs observed source capture and strict deterministic identity on the
-- frequent path. Advisory fuzzy relationship discovery, lifecycle enrichment, ontology,
-- graph materialization, reference-version synchronization, mapping drift and full QA run
-- independently so advisory lag cannot block fresh observed curves or canonical identity.

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES
(
  'runtime_topology','hot_path_v1_v10_identity','operations',
  'Layered institutional hot path v1',
  'Promoted frequent path: streamed state-pointer full-universe CDC v10, optional raw archive flush, strict deterministic identity normalization, and layer-specific observed/identity readiness gates. Fuzzy discovery, ontology and graph are explicitly excluded from this cadence-critical path.',
  TRUE,'institutional_worker.py'
),
(
  'runtime_topology','enrichment_path_v1_async','operations',
  'Asynchronous institutional enrichment path v1',
  'Runs advisory fuzzy review discovery, canonical lifecycle synchronization, ontology, graph materialization, reference-version synchronization, mapping drift and full institutional QA independently from the frequent observed/identity path. Inferred relationships remain labeled and never become implicit identity merges.',
  TRUE,'institutional_enrichment_worker.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
