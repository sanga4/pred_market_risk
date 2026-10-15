-- EventRisk institutional v5.2: interval-based point-in-time mapping QA.
--
-- Preserves the QA v9 two-phase read-snapshot/publication barrier while replacing the historical
-- observation x mapping range join with an equivalent interval invariant: each contract's mapping
-- history is one continuous non-overlapping chain with exactly one open final interval, and the
-- first institutional observation is covered by that chain. Enrichment v9 owns this QA generation.

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES
(
  'institutional_qa','institutional_qa_v10_interval_mapping_coverage','quality',
  'Institutional QA v10 interval mapping coverage',
  'Preserves QA v9 snapshot isolation and rule identities while proving point-in-time mapping cardinality through continuous mapping-interval chains plus first institutional observation coverage, avoiding the full observation-by-mapping range join.',
  TRUE,'institutional_qa_v10.py'
),
(
  'runtime_topology','enrichment_path_v9_interval_qa','operations',
  'Asynchronous enrichment path v9',
  'Runs advisory relationship discovery, lifecycle, ontology, graph, reference versioning, drift checks and QA v10 independently from the frequent observed/identity hot path. Inferred and advisory relationships remain non-authoritative and explicitly labeled.',
  TRUE,'institutional_enrichment_worker_v9.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
