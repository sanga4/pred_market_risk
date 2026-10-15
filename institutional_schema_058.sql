-- EventRisk institutional v5.8: enrichment topology with non-inferential graph execution.

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES (
  'runtime_topology','enrichment_path_v11_no_issuer_inference','operations',
  'Asynchronous enrichment path v11 without issuer inference',
  'Preserves stable-snapshot enrichment, advisory mapping, lifecycle, ontology, reference versioning, mapping drift and fail-closed QA while requiring graph_execution_v4_no_issuer_inference. Security names cannot synthesize legal entities or issuer ownership in this topology.',
  TRUE,'institutional_enrichment_worker_v11.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
