-- EventRisk institutional v4.7: provenance-correct QA v6.
--
-- Graph-edge evidence and reviewed event-relationship evidence are separate normalized namespaces.
-- QA v6 validates reviewed graph edges against er_edge_evidence(edge_id,evidence_id) while keeping
-- er_event_relationship_evidence(relationship_id,evidence_id) for event-relationship provenance.

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES
(
  'institutional_qa','institutional_qa_v6_graph_evidence','operations',
  'Provenance-correct institutional QA v6',
  'Preserves concurrency-safe QA v5 and validates reviewed graph edges against the normalized er_edge_evidence namespace. Event-relationship evidence remains independently keyed by relationship_id.',
  TRUE,'institutional_qa_v6.py'
),
(
  'runtime_topology','enrichment_path_v3_provenance_qa','operations',
  'Asynchronous enrichment path v3',
  'Runs the advisory enrichment pipeline with QA v6 so graph-edge and event-relationship provenance are validated against their correct normalized evidence tables.',
  TRUE,'institutional_enrichment_worker_v3.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
