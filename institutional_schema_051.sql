-- EventRisk institutional v5.1: two-phase read-only QA execution.
--
-- Rule definitions, issue identifiers, severities and readiness thresholds remain unchanged.
-- Expensive validation queries execute against one read-only snapshot; issue-ledger mutation is
-- deferred until a core fingerprint barrier proves the evaluated snapshot is still publishable,
-- then synchronized in one short write transaction.

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES
(
  'institutional_qa','institutional_qa_v9_two_phase_read_snapshot','quality',
  'Institutional QA v9 two-phase snapshot evaluation',
  'Preserves the full QA v8 rule set while separating expensive read-only historical validation from quality-ledger publication. Findings are published only when source/identity fingerprints are unchanged after evaluation; otherwise QA defers without mutating issue state.',
  TRUE,'institutional_qa_v9.py'
),
(
  'runtime_topology','enrichment_path_v8_local_pairs_two_phase_qa','operations',
  'Enrichment path v8 isolated advisory mapping and two-phase QA',
  'Runs advisory relationship discovery on enrichment Flight compute, lifecycle/ontology/graph/reference/drift enrichment, then full two-phase QA with bounded stability retries. Deterministic canonical identity remains owned by the hot path.',
  TRUE,'institutional_enrichment_worker_v8.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
