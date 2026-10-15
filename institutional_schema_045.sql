-- EventRisk institutional v5.0: role-aware contract evidence QA.
--
-- Source-contract versions intentionally retain two raw-evidence roles (event_payload and
-- market_payload). QA validates required roles per version rather than comparing raw evidence-row
-- cardinality to version cardinality.

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES
(
  'institutional_qa','institutional_qa_v7_role_aware_contract_evidence','operations',
  'Role-aware institutional QA v7',
  'Preserves QA v6 and validates event_payload + market_payload evidence independently for every source-contract metadata version, eliminating cardinality false positives from valid multi-role evidence.',
  TRUE,'institutional_qa_v7.py'
),
(
  'runtime_topology','enrichment_path_v4_role_aware_qa','operations',
  'Asynchronous enrichment path v4',
  'Runs advisory enrichment with QA v7: concurrency-safe source health, normalized graph provenance and role-aware source-contract evidence validation.',
  TRUE,'institutional_enrichment_worker_v4.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
