-- EventRisk institutional v4.9: advisory enrichment cannot mutate deterministic identity.
--
-- This schema records the operational boundary introduced after live concurrency testing:
-- source-native canonical identity and strict exact identity belong exclusively to the hot
-- deterministic identity lane. The asynchronous relationship-discovery lane may maintain its
-- candidate index and immutable review history, but it cannot create canonical events/mappings or
-- auto-accept same-event identity.

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES
(
  'mapping_advisory_execution','mapping_advisory_v1_candidate_only','normalization',
  'Advisory relationship discovery v1',
  'Reuses mapping v8 semantic-version candidate discovery while explicitly disabling source-native canonical bootstrap and automatic exact identity merges. Candidate suggestions are advisory; deterministic identity remains owned by institutional_mapping_identity_v1.',
  TRUE,'institutional_mapping_advisory_v1.py'
),
(
  'runtime_topology','enrichment_path_v6_advisory_only_snapshot_qa','operations',
  'Asynchronous enrichment path v6',
  'Runs advisory-only relationship candidate discovery, lifecycle, ontology, graph, reference versions, drift and snapshot-safe QA. The enrichment lane is prohibited from mutating deterministic canonical identity.',
  TRUE,'institutional_enrichment_worker_v6.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
