-- EventRisk institutional v5.0: isolate advisory pair discovery from the shared hot-path warehouse.
--
-- Candidate judgement remains mapping_rules_v8_changed_token_frequency. This migration records
-- only the execution change: rare-token frequency counting, pair construction and overlap
-- filtering run on enrichment Flight compute after bounded warehouse reads. Deterministic
-- canonical identity remains exclusively owned by institutional_mapping_identity_v1.

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES (
  'advisory_mapping_execution','mapping_advisory_v2_local_pair_discovery','operations',
  'Advisory mapping local pair discovery v2',
  'Preserves mapping v8 candidate/review semantics while moving expensive token-frequency and pair construction out of the shared MotherDuck warehouse and onto enrichment Flight compute. The advisory lane cannot create source-native canonical identities or auto-merge same-event candidates.',
  TRUE,'institutional_mapping_advisory_v2.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
