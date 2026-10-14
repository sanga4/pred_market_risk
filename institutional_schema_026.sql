-- EventRisk institutional v3.1: set-based reference/version, drift and QA execution.

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES
(
  'reference_versioning','reference_versions_v2_set_based','operations',
  'Set-based reference master versioning v2',
  'Compares current source/entity/instrument master rows to latest point-in-time versions set-wise, closes changed intervals in bulk, and inserts next versions transactionally.',
  TRUE,'institutional_reference_versions_v2.py'
),
(
  'mapping_drift','mapping_drift_v2_set_based','quality_control',
  'Set-based same-event semantic drift detection v2',
  'Queries only active cross-source members of merged canonical events, applies the unchanged strict title/deadline/resolution-rule equivalence test to those pairs, and synchronizes drift quality issues set-wise.',
  TRUE,'institutional_mapping_drift_v2.py'
),
(
  'data_quality','institutional_qa_v4_set_based','quality_control',
  'Set-based institutional QA v4',
  'Preserves existing institutional QA rule codes and severity policy while bulk-opening, reopening and resolving issues from current violation sets instead of looping through full contract, event, raw-snapshot or reference universes.',
  TRUE,'institutional_qa_v4.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
