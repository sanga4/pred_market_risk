-- EventRisk institutional v2.9: indexed mapping execution with semantic candidate history.

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES (
  'event_mapping','mapping_rules_v6_indexed_history','normalization',
  'Indexed incremental canonical mapping with semantic candidate history v6',
  'Uses the persistent v5 title/rule/token index and semantic-version work queue, preserves the conservative exact-title/deadline/resolution-rule merge rules, keeps the legacy pair-level current review queue for compatibility, and records every regenerated candidate against exact left/right semantic versions. Semantic edits reopen current review while preserving prior review state in immutable history.',
  TRUE,'institutional_mapping_v6.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
