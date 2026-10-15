-- EventRisk institutional v3.5: changed-token-scoped mapping frequency calculation.
--
-- Mapping judgement is unchanged from v7. Global token rarity is still measured against the
-- entire active contract universe, but only tokens that occur in the current semantic work
-- queue are counted. This removes a full 2M+ row GROUP BY from steady-state passes while
-- preserving exact rare-token eligibility, exact-title matching, deadline/rule semantics,
-- review behavior and semantic-version provenance.

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES (
  'event_mapping','mapping_rules_v8_changed_token_frequency','normalization',
  'Indexed semantic-version mapping v8 with changed-token frequency scope',
  'Preserves mapping v7 identity, deadline, resolution-rule, confidence, review, auto-merge and semantic-version history semantics. Global active token frequency is computed only for tokens present on contracts in the current semantic work queue, avoiding an all-token aggregation on every incremental pass.',
  TRUE,'institutional_mapping_v8.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
