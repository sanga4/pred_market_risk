-- EventRisk institutional v3.4: cheaper indexed mapping overlap filtering.
--
-- Candidate judgement is unchanged from mapping v6. SQL still discovers exact-title and
-- bounded rare-token cross-source pair IDs. v7 removes the expensive pair-to-token-index
-- shared-token aggregation and evaluates the same meaningful-token overlap threshold from
-- the already fetched normalized pair titles in Python.

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES (
  'event_mapping','mapping_rules_v7_python_overlap','normalization',
  'Indexed semantic-version mapping v7 with Python overlap filter',
  'Preserves mapping v6 identity, deadline, resolution-rule, confidence, candidate-history and review semantics. Pair discovery remains exact-title plus bounded rare-token SQL blocking; the meaningful-token overlap threshold is computed from fetched normalized pair titles instead of a second pair-to-token-index aggregation.',
  TRUE,'institutional_mapping_v7.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
