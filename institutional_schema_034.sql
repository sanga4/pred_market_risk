-- EventRisk institutional v3.9: layer-specific distribution readiness.
--
-- Readiness is separated by product boundary so slow enrichment never masks the condition of the
-- observed market-data layer. `observed` covers Layer 1, `identity` covers deterministic Layer 2
-- canonicalization, and `enrichment` covers advisory/derived normalization such as fuzzy review,
-- ontology and graph materialization.

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES (
  'distribution_readiness','institutional_readiness_v4_layered','quality',
  'Layer-specific institutional distribution readiness v4',
  'Reports independent readiness for observed source data, deterministic canonical identity, and advisory enrichment. Core distribution readiness requires observed plus deterministic identity only; fuzzy review/ontology/graph enrichment is reported separately and cannot make the observed feed appear stale.',
  TRUE,'institutional_readiness_v4.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
