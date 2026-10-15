-- EventRisk institutional v5.7: graph execution without synthetic issuer inference.

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES (
  'graph_execution','graph_execution_v4_no_issuer_inference','knowledge_graph',
  'Incremental graph execution v4 without issuer inference',
  'Preserves graph_rules_v2_versioned_mentions, interval provenance and explicit entity-alias mention detection while removing the legacy path that could synthesize legal entities and issuer links from security names. The institutional instrument master is read for coverage only; issuer/entity relationships require explicit sourced or separately labeled inferred/reviewed evidence.',
  TRUE,'institutional_graph_v4.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
