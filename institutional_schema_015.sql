-- EventRisk institutional v2.1: full-universe QA/readiness methodology registry.

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES
  ('data_quality','institutional_qa_v3','quality','Institutional full-universe and durability QA v3',
   'Extends semantic/temporal QA with latest full-universe run completeness, conservative contract-presence coherence, raw-evidence durability and stuck-ingestion detection. It opens issues but never rewrites source observations.',TRUE,
   'institutional_qa_v3.py'),
  ('distribution_readiness','institutional_readiness_v3','quality','Institutional distribution readiness v3',
   'Fail-closed distribution gate requiring complete full-universe source runs, coherent presence state, recoverable raw evidence, point-in-time reconstructability and the prior v2 semantic/provenance controls.',TRUE,
   'institutional_readiness_v3.py'),
  ('canonical_lifecycle','canonical_lifecycle_v1','normalization','Canonical event lifecycle synchronization v1',
   'Versions canonical serving status from mapped source-contract state without predicting outcomes. Superseded identity remains separate from source lifecycle and is never overwritten.',TRUE,
   'institutional_lifecycle.py')
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
