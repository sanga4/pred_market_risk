-- EventRisk institutional v4.3: set-based canonical lifecycle synchronization.
--
-- Execution-only upgrade: preserve canonical lifecycle semantics while deriving target status
-- for the full mapped universe in one grouped query and versioning only changed events inside
-- one transaction. Superseded canonicals remain reserved for identity merges.

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES
(
  'canonical_lifecycle','canonical_lifecycle_v2_set_based','normalization',
  'Set-based canonical lifecycle v2',
  'Derives active/resolved/closed canonical service lifecycle from current mapped source-contract state using one grouped SQL pass, then versions only changed canonical events transactionally. Superseded events are never overwritten.',
  TRUE,'institutional_lifecycle_v2.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
