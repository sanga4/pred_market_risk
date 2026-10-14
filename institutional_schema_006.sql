-- EventRisk institutional v1.4: mapping-review semantics.

ALTER TABLE er_mapping_candidates ADD COLUMN IF NOT EXISTS resolution_similarity DOUBLE;
ALTER TABLE er_mapping_candidates ADD COLUMN IF NOT EXISTS resolution_compatible BOOLEAN;
ALTER TABLE er_mapping_candidates ADD COLUMN IF NOT EXISTS rules_available_both BOOLEAN;

CREATE OR REPLACE VIEW er_pending_mapping_review AS
SELECT
  mc.*,
  l.source_id AS left_source,
  l.current_title AS left_title,
  l.current_resolution_rules AS left_resolution_rules,
  l.closes_at AS left_close,
  r.source_id AS right_source,
  r.current_title AS right_title,
  r.current_resolution_rules AS right_resolution_rules,
  r.closes_at AS right_close
FROM er_mapping_candidates mc
JOIN er_source_contracts l ON l.source_contract_id=mc.left_source_contract_id
JOIN er_source_contracts r ON r.source_contract_id=mc.right_source_contract_id
WHERE mc.status='pending';
