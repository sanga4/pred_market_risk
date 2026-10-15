-- EventRisk institutional v5.9: bulk release generation with point-in-time instrument reference attributes.

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES (
  'bulk_release','institutional_exports_v4_instrument_reference','distribution',
  'Point-in-time institutional release v4 with instrument reference attributes',
  'Extends point-in-time release v3 with source-described instrument reference attribute history and as-of snapshots. Source update timestamps remain metadata; recorded_at/valid_from controls what EventRisk knew at the requested cutoff. No issuer relationships are inferred.',
  TRUE,'institutional_exports_v4.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
