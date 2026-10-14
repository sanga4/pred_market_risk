-- EventRisk institutional v1.9: point-in-time reference masters.
-- Current source/entity/instrument tables remain convenient serving tables; these version
-- ledgers preserve what EventRisk recorded over time for reproducible research releases.

CREATE TABLE IF NOT EXISTS er_source_versions (
  source_id VARCHAR NOT NULL,
  version_no INTEGER NOT NULL,
  source_name VARCHAR NOT NULL,
  source_type VARCHAR NOT NULL,
  base_url VARCHAR,
  redistribution_status VARCHAR,
  attribution_required BOOLEAN,
  active BOOLEAN,
  change_type VARCHAR NOT NULL,
  valid_from TIMESTAMPTZ NOT NULL,
  valid_to TIMESTAMPTZ,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (source_id, version_no)
);

CREATE TABLE IF NOT EXISTS er_entity_versions (
  entity_id VARCHAR NOT NULL,
  version_no INTEGER NOT NULL,
  entity_type VARCHAR NOT NULL,
  canonical_name VARCHAR NOT NULL,
  legal_name VARCHAR,
  country_code VARCHAR,
  active BOOLEAN,
  change_type VARCHAR NOT NULL,
  valid_from TIMESTAMPTZ NOT NULL,
  valid_to TIMESTAMPTZ,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (entity_id, version_no)
);

CREATE TABLE IF NOT EXISTS er_instrument_versions (
  instrument_id VARCHAR NOT NULL,
  version_no INTEGER NOT NULL,
  issuer_entity_id VARCHAR,
  instrument_type VARCHAR NOT NULL,
  canonical_name VARCHAR,
  currency VARCHAR,
  exchange_mic VARCHAR,
  active BOOLEAN,
  change_type VARCHAR NOT NULL,
  valid_from TIMESTAMPTZ NOT NULL,
  valid_to TIMESTAMPTZ,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (instrument_id, version_no)
);

-- Bootstrap what is knowable now. `recorded_at` intentionally remains migration time:
-- EventRisk must not pretend it possessed point-in-time reference versions before it did.
INSERT INTO er_source_versions(
  source_id,version_no,source_name,source_type,base_url,redistribution_status,
  attribution_required,active,change_type,valid_from
)
SELECT source_id,1,source_name,source_type,base_url,redistribution_status,
       attribution_required,active,'versioning_bootstrap',coalesce(created_at,now())
FROM er_sources s
WHERE NOT EXISTS (SELECT 1 FROM er_source_versions v WHERE v.source_id=s.source_id);

INSERT INTO er_entity_versions(
  entity_id,version_no,entity_type,canonical_name,legal_name,country_code,active,change_type,valid_from
)
SELECT entity_id,1,entity_type,canonical_name,legal_name,country_code,active,
       'versioning_bootstrap',coalesce(created_at,now())
FROM er_entities e
WHERE NOT EXISTS (SELECT 1 FROM er_entity_versions v WHERE v.entity_id=e.entity_id);

INSERT INTO er_instrument_versions(
  instrument_id,version_no,issuer_entity_id,instrument_type,canonical_name,currency,
  exchange_mic,active,change_type,valid_from
)
SELECT instrument_id,1,issuer_entity_id,instrument_type,canonical_name,currency,
       exchange_mic,active,'versioning_bootstrap',coalesce(created_at,now())
FROM er_instruments i
WHERE NOT EXISTS (SELECT 1 FROM er_instrument_versions v WHERE v.instrument_id=i.instrument_id);

CREATE INDEX IF NOT EXISTS er_source_versions_recorded_idx ON er_source_versions(source_id,recorded_at);
CREATE INDEX IF NOT EXISTS er_entity_versions_recorded_idx ON er_entity_versions(entity_id,recorded_at);
CREATE INDEX IF NOT EXISTS er_instrument_versions_recorded_idx ON er_instrument_versions(instrument_id,recorded_at);

CREATE OR REPLACE VIEW er_latest_source_versions AS
SELECT * EXCLUDE(rn) FROM (
  SELECT v.*,row_number() OVER(PARTITION BY source_id ORDER BY version_no DESC) rn
  FROM er_source_versions v
) WHERE rn=1;

CREATE OR REPLACE VIEW er_latest_entity_versions AS
SELECT * EXCLUDE(rn) FROM (
  SELECT v.*,row_number() OVER(PARTITION BY entity_id ORDER BY version_no DESC) rn
  FROM er_entity_versions v
) WHERE rn=1;

CREATE OR REPLACE VIEW er_latest_instrument_versions AS
SELECT * EXCLUDE(rn) FROM (
  SELECT v.*,row_number() OVER(PARTITION BY instrument_id ORDER BY version_no DESC) rn
  FROM er_instrument_versions v
) WHERE rn=1;
