-- EventRisk institutional v5.6: provenance-backed instrument reference bridge.
--
-- This migration does not infer legal issuers or investment relationships. It creates a
-- point-in-time reference layer for instrument records sourced from an explicitly identified
-- security master. Source timestamps are preserved as metadata, while valid_from reflects when
-- EventRisk recorded the reference state so historical/as-known research cannot see future data.

CREATE TABLE IF NOT EXISTS er_reference_sync_runs (
  sync_run_id VARCHAR PRIMARY KEY,
  source_system VARCHAR NOT NULL,
  method_version VARCHAR NOT NULL,
  started_at TIMESTAMPTZ NOT NULL,
  finished_at TIMESTAMPTZ,
  status VARCHAR NOT NULL,
  records_received BIGINT DEFAULT 0,
  instruments_upserted BIGINT DEFAULT 0,
  instruments_deactivated BIGINT DEFAULT 0,
  attribute_versions_created BIGINT DEFAULT 0,
  details VARCHAR
);

CREATE TABLE IF NOT EXISTS er_instrument_reference_attributes (
  instrument_id VARCHAR PRIMARY KEY,
  source_system VARCHAR NOT NULL,
  source_ticker VARCHAR NOT NULL,
  source_name VARCHAR,
  exchange_label VARCHAR,
  asset_class VARCHAR,
  sector VARCHAR,
  industry VARCHAR,
  country VARCHAR,
  source_record_source VARCHAR,
  source_updated_at TIMESTAMPTZ,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS er_instrument_reference_attribute_versions (
  instrument_id VARCHAR NOT NULL,
  version_no INTEGER NOT NULL,
  source_system VARCHAR NOT NULL,
  source_ticker VARCHAR NOT NULL,
  source_name VARCHAR,
  exchange_label VARCHAR,
  asset_class VARCHAR,
  sector VARCHAR,
  industry VARCHAR,
  country VARCHAR,
  source_record_source VARCHAR,
  source_updated_at TIMESTAMPTZ,
  change_type VARCHAR NOT NULL,
  valid_from TIMESTAMPTZ NOT NULL,
  valid_to TIMESTAMPTZ,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (instrument_id, version_no)
);

CREATE OR REPLACE VIEW er_latest_instrument_reference_attribute_versions AS
SELECT *
FROM er_instrument_reference_attribute_versions
QUALIFY row_number() OVER (
  PARTITION BY instrument_id ORDER BY version_no DESC
)=1;

CREATE OR REPLACE VIEW er_instrument_reference_current AS
SELECT i.instrument_id,
       i.issuer_entity_id,
       i.instrument_type,
       i.canonical_name,
       i.currency,
       i.exchange_mic,
       i.active,
       a.source_system,
       a.source_ticker,
       a.source_name,
       a.exchange_label,
       a.asset_class,
       a.sector,
       a.industry,
       a.country,
       a.source_record_source,
       a.source_updated_at,
       a.recorded_at AS reference_recorded_at,
       a.updated_at AS reference_updated_at
FROM er_instruments i
JOIN er_instrument_reference_attributes a USING(instrument_id);

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES (
  'instrument_reference_bridge','eventrisk_security_master_bridge_v1','reference_data',
  'EventRisk security-master instrument bridge v1',
  'Deterministically maps the existing EventRisk security universe into institutional instrument records and ticker identifiers without inferring issuer entities. Exchange strings are retained as source labels rather than asserted as MICs. Reference-state versions begin when EventRisk records the data; source update timestamps are preserved separately.',
  TRUE,'institutional_security_master_sync.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
