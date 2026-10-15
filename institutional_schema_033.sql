-- EventRisk institutional v3.8: transactional current-state pointers for bounded CDC.
--
-- Immutable raw snapshots and market observations remain the historical source of truth. These
-- tables are materialized current-state accelerators only: they let each poll compare against the
-- latest known raw payload / market state with keyed joins instead of repeatedly window-scanning
-- the entire append-only history. They are updated inside the same source transaction as history.

CREATE TABLE IF NOT EXISTS er_raw_current_snapshots (
  source_id VARCHAR NOT NULL,
  source_object_type VARCHAR NOT NULL,
  source_object_id VARCHAR NOT NULL,
  raw_snapshot_id VARCHAR NOT NULL,
  content_hash VARCHAR,
  current_since TIMESTAMPTZ NOT NULL,
  last_seen_at TIMESTAMPTZ NOT NULL,
  last_ingestion_run_id VARCHAR,
  PRIMARY KEY (source_id, source_object_type, source_object_id)
);

INSERT INTO er_raw_current_snapshots(
  source_id,source_object_type,source_object_id,raw_snapshot_id,content_hash,
  current_since,last_seen_at,last_ingestion_run_id
)
SELECT source_id,source_object_type,source_object_id,raw_snapshot_id,content_hash,
       received_at,received_at,ingestion_run_id
FROM er_raw_snapshots
QUALIFY row_number() OVER (
  PARTITION BY source_id,source_object_type,source_object_id
  ORDER BY received_at DESC,raw_snapshot_id DESC
)=1
ON CONFLICT(source_id,source_object_type,source_object_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS er_market_current_state (
  source_contract_id VARCHAR PRIMARY KEY,
  source_id VARCHAR NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL,
  received_at TIMESTAMPTZ NOT NULL,
  yes_bid DOUBLE,
  yes_ask DOUBLE,
  no_bid DOUBLE,
  no_ask DOUBLE,
  midpoint DOUBLE,
  last_trade DOUBLE,
  displayed_probability DOUBLE,
  volume DOUBLE,
  volume_24h DOUBLE,
  open_interest DOUBLE,
  liquidity DOUBLE,
  orderbook_depth DOUBLE,
  spread DOUBLE,
  market_status VARCHAR,
  raw_snapshot_id VARCHAR,
  methodology_version VARCHAR,
  last_polled_at TIMESTAMPTZ NOT NULL,
  last_ingestion_run_id VARCHAR
);

INSERT INTO er_market_current_state(
  source_contract_id,source_id,observed_at,received_at,yes_bid,yes_ask,no_bid,no_ask,
  midpoint,last_trade,displayed_probability,volume,volume_24h,open_interest,liquidity,
  orderbook_depth,spread,market_status,raw_snapshot_id,methodology_version,last_polled_at,
  last_ingestion_run_id
)
SELECT o.source_contract_id,o.source_id,o.observed_at,o.received_at,o.yes_bid,o.yes_ask,o.no_bid,o.no_ask,
       o.midpoint,o.last_trade,o.displayed_probability,o.volume,o.volume_24h,o.open_interest,o.liquidity,
       o.orderbook_depth,o.spread,o.market_status,o.raw_snapshot_id,o.methodology_version,o.received_at,
       r.ingestion_run_id
FROM er_latest_market_observations o
LEFT JOIN er_raw_snapshots r ON r.raw_snapshot_id=o.raw_snapshot_id
ON CONFLICT(source_contract_id) DO NOTHING;

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES
(
  'cdc_current_state','cdc_current_state_v1','ingestion',
  'Transactional current-state pointers v1',
  'Maintains keyed current raw-payload and current market-state tables inside the same transaction as immutable history. Current tables are accelerators only; append-only raw snapshots and market observations remain authoritative for historical and point-in-time reconstruction.',
  TRUE,'institutional_schema_033.sql'
),
(
  'source_ingestion','change_data_capture_v10_stateful','ingestion',
  'Streamed state-pointer full-universe CDC v10',
  'Preserves v9 streamed bounded collection, atomic source rollback, immutable raw evidence, semantic versioning, state-change observation sampling and staged presence reconciliation while replacing per-chunk historical latest-state scans with transactional keyed current-state pointers.',
  TRUE,'institutional_ingest_v10.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
