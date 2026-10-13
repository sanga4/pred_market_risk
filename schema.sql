CREATE TABLE IF NOT EXISTS market_metadata (
  venue VARCHAR NOT NULL,
  market_id VARCHAR NOT NULL,
  event_id VARCHAR,
  condition_id VARCHAR,
  slug VARCHAR,
  yes_token_id VARCHAR,
  no_token_id VARCHAR,
  liquidity DOUBLE,
  description VARCHAR,
  active BOOLEAN DEFAULT TRUE,
  source_updated_at TIMESTAMPTZ,
  fetched_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (venue, market_id)
);

CREATE TABLE IF NOT EXISTS canonical_events (
  canonical_event_id VARCHAR PRIMARY KEY,
  venue VARCHAR NOT NULL,
  source_event_id VARCHAR NOT NULL,
  title VARCHAR NOT NULL,
  category VARCHAR DEFAULT 'geopolitics',
  region VARCHAR DEFAULT 'global',
  theme VARCHAR DEFAULT 'other',
  eligible BOOLEAN DEFAULT TRUE,
  methodology_version VARCHAR DEFAULT 'rules_v1',
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS event_markets (
  canonical_event_id VARCHAR NOT NULL,
  venue VARCHAR NOT NULL,
  market_id VARCHAR NOT NULL,
  outcome_label VARCHAR,
  close_time TIMESTAMPTZ,
  weight DOUBLE DEFAULT 1.0,
  updated_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (canonical_event_id, venue, market_id)
);

CREATE TABLE IF NOT EXISTS market_features (
  venue VARCHAR NOT NULL,
  market_id VARCHAR NOT NULL,
  event_id VARCHAR,
  event_title VARCHAR,
  market_title VARCHAR,
  close_time TIMESTAMPTZ,
  latest_ts TIMESTAMPTZ,
  prob DOUBLE,
  prob_1h DOUBLE,
  prob_24h DOUBLE,
  delta_1h DOUBLE,
  delta_24h DOUBLE,
  yes_bid DOUBLE,
  yes_ask DOUBLE,
  spread DOUBLE,
  last DOUBLE,
  volume DOUBLE,
  volume_24h DOUBLE,
  updated_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (venue, market_id)
);

CREATE TABLE IF NOT EXISTS probability_history (
  venue VARCHAR NOT NULL,
  market_id VARCHAR NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  prob DOUBLE NOT NULL,
  source VARCHAR NOT NULL,
  resolution_minutes INTEGER,
  inserted_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (venue, market_id, ts, source)
);

CREATE TABLE IF NOT EXISTS securities (
  ticker VARCHAR PRIMARY KEY,
  name VARCHAR,
  exchange VARCHAR,
  quote_type VARCHAR,
  asset_class VARCHAR DEFAULT 'equity',
  sector VARCHAR,
  industry VARCHAR,
  country VARCHAR,
  tracked BOOLEAN DEFAULT FALSE,
  source VARCHAR,
  last_requested_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ticker_prices (
  ticker VARCHAR NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  interval VARCHAR NOT NULL,
  open DOUBLE,
  high DOUBLE,
  low DOUBLE,
  close DOUBLE,
  volume DOUBLE,
  source VARCHAR NOT NULL,
  inserted_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (ticker, ts, interval, source)
);

CREATE TABLE IF NOT EXISTS event_security_map (
  canonical_event_id VARCHAR NOT NULL,
  ticker VARCHAR NOT NULL,
  relevance DOUBLE NOT NULL,
  exposure_sign DOUBLE,
  reason VARCHAR,
  methodology_version VARCHAR DEFAULT 'rules_v1',
  updated_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (canonical_event_id, ticker)
);

CREATE TABLE IF NOT EXISTS market_security_coefficients (
  venue VARCHAR NOT NULL,
  market_id VARCHAR NOT NULL,
  ticker VARCHAR NOT NULL,
  beta_per_1pp DOUBLE,
  standard_error DOUBLE,
  t_stat DOUBLE,
  r_squared DOUBLE,
  n_obs INTEGER,
  sample_start TIMESTAMPTZ,
  sample_end TIMESTAMPTZ,
  confidence VARCHAR,
  model_version VARCHAR,
  updated_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (venue, market_id, ticker)
);

CREATE TABLE IF NOT EXISTS model_runs (
  run_id VARCHAR PRIMARY KEY,
  job_type VARCHAR NOT NULL,
  started_at TIMESTAMPTZ NOT NULL,
  finished_at TIMESTAMPTZ,
  status VARCHAR NOT NULL,
  details VARCHAR
);

CREATE TABLE IF NOT EXISTS pipeline_state (
  key VARCHAR PRIMARY KEY,
  value VARCHAR,
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE OR REPLACE VIEW probability_history_all AS
SELECT venue, market_id, ts, prob, 'collector' AS source
FROM ticks
WHERE prob IS NOT NULL
UNION ALL
SELECT venue, market_id, ts, prob, source
FROM probability_history;

CREATE OR REPLACE VIEW ticker_event_feed AS
SELECT
  esm.ticker,
  ce.canonical_event_id,
  ce.title AS event_title,
  ce.category,
  ce.region,
  ce.theme,
  em.market_id,
  mf.market_title,
  mf.close_time,
  mf.latest_ts,
  mf.prob,
  mf.delta_1h,
  mf.delta_24h,
  mf.volume,
  mf.volume_24h,
  mf.spread,
  esm.relevance,
  esm.exposure_sign,
  esm.reason,
  msc.beta_per_1pp,
  msc.standard_error,
  msc.t_stat,
  msc.r_squared,
  msc.n_obs,
  msc.confidence,
  msc.model_version,
  CASE
    WHEN msc.beta_per_1pp IS NOT NULL AND mf.delta_24h IS NOT NULL
    THEN msc.beta_per_1pp * (mf.delta_24h * 100.0)
    ELSE NULL
  END AS estimated_24h_return
FROM event_security_map esm
JOIN canonical_events ce USING (canonical_event_id)
JOIN event_markets em USING (canonical_event_id)
JOIN market_features mf ON mf.venue = em.venue AND mf.market_id = em.market_id
LEFT JOIN market_security_coefficients msc
  ON msc.venue = em.venue AND msc.market_id = em.market_id AND msc.ticker = esm.ticker
WHERE ce.eligible = TRUE;
