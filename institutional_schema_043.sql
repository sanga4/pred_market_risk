-- EventRisk institutional v4.8: transactional single-flight runtime lease.
--
-- Scheduled triggers may overlap when a prior full-universe refresh runs long. This lease is an
-- operational coordination primitive only: one hot writer owns it at a time; skipped invocations
-- are auditable; crashed owners expire automatically. Source/history data remains authoritative.

CREATE TABLE IF NOT EXISTS er_runtime_leases (
  lease_name VARCHAR PRIMARY KEY,
  owner_id VARCHAR,
  acquired_at TIMESTAMPTZ,
  heartbeat_at TIMESTAMPTZ,
  expires_at TIMESTAMPTZ,
  last_released_at TIMESTAMPTZ,
  method_version VARCHAR NOT NULL DEFAULT 'runtime_singleflight_v1'
);

INSERT INTO er_runtime_leases(lease_name,method_version)
VALUES ('hot_ingestion','runtime_singleflight_v1')
ON CONFLICT(lease_name) DO NOTHING;

CREATE TABLE IF NOT EXISTS er_runtime_lease_events (
  lease_event_id VARCHAR PRIMARY KEY,
  lease_name VARCHAR NOT NULL,
  owner_id VARCHAR NOT NULL,
  event_type VARCHAR NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata VARCHAR
);

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES
(
  'runtime_singleflight','runtime_singleflight_v1','operations',
  'Hot-path single-flight lease v1',
  'Prevents overlapping institutional hot-path writers when frequent Flight triggers overlap. Lease acquisition is transactional, skipped-busy invocations are explicit, normal completion releases immediately, and killed owners become eligible for takeover after lease expiry.',
  TRUE,'institutional_runtime_lease.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
