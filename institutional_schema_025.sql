-- EventRisk institutional v3.0: restart-safe incremental graph execution.
-- Graph meaning remains graph_rules_v2_versioned_mentions; this migration changes execution only.

CREATE TABLE IF NOT EXISTS er_graph_runs (
  run_id VARCHAR PRIMARY KEY,
  execution_version VARCHAR NOT NULL,
  graph_version VARCHAR NOT NULL,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  status VARCHAR NOT NULL DEFAULT 'running',
  intervals_total BIGINT DEFAULT 0,
  intervals_processed BIGINT DEFAULT 0,
  edges_written BIGINT DEFAULT 0,
  evidence_written BIGINT DEFAULT 0,
  details VARCHAR
);

CREATE TABLE IF NOT EXISTS er_graph_interval_state (
  mapping_id VARCHAR NOT NULL,
  source_contract_id VARCHAR NOT NULL,
  semantic_version_no INTEGER NOT NULL,
  interval_hash VARCHAR NOT NULL,
  alias_fingerprint VARCHAR NOT NULL,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_run_id VARCHAR NOT NULL,
  PRIMARY KEY(mapping_id,source_contract_id,semantic_version_no)
);

CREATE TABLE IF NOT EXISTS er_graph_edge_origins (
  edge_id VARCHAR PRIMARY KEY,
  mapping_id VARCHAR NOT NULL,
  source_contract_id VARCHAR NOT NULL,
  semantic_version_no INTEGER NOT NULL,
  graph_version VARCHAR NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS er_graph_state (
  state_key VARCHAR PRIMARY KEY,
  state_value VARCHAR,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS er_graph_interval_state_active_idx
  ON er_graph_interval_state(active,source_contract_id,semantic_version_no);
CREATE INDEX IF NOT EXISTS er_graph_edge_origins_interval_idx
  ON er_graph_edge_origins(mapping_id,source_contract_id,semantic_version_no);
CREATE INDEX IF NOT EXISTS er_graph_runs_status_idx
  ON er_graph_runs(status,started_at);

CREATE OR REPLACE VIEW er_graph_current_intervals_v3 AS
SELECT
  m.mapping_id,
  m.canonical_event_id,
  m.valid_from AS mapping_from,
  m.valid_to AS mapping_to,
  v.source_contract_id,
  v.version_no AS semantic_version_no,
  v.title,
  v.description,
  v.resolution_rules,
  v.source_observed_at,
  v.valid_from AS version_from,
  v.valid_to AS version_to,
  v.source_id,
  v.event_raw_snapshot_id,
  v.market_raw_snapshot_id,
  CASE
    WHEN m.valid_from IS NULL THEN v.valid_from
    WHEN v.valid_from IS NULL THEN m.valid_from
    ELSE greatest(m.valid_from,v.valid_from)
  END AS interval_valid_from,
  CASE
    WHEN m.valid_to IS NULL AND v.valid_to IS NULL THEN NULL
    WHEN m.valid_to IS NULL THEN v.valid_to
    WHEN v.valid_to IS NULL THEN m.valid_to
    ELSE least(m.valid_to,v.valid_to)
  END AS interval_valid_to,
  md5(concat_ws(chr(31),
    coalesce(m.canonical_event_id,''),
    coalesce(cast(m.valid_from AS VARCHAR),''),coalesce(cast(m.valid_to AS VARCHAR),''),
    coalesce(v.title,''),coalesce(v.description,''),coalesce(v.resolution_rules,''),
    coalesce(cast(v.source_observed_at AS VARCHAR),''),
    coalesce(cast(v.valid_from AS VARCHAR),''),coalesce(cast(v.valid_to AS VARCHAR),''),
    coalesce(v.source_id,''),coalesce(v.event_raw_snapshot_id,''),coalesce(v.market_raw_snapshot_id,'')
  )) AS interval_hash
FROM er_event_mappings m
JOIN er_contract_semantic_versions v USING(source_contract_id)
WHERE (v.valid_to IS NULL OR v.valid_to>m.valid_from)
  AND (m.valid_to IS NULL OR m.valid_to>v.valid_from);

CREATE OR REPLACE VIEW er_graph_interval_work_queue_v3 AS
SELECT i.*,
       s.interval_hash AS processed_interval_hash,
       s.alias_fingerprint AS processed_alias_fingerprint,
       s.active AS processed_active
FROM er_graph_current_intervals_v3 i
LEFT JOIN er_graph_interval_state s
  ON s.mapping_id=i.mapping_id
 AND s.source_contract_id=i.source_contract_id
 AND s.semantic_version_no=i.semantic_version_no
WHERE s.mapping_id IS NULL
   OR s.interval_hash<>i.interval_hash
   OR s.active=FALSE;

CREATE OR REPLACE VIEW er_graph_stale_intervals_v3 AS
SELECT s.*
FROM er_graph_interval_state s
WHERE s.active=TRUE
  AND NOT EXISTS (
    SELECT 1 FROM er_graph_current_intervals_v3 i
    WHERE i.mapping_id=s.mapping_id
      AND i.source_contract_id=s.source_contract_id
      AND i.semantic_version_no=s.semantic_version_no
  );

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES (
  'graph_execution','graph_execution_v3_incremental','operations',
  'Incremental batched graph execution v3',
  'Preserves graph_rules_v2_versioned_mentions edge identity and semantics while tracking mapping/semantic intervals, processing only missing or changed intervals after the initial build, bulk-upserting edges/evidence/origins, and reprocessing all intervals only when the active alias fingerprint changes.',
  TRUE,'institutional_graph_v3.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES (
  'graph_run_recovery','graph_run_recovery_v1','operations',
  'Abandoned graph-run recovery v1',
  'Marks graph runs left running beyond a conservative timeout as abandoned. Interval state makes interrupted passes idempotently resumable without rewriting historical edges.',
  TRUE,'institutional_graph_v3.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
