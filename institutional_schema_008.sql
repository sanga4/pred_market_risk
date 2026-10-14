-- EventRisk institutional v1.6: efficient immutable raw archive bundles.
-- Many raw snapshots from one ingestion run can share one verified tar.gz object while each
-- snapshot retains its own member path and raw-payload hash.

ALTER TABLE er_raw_archive_objects ADD COLUMN IF NOT EXISTS archive_member VARCHAR;
ALTER TABLE er_raw_archive_objects ADD COLUMN IF NOT EXISTS archive_bundle_hash VARCHAR;
ALTER TABLE er_raw_archive_objects ADD COLUMN IF NOT EXISTS bundle_byte_size BIGINT;
ALTER TABLE er_raw_archive_objects ADD COLUMN IF NOT EXISTS ingestion_run_id VARCHAR;

CREATE INDEX IF NOT EXISTS er_raw_snapshots_run_idx
  ON er_raw_snapshots(ingestion_run_id, received_at);

CREATE OR REPLACE VIEW er_raw_lineage_status AS
SELECT
  r.raw_snapshot_id,
  r.source_id,
  r.source_object_type,
  r.source_object_id,
  r.source_observed_at,
  r.received_at,
  r.content_hash,
  r.ingestion_run_id,
  r.payload_storage,
  r.payload_compacted_at,
  a.archive_provider,
  a.archive_bucket,
  a.archive_key,
  a.archive_member,
  a.archive_bundle_hash,
  a.status AS archive_status,
  a.archived_at,
  CASE
    WHEN r.payload_json IS NOT NULL THEN 'queryable_database_copy'
    WHEN a.status='archived' THEN 'archive_only_verified'
    ELSE 'payload_unavailable'
  END AS lineage_state
FROM er_raw_snapshots r
LEFT JOIN er_raw_archive_objects a USING(raw_snapshot_id);
