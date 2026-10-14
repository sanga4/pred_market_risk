-- EventRisk institutional v1.5: object storage is the durable raw source of truth.
-- After a raw payload has been independently archived and hash-verified, deployments may
-- compact the duplicate analytical-database JSON body while retaining lineage metadata.

ALTER TABLE er_raw_snapshots ALTER COLUMN payload_json DROP NOT NULL;

ALTER TABLE er_raw_snapshots ADD COLUMN IF NOT EXISTS payload_storage VARCHAR DEFAULT 'database';
ALTER TABLE er_raw_snapshots ADD COLUMN IF NOT EXISTS payload_compacted_at TIMESTAMPTZ;

CREATE OR REPLACE VIEW er_raw_lineage_status AS
SELECT
  r.raw_snapshot_id,
  r.source_id,
  r.source_object_type,
  r.source_object_id,
  r.source_observed_at,
  r.received_at,
  r.content_hash,
  r.payload_storage,
  r.payload_compacted_at,
  a.archive_provider,
  a.archive_bucket,
  a.archive_key,
  a.status AS archive_status,
  a.archived_at,
  CASE
    WHEN r.payload_json IS NOT NULL THEN 'queryable_database_copy'
    WHEN a.status='archived' THEN 'archive_only_verified'
    ELSE 'payload_unavailable'
  END AS lineage_state
FROM er_raw_snapshots r
LEFT JOIN er_raw_archive_objects a USING(raw_snapshot_id);
