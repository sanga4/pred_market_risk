-- Independent raw-payload archive bookkeeping.
-- Raw JSON remains in er_raw_snapshots for queryability; this table tracks immutable
-- object-storage copies so analytical database loss/rebuild does not destroy provenance.

CREATE TABLE IF NOT EXISTS er_raw_archive_objects (
  raw_snapshot_id VARCHAR PRIMARY KEY,
  archive_provider VARCHAR NOT NULL,
  archive_bucket VARCHAR NOT NULL,
  archive_key VARCHAR NOT NULL,
  content_hash VARCHAR NOT NULL,
  byte_size BIGINT,
  compression VARCHAR DEFAULT 'gzip',
  archived_at TIMESTAMPTZ DEFAULT now(),
  status VARCHAR DEFAULT 'archived',
  error_message VARCHAR
);

CREATE OR REPLACE VIEW er_unarchived_raw_snapshots AS
SELECT r.*
FROM er_raw_snapshots r
LEFT JOIN er_raw_archive_objects a USING(raw_snapshot_id)
WHERE a.raw_snapshot_id IS NULL OR a.status <> 'archived';
