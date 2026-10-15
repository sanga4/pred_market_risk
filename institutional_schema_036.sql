-- EventRisk institutional v4.1: stage bounded prefetch full-universe CDC v11.
--
-- v11 preserves v10 transactional current-state pointers, immutable raw evidence, state-change
-- observations, semantic versioning, staged presence reconciliation and whole-source rollback.
-- It adds a single bounded prefetch slot so the next source chunk can be fetched while the
-- current chunk is staged and persisted. This overlaps network latency with database work without
-- materializing the full source universe or introducing concurrent source write transactions.

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES
(
  'source_ingestion','change_data_capture_v11_prefetched','ingestion',
  'Bounded-prefetch state-pointer full-universe CDC v11',
  'Preserves v10 source semantics and atomicity while using a one-chunk producer queue to overlap next-chunk network fetch with staging, CDC and persistence of the current chunk. Queue capacity is one chunk; full-universe traversal, raw evidence, current-state pointers and source-level rollback semantics are unchanged.',
  TRUE,'institutional_ingest_v11.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
