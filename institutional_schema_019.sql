-- EventRisk institutional v2.4: parallel source fetch with serialized transactional writes.

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES (
  'source_ingestion','parallel_fetch_serial_write_v5','ingestion',
  'Parallel source fetch / serialized set-based persistence v5',
  'Fetches independent source universes concurrently with thread-local HTTP sessions, then persists each source in deterministic caller order through the v4 set-based transactional writer. Database writes never overlap; source failures are isolated and explicitly recorded.',
  TRUE,'institutional_ingest_v5.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
