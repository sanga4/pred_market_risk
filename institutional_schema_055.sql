-- EventRisk institutional v5.5: parallel independent source transactions under one hot lease.
--
-- Source semantics and per-source transaction boundaries are unchanged. Kalshi and Polymarket
-- execute the proven v11 whole-source writer concurrently on separate threads/connections while a
-- single EventRisk hot_ingestion lease prevents overlapping hot-worker generations. Strict identity
-- runs after all source futures terminate; readiness remains a separate fail-closed read-only gate.

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES
(
  'ingestion_orchestration','parallel_source_transactions_v13','operations',
  'Parallel independent source transactions v13',
  'Executes each active source through the unchanged v11 whole-source transaction concurrently on separate threads/connections. Source state remains source-keyed, transaction atomicity remains per source, and any source failure is surfaced to the hot worker as a failed cycle.',
  TRUE,'institutional_ingest_v13.py'
),
(
  'runtime_topology','hot_path_v7_parallel_sources_identity_readiness6','operations',
  'Hot path v7 parallel sources',
  'Runs v13 parallel source capture under the global single-flight hot_ingestion lease, then raw archive, strict deterministic identity and recovery. The writer lease is released before mandatory read-only observed/identity readiness v6.',
  TRUE,'institutional_worker_v7.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
