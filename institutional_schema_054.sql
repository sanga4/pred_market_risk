-- EventRisk institutional v5.4: cross-source bounded priming and short writer lease.
--
-- Observed/identity semantics are unchanged. v12 primes at most one bounded chunk of the next
-- source while the current source is persisted, but retains sequential source writes and v11's
-- whole-source transaction. Worker v6 releases the single-flight writer lease after ingestion,
-- archival, deterministic identity and recovery; read-only readiness runs afterward so it cannot
-- make the next five-minute writer invocation skip.

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES
(
  'ingestion_orchestration','cross_source_prime_v12','operations',
  'Bounded cross-source first-chunk priming v12',
  'Fetches at most one bounded first chunk of source N+1 while source N is persisted, then hands the primed iterator to the unchanged v11 writer. Source writes remain sequential and whole-source transactional.',
  TRUE,'institutional_ingest_v12.py'
),
(
  'runtime_topology','hot_path_v6_v12_short_writer_lease_readiness6','operations',
  'Hot path v6 with short writer lease',
  'Runs v12 observed capture, raw archive, strict deterministic identity and recovery under the single-flight writer lease; releases the lease before read-only readiness v6 so readiness latency cannot block the next scheduled writer.',
  TRUE,'institutional_worker_v6.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
