-- EventRisk institutional v4.5: concurrency-safe enrichment QA.
--
-- The frequent ingestion lane is the sole writer of er_source_health. Enrichment QA observes
-- source-health timestamps/status and records stale-source violations only in er_quality_issues,
-- avoiding write/write conflicts while preserving the same freshness control.

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES
(
  'institutional_qa','institutional_qa_v5_concurrency_safe','operations',
  'Concurrency-safe institutional QA v5',
  'Preserves all QA v4 rules and issue-ledger semantics while making source freshness read-only with respect to er_source_health. Ingestion exclusively owns source-health state; QA records freshness violations in er_quality_issues.',
  TRUE,'institutional_qa_v5.py'
),
(
  'runtime_topology','enrichment_path_v2_concurrency_safe_qa','operations',
  'Asynchronous enrichment path v2',
  'Runs fuzzy review discovery, lifecycle, ontology, graph, reference versioning, mapping drift, QA v5 and enrichment readiness independently of the five-minute observed-data hot path. QA does not mutate ingestion-owned source-health rows.',
  TRUE,'institutional_enrichment_worker_v2.py'
)
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
