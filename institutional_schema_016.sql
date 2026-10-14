-- EventRisk institutional v2.2: scalable ingestion and lossless delivery methodology registry.

INSERT INTO er_methodology_registry(
  methodology_id,methodology_version,methodology_type,name,description,deterministic,source_code_ref
)
VALUES
  ('source_ingestion','set_based_ingest_v4','ingestion','Set-based remote warehouse ingestion v4',
   'Stages one source universe in Arrow and persists raw snapshots, current contracts, metadata versions, full semantic versions and market observations with set-based SQL. The data contract is unchanged from v3; the implementation removes per-contract remote database round trips.',TRUE,
   'institutional_ingest_v4.py'),
  ('observation_stream','compound_cursor_v2_1','distribution','Lossless observation stream cursor v2.1',
   'Orders observations by received_at, source_contract_id and observed_at and returns an opaque exclusive resume token so same-timestamp batches cannot be skipped across pages or reconnects.',TRUE,
   'institutional_stream.py'),
  ('change_feed','compound_cursor_changefeed_v1','distribution','Cursor-based institutional change feed v1',
   'REST synchronization feed using the same lossless compound cursor as streaming. Historical replay defaults to the canonical mapping EventRisk knew when each observation was received.',TRUE,
   'institutional_changefeed_api.py')
ON CONFLICT(methodology_id,methodology_version) DO NOTHING;
