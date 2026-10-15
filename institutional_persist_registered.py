"""Persist an already-registered institutional ingestion batch.

The caller must provide TEMP tables named `_batch_raw`, `_batch_contracts` and
`_batch_observations` with the same schemas produced by institutional_ingest_v4._register_batch.
This is the v4 persistence body without a second Arrow registration/materialization step.
"""

from __future__ import annotations


def persist_registered_batch(con, *, method_version: str) -> dict:
    con.execute("SET TimeZone='UTC'")

    con.execute("""
        CREATE OR REPLACE TEMP TABLE _contract_changes AS
        SELECT b.*,
               c.source_contract_id IS NULL AS is_new,
               coalesce((SELECT max(v.version_no) FROM er_source_contract_versions v
                         WHERE v.source_contract_id=b.source_contract_id),0)+1 AS next_version
        FROM _batch_contracts b
        LEFT JOIN er_source_contracts c USING(source_contract_id)
        WHERE c.source_contract_id IS NULL
           OR c.current_title IS DISTINCT FROM b.title
           OR c.current_description IS DISTINCT FROM b.description
           OR c.status IS DISTINCT FROM b.status
           OR c.opens_at IS DISTINCT FROM b.opens_at
           OR c.closes_at IS DISTINCT FROM b.closes_at
           OR c.resolves_at IS DISTINCT FROM b.resolves_at
           OR c.resolution_authority IS DISTINCT FROM b.resolution_authority
           OR c.current_resolution_rules IS DISTINCT FROM b.resolution_rules
           OR c.source_url IS DISTINCT FROM b.source_url
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _semantic_changes AS
        SELECT b.*,
               s.source_contract_id IS NULL AS is_new_semantic,
               coalesce((SELECT max(v.version_no) FROM er_contract_semantic_versions v
                         WHERE v.source_contract_id=b.source_contract_id),0)+1 AS next_semantic_version
        FROM _batch_contracts b
        LEFT JOIN er_latest_contract_semantic_versions s USING(source_contract_id)
        WHERE s.source_contract_id IS NULL
           OR s.source_id IS DISTINCT FROM b.source_id
           OR s.source_market_id IS DISTINCT FROM b.source_market_id
           OR s.source_event_id IS DISTINCT FROM b.source_event_id
           OR s.title IS DISTINCT FROM b.title
           OR s.description IS DISTINCT FROM b.description
           OR s.outcome_type IS DISTINCT FROM b.outcome_type
           OR s.status IS DISTINCT FROM b.status
           OR s.opens_at IS DISTINCT FROM b.opens_at
           OR s.closes_at IS DISTINCT FROM b.closes_at
           OR s.resolves_at IS DISTINCT FROM b.resolves_at
           OR s.resolution_authority IS DISTINCT FROM b.resolution_authority
           OR s.resolution_rules IS DISTINCT FROM b.resolution_rules
           OR s.source_url IS DISTINCT FROM b.source_url
    """)

    con.execute("""
        INSERT INTO er_raw_snapshots(
          raw_snapshot_id,source_id,source_object_type,source_object_id,source_observed_at,
          received_at,content_hash,payload_json,ingestion_run_id)
        SELECT raw_snapshot_id,source_id,source_object_type,source_object_id,source_observed_at,
               received_at,content_hash,payload_json,ingestion_run_id FROM _batch_raw
    """)

    con.execute("""
        UPDATE er_source_contract_versions v SET valid_to=b.received_at
        FROM _contract_changes b
        WHERE v.source_contract_id=b.source_contract_id AND v.valid_to IS NULL AND b.is_new=FALSE
    """)
    con.execute("""
        INSERT INTO er_source_contract_versions(
          source_contract_id,version_no,title,description,status,resolution_rules,opens_at,closes_at,
          resolves_at,source_observed_at,received_at,valid_from,raw_snapshot_id)
        SELECT source_contract_id,next_version,title,description,status,resolution_rules,opens_at,closes_at,
               resolves_at,observed_at,received_at,received_at,market_raw_id
        FROM _contract_changes
    """)
    con.execute("""
        INSERT INTO er_contract_version_evidence(source_contract_id,version_no,evidence_role,raw_snapshot_id)
        SELECT source_contract_id,next_version,'event_payload',event_raw_id FROM _contract_changes
        UNION ALL
        SELECT source_contract_id,next_version,'market_payload',market_raw_id FROM _contract_changes
    """)

    con.execute("""
        UPDATE er_contract_semantic_versions v SET valid_to=b.received_at
        FROM _semantic_changes b
        WHERE v.source_contract_id=b.source_contract_id AND v.valid_to IS NULL AND b.is_new_semantic=FALSE
    """)
    con.execute("""
        INSERT INTO er_contract_semantic_versions(
          source_contract_id,version_no,source_id,source_market_id,source_event_id,title,description,
          outcome_type,status,opens_at,closes_at,resolves_at,resolution_authority,resolution_rules,
          source_url,source_observed_at,received_at,valid_from,event_raw_snapshot_id,
          market_raw_snapshot_id,metadata_completeness)
        SELECT source_contract_id,next_semantic_version,source_id,source_market_id,source_event_id,title,description,
               outcome_type,status,opens_at,closes_at,resolves_at,resolution_authority,resolution_rules,
               source_url,observed_at,received_at,received_at,event_raw_id,market_raw_id,'full_v2'
        FROM _semantic_changes
    """)

    con.execute("""
        INSERT INTO er_source_contracts(
          source_contract_id,source_id,source_market_id,source_event_id,current_title,current_description,
          outcome_type,status,opens_at,closes_at,resolves_at,resolution_authority,current_resolution_rules,
          source_url,first_seen_at,last_seen_at,active)
        SELECT source_contract_id,source_id,source_market_id,source_event_id,title,description,outcome_type,
               status,opens_at,closes_at,resolves_at,resolution_authority,resolution_rules,source_url,
               received_at,received_at,active
        FROM _batch_contracts
        ON CONFLICT(source_contract_id) DO UPDATE SET
          source_event_id=excluded.source_event_id,current_title=excluded.current_title,
          current_description=excluded.current_description,outcome_type=excluded.outcome_type,
          status=excluded.status,opens_at=excluded.opens_at,closes_at=excluded.closes_at,
          resolves_at=excluded.resolves_at,resolution_authority=excluded.resolution_authority,
          current_resolution_rules=excluded.current_resolution_rules,source_url=excluded.source_url,
          last_seen_at=excluded.last_seen_at,active=excluded.active
    """)

    con.execute("""
        INSERT INTO er_market_observations(
          source_id,source_contract_id,observed_at,received_at,processed_at,yes_bid,yes_ask,no_bid,no_ask,
          midpoint,last_trade,displayed_probability,volume,volume_24h,open_interest,liquidity,orderbook_depth,
          spread,market_status,raw_snapshot_id,methodology_version)
        SELECT * FROM _batch_observations
    """)

    con.execute("""
        INSERT INTO er_revisions(
          revision_id,object_type,object_id,revision_type,reason,previous_version,new_version,
          effective_at,recorded_at,actor)
        SELECT 'rev_' || md5(source_contract_id || '|contract|' || cast(next_version AS varchar) || '|' || cast(epoch_us(received_at) AS varchar)),
               'source_contract',source_contract_id,
               CASE WHEN is_new THEN 'initial_capture' ELSE 'metadata_change' END,
               'Set-based source contract capture',
               CASE WHEN is_new THEN NULL ELSE cast(next_version-1 AS varchar) END,
               cast(next_version AS varchar),received_at,received_at,?
        FROM _contract_changes
        ON CONFLICT(revision_id) DO NOTHING
    """, [method_version])
    con.execute("""
        INSERT INTO er_revisions(
          revision_id,object_type,object_id,revision_type,reason,previous_version,new_version,
          effective_at,recorded_at,actor)
        SELECT 'rev_' || md5(source_contract_id || '|semantic|' || cast(next_semantic_version AS varchar) || '|' || cast(epoch_us(received_at) AS varchar)),
               'source_contract_semantics',source_contract_id,
               CASE WHEN is_new_semantic THEN 'initial_full_semantic_capture' ELSE 'semantic_metadata_change' END,
               'Prospective full source-contract semantic ledger',
               CASE WHEN is_new_semantic THEN NULL ELSE cast(next_semantic_version-1 AS varchar) END,
               cast(next_semantic_version AS varchar),received_at,received_at,?
        FROM _semantic_changes
        ON CONFLICT(revision_id) DO NOTHING
    """, [method_version])

    changed = int(con.execute("SELECT count(*) FROM _contract_changes").fetchone()[0])
    semantic_changed = int(con.execute("SELECT count(*) FROM _semantic_changes").fetchone()[0])
    seen_ids = {r[0] for r in con.execute("SELECT source_contract_id FROM _batch_contracts").fetchall()}
    return {
        "contract_versions_created": changed,
        "semantic_versions_created": semantic_changed,
        "seen_contract_ids": seen_ids,
    }
