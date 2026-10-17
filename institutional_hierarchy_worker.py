"""Bounded, resumable semantic inference and transactional event→subject materialization.

All source contracts, canonical identities and legacy event-family classifications
remain unchanged. Inference workers can use disjoint input-hash partitions; projection
is a single writer and uses [valid_from, valid_to) intervals from processing time.
"""
from __future__ import annotations

import argparse
import json
import os
import time
import uuid

from db import connect
from institutional_runtime_lease import acquire_runtime_lease, release_runtime_lease
from institutional_semantic_model import ALGORITHM_VERSION, canonical_json, digest, manifest


def _rows(cur):
    return [dict(zip([c[0] for c in cur.description], row)) for row in cur.fetchall()]


def _lease(name):
    con = connect()
    try:
        con.execute("INSERT INTO er_runtime_leases(lease_name) VALUES (?) ON CONFLICT DO NOTHING", [name])
    finally:
        con.close()
    lease = acquire_runtime_lease(name, ttl_seconds=900)
    if not lease.acquired:
        raise RuntimeError(f"Hierarchy lane is already running: {name}")
    return lease


def _finish(con, _run_id, status, **details):
    con.execute("""UPDATE er_hierarchy_runs SET finished_at=now(),status=?,
        inputs_processed=?,mappings_changed=?,details_json=? WHERE run_id=?""", [
        status, details.get("inputs_processed", 0), details.get("mappings_changed", 0),
        canonical_json(details), _run_id,
    ])


def recover_stopped(records):
    """Operator recovery after separately verifying Flight terminal status.

    Never expire a live worker by age alone. Exact owner/run matching protects any
    replacement process that acquired the lane while recovery was being prepared.
    """
    con = connect()
    recovered = []
    try:
        con.execute("BEGIN")
        try:
            for record in records:
                if record.get("confirmed_terminal") is not True or record.get("terminal_status") not in {"CANCELLED", "FAILED"} or not isinstance(record.get("flight_run_number"), int):
                    raise ValueError("Verify and record the terminal Flight run before recovering its lease")
                row = con.execute("SELECT partition_id,partition_count,model_id,status FROM er_hierarchy_runs WHERE run_id=? AND operation='inference'", [record["run_id"]]).fetchone()
                if not row:
                    raise ValueError("Unknown inference run")
                lane = f"hierarchy_inference_{row[1]}_{row[0]}"
                owner = con.execute("SELECT owner_id FROM er_runtime_leases WHERE lease_name=?", [lane]).fetchone()
                if not owner or owner[0] is None:
                    continue
                if owner[0] != record["expected_owner_id"] or row[3] != "running":
                    raise ValueError("Lease/run changed; inspect the replacement owner before recovery")
                chars = "".join(c for i,c in enumerate("0123456789abcdef") if i % row[1] == row[0])
                cached = con.execute("SELECT count(*) FROM er_hierarchy_prediction_cache WHERE model_id=? AND contains(?,substr(input_hash,1,1))", [row[2],chars]).fetchone()[0]
                con.execute("""UPDATE er_runtime_leases SET owner_id=NULL,acquired_at=NULL,heartbeat_at=NULL,
                    expires_at=NULL,last_released_at=now() WHERE lease_name=? AND owner_id=?""", [lane,record["expected_owner_id"]])
                con.execute("""INSERT INTO er_runtime_lease_events(lease_event_id,lease_name,owner_id,event_type,occurred_at,metadata)
                    VALUES (?,?,?,'stopped_worker_recovered',now(),?)""", ["leaseevt_"+uuid.uuid4().hex,lane,record["expected_owner_id"],canonical_json(record)])
                _finish(con,record["run_id"],"interrupted",inputs_processed=cached,recovery=record)
                recovered.append(record["run_id"])
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK"); raise
        return {"recovered_runs": recovered}
    finally:
        con.close()


def infer(*, max_rows=50000, partition_id=0, partition_count=1, classifier=None, max_seconds=420):
    if partition_count not in {1, 2, 4, 8, 16} or not 0 <= partition_id < partition_count:
        raise ValueError("Use a valid input-hash partition: count=1,2,4,8,16")
    if not 1 <= max_rows <= 250000:
        raise ValueError("max_rows must be 1–250000")
    if not 1 <= max_seconds <= 480:
        raise ValueError("Inference budget must be 1–480 seconds within the Flight timeout")
    model = manifest()
    model_id = model["model_id"]
    lease = _lease(f"hierarchy_inference_{partition_count}_{partition_id}")
    con = connect()
    run_id = "hrun_" + uuid.uuid4().hex
    started = time.monotonic()
    processed = 0
    try:
        if not con.execute("SELECT 1 FROM er_hierarchy_models WHERE model_id=?", [model_id]).fetchone():
            raise RuntimeError("Initialize and register this pinned model before inference")
        con.execute("""INSERT INTO er_hierarchy_runs(run_id,operation,model_id,partition_id,partition_count,status)
            VALUES (?,'inference',?,?,?,'running')""", [run_id, model_id, partition_id, partition_count])
        chars = "".join(c for i, c in enumerate("0123456789abcdef") if i % partition_count == partition_id)
        pending = _rows(con.execute("""
            WITH inputs AS (
              SELECT DISTINCT sha256(canonical_name) AS input_hash,canonical_name AS input_text
              FROM er_canonical_events
            )
            SELECT i.* FROM inputs i
            LEFT JOIN er_hierarchy_prediction_cache c ON c.model_id=? AND c.input_hash=i.input_hash
            WHERE c.cache_key IS NULL AND contains(?,substr(i.input_hash,1,1))
            ORDER BY i.input_hash LIMIT ?
        """, [model_id, chars, max_rows]))
        if pending and classifier is None:
            from institutional_semantic_model import SemanticClassifier
            classifier = SemanticClassifier()
        import pyarrow as pa
        prior_batch_seconds = 0
        for begin in range(0, len(pending), 5000):
            if processed and time.monotonic() - started + prior_batch_seconds * 1.5 >= max_seconds:
                break
            batch_started = time.monotonic()
            batch = pending[begin:begin + 5000]
            outputs = classifier.predict([item["input_text"] for item in batch])
            inference_seconds = time.monotonic() - batch_started
            if len(outputs) != len(batch):
                raise RuntimeError("Classifier output cardinality mismatch")
            staged = []
            for item, output in zip(batch, outputs):
                staged.append({
                    "cache_key": digest(model_id + "|" + item["input_hash"]), "model_id": model_id,
                    **item, **{k: output[k] for k in ["subject_id", "theme_id", "candidate_subject_id", "similarity", "margin", "abstained", "reason"]},
                    "output_json": canonical_json(output),
                })
            con.register("_hierarchy_predictions", pa.Table.from_pylist(staged))
            try:
                # ON CONFLICT is not permission to overwrite a prior prediction.
                conflicts = con.execute("""SELECT count(*) FROM _hierarchy_predictions p
                    JOIN er_hierarchy_prediction_cache c USING(cache_key)
                    WHERE c.subject_id<>p.subject_id OR c.theme_id<>p.theme_id OR c.input_hash<>p.input_hash""").fetchone()[0]
                if conflicts:
                    raise RuntimeError("Immutable inference-cache conflict; model or runtime drift requires investigation")
                for attempt in range(3):
                    try:
                        con.execute("INSERT INTO er_hierarchy_prediction_cache BY NAME SELECT * FROM _hierarchy_predictions ON CONFLICT DO NOTHING")
                        break
                    except Exception as exc:
                        if attempt == 2 or "conflict" not in str(exc).lower():
                            raise
                        time.sleep(0.5 * (attempt + 1))
            finally:
                con.unregister("_hierarchy_predictions")
            processed += len(batch)
            prior_batch_seconds = time.monotonic() - batch_started
            print(canonical_json({"run_id": run_id, "partition": partition_id, "processed": processed, "selected": len(pending),
                "batch_inference_seconds": round(inference_seconds,3),
                "batch_persistence_seconds": round(prior_batch_seconds-inference_seconds,3),
                "elapsed_seconds": round(time.monotonic()-started,3)}), flush=True)
        result = {"run_id": run_id, "model_id": model_id, "inputs_processed": processed,
                  "inputs_selected": len(pending), "selection_complete": processed == len(pending),
                  "implementation_git_ref": os.environ.get("EVENTRISK_GIT_REF"),
                  "elapsed_seconds": round(time.monotonic() - started, 3),
                  "titles_per_second": round(processed / max(time.monotonic() - started, 1e-6), 3)}
        _finish(con, run_id, "success", **result)
        return result
    except Exception as exc:
        _finish(con, run_id, "failed", inputs_processed=processed, error=str(exc)[:2000])
        raise
    finally:
        con.close()
        release_runtime_lease(lease)


def project():
    """Publish complete current paths, explicitly marking pending and abstained inputs."""
    model = manifest()
    model_id, taxonomy_version = model["model_id"], model["taxonomy_version"]
    lease = _lease("hierarchy_projection")
    con = connect()
    run_id = "hrun_" + uuid.uuid4().hex
    try:
        con.execute("INSERT INTO er_hierarchy_runs(run_id,operation,model_id,status) VALUES (?,'projection',?,'running')", [run_id, model_id])
        con.execute("BEGIN")
        try:
            # Materialize one stable source snapshot before closing any old intervals.
            con.execute("""CREATE OR REPLACE TEMP TABLE _hierarchy_desired AS
                WITH source_ids AS (
                    SELECT canonical_event_id,list(mapping_id ORDER BY mapping_id) AS mapping_ids
                    FROM er_event_mappings WHERE valid_from<=now() AND valid_to IS NULL GROUP BY canonical_event_id
                )
                SELECT e.canonical_event_id,sha256(e.canonical_name) AS input_hash,
                       coalesce(o.subject_id,c.subject_id,'unclassified') AS subject_id,
                       ? AS taxonomy_version,? AS model_id,c.cache_key,
                       CASE WHEN o.override_id IS NOT NULL THEN 'reviewed_override'
                            WHEN c.cache_key IS NULL THEN 'pending_inference'
                            WHEN c.abstained THEN 'abstained' ELSE 'model_assigned_provisional' END AS assignment_status,
                       CASE WHEN o.override_id IS NOT NULL THEN 'human_reviewed' ELSE 'unreviewed' END AS review_status,
                       CASE WHEN o.override_id IS NOT NULL THEN 'reviewed' ELSE 'inferred' END AS edge_class,
                       ? AS method_version,e.event_family_id AS source_event_family_id,
                       json_object('canonical_event_id',e.canonical_event_id,'title',e.canonical_name,
                         'title_sha256',sha256(e.canonical_name),'source_event_family_id',e.event_family_id,
                         'source_mapping_ids',s.mapping_ids,'override_id',o.override_id) AS evidence_json
                FROM er_canonical_events e
                LEFT JOIN er_hierarchy_prediction_cache c ON c.model_id=? AND c.input_hash=sha256(e.canonical_name)
                LEFT JOIN er_hierarchy_overrides o ON o.canonical_event_id=e.canonical_event_id
                    AND o.input_hash=sha256(e.canonical_name) AND o.taxonomy_version=?
                    AND o.valid_from<=now() AND o.valid_to IS NULL
                LEFT JOIN source_ids s ON s.canonical_event_id=e.canonical_event_id
            """, [taxonomy_version, model_id, ALGORITHM_VERSION, model_id, taxonomy_version])
            duplicates = con.execute("""SELECT count(*) FROM (SELECT canonical_event_id FROM _hierarchy_desired
                GROUP BY canonical_event_id HAVING count(*)<>1)""").fetchone()[0]
            if duplicates:
                raise RuntimeError("Ambiguous current overrides; cannot publish hierarchy")
            con.execute("""CREATE OR REPLACE TEMP TABLE _hierarchy_changes AS
                WITH latest AS (SELECT canonical_event_id,max(version_no) AS version_no
                    FROM er_hierarchy_event_subject_mappings GROUP BY canonical_event_id)
                SELECT d.*,coalesce(l.version_no,0)+1 AS version_no
                FROM _hierarchy_desired d LEFT JOIN latest l USING(canonical_event_id)
                LEFT JOIN er_hierarchy_event_subject_mappings m
                  ON m.canonical_event_id=d.canonical_event_id AND m.valid_to IS NULL
                WHERE m.mapping_id IS NULL OR m.subject_id IS DISTINCT FROM d.subject_id
                   OR m.model_id IS DISTINCT FROM d.model_id OR m.input_hash IS DISTINCT FROM d.input_hash
                   OR m.assignment_status IS DISTINCT FROM d.assignment_status OR m.review_status IS DISTINCT FROM d.review_status
                   OR m.source_event_family_id IS DISTINCT FROM d.source_event_family_id
                   OR json_extract_string(m.evidence_json,'$.override_id') IS DISTINCT FROM json_extract_string(d.evidence_json,'$.override_id')
            """)
            changed = int(con.execute("SELECT count(*) FROM _hierarchy_changes").fetchone()[0])
            con.execute("""UPDATE er_hierarchy_event_subject_mappings m SET valid_to=now()
                FROM _hierarchy_changes c WHERE m.canonical_event_id=c.canonical_event_id AND m.valid_to IS NULL""")
            con.execute("""INSERT INTO er_hierarchy_event_subject_mappings
                SELECT 'hmap_'||sha256(canonical_event_id||'|'||cast(version_no AS VARCHAR)||'|'||?),
                       canonical_event_id,version_no,subject_id,taxonomy_version,model_id,input_hash,cache_key,
                       assignment_status,review_status,edge_class,method_version,source_event_family_id,
                       evidence_json,now(),NULL,? FROM _hierarchy_changes""", [run_id, run_id])
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        result = {"run_id": run_id, "model_id": model_id, "mappings_changed": changed}
        _finish(con, run_id, "success", **result)
        return result
    except Exception as exc:
        _finish(con, run_id, "failed", error=str(exc)[:2000])
        raise
    finally:
        con.close()
        release_runtime_lease(lease)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("operation", choices=["init", "infer", "project", "cycle"])
    parser.add_argument("--max-rows", type=int, default=int(os.getenv("HIERARCHY_MAX_ROWS", "50000")))
    parser.add_argument("--partition-id", type=int, default=int(os.getenv("HIERARCHY_PARTITION_ID", "0")))
    parser.add_argument("--partition-count", type=int, default=int(os.getenv("HIERARCHY_PARTITION_COUNT", "1")))
    args = parser.parse_args()
    if args.operation == "init":
        from institutional_hierarchy_schema import ensure_schema, register_model
        result = {"schema": ensure_schema(), "model": register_model()}
    elif args.operation == "project":
        result = project()
    else:
        result = infer(max_rows=args.max_rows, partition_id=args.partition_id, partition_count=args.partition_count)
        if args.operation == "cycle":
            result["projection"] = project()
    print(canonical_json(result), flush=True)


if __name__ == "__main__":
    main()
