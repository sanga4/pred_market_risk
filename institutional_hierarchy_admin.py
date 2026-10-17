"""Controlled CLI for blinded gold collection, reviewed overrides and promotion.

The public service exposes no writes. Reviewers must supply their own judgments;
neither model predictions nor legacy categories become independent gold labels.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import uuid

from db import connect
from institutional_api import rows_dict
from institutional_semantic_model import canonical_json, digest, load_taxonomy, manifest


def export_review(*, count=1000, seed="eventrisk-blind-review-v1", con=None):
    if not 1 <= count <= 10000:
        raise ValueError("Review sample size must be 1–10000")
    owned = con is None
    con = con or connect()
    try:
        # One question per family avoids counting repeated horizons as independent.
        examples = rows_dict(con.execute("""WITH candidates AS (
            SELECT canonical_event_id AS example_id,canonical_name AS input_text,
                   coalesce(event_family_id,canonical_event_id) AS group_id,
                   row_number() OVER(PARTITION BY coalesce(event_family_id,canonical_event_id)
                     ORDER BY sha256(canonical_event_id||?)) AS rn
            FROM er_canonical_events WHERE status='active')
            SELECT example_id,input_text,group_id FROM candidates WHERE rn=1
            ORDER BY sha256(group_id||?) LIMIT ?""", [seed, seed, count]))
        if not examples:
            raise ValueError("No active events available for review")
        for e in examples:
            e["input_hash"] = digest(e["input_text"])
            e["split"] = "test" if int(digest(e["group_id"] + seed)[:8], 16) % 5 else "development"
        protocol = {"model_id": manifest()["model_id"], "taxonomy_version": manifest()["taxonomy_version"],
                    "sample_design": "deterministic seeded active family sample; one event per family; 80/20 group-disjoint test/development",
                    "seed": seed, "snapshot_at": datetime.now(timezone.utc).isoformat(),
                    "blinded": True, "two_independent_reviewers_required": True,
                    "no_test_set_threshold_tuning": True}
        content_hash = digest(canonical_json(examples))
        dataset_id = "hgold_" + content_hash[:24]
        con.execute("""INSERT INTO er_hierarchy_evaluation_sets(dataset_id,dataset_kind,content_sha256,description,protocol_json)
            VALUES (?,'human_review_pending',?,'Blinded production sample; not yet reviewed',?) ON CONFLICT DO NOTHING""",
            [dataset_id, content_hash, canonical_json(protocol)])
        import pyarrow as pa
        pending = [{"dataset_id": dataset_id, **{k: e[k] for k in ("example_id", "input_text", "group_id", "split")},
                    "subject_id": "unclassified", "review_status": "pending_review"} for e in examples]
        con.register("_review_pending", pa.Table.from_pylist(pending))
        try:
            con.execute("INSERT INTO er_hierarchy_labels BY NAME SELECT * FROM _review_pending ON CONFLICT DO NOTHING")
        finally:
            con.unregister("_review_pending")
        return {"dataset_id": dataset_id, "content_sha256": content_hash, "protocol": protocol,
                "instructions": "Two reviewers label independently using the subject catalog. Resolve disagreements with a third adjudicator. Do not consult model outputs. Never tune on test labels.",
                "subject_catalog": [{"subject_id": s["id"], "name": s["name"], "theme": s["theme"]} for s in load_taxonomy()["subjects"]],
                "examples": [{**e, "reviewer_a": "", "subject_a": "", "reviewer_b": "", "subject_b": "",
                              "adjudicator": "", "subject_id": "", "notes": ""} for e in examples]}
    finally:
        if owned:
            con.close()


def import_review(payload, *, con=None):
    owned = con is None
    con = con or connect()
    try:
        dataset = con.execute("SELECT content_sha256,protocol_json FROM er_hierarchy_evaluation_sets WHERE dataset_id=?", [payload["dataset_id"]]).fetchone()
        if not dataset or payload.get("content_sha256") != dataset[0]:
            raise ValueError("Review must match a registered immutable sample")
        if payload.get("independent_review_attestation") is not True:
            raise ValueError("Explicit independent_review_attestation is required")
        originals = {e["example_id"]: e for e in rows_dict(con.execute("SELECT * FROM er_hierarchy_labels WHERE dataset_id=?", [payload["dataset_id"]]))}
        examples = payload["examples"]
        if len(examples) != len(originals) or {e["example_id"] for e in examples} != set(originals):
            raise ValueError("All sampled examples must be reviewed exactly once; no cherry picking")
        allowed = {s["id"] for s in load_taxonomy()["subjects"]}
        for e in examples:
            old = originals[e["example_id"]]
            if any(e.get(k) != old[k] for k in ("input_text", "group_id", "split")) or e.get("input_hash") != digest(old["input_text"]):
                raise ValueError("Sample text, hash, group and split must remain unchanged")
            a, b = e.get("reviewer_a", "").strip(), e.get("reviewer_b", "").strip()
            if not a or not b or a.casefold() == b.casefold():
                raise ValueError("Two distinct independent reviewers are required")
            if any(e.get(k) not in allowed for k in ("subject_a", "subject_b", "subject_id")):
                raise ValueError("All judgments must use the frozen subject catalog")
            if e["subject_a"] == e["subject_b"]:
                if e["subject_id"] != e["subject_a"]:
                    raise ValueError("Agreed labels must match the final subject")
            elif not e.get("adjudicator", "").strip() or e["adjudicator"].strip().casefold() in {a.casefold(), b.casefold()}:
                raise ValueError("Disagreements require a third adjudicator")
            if old["review_status"] != "pending_review":
                raise ValueError("Reviewed datasets are immutable; export a new sample/version")
        con.execute("BEGIN")
        try:
            for e in examples:
                con.execute("""UPDATE er_hierarchy_labels SET subject_id=?,reviewer=?,review_status='human_reviewed',
                    reviewed_at=now(),review_evidence_json=? WHERE dataset_id=? AND example_id=?""",
                    [e["subject_id"], e.get("adjudicator") or e["reviewer_a"], canonical_json(e), payload["dataset_id"], e["example_id"]])
            con.execute("UPDATE er_hierarchy_evaluation_sets SET dataset_kind='human_reviewed_production' WHERE dataset_id=?", [payload["dataset_id"]])
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK"); raise
        return {"dataset_id": payload["dataset_id"], "reviewed": len(examples),
                "reviewer_agreement": sum(e["subject_a"] == e["subject_b"] for e in examples) / len(examples)}
    finally:
        if owned:
            con.close()


def evaluate_gold(dataset_id, *, classifier=None, con=None):
    from institutional_hierarchy_metrics import evaluate, promotion_gate, stability
    from institutional_semantic_model import SemanticClassifier
    owned = con is None
    con = con or connect()
    try:
        dataset = con.execute("SELECT dataset_kind,protocol_json FROM er_hierarchy_evaluation_sets WHERE dataset_id=?", [dataset_id]).fetchone()
        if not dataset or dataset[0] != "human_reviewed_production":
            raise ValueError("Dataset needs independently reviewed production labels")
        protocol = json.loads(dataset[1])
        if protocol["model_id"] != manifest()["model_id"]:
            raise ValueError("This holdout was frozen for a different model; create a fresh blind holdout")
        examples = rows_dict(con.execute("SELECT * FROM er_hierarchy_labels WHERE dataset_id=? AND split='test' ORDER BY example_id", [dataset_id]))
        if not examples or any(e["review_status"] != "human_reviewed" for e in examples):
            raise ValueError("Complete independent test labels are required")
        classifier = classifier or SemanticClassifier()
        texts = [e["input_text"] for e in examples]
        predictions = classifier.predict(texts, full_scores=True)
        metrics = evaluate(examples, predictions, load_taxonomy(), dataset_kind="human_reviewed_production")
        metrics["repeatability"] = stability(predictions, classifier.predict(texts, batch_size=1))
        evidence = [json.loads(e["review_evidence_json"]) for e in examples]
        metrics["reviewer_agreement"] = sum(e["subject_a"] == e["subject_b"] for e in evidence) / len(evidence)
        metrics["protocol"] = protocol
        metrics["promotion_gate"] = promotion_gate(metrics)
        evaluation_id = "heval_" + uuid.uuid4().hex
        con.execute("""INSERT INTO er_hierarchy_evaluations(evaluation_id,dataset_id,model_id,metrics_json,predictions_json,evaluation_kind)
            VALUES (?,?,?,?,?,'human_reviewed_production')""", [evaluation_id,dataset_id,manifest()["model_id"],canonical_json(metrics),canonical_json([
                {**p,"example_id":e["example_id"],"expected_subject_id":e["subject_id"],"input_hash":digest(e["input_text"])}
                for e,p in zip(examples,predictions)])])
        return {"evaluation_id": evaluation_id, "metrics": metrics}
    finally:
        if owned:
            con.close()


def approve(evaluation_id, reviewer, notes, *, con=None):
    from institutional_hierarchy_metrics import promotion_gate
    if not reviewer.strip() or not notes.strip():
        raise ValueError("Approval needs an accountable reviewer and rationale")
    owned = con is None
    con = con or connect()
    try:
        row = con.execute("SELECT model_id,metrics_json FROM er_hierarchy_evaluations WHERE evaluation_id=? AND evaluation_kind='human_reviewed_production'", [evaluation_id]).fetchone()
        if not row or not promotion_gate(json.loads(row[1]))["ready"]:
            raise ValueError("Institutional promotion blocked by independent validation gate")
        con.execute("BEGIN")
        try:
            con.execute("INSERT INTO er_hierarchy_model_approvals(model_id,evaluation_id,reviewer,approval_notes) VALUES (?,?,?,?)", [row[0],evaluation_id,reviewer,notes])
            con.execute("UPDATE er_hierarchy_models SET validation_status='approved_with_independent_gold' WHERE model_id=?", [row[0]])
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK"); raise
        return {"model_id": row[0], "evaluation_id": evaluation_id, "approved": True}
    finally:
        if owned:
            con.close()


def override(event_id, subject_id, reviewer, reason, *, expected_input_hash, con=None):
    if not reviewer.strip() or not reason.strip():
        raise ValueError("Override needs a reviewer and evidence/rationale")
    owned = con is None
    con = con or connect()
    try:
        row = con.execute("SELECT sha256(canonical_name) FROM er_canonical_events WHERE canonical_event_id=?", [event_id]).fetchone()
        if not row or row[0] != expected_input_hash:
            raise ValueError("Event input changed; review the current title before overriding")
        if subject_id not in {s["id"] for s in load_taxonomy()["subjects"]}:
            raise ValueError("Unknown subject")
        con.execute("BEGIN")
        try:
            con.execute("UPDATE er_hierarchy_overrides SET valid_to=now() WHERE canonical_event_id=? AND valid_to IS NULL", [event_id])
            override_id = "hovr_" + uuid.uuid4().hex
            con.execute("""INSERT INTO er_hierarchy_overrides(override_id,canonical_event_id,input_hash,taxonomy_version,subject_id,reviewer,reason)
                VALUES (?,?,?,?,?,?,?)""", [override_id,event_id,row[0],manifest()["taxonomy_version"],subject_id,reviewer,reason])
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK"); raise
        return {"override_id": override_id, "projection_required": True}
    finally:
        if owned:
            con.close()


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="operation", required=True)
    export = sub.add_parser("export-review"); export.add_argument("--count", type=int, default=1000); export.add_argument("--seed", default="eventrisk-blind-review-v1"); export.add_argument("--output", required=True)
    imp = sub.add_parser("import-review"); imp.add_argument("file")
    ev = sub.add_parser("evaluate-gold"); ev.add_argument("dataset_id")
    ap = sub.add_parser("approve"); ap.add_argument("evaluation_id"); ap.add_argument("--reviewer", required=True); ap.add_argument("--notes", required=True)
    ov = sub.add_parser("override"); ov.add_argument("event_id"); ov.add_argument("subject_id"); ov.add_argument("--input-hash", required=True); ov.add_argument("--reviewer", required=True); ov.add_argument("--reason", required=True)
    args = parser.parse_args()
    if args.operation == "export-review":
        result = export_review(count=args.count,seed=args.seed)
        Path(args.output).write_text(json.dumps(result,indent=2))
        result = {"dataset_id": result["dataset_id"], "examples": len(result["examples"]), "output": args.output}
    elif args.operation == "import-review": result = import_review(json.loads(Path(args.file).read_text()))
    elif args.operation == "evaluate-gold": result = evaluate_gold(args.dataset_id)
    elif args.operation == "approve": result = approve(args.evaluation_id,args.reviewer,args.notes)
    else: result = override(args.event_id,args.subject_id,args.reviewer,args.reason,expected_input_hash=args.input_hash)
    print(canonical_json(result),flush=True)


if __name__ == "__main__":
    main()
