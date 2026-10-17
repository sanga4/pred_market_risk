"""Authenticated read-only semantic hierarchy, history, taxonomy and quality API."""
from datetime import datetime
import json
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from institutional_api import db_read, rows_dict
from institutional_hierarchy_queries import operational_metrics
from institutional_semantic_model import manifest

router = APIRouter(prefix="/v1/hierarchy", tags=["semantic-hierarchy"])


@router.get("/review-datasets")
def review_datasets():
    con = db_read()
    try:
        return {"items": rows_dict(con.execute("""SELECT d.dataset_id,d.created_at,d.description,count(l.example_id) AS examples
            FROM er_hierarchy_evaluation_sets d JOIN er_hierarchy_labels l USING(dataset_id)
            WHERE d.dataset_kind='human_review_pending' GROUP BY ALL ORDER BY d.created_at DESC LIMIT 20"""))}
    finally:
        con.close()


@router.get("/review-datasets/{dataset_id}")
def blind_review_dataset(dataset_id: str):
    from institutional_semantic_model import digest, load_taxonomy
    con = db_read()
    try:
        rows = rows_dict(con.execute("SELECT * FROM er_hierarchy_evaluation_sets WHERE dataset_id=? AND dataset_kind='human_review_pending'", [dataset_id]))
        if not rows:
            raise HTTPException(404,"Pending blinded review sample not found")
        dataset = rows[0]
        examples = rows_dict(con.execute("SELECT example_id,input_text,group_id,split FROM er_hierarchy_labels WHERE dataset_id=? ORDER BY example_id", [dataset_id]))
        return {"dataset_id":dataset_id,"content_sha256":dataset["content_sha256"],"protocol":json.loads(dataset["protocol_json"]),
                "independent_review_attestation":False,
                "instructions":"Review independently before consulting predictions. Two distinct reviewers are required; disagreements need a third adjudicator. Complete the entire sample without changing inputs, groups or splits.",
                "subject_catalog":[{"subject_id":s["id"],"name":s["name"],"theme":s["theme"]} for s in load_taxonomy()["subjects"]],
                "examples":[{**e,"input_hash":digest(e["input_text"]),"reviewer_a":"","reviewer_b":"","subject_a":"","subject_b":"","adjudicator":"","subject_id":"","notes":""} for e in examples]}
    finally:
        con.close()


@router.get("/metrics")
def metrics():
    con = db_read()
    try:
        con.execute("BEGIN")
        return operational_metrics(con)
    finally:
        con.close()


@router.get("/taxonomy")
def taxonomy(version: Optional[str] = None):
    version = version or manifest()["taxonomy_version"]
    con = db_read()
    try:
        rows = rows_dict(con.execute("SELECT * FROM er_hierarchy_taxonomies WHERE taxonomy_version=?", [version]))
        if not rows:
            raise HTTPException(404, "Taxonomy version not found")
        return {"taxonomy_version": version, "sha256": rows[0]["content_sha256"],
                "definition": json.loads(rows[0]["definition_json"]), "created_at": rows[0]["created_at"]}
    finally:
        con.close()


@router.get("/events/{event_id}")
def event_hierarchy(event_id: str, at: Optional[datetime] = None):
    con = db_read()
    try:
        params = [event_id]
        validity = "m.valid_from<=now() AND m.valid_to IS NULL"
        if at is not None:
            validity = "m.valid_from<=? AND (m.valid_to IS NULL OR m.valid_to>?)"
            params.extend([at, at])
        rows = rows_dict(con.execute(f"""
            SELECT m.*,s.canonical_name AS subject_name,st.theme_id,t.canonical_name AS theme_name,
                   c.similarity,c.margin,c.output_json,mo.validation_status AS model_validation_status
            FROM er_hierarchy_event_subject_mappings m
            JOIN er_hierarchy_subjects s ON s.taxonomy_version=m.taxonomy_version AND s.subject_id=m.subject_id
            JOIN er_hierarchy_subject_theme_mappings st ON st.taxonomy_version=m.taxonomy_version AND st.subject_id=m.subject_id
            JOIN er_hierarchy_themes t ON t.taxonomy_version=st.taxonomy_version AND t.theme_id=st.theme_id
            JOIN er_hierarchy_models mo ON mo.model_id=m.model_id
            LEFT JOIN er_hierarchy_prediction_cache c ON c.cache_key=m.cache_key
            WHERE m.canonical_event_id=? AND {validity}
            ORDER BY m.version_no DESC
        """, params))
        if not rows:
            raise HTTPException(404, "No hierarchy mapping known at the requested time")
        if len(rows) != 1:
            raise HTTPException(503, "Hierarchy interval integrity failure")
        row = rows[0]
        row["evidence"] = json.loads(row.pop("evidence_json"))
        output = row.pop("output_json")
        row["model_output"] = json.loads(output) if output else None
        return {"mapping": row, "as_known_at": at, "basis": "processing-time hierarchy validity; no backdated model knowledge"}
    finally:
        con.close()


@router.get("/review")
def review(after: Optional[str] = None, limit: int = Query(50, ge=1, le=250), abstained_only: bool = True):
    con = db_read()
    try:
        params = []
        where = ["1=1"]
        if after:
            where.append("canonical_event_id>?"); params.append(after)
        if abstained_only:
            where.append("assignment_status IN ('abstained','pending_inference')")
        params.append(limit + 1)
        rows = rows_dict(con.execute(f"""SELECT canonical_event_id,canonical_name,subject_id,theme_id,
                candidate_subject_id,similarity,margin,assignment_status,review_status,model_id,input_hash
            FROM er_hierarchy_review_queue WHERE {' AND '.join(where)}
            ORDER BY canonical_event_id LIMIT ?""", params))
        items = rows[:limit]
        return {"items": items, "next_after": items[-1]["canonical_event_id"] if len(rows)>limit else None,
                "note": "Review candidates are not independent evaluation labels. Review occurs through controlled admin tooling."}
    finally:
        con.close()
