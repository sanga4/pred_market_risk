"""Read-only operational/validation state for the semantic hierarchy."""
import json
from datetime import datetime, timezone
from institutional_api import rows_dict


def operational_metrics(con):
    measured_at = datetime.now(timezone.utc)
    coverage = rows_dict(con.execute("""
        SELECT count(*) AS canonical_events,
               count(h.mapping_id) AS mapped_events,
               count(*) FILTER(WHERE h.assignment_status='pending_inference' OR h.mapping_id IS NULL) AS pending_events,
               count(*) FILTER(WHERE h.assignment_status='abstained') AS abstained_events,
               count(*) FILTER(WHERE h.assignment_status='model_assigned_provisional') AS provisional_events,
               count(*) FILTER(WHERE h.review_status='human_reviewed') AS human_reviewed_events,
               count(*) FILTER(WHERE h.input_hash IS DISTINCT FROM sha256(e.canonical_name)) AS stale_input_events,
               count(*) FILTER(WHERE e.status='active') AS active_events,
               count(*) FILTER(WHERE e.status='active' AND h.mapping_id IS NULL) AS active_unmapped_events
        FROM er_canonical_events e LEFT JOIN er_hierarchy_current h USING(canonical_event_id)
    """))[0]
    model = rows_dict(con.execute("SELECT * FROM er_hierarchy_models ORDER BY created_at DESC,model_id DESC LIMIT 1"))
    model_id = model[0]["model_id"] if model else None
    pending_inputs = int(con.execute("""SELECT count(*) FROM (
        SELECT DISTINCT sha256(e.canonical_name) AS input_hash FROM er_canonical_events e
        LEFT JOIN er_hierarchy_prediction_cache c ON c.input_hash=sha256(e.canonical_name) AND c.model_id=?
        WHERE c.cache_key IS NULL)
    """, [model_id]).fetchone()[0])
    integrity = {
        "multiple_current_mappings": int(con.execute("""SELECT count(*) FROM (
            SELECT canonical_event_id FROM er_hierarchy_event_subject_mappings
            WHERE valid_to IS NULL GROUP BY canonical_event_id HAVING count(*)<>1)""").fetchone()[0]),
        "overlapping_intervals": int(con.execute("""SELECT count(*) FROM (
            SELECT valid_from,lag(valid_to) OVER(PARTITION BY canonical_event_id ORDER BY version_no) AS prior_end,
                   row_number() OVER(PARTITION BY canonical_event_id ORDER BY version_no) AS rn
            FROM er_hierarchy_event_subject_mappings) WHERE rn>1 AND (prior_end IS NULL OR prior_end>valid_from)""").fetchone()[0]),
        "orphan_subject_theme_links": int(con.execute("""SELECT count(*) FROM er_hierarchy_subject_theme_mappings m
            LEFT JOIN er_hierarchy_subjects s ON s.taxonomy_version=m.taxonomy_version AND s.subject_id=m.subject_id
            LEFT JOIN er_hierarchy_themes t ON t.taxonomy_version=m.taxonomy_version AND t.theme_id=m.theme_id
            WHERE s.subject_id IS NULL OR t.theme_id IS NULL""").fetchone()[0]),
        "orphan_event_subject_links": int(con.execute("""SELECT count(*) FROM er_hierarchy_event_subject_mappings m
            LEFT JOIN er_canonical_events e USING(canonical_event_id)
            LEFT JOIN er_hierarchy_subjects s ON s.taxonomy_version=m.taxonomy_version AND s.subject_id=m.subject_id
            WHERE e.canonical_event_id IS NULL OR s.subject_id IS NULL""").fetchone()[0]),
        "cache_theme_inconsistencies": int(con.execute("""SELECT count(*) FROM er_hierarchy_prediction_cache c
            JOIN er_hierarchy_models mo USING(model_id)
            LEFT JOIN er_hierarchy_subject_theme_mappings st
              ON st.subject_id=c.subject_id AND st.taxonomy_version=mo.taxonomy_version
            WHERE st.theme_id IS NULL OR st.theme_id<>c.theme_id""").fetchone()[0]),
        "invalid_intervals": int(con.execute("SELECT count(*) FROM er_hierarchy_event_subject_mappings WHERE valid_to<=valid_from").fetchone()[0]),
        "multiple_current_overrides": int(con.execute("""SELECT count(*) FROM (
            SELECT canonical_event_id FROM er_hierarchy_overrides WHERE valid_to IS NULL
            GROUP BY canonical_event_id HAVING count(*)>1)""").fetchone()[0]),
        "model_taxonomy_mismatches": int(con.execute("""SELECT count(*) FROM er_hierarchy_event_subject_mappings m
            LEFT JOIN er_hierarchy_models mo USING(model_id)
            WHERE mo.model_id IS NULL OR mo.taxonomy_version<>m.taxonomy_version""").fetchone()[0]),
    }
    runs = rows_dict(con.execute("SELECT * FROM er_hierarchy_runs ORDER BY started_at DESC LIMIT 12"))
    evaluations = rows_dict(con.execute("""SELECT e.evaluation_id,e.dataset_id,e.model_id,e.measured_at,e.evaluation_kind,e.metrics_json
        FROM er_hierarchy_evaluations e WHERE model_id=? ORDER BY measured_at DESC LIMIT 5""", [model_id]))
    for evaluation in evaluations:
        evaluation["metrics"] = json.loads(evaluation.pop("metrics_json"))
    gold = rows_dict(con.execute("""SELECT evaluation_id,metrics_json FROM er_hierarchy_evaluations
        WHERE model_id=? AND evaluation_kind='human_reviewed_production' ORDER BY measured_at DESC LIMIT 1""", [model_id]))
    for item in gold:
        item["metrics"] = json.loads(item.pop("metrics_json"))
    gold_gate = gold[0]["metrics"].get("promotion_gate", {}) if gold else {"ready": False, "failed_controls": ["independent_production_gold_missing"]}
    recent_projections = rows_dict(con.execute("SELECT * FROM er_hierarchy_runs WHERE operation='projection' AND status='success' ORDER BY started_at DESC LIMIT 1"))
    approved = bool(con.execute("SELECT 1 FROM er_hierarchy_model_approvals WHERE model_id=? AND evaluation_id=? LIMIT 1", [model_id, gold[0]["evaluation_id"] if gold else None]).fetchone())
    distribution = rows_dict(con.execute("""SELECT subject_id,subject_name,theme_id,assignment_status,count(*) AS events
        FROM er_hierarchy_current GROUP BY ALL ORDER BY events DESC,subject_id"""))
    return {
        "measured_at": measured_at,
        "coverage": coverage, "pending_distinct_inputs": pending_inputs, "integrity": integrity,
        "structurally_ready": not any(integrity.values()) and coverage["mapped_events"] == coverage["canonical_events"] and coverage["stale_input_events"] == 0,
        "inference_complete": pending_inputs == 0 and coverage["pending_events"] == 0,
        "institutional_validation_ready": bool(gold_gate.get("ready")) and approved,
        "validation_gate": {**gold_gate, "explicit_model_approval": approved},
        "model": model[0] if model else None, "evaluations": evaluations, "recent_runs": runs,
        "distribution": distribution,
        "last_projection": recent_projections[0] if recent_projections else None,
        "production_precision": gold[0]["metrics"].get("production_gold_precision") if gold else None,
        "production_recall": gold[0]["metrics"].get("production_gold_recall") if gold else None,
        "calibration_status": "uncalibrated_cosine_scores",
        "note": "Structural coverage, repeatability and authored challenge metrics are separate from independently reviewed production accuracy. Missing gold metrics remain null.",
    }
