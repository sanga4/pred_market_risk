from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel, Field

from db import connect
from institutional_archive_v2 import restore_payload
from institutional_mapping_v3 import DIRECTIONS, RELATIONSHIP_TYPES, review_candidate

router = APIRouter(tags=["institutional-ops"])


def rows_dict(cur):
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def db_read():
    try:
        return connect(read_only=True)
    except Exception as exc:
        raise HTTPException(503, f"Institutional data store unavailable: {exc}")


def admin_auth(authorization: Optional[str] = Header(default=None), x_admin_token: Optional[str] = Header(default=None)):
    expected = os.getenv("EVENTRISK_ADMIN_TOKEN")
    if not expected:
        raise HTTPException(503, "Administrative API is disabled until EVENTRISK_ADMIN_TOKEN is configured")
    supplied = x_admin_token
    if not supplied and authorization and authorization.lower().startswith("bearer "):
        supplied = authorization.split(" ", 1)[1]
    if supplied != expected:
        raise HTTPException(401, "Invalid administrative token")
    return "admin"


@router.get("/v1/sources")
def list_sources():
    con = db_read()
    try:
        return rows_dict(con.execute(
            """SELECT s.source_id,s.source_name,s.source_type,s.base_url,s.redistribution_status,
                      s.attribution_required,s.active,h.last_attempt_at,h.last_success_at,
                      h.last_observation_at,h.last_run_status,h.records_last_run,
                      h.consecutive_failures,h.health_status,h.details
               FROM er_sources s LEFT JOIN er_source_health h USING(source_id)
               ORDER BY s.source_id"""
        ))
    finally:
        con.close()


@router.get("/v1/contracts/{contract_id}")
def get_contract(contract_id: str):
    con = db_read()
    try:
        contract = rows_dict(con.execute("SELECT * FROM er_source_contracts WHERE source_contract_id=?", [contract_id]))
        if not contract:
            raise HTTPException(404, "Source contract not found")
        versions = rows_dict(con.execute(
            "SELECT * FROM er_source_contract_versions WHERE source_contract_id=? ORDER BY version_no", [contract_id]
        ))
        mappings = rows_dict(con.execute(
            "SELECT * FROM er_event_mappings WHERE source_contract_id=? ORDER BY valid_from", [contract_id]
        ))
        evidence = rows_dict(con.execute(
            """SELECT e.*,r.source_id,r.source_object_type,r.source_object_id,r.source_observed_at,
                      r.received_at,r.content_hash,r.payload_storage,a.status archive_status,
                      a.archive_key,a.archive_member,a.archive_bundle_hash
               FROM er_contract_version_evidence e
               LEFT JOIN er_raw_snapshots r USING(raw_snapshot_id)
               LEFT JOIN er_raw_archive_objects a USING(raw_snapshot_id)
               WHERE e.source_contract_id=? ORDER BY e.version_no,e.evidence_role""",
            [contract_id],
        ))
        return {"contract": contract[0], "versions": versions, "mappings": mappings, "evidence": evidence}
    finally:
        con.close()


@router.get("/internal/raw-snapshots/{raw_snapshot_id}", dependencies=[Depends(admin_auth)])
def raw_snapshot_audit(raw_snapshot_id: str):
    """Restore one exact source payload and independently verify its recorded SHA-256."""
    con = db_read()
    try:
        rows = rows_dict(con.execute(
            """SELECT r.raw_snapshot_id,r.source_id,r.source_object_type,r.source_object_id,
                      r.source_observed_at,r.received_at,r.content_hash,r.payload_json,
                      r.payload_storage,r.payload_compacted_at,r.ingestion_run_id,
                      a.status archive_status,a.archive_key,a.archive_member,a.archive_bundle_hash,
                      a.archived_at
               FROM er_raw_snapshots r
               LEFT JOIN er_raw_archive_objects a USING(raw_snapshot_id)
               WHERE r.raw_snapshot_id=?""",
            [raw_snapshot_id],
        ))
        if not rows:
            raise HTTPException(404, "Raw snapshot not found")
        meta = rows[0]
        payload_json = meta.pop("payload_json")
    finally:
        con.close()

    if payload_json is not None:
        payload = payload_json.encode("utf-8")
        retrieval = "database"
    else:
        try:
            payload = restore_payload(raw_snapshot_id)
            retrieval = "verified_object_archive"
        except Exception as exc:
            raise HTTPException(503, f"Archived raw payload could not be restored: {exc}")

    digest = hashlib.sha256(payload).hexdigest()
    if digest != meta.get("content_hash"):
        raise HTTPException(500, "Raw payload failed integrity verification")
    try:
        parsed = json.loads(payload.decode("utf-8"))
    except Exception:
        parsed = payload.decode("utf-8", errors="replace")
    return {"metadata": meta, "retrieval": retrieval, "hash_verified": True, "payload": parsed}


@router.get("/v1/events/{event_id}/as-of")
def event_as_of(event_id: str, at: datetime):
    con = db_read()
    try:
        event = rows_dict(con.execute("SELECT * FROM er_canonical_events WHERE canonical_event_id=?", [event_id]))
        if not event:
            raise HTTPException(404, "Canonical event not found")
        rows = rows_dict(con.execute(
            """WITH eligible AS (
                 SELECT o.*,c.source_market_id,
                        row_number() OVER(PARTITION BY o.source_contract_id ORDER BY o.received_at DESC,o.observed_at DESC) rn
                 FROM er_market_observations o
                 JOIN er_source_contracts c USING(source_contract_id)
                 JOIN er_event_mappings m USING(source_contract_id)
                 WHERE m.canonical_event_id=?
                   AND m.valid_from<=?
                   AND (m.valid_to IS NULL OR m.valid_to>?)
                   AND o.received_at<=?
               )
               SELECT * EXCLUDE(rn) FROM eligible WHERE rn=1 ORDER BY source_id,source_market_id""",
            [event_id, at, at, at],
        ))
        return {"as_of": at, "event": event[0], "source_observations": rows}
    finally:
        con.close()


@router.get("/v1/event-families/{family_id}/term-structure")
def term_structure(family_id: str, as_of: Optional[datetime] = None):
    con = db_read()
    try:
        family = rows_dict(con.execute("SELECT * FROM er_event_families WHERE event_family_id=?", [family_id]))
        if not family:
            raise HTTPException(404, "Event family not found")
        if as_of is None:
            rows = rows_dict(con.execute(
                """SELECT * FROM er_term_structure_latest
                   WHERE event_family_id=?
                   ORDER BY resolution_deadline,canonical_event_id,source_id""",
                [family_id],
            ))
        else:
            rows = rows_dict(con.execute(
                """WITH obs AS (
                     SELECT e.canonical_event_id,e.canonical_name event_name,e.resolution_deadline,
                            o.source_id,o.source_contract_id,o.observed_at,o.received_at,
                            o.displayed_probability,o.yes_bid,o.yes_ask,o.volume,o.open_interest,o.liquidity,o.spread,
                            row_number() OVER(PARTITION BY o.source_contract_id ORDER BY o.received_at DESC,o.observed_at DESC) rn
                     FROM er_canonical_events e
                     JOIN er_event_mappings m USING(canonical_event_id)
                     JOIN er_market_observations o USING(source_contract_id)
                     WHERE e.event_family_id=?
                       AND m.valid_from<=? AND (m.valid_to IS NULL OR m.valid_to>?)
                       AND o.received_at<=?
                   )
                   SELECT * EXCLUDE(rn) FROM obs WHERE rn=1
                   ORDER BY resolution_deadline,canonical_event_id,source_id""",
                [family_id, as_of, as_of, as_of],
            ))
        return {
            "family": family[0],
            "as_of": as_of,
            "curve": rows,
            "note": "Term structure preserves source observations; no EventRisk consensus is imposed.",
        }
    finally:
        con.close()


@router.get("/v1/events/{event_id}/source-comparison")
def source_comparison(event_id: str):
    con = db_read()
    try:
        rows = rows_dict(con.execute(
            """SELECT canonical_event_id,source_id,source_market_id,source_contract_id,observed_at,
                      displayed_probability,yes_bid,yes_ask,midpoint,last_trade,volume,volume_24h,
                      open_interest,liquidity,spread,mapping_review_status
               FROM er_event_source_snapshot WHERE canonical_event_id=?
               ORDER BY source_id,source_market_id""",
            [event_id],
        ))
        probs = [r["displayed_probability"] for r in rows if r.get("displayed_probability") is not None]
        summary = None
        if probs:
            probs_sorted = sorted(probs)
            n = len(probs_sorted)
            median = probs_sorted[n // 2] if n % 2 else (probs_sorted[n // 2 - 1] + probs_sorted[n // 2]) / 2
            summary = {"source_count": len(probs), "min": min(probs), "max": max(probs), "median": median,
                       "range": max(probs) - min(probs)}
        return {"canonical_event_id": event_id, "descriptive_summary": summary, "sources": rows,
                "note": "Descriptive statistics only; EventRisk does not prescribe a consensus probability in Layer 1/2."}
    finally:
        con.close()


@router.get("/v1/quality")
def quality_summary(severity: Optional[str] = None, limit: int = Query(250, ge=1, le=2000)):
    con = db_read()
    try:
        where = ["status='open'"]
        params = []
        if severity:
            where.append("severity=?")
            params.append(severity)
        params.append(limit)
        issues = rows_dict(con.execute(
            f"SELECT * FROM er_quality_issues WHERE {' AND '.join(where)} ORDER BY detected_at DESC LIMIT ?", params
        ))
        counts = rows_dict(con.execute(
            "SELECT severity,count(*) count FROM er_quality_issues WHERE status='open' GROUP BY severity ORDER BY severity"
        ))
        return {"counts": counts, "issues": issues}
    finally:
        con.close()


@router.get("/v1/graph/{node_id}")
def graph_node(node_id: str, include_inferred: bool = False, limit: int = Query(500, ge=1, le=5000)):
    con = db_read()
    try:
        params = [node_id, node_id]
        inferred = "" if include_inferred else "AND edge_class<>'inferred'"
        params.append(limit)
        edges = rows_dict(con.execute(
            f"""SELECT * FROM er_graph_edges
                WHERE (source_node_id=? OR target_node_id=?) {inferred}
                  AND valid_to IS NULL
                ORDER BY relationship_type,created_at LIMIT ?""", params
        ))
        return {"node_id": node_id, "include_inferred": include_inferred, "edges": edges}
    finally:
        con.close()


class MappingReview(BaseModel):
    action: str = Field(pattern="^(same_event|relationship|related_only|reject)$")
    relationship_type: Optional[str] = None
    direction: str = Field(default="undirected", pattern="^(left_to_right|right_to_left|undirected)$")
    notes: Optional[str] = Field(default=None, max_length=4000)
    reviewer: str = Field(default="admin", min_length=1, max_length=200)


@router.get("/internal/mapping-candidates", dependencies=[Depends(admin_auth)])
def mapping_candidates(status: str = "pending", limit: int = Query(250, ge=1, le=2000)):
    con = db_read()
    try:
        return rows_dict(con.execute(
            """SELECT mc.*,
                      l.source_id left_source,l.current_title left_title,
                      l.current_resolution_rules left_resolution_rules,l.closes_at left_close,
                      r.source_id right_source,r.current_title right_title,
                      r.current_resolution_rules right_resolution_rules,r.closes_at right_close
               FROM er_mapping_candidates mc
               JOIN er_source_contracts l ON l.source_contract_id=mc.left_source_contract_id
               JOIN er_source_contracts r ON r.source_contract_id=mc.right_source_contract_id
               WHERE mc.status=? ORDER BY mc.overall_confidence DESC,mc.created_at LIMIT ?""",
            [status, limit],
        ))
    finally:
        con.close()


@router.post("/internal/mapping-candidates/{candidate_id}/review", dependencies=[Depends(admin_auth)])
def review_mapping(candidate_id: str, body: MappingReview):
    relationship_type = body.relationship_type
    if body.action == "related_only":
        relationship_type = relationship_type or "related_to"
    if body.action == "relationship":
        if not relationship_type:
            raise HTTPException(400, "relationship_type is required when action=relationship")
        if relationship_type not in RELATIONSHIP_TYPES:
            raise HTTPException(400, f"relationship_type must be one of {sorted(RELATIONSHIP_TYPES)}")
    if body.direction not in DIRECTIONS:
        raise HTTPException(400, f"direction must be one of {sorted(DIRECTIONS)}")
    try:
        return review_candidate(
            candidate_id,
            body.action,
            body.reviewer,
            body.notes,
            relationship_type=relationship_type,
            direction=body.direction,
        )
    except KeyError:
        raise HTTPException(404, "Mapping candidate not found")
    except ValueError as exc:
        raise HTTPException(409, str(exc))
