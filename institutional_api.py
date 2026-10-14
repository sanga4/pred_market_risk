from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from db import connect

router = APIRouter(prefix="/v1", tags=["institutional-data"])


def rows_dict(cur):
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def one_or_404(cur, detail="Not found"):
    rows = rows_dict(cur)
    if not rows:
        raise HTTPException(404, detail)
    return rows[0]


def db_read():
    try:
        return connect(read_only=True)
    except Exception as exc:
        raise HTTPException(503, f"Institutional data store unavailable: {exc}")


@router.get("/events")
def list_events(
    family_id: Optional[str] = None,
    status: Optional[str] = "active",
    category: Optional[str] = None,
    include_unserved: bool = False,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """List canonical events with live source coverage by default.

    `include_unserved=true` is primarily for taxonomy/audit work. Superseded canonicals are
    preserved historically rather than silently deleted.
    """
    con = db_read()
    try:
        where = ["1=1"]
        params = []
        if family_id:
            where.append("e.event_family_id = ?")
            params.append(family_id)
        if status:
            where.append("e.status = ?")
            params.append(status)
        if category:
            where.append("f.category = ?")
            params.append(category)
        params.extend([limit, offset])
        source_relation = "er_canonical_events" if include_unserved else "er_served_canonical_events"
        coverage = (
            "NULL AS active_contract_count, NULL AS source_count, NULL AS latest_received_at"
            if include_unserved
            else "e.active_contract_count, e.source_count, e.latest_received_at"
        )
        return rows_dict(con.execute(f"""
            SELECT
              e.canonical_event_id,e.event_family_id,e.canonical_name,e.canonical_definition,
              e.event_class,e.resolution_deadline,e.outcome_type,e.status,e.taxonomy_version,
              f.canonical_name AS event_family_name,f.category,f.subcategory,e.updated_at,
              {coverage}
            FROM {source_relation} e
            LEFT JOIN er_event_families f USING(event_family_id)
            WHERE {' AND '.join(where)}
            ORDER BY e.updated_at DESC,e.canonical_event_id
            LIMIT ? OFFSET ?
        """, params))
    finally:
        con.close()


@router.get("/events/{event_id}")
def get_event(event_id: str):
    con = db_read()
    try:
        event = one_or_404(con.execute("""
            SELECT e.*,f.canonical_name AS event_family_name,f.category,f.subcategory,
                   coalesce(s.active_contract_count,0) active_contract_count,
                   coalesce(s.source_count,0) source_count,s.latest_received_at
            FROM er_canonical_events e
            LEFT JOIN er_event_families f USING(event_family_id)
            LEFT JOIN er_served_canonical_events s USING(canonical_event_id)
            WHERE e.canonical_event_id=?
        """, [event_id]), "Canonical event not found")
        contracts = rows_dict(con.execute("""
            SELECT m.mapping_id,m.mapping_type,m.confidence AS mapping_confidence,
                   m.mapping_method,m.method_version,m.review_status,m.review_notes,
                   c.source_contract_id,c.source_id,c.source_market_id,c.current_title,
                   c.current_description,c.status AS contract_status,c.opens_at,c.closes_at,
                   c.resolves_at,c.resolution_authority,c.current_resolution_rules,c.source_url,
                   o.observed_at,o.received_at,o.yes_bid,o.yes_ask,o.midpoint,o.last_trade,
                   o.displayed_probability,o.volume,o.volume_24h,o.open_interest,o.liquidity,
                   o.orderbook_depth,o.spread,o.market_status,
                   o.methodology_version AS observation_methodology_version
            FROM er_event_mappings m
            JOIN er_source_contracts c ON c.source_contract_id=m.source_contract_id
            LEFT JOIN er_latest_market_observations o ON o.source_contract_id=c.source_contract_id
            WHERE m.canonical_event_id=? AND m.valid_to IS NULL
            ORDER BY c.source_id,c.source_market_id
        """, [event_id]))
        return {"event": event, "source_contracts": contracts}
    finally:
        con.close()


@router.get("/events/{event_id}/history")
def event_history(
    event_id: str,
    source_id: Optional[str] = None,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    limit: int = Query(10000, ge=1, le=100000),
):
    """Restated history using today's active mapping. Use `/curve?mapping_basis=as_known` for backtests."""
    con = db_read()
    try:
        if not con.execute("SELECT 1 FROM er_canonical_events WHERE canonical_event_id=? LIMIT 1", [event_id]).fetchone():
            raise HTTPException(404, "Canonical event not found")
        where = ["m.canonical_event_id=?", "m.valid_to IS NULL"]
        params = [event_id]
        if source_id:
            where.append("o.source_id=?")
            params.append(source_id)
        if start:
            where.append("o.observed_at>=?")
            params.append(start)
        if end:
            where.append("o.observed_at<=?")
            params.append(end)
        params.append(limit)
        return rows_dict(con.execute(f"""
            SELECT o.source_id,o.source_contract_id,c.source_market_id,o.observed_at,o.received_at,
                   o.processed_at,o.yes_bid,o.yes_ask,o.no_bid,o.no_ask,o.midpoint,o.last_trade,
                   o.displayed_probability,o.volume,o.volume_24h,o.open_interest,o.liquidity,
                   o.orderbook_depth,o.spread,o.market_status,o.methodology_version
            FROM er_market_observations o
            JOIN er_source_contracts c USING(source_contract_id)
            JOIN er_event_mappings m USING(source_contract_id)
            WHERE {' AND '.join(where)}
            ORDER BY o.observed_at,o.received_at LIMIT ?
        """, params))
    finally:
        con.close()


@router.get("/events/{event_id}/relationships")
def event_relationships(event_id: str, include_inferred: bool = False, include_expired: bool = False):
    con = db_read()
    try:
        if not con.execute("SELECT 1 FROM er_canonical_events WHERE canonical_event_id=?", [event_id]).fetchone():
            raise HTTPException(404, "Canonical event not found")
        where = ["(source_event_id=? OR target_event_id=?)"]
        params = [event_id, event_id]
        if not include_inferred:
            where.append("relationship_class<>'inferred'")
        if not include_expired:
            where.append("valid_to IS NULL")
        relationships = rows_dict(con.execute(f"""
            SELECT * FROM er_event_relationships
            WHERE {' AND '.join(where)} ORDER BY relationship_type,created_at
        """, params))
        if relationships:
            ids = [r["relationship_id"] for r in relationships]
            marks = ",".join("?" for _ in ids)
            evidence = rows_dict(con.execute(
                f"SELECT * FROM er_event_relationship_evidence WHERE relationship_id IN ({marks}) ORDER BY relationship_id,added_at",
                ids,
            ))
            by_rel = {}
            for item in evidence:
                by_rel.setdefault(item["relationship_id"], []).append(item)
            for relationship in relationships:
                relationship["evidence"] = by_rel.get(relationship["relationship_id"], [])
        return relationships
    finally:
        con.close()


@router.get("/entities/{entity_id}")
def get_entity(entity_id: str):
    con = db_read()
    try:
        entity = one_or_404(con.execute("SELECT * FROM er_entities WHERE entity_id=?", [entity_id]), "Entity not found")
        identifiers = rows_dict(con.execute("""
            SELECT identifier_type,identifier_value,source,valid_from,valid_to,is_primary,created_at
            FROM er_entity_identifiers WHERE entity_id=?
            ORDER BY is_primary DESC,identifier_type
        """, [entity_id]))
        return {"entity": entity, "identifiers": identifiers}
    finally:
        con.close()


@router.get("/entities/{entity_id}/relationships")
def entity_relationships(entity_id: str, include_inferred: bool = False, include_expired: bool = False):
    con = db_read()
    try:
        where = ["(source_node_id=? OR target_node_id=?)"]
        params = [entity_id, entity_id]
        if not include_inferred:
            where.append("edge_class<>'inferred'")
        if not include_expired:
            where.append("valid_to IS NULL")
        edges = rows_dict(con.execute(f"""
            SELECT * FROM er_graph_edges WHERE {' AND '.join(where)}
            ORDER BY relationship_type,created_at
        """, params))
        if edges:
            ids = [e["edge_id"] for e in edges]
            marks = ",".join("?" for _ in ids)
            evidence = rows_dict(con.execute(
                f"SELECT * FROM er_edge_evidence WHERE edge_id IN ({marks}) ORDER BY edge_id,added_at", ids
            ))
            by_edge = {}
            for item in evidence:
                by_edge.setdefault(item["edge_id"], []).append(item)
            for edge in edges:
                edge["evidence"] = by_edge.get(edge["edge_id"], [])
        return edges
    finally:
        con.close()


@router.get("/changes")
def changes(since: datetime, source_id: Optional[str] = None, limit: int = Query(1000, ge=1, le=10000)):
    con = db_read()
    try:
        where = ["o.received_at>=?"]
        params = [since]
        if source_id:
            where.append("o.source_id=?")
            params.append(source_id)
        params.append(limit)
        return rows_dict(con.execute(f"""
            SELECT m.canonical_event_id,o.source_id,o.source_contract_id,c.source_market_id,
                   o.observed_at,o.received_at,o.displayed_probability,o.yes_bid,o.yes_ask,
                   o.last_trade,o.volume,o.open_interest,o.spread,o.market_status
            FROM er_market_observations o
            JOIN er_source_contracts c USING(source_contract_id)
            LEFT JOIN er_latest_contract_mappings m USING(source_contract_id)
            WHERE {' AND '.join(where)}
            ORDER BY o.received_at LIMIT ?
        """, params))
    finally:
        con.close()
