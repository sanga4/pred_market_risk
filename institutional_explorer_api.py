"""Read-only discovery of the existing event -> family -> category hierarchy.

The explorer calls event families "subjects" and stored family categories "themes".
Those are presentation aliases, not a claim that subject_entity_id has been populated
or that an entity mention establishes a causal/security relationship. No schema or
identity is changed here. Discovery touches current state, never observation history.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from institutional_api import db_read, rows_dict

router = APIRouter(prefix="/v1/explorer", tags=["institutional-explorer"])

DEFINITIONS = {
    "event": "A canonical EventRisk outcome with at least one active source contract.",
    "subject": "An existing event family: a normalized grouping of related questions or horizons, not proof of event equivalence.",
    "theme": "The subject family's stored category. Rule-based taxonomy, not a reviewed causal relationship; 'other' and 'unclassified' retain their original meaning.",
    "entity_mention": "A separately evidenced source assertion. A mention does not establish the event's formal subject or a security impact.",
    "securities": "Reference instruments are separate. This explorer stops at themes; it does not infer security exposure.",
}


def _live(*, q=None, theme=None, subject_id=None, source=None):
    """One row per currently served active event; source filters apply before counts."""
    where = [
        "e.status='active'", "c.active=TRUE", "m.valid_from<=now()", "m.valid_to IS NULL",
    ]
    params = []
    if q:
        where.append("(contains(lower(e.canonical_name),lower(?)) OR "
                     "contains(lower(coalesce(f.canonical_name,'')),lower(?)) OR e.canonical_event_id=?)")
        params.extend([q, q, q])
    if theme:
        where.append("coalesce(nullif(f.category,''),'unclassified')=?")
        params.append(theme)
    if subject_id:
        where.append("f.event_family_id=?")
        params.append(subject_id)
    if source:
        where.append("c.source_id=?")
        params.append(source)
    return f"""WITH live AS (
        SELECT e.canonical_event_id,e.canonical_name,e.event_class,e.taxonomy_version,
               e.resolution_deadline,e.subject_entity_id,
               f.event_family_id AS subject_id,f.canonical_name AS subject_name,
               coalesce(nullif(f.category,''),'unclassified') AS theme_id,
               count(DISTINCT c.source_contract_id) AS contract_count,
               count(DISTINCT c.source_id) AS source_count,
               string_agg(DISTINCT c.source_id,',' ORDER BY c.source_id) AS sources,
               max(s.received_at) AS latest_received_at,max(s.last_polled_at) AS latest_polled_at
        FROM er_canonical_events e
        JOIN er_event_mappings m USING(canonical_event_id)
        JOIN er_source_contracts c USING(source_contract_id)
        LEFT JOIN er_event_families f USING(event_family_id)
        LEFT JOIN er_market_current_state s USING(source_contract_id)
        WHERE {' AND '.join(where)}
        GROUP BY ALL
    ) """, params


def _page(rows, limit, key):
    items = rows[:limit]
    return {
        "items": items,
        "count": len(items),
        "next_after": items[-1][key] if len(rows) > limit and items else None,
        "as_of": datetime.now(timezone.utc),
    }


@router.get("/summary")
def summary(source: Optional[str] = Query(None, max_length=100)):
    """Current coverage and descriptive data gaps, without reading historical ticks."""
    con = db_read()
    try:
        cte, params = _live(source=source)
        coverage = rows_dict(con.execute(cte + """
            SELECT count(*) AS events,count(DISTINCT subject_id) AS subjects,
                   count(DISTINCT theme_id) AS themes,
                   count(*) FILTER (WHERE theme_id IN ('other','unclassified')) AS uncategorized_events,
                   count(*) FILTER (WHERE subject_id IS NULL) AS events_without_family,
                   count(*) FILTER (WHERE subject_entity_id IS NOT NULL) AS formal_subject_events,
                   count(*) FILTER (WHERE source_count>1) AS cross_source_events,
                   max(latest_received_at) AS latest_received_at
            FROM live
        """, params))[0]
        themes = rows_dict(con.execute(cte + """
            SELECT theme_id,count(*) AS event_count,count(DISTINCT subject_id) AS subject_count,
                   sum(contract_count) AS contract_count
            FROM live GROUP BY theme_id ORDER BY event_count DESC,theme_id
        """, params))
        reference = rows_dict(con.execute("""
            SELECT count(*) AS instruments,
                   count(*) FILTER (WHERE issuer_entity_id IS NOT NULL) AS instruments_with_issuer
            FROM er_instruments WHERE active=TRUE
        """))[0]
        entities = int(con.execute("SELECT count(*) FROM er_entities WHERE active=TRUE").fetchone()[0])
        sources = rows_dict(con.execute("""
            SELECT s.source_id,s.source_name,h.health_status,h.last_success_at,h.last_observation_at
            FROM er_sources s LEFT JOIN er_source_health h USING(source_id)
            WHERE s.active=TRUE ORDER BY s.source_id
        """))
        return {
            "as_of": datetime.now(timezone.utc), "source_filter": source,
            "coverage": coverage, "themes": themes, "sources": sources,
            "reference": {**reference, "entities": entities}, "definitions": DEFINITIONS,
            "basis": "Current active canonical events and active source contracts; current taxonomy. Not an as-known historical snapshot.",
        }
    finally:
        con.close()


@router.get("/events")
def events(
    q: Optional[str] = Query(None, max_length=200),
    theme: Optional[str] = Query(None, max_length=100),
    subject_id: Optional[str] = Query(None, max_length=200),
    source: Optional[str] = Query(None, max_length=100),
    after: Optional[str] = Query(None, max_length=200),
    limit: int = Query(30, ge=1, le=100),
):
    """Search the complete live event universe with stable-ID keyset pagination."""
    con = db_read()
    try:
        cte, params = _live(q=q, theme=theme, subject_id=subject_id, source=source)
        where = "WHERE canonical_event_id>?" if after else ""
        if after:
            params.append(after)
        params.append(limit + 1)
        rows = rows_dict(con.execute(cte + f"""
            SELECT * FROM live {where} ORDER BY canonical_event_id LIMIT ?
        """, params))
        return _page(rows, limit, "canonical_event_id")
    finally:
        con.close()


@router.get("/subjects")
def subjects(
    q: Optional[str] = Query(None, max_length=200),
    theme: Optional[str] = Query(None, max_length=100),
    source: Optional[str] = Query(None, max_length=100),
    after: Optional[str] = Query(None, max_length=200),
    limit: int = Query(30, ge=1, le=100),
):
    """Discover event families and their theme assignments across all live events."""
    con = db_read()
    try:
        cte, params = _live(q=q, theme=theme, source=source)
        where = ["subject_id IS NOT NULL"]
        if after:
            where.append("subject_id>?")
            params.append(after)
        params.append(limit + 1)
        rows = rows_dict(con.execute(cte + f"""
            SELECT subject_id,subject_name,theme_id,count(*) AS event_count,
                   sum(contract_count) AS contract_count,max(latest_received_at) AS latest_received_at
            FROM live WHERE {' AND '.join(where)}
            GROUP BY subject_id,subject_name,theme_id ORDER BY subject_id LIMIT ?
        """, params))
        return {**_page(rows, limit, "subject_id"), "definition": DEFINITIONS["subject"]}
    finally:
        con.close()


@router.get("/events/{event_id}/mappings")
def mappings(event_id: str, include_expired: bool = False):
    """Trace a selected event to its family, category, contracts and mention evidence.

    Expired contract mappings are available for audit, but are never used to compute
    current probability comparisons. Family/category values remain current taxonomy.
    """
    con = db_read()
    try:
        found = rows_dict(con.execute("""
            SELECT * FROM er_canonical_events WHERE canonical_event_id=?
        """, [event_id]))
        if not found:
            raise HTTPException(404, "Canonical event not found")
        event = found[0]
        family = rows_dict(con.execute("""
            SELECT * FROM er_event_families WHERE event_family_id=?
        """, [event["event_family_id"]]))
        subject = family[0] if family else None
        family_versions = rows_dict(con.execute("""
            SELECT version_no,method,method_version,review_status,valid_from,valid_to
            FROM er_event_family_versions WHERE event_family_id=?
            ORDER BY version_no DESC LIMIT 1
        """, [event["event_family_id"]]))
        current = "" if include_expired else "AND m.valid_from<=now() AND m.valid_to IS NULL"
        contracts = rows_dict(con.execute(f"""
            SELECT m.mapping_id,m.mapping_type,m.confidence,m.mapping_method,m.method_version,
                   m.review_status,m.review_notes,m.valid_from,m.valid_to,
                   c.source_contract_id,c.source_id,c.source_market_id,c.current_title,
                   c.active,c.status AS contract_status,c.source_url,c.current_resolution_rules,
                   s.observed_at,s.received_at,s.last_polled_at,s.displayed_probability,s.yes_bid,s.yes_ask,
                   s.spread,s.volume_24h,s.liquidity,s.methodology_version,
                   (c.active AND m.valid_from<=now() AND m.valid_to IS NULL) AS is_current
            FROM er_event_mappings m JOIN er_source_contracts c USING(source_contract_id)
            LEFT JOIN er_market_current_state s USING(source_contract_id)
            WHERE m.canonical_event_id=? {current}
            ORDER BY m.valid_from DESC,c.source_id,m.mapping_id
        """, [event_id]))
        mentions = rows_dict(con.execute("""
            SELECT g.*,e.canonical_name AS entity_name,e.entity_type
            FROM er_graph_edges g JOIN er_entities e ON e.entity_id=g.target_node_id
            WHERE g.source_node_id=? AND g.source_node_type='canonical_event'
              AND g.relationship_type='mentions' AND g.edge_class='source_asserted'
              AND g.valid_from<=now() AND g.valid_to IS NULL
            ORDER BY e.canonical_name,g.edge_id
        """, [event_id]))
        evidence = rows_dict(con.execute("""
            SELECT ev.* FROM er_edge_evidence ev
            JOIN er_graph_edges g USING(edge_id)
            WHERE g.source_node_id=? AND g.relationship_type='mentions'
              AND g.edge_class='source_asserted' AND g.valid_from<=now() AND g.valid_to IS NULL
            ORDER BY ev.edge_id,ev.evidence_id
        """, [event_id]))
        by_edge = {}
        for item in evidence:
            by_edge.setdefault(item["edge_id"], []).append(item)
        for edge in mentions:
            edge["evidence"] = by_edge.get(edge["edge_id"], [])

        current_contracts = [c for c in contracts if c["is_current"]]
        probabilities = [c["displayed_probability"] for c in current_contracts
                         if c["displayed_probability"] is not None]
        source_ids = sorted({c["source_id"] for c in current_contracts})
        # No range is called cross-source unless at least two distinct venues supply a price.
        priced_sources = {c["source_id"] for c in current_contracts if c["displayed_probability"] is not None}
        theme_id = (subject.get("category") if subject else None) or "unclassified"
        return {
            "as_of": datetime.now(timezone.utc), "event": event, "subject": subject,
            "theme": {"theme_id": theme_id, "assignment": "stored_family_category",
                      "family_version": family_versions[0] if family_versions else None,
                      "event_class": event["event_class"], "taxonomy_version": event["taxonomy_version"]},
            "source_contracts": contracts, "entity_mentions": mentions,
            "insights": {
                "current_contract_count": len(current_contracts), "source_ids": source_ids,
                "priced_source_count": len(priced_sources),
                "probability_range": max(probabilities) - min(probabilities) if len(priced_sources)>1 else None,
                "theme_needs_review": theme_id in {"other", "unclassified"},
                "formal_subject_present": event["subject_entity_id"] is not None,
                "note": "Descriptive current observations only. Family membership and entity mentions do not establish event equivalence or security exposure.",
            },
            "definitions": DEFINITIONS, "include_expired": include_expired,
            "basis": "Current taxonomy and latest per-contract observations, including on expired mappings when requested. Use /as-known or /history-point-in-time for historical reconstruction.",
        }
    finally:
        con.close()
