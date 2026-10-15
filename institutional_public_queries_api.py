"""Pure read-only query surfaces for the investor-facing institutional API.

This module intentionally contains no administrative actions, mapping review, archive restoration,
API-key creation/revocation, release generation, migrations, ingestion, or enrichment imports.
It is safe to import inside the stateless public API without initializing write-side model code.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from db import connect

router = APIRouter(tags=["institutional-public-queries"])


def rows_dict(cur):
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def db_read():
    try:
        return connect(read_only=True)
    except Exception as exc:
        raise HTTPException(503, f"Institutional data store unavailable: {exc}")


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
        contract = rows_dict(
            con.execute("SELECT * FROM er_source_contracts WHERE source_contract_id=?", [contract_id])
        )
        if not contract:
            raise HTTPException(404, "Source contract not found")
        versions = rows_dict(con.execute(
            "SELECT * FROM er_source_contract_versions WHERE source_contract_id=? ORDER BY version_no",
            [contract_id],
        ))
        mappings = rows_dict(con.execute(
            "SELECT * FROM er_event_mappings WHERE source_contract_id=? ORDER BY valid_from",
            [contract_id],
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


@router.get("/v1/events/{event_id}/as-of")
def event_as_of(event_id: str, at: datetime):
    con = db_read()
    try:
        event = rows_dict(
            con.execute("SELECT * FROM er_canonical_events WHERE canonical_event_id=?", [event_id])
        )
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
        family = rows_dict(
            con.execute("SELECT * FROM er_event_families WHERE event_family_id=?", [family_id])
        )
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
            median = (
                probs_sorted[n // 2]
                if n % 2
                else (probs_sorted[n // 2 - 1] + probs_sorted[n // 2]) / 2
            )
            summary = {
                "source_count": len(probs),
                "min": min(probs),
                "max": max(probs),
                "median": median,
                "range": max(probs) - min(probs),
            }
        return {
            "canonical_event_id": event_id,
            "descriptive_summary": summary,
            "sources": rows,
            "note": (
                "Descriptive statistics only; EventRisk does not prescribe a consensus "
                "probability in Layer 1/2."
            ),
        }
    finally:
        con.close()


@router.get("/v1/instruments")
def list_instruments(
    ticker: Optional[str] = None,
    sector: Optional[str] = None,
    country: Optional[str] = None,
    active: bool = True,
    after: Optional[str] = None,
    limit: int = Query(250, ge=1, le=2000),
):
    """Discover reference instruments using stable keyset pagination.

    Source exchange labels are intentionally distinct from `exchange_mic`; issuer_entity_id and
    exchange_mic remain null until a stronger reference source actually establishes them.
    """
    con = db_read()
    try:
        where = ["r.active=?"]
        params: list[object] = [active]
        if ticker:
            where.append("upper(r.source_ticker)=upper(?)")
            params.append(ticker)
        if sector:
            where.append("r.sector=?")
            params.append(sector)
        if country:
            where.append("r.country=?")
            params.append(country)
        if after:
            where.append("r.instrument_id>?")
            params.append(after)
        params.append(limit + 1)
        rows = rows_dict(con.execute(
            f"""SELECT instrument_id,issuer_entity_id,instrument_type,canonical_name,currency,
                       exchange_mic,active,source_system,source_ticker,source_name,exchange_label,
                       asset_class,sector,industry,country,source_record_source,source_updated_at,
                       reference_recorded_at,reference_updated_at
                FROM er_instrument_reference_current r
                WHERE {' AND '.join(where)}
                ORDER BY instrument_id
                LIMIT ?""",
            params,
        ))
        has_more = len(rows) > limit
        items = rows[:limit]
        return {
            "items": items,
            "next_after": items[-1]["instrument_id"] if has_more and items else None,
            "count": len(items),
            "filters": {"ticker": ticker, "sector": sector, "country": country, "active": active},
            "note": "Reference instruments are source-described; null issuer/MIC fields are intentionally unasserted.",
        }
    finally:
        con.close()


@router.get("/v1/instruments/{instrument_id}/as-of")
def instrument_as_of(instrument_id: str, at: datetime):
    con = db_read()
    try:
        instrument = rows_dict(con.execute(
            """SELECT * FROM er_instrument_versions
               WHERE instrument_id=? AND valid_from<=? AND (valid_to IS NULL OR valid_to>?)
               ORDER BY version_no DESC LIMIT 1""",
            [instrument_id, at, at],
        ))
        reference = rows_dict(con.execute(
            """SELECT * FROM er_instrument_reference_attribute_versions
               WHERE instrument_id=? AND valid_from<=? AND (valid_to IS NULL OR valid_to>?)
               ORDER BY version_no DESC LIMIT 1""",
            [instrument_id, at, at],
        ))
        if not instrument and not reference:
            raise HTTPException(404, "Instrument not known at requested as-of time")
        identifiers = rows_dict(con.execute(
            """SELECT instrument_id,identifier_type,identifier_value,source,valid_from,valid_to
               FROM er_instrument_identifiers
               WHERE instrument_id=? AND (valid_from IS NULL OR valid_from<=?)
                 AND (valid_to IS NULL OR valid_to>?)
               ORDER BY identifier_type,identifier_value""",
            [instrument_id, at, at],
        ))
        return {
            "instrument_id": instrument_id,
            "as_of": at,
            "instrument": instrument[0] if instrument else None,
            "reference": reference[0] if reference else None,
            "identifiers": identifiers,
        }
    finally:
        con.close()


@router.get("/v1/instruments/{instrument_id}")
def get_instrument(instrument_id: str):
    con = db_read()
    try:
        current = rows_dict(con.execute(
            "SELECT * FROM er_instrument_reference_current WHERE instrument_id=?",
            [instrument_id],
        ))
        if not current:
            raise HTTPException(404, "Instrument not found")
        identifiers = rows_dict(con.execute(
            """SELECT * FROM er_instrument_identifiers
               WHERE instrument_id=? ORDER BY identifier_type,identifier_value,valid_from""",
            [instrument_id],
        ))
        instrument_versions = rows_dict(con.execute(
            "SELECT * FROM er_instrument_versions WHERE instrument_id=? ORDER BY version_no",
            [instrument_id],
        ))
        reference_versions = rows_dict(con.execute(
            """SELECT * FROM er_instrument_reference_attribute_versions
               WHERE instrument_id=? ORDER BY version_no""",
            [instrument_id],
        ))
        issuer = None
        issuer_id = current[0].get("issuer_entity_id")
        if issuer_id:
            rows = rows_dict(con.execute("SELECT * FROM er_entities WHERE entity_id=?", [issuer_id]))
            issuer = rows[0] if rows else None
        return {
            "instrument": current[0],
            "identifiers": identifiers,
            "instrument_versions": instrument_versions,
            "reference_versions": reference_versions,
            "issuer": issuer,
            "note": "Issuer and MIC are returned only when explicitly established by reference data; EventRisk does not infer them from names.",
        }
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
            f"SELECT * FROM er_quality_issues WHERE {' AND '.join(where)} ORDER BY detected_at DESC LIMIT ?",
            params,
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
                ORDER BY relationship_type,created_at LIMIT ?""",
            params,
        ))
        return {"node_id": node_id, "include_inferred": include_inferred, "edges": edges}
    finally:
        con.close()


@router.get("/v1/releases")
def releases(limit: int = Query(50, ge=1, le=500)):
    con = db_read()
    try:
        return rows_dict(
            con.execute("SELECT * FROM er_data_releases ORDER BY generated_at DESC LIMIT ?", [limit])
        )
    finally:
        con.close()


@router.get("/v1/releases/{release_id}")
def release(release_id: str):
    con = db_read()
    try:
        releases = rows_dict(
            con.execute("SELECT * FROM er_data_releases WHERE release_id=?", [release_id])
        )
        if not releases:
            raise HTTPException(404, "Release not found")
        datasets = rows_dict(con.execute(
            "SELECT * FROM er_export_manifests WHERE release_id=? ORDER BY dataset_name",
            [release_id],
        ))
        return {"release": releases[0], "datasets": datasets}
    finally:
        con.close()
