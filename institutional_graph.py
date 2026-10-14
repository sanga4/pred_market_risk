"""Build only conservative, provenance-aware knowledge-graph edges.

The graph intentionally avoids investment-impact claims.  It creates:
- structural source-contract -> canonical-event mappings elsewhere
- event -> entity `mentions` edges when the source text explicitly names a known entity
- issuer/instrument structure from the existing security master when available

Anything inferred is labelled as inferred and is never exposed as factual by default.
"""

from __future__ import annotations

import hashlib
import re
import uuid
from typing import Iterable

from db import connect

GRAPH_VERSION = "graph_rules_v1"

# A conservative seed gazetteer.  These are identity aliases, not economic-impact claims.
SEED_ENTITIES = [
    ("geo_china", "geography", "China", "CN", ["china", "prc", "people's republic of china"]),
    ("geo_taiwan", "geography", "Taiwan", "TW", ["taiwan"]),
    ("geo_united_states", "geography", "United States", "US", ["united states", "u.s.", "us"]),
    ("geo_russia", "geography", "Russia", "RU", ["russia", "russian federation"]),
    ("geo_ukraine", "geography", "Ukraine", "UA", ["ukraine"]),
    ("geo_iran", "geography", "Iran", "IR", ["iran"]),
    ("geo_israel", "geography", "Israel", "IL", ["israel"]),
    ("geo_gaza", "geography", "Gaza Strip", "PS", ["gaza", "gaza strip"]),
    ("geo_strait_hormuz", "geography", "Strait of Hormuz", None, ["strait of hormuz", "hormuz"]),
    ("org_federal_reserve", "organization", "Federal Reserve", "US", ["federal reserve", "fed", "fomc"]),
    ("org_nato", "organization", "NATO", None, ["nato", "north atlantic treaty organization"]),
    ("org_opec", "organization", "OPEC", None, ["opec", "organization of the petroleum exporting countries"]),
]


def stable_id(prefix: str, *parts: object, length: int = 24) -> str:
    raw = "\x1f".join("" if p is None else str(p) for p in parts)
    return f"{prefix}_{hashlib.sha256(raw.encode()).hexdigest()[:length]}"


def seed_entities(con):
    for entity_id, entity_type, name, country, aliases in SEED_ENTITIES:
        con.execute(
            """INSERT INTO er_entities(entity_id,entity_type,canonical_name,country_code)
               VALUES (?,?,?,?) ON CONFLICT(entity_id) DO UPDATE SET
                 entity_type=excluded.entity_type,canonical_name=excluded.canonical_name,
                 country_code=excluded.country_code,updated_at=now()""",
            [entity_id, entity_type, name, country],
        )
        for i, alias in enumerate(aliases):
            con.execute(
                """INSERT INTO er_entity_identifiers(entity_id,identifier_type,identifier_value,source,is_primary)
                   VALUES (?,?,?,?,?) ON CONFLICT(entity_id,identifier_type,identifier_value) DO NOTHING""",
                [entity_id, "alias", alias, "eventrisk_seed", i == 0],
            )


def _contains_alias(text: str, alias: str) -> bool:
    if not text or not alias:
        return False
    escaped = re.escape(alias.lower())
    return re.search(rf"(?<![a-z0-9]){escaped}(?![a-z0-9])", text.lower()) is not None


def refresh_event_mentions(con) -> int:
    seed_entities(con)
    aliases = con.execute(
        """SELECT i.entity_id,i.identifier_value,e.canonical_name,e.entity_type
           FROM er_entity_identifiers i JOIN er_entities e USING(entity_id)
           WHERE i.identifier_type='alias' AND e.active=TRUE"""
    ).fetchall()
    contracts = con.execute(
        """SELECT m.canonical_event_id,c.source_contract_id,c.source_id,c.current_title,
                  c.current_description,c.current_resolution_rules
           FROM er_latest_contract_mappings m
           JOIN er_source_contracts c USING(source_contract_id)"""
    ).fetchall()
    created = 0
    for event_id, contract_id, source_id, title, description, rules in contracts:
        text = "\n".join(x for x in [title, description, rules] if x)
        for entity_id, alias, canonical_name, entity_type in aliases:
            if not _contains_alias(text, alias):
                continue
            edge_id = stable_id("edge", event_id, "mentions", entity_id, contract_id)
            con.execute(
                """INSERT INTO er_graph_edges(
                     edge_id,source_node_id,source_node_type,relationship_type,target_node_id,target_node_type,
                     edge_class,confidence,assertion_text,inference_method,method_version,human_reviewed,review_status)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(edge_id) DO NOTHING""",
                [edge_id, event_id, "canonical_event", "mentions", entity_id, entity_type,
                 "source_asserted", 1.0, f"Source contract explicitly mentions {canonical_name}",
                 None, GRAPH_VERSION, False, "source_asserted"],
            )
            evidence_id = stable_id("evd", edge_id, contract_id)
            con.execute(
                """INSERT INTO er_edge_evidence(
                     edge_id,evidence_id,evidence_type,source_id,source_record_id,excerpt)
                   VALUES (?,?,?,?,?,?) ON CONFLICT(edge_id,evidence_id) DO NOTHING""",
                [edge_id, evidence_id, "source_contract_text", source_id, contract_id, title[:1000] if title else None],
            )
            created += 1
    return created


def bootstrap_security_master(con) -> dict:
    """Create entity/instrument identity nodes from the existing retail security master when present.

    These records remain source-attributed and are not connected to events merely because a ticker exists.
    """
    try:
        rows = con.execute(
            "SELECT ticker,name,exchange,quote_type,asset_class,sector,industry,country FROM securities"
        ).fetchall()
    except Exception:
        return {"entities": 0, "instruments": 0}
    entities = instruments = 0
    for ticker, name, exchange, quote_type, asset_class, sector, industry, country in rows:
        if not ticker:
            continue
        legal_name = name or ticker
        entity_id = stable_id("ent", (legal_name or ticker).lower(), country or "")
        instrument_id = stable_id("ins", ticker, exchange or "")
        con.execute(
            """INSERT INTO er_entities(entity_id,entity_type,canonical_name,legal_name,country_code)
               VALUES (?,?,?,?,?) ON CONFLICT(entity_id) DO NOTHING""",
            [entity_id, "legal_entity", legal_name, legal_name, country],
        )
        con.execute(
            """INSERT INTO er_entity_identifiers(entity_id,identifier_type,identifier_value,source,is_primary)
               VALUES (?,?,?,?,?) ON CONFLICT(entity_id,identifier_type,identifier_value) DO NOTHING""",
            [entity_id, "legacy_ticker_owner", ticker, "legacy_security_master", True],
        )
        con.execute(
            """INSERT INTO er_instruments(instrument_id,issuer_entity_id,instrument_type,canonical_name)
               VALUES (?,?,?,?) ON CONFLICT(instrument_id) DO NOTHING""",
            [instrument_id, entity_id, asset_class or quote_type or "equity", f"{ticker} {legal_name}"],
        )
        con.execute(
            """INSERT INTO er_instrument_identifiers(instrument_id,identifier_type,identifier_value,source)
               VALUES (?,?,?,?) ON CONFLICT(instrument_id,identifier_type,identifier_value) DO NOTHING""",
            [instrument_id, "ticker", ticker, "legacy_security_master"],
        )
        entities += 1
        instruments += 1
    return {"entities": entities, "instruments": instruments}


def run_graph_pass() -> dict:
    con = connect()
    try:
        identity = bootstrap_security_master(con)
        mentions = refresh_event_mentions(con)
        return {**identity, "mention_edges_considered": mentions}
    finally:
        con.close()


if __name__ == "__main__":
    print(run_graph_pass())
