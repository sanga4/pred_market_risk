"""Deterministic event ontology normalization for EventRisk Layer 2.

This classifies what an event *is* (rates, inflation, geopolitics, etc.) and extracts
simple threshold semantics. It never predicts security impact or portfolio P&L.
"""

from __future__ import annotations

import re
import uuid
from collections import Counter

from db import connect

TAXONOMY_VERSION = "event_ontology_v1"

CLASS_RULES = [
    ("geopolitics", re.compile(r"\b(war|invasion|invade|blockade|military|missile|nuclear|sanction|ceasefire|nato|taiwan|ukraine|russia|iran|israel|gaza|hormuz|tariff|trade war)\b", re.I)),
    ("elections", re.compile(r"\b(election|electoral|president|prime minister|congress|senate|house|governor|vote|ballot)\b", re.I)),
    ("rates", re.compile(r"\b(fed|fomc|interest rate|rate cut|rate hike|basis points|bps|central bank|ecb|boe|boj)\b", re.I)),
    ("inflation", re.compile(r"\b(cpi|inflation|pce|consumer price|price index)\b", re.I)),
    ("labor", re.compile(r"\b(unemployment|payroll|jobs report|jobless|employment|nonfarm)\b", re.I)),
    ("growth_recession", re.compile(r"\b(recession|gdp|economic growth|contraction|soft landing)\b", re.I)),
    ("commodities_energy", re.compile(r"\b(oil|crude|brent|wti|natural gas|gold|copper|opec|commodity)\b", re.I)),
    ("corporate", re.compile(r"\b(merger|acquisition|takeover|bankruptcy|chapter 11|earnings|ipo|buyout)\b", re.I)),
    ("crypto", re.compile(r"\b(bitcoin|btc|ethereum|eth|crypto|cryptocurrency|solana)\b", re.I)),
    ("technology_science", re.compile(r"\b(ai|artificial intelligence|semiconductor|chip|space|launch|science|technology|fda|drug approval)\b", re.I)),
]

THRESHOLD_RE = re.compile(
    r"\b(?P<op>above|over|greater than|at least|below|under|less than|at most)\s*"
    r"(?P<currency>\$)?(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>%|percent|bps|basis points|million|billion|trillion)?",
    re.I,
)


def classify(text: str) -> str:
    for event_class, pattern in CLASS_RULES:
        if pattern.search(text or ""):
            return event_class
    return "other"


def extract_threshold(text: str):
    match = THRESHOLD_RE.search(text or "")
    if not match:
        return None, None, None
    op = match.group("op").lower()
    predicate = {
        "above": "greater_than",
        "over": "greater_than",
        "greater than": "greater_than",
        "at least": "greater_than_or_equal",
        "below": "less_than",
        "under": "less_than",
        "less than": "less_than",
        "at most": "less_than_or_equal",
    }[op]
    value = float(match.group("value"))
    unit = match.group("unit")
    if match.group("currency"):
        unit = f"USD_{unit.lower()}" if unit else "USD"
    elif unit:
        unit = "percent" if unit.lower() in {"%", "percent"} else unit.lower().replace(" ", "_")
    return predicate, value, unit


def run_ontology_pass() -> dict:
    con = connect()
    changed = 0
    revisions = 0
    try:
        rows = con.execute(
            """SELECT canonical_event_id,event_family_id,canonical_name,canonical_definition,
                      event_class,predicate,threshold_value,threshold_unit,taxonomy_version
               FROM er_canonical_events"""
        ).fetchall()
        for event_id, family_id, name, definition, old_class, old_predicate, old_value, old_unit, old_version in rows:
            text = f"{name or ''}\n{definition or ''}"
            event_class = classify(text)
            predicate, value, unit = extract_threshold(text)
            new_tuple = (event_class, predicate, value, unit, TAXONOMY_VERSION)
            old_tuple = (old_class, old_predicate, old_value, old_unit, old_version)
            if new_tuple == old_tuple:
                continue
            con.execute(
                """UPDATE er_canonical_events SET event_class=?,predicate=?,threshold_value=?,threshold_unit=?,
                          taxonomy_version=?,updated_at=now() WHERE canonical_event_id=?""",
                [event_class, predicate, value, unit, TAXONOMY_VERSION, event_id],
            )
            con.execute(
                """INSERT INTO er_revisions(
                     revision_id,object_type,object_id,revision_type,reason,previous_version,new_version,effective_at)
                   VALUES (?,?,?,?,?,?,?,now())""",
                [f"rev_{uuid.uuid4().hex}", "canonical_event", event_id, "taxonomy_normalization",
                 "Deterministic Layer 2 event classification/threshold normalization",
                 str(old_tuple), str(new_tuple)],
            )
            changed += 1
            revisions += 1

        families = con.execute("SELECT event_family_id FROM er_event_families").fetchall()
        for (family_id,) in families:
            classes = [r[0] for r in con.execute(
                "SELECT event_class FROM er_canonical_events WHERE event_family_id=? AND event_class IS NOT NULL",
                [family_id],
            ).fetchall()]
            if not classes:
                continue
            category = Counter(classes).most_common(1)[0][0]
            con.execute(
                "UPDATE er_event_families SET category=?,updated_at=now() WHERE event_family_id=?",
                [category, family_id],
            )
        return {"events_normalized": changed, "revisions_recorded": revisions, "taxonomy_version": TAXONOMY_VERSION}
    finally:
        con.close()


if __name__ == "__main__":
    print(run_ontology_pass())
