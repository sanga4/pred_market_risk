"""Deterministic event ontology normalization for EventRisk Layer 2.

This classifies what an event *is* (rates, inflation, geopolitics, etc.) and extracts
simple threshold semantics. It never predicts security impact or portfolio P&L.
Canonical event/family metadata is versioned before every change for point-in-time research.
"""

from __future__ import annotations

import re
import uuid
from collections import Counter
from datetime import datetime, timezone

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


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


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


def ensure_event_baseline(con, event_id: str) -> int:
    latest = con.execute(
        "SELECT max(version_no) FROM er_canonical_event_versions WHERE canonical_event_id=?",
        [event_id],
    ).fetchone()[0]
    if latest is not None:
        return int(latest)
    con.execute(
        """INSERT INTO er_canonical_event_versions(
             canonical_event_id,version_no,event_family_id,canonical_name,canonical_definition,
             subject_entity_id,predicate,object_entity_id,event_class,geography_entity_id,
             threshold_value,threshold_unit,measurement_name,observation_period_start,observation_period_end,
             resolution_deadline,outcome_type,status,taxonomy_version,change_type,method,method_version,
             review_status,valid_from)
           SELECT canonical_event_id,1,event_family_id,canonical_name,canonical_definition,
                  subject_entity_id,predicate,object_entity_id,event_class,geography_entity_id,
                  threshold_value,threshold_unit,measurement_name,observation_period_start,observation_period_end,
                  resolution_deadline,outcome_type,status,taxonomy_version,
                  'initial','canonical_bootstrap',taxonomy_version,'system',created_at
           FROM er_canonical_events WHERE canonical_event_id=?""",
        [event_id],
    )
    return 1


def append_event_version(con, event_id: str, change_at: datetime, change_type: str) -> int:
    latest = ensure_event_baseline(con, event_id)
    con.execute(
        "UPDATE er_canonical_event_versions SET valid_to=? WHERE canonical_event_id=? AND version_no=? AND valid_to IS NULL",
        [change_at, event_id, latest],
    )
    new_version = latest + 1
    con.execute(
        """INSERT INTO er_canonical_event_versions(
             canonical_event_id,version_no,event_family_id,canonical_name,canonical_definition,
             subject_entity_id,predicate,object_entity_id,event_class,geography_entity_id,
             threshold_value,threshold_unit,measurement_name,observation_period_start,observation_period_end,
             resolution_deadline,outcome_type,status,taxonomy_version,change_type,method,method_version,
             review_status,valid_from)
           SELECT canonical_event_id,?,event_family_id,canonical_name,canonical_definition,
                  subject_entity_id,predicate,object_entity_id,event_class,geography_entity_id,
                  threshold_value,threshold_unit,measurement_name,observation_period_start,observation_period_end,
                  resolution_deadline,outcome_type,status,taxonomy_version,
                  ?,'event_ontology',?,'system',?
           FROM er_canonical_events WHERE canonical_event_id=?""",
        [new_version, change_type, TAXONOMY_VERSION, change_at, event_id],
    )
    return new_version


def ensure_family_baseline(con, family_id: str) -> int:
    latest = con.execute(
        "SELECT max(version_no) FROM er_event_family_versions WHERE event_family_id=?",
        [family_id],
    ).fetchone()[0]
    if latest is not None:
        return int(latest)
    con.execute(
        """INSERT INTO er_event_family_versions(
             event_family_id,version_no,canonical_name,category,subcategory,description,
             change_type,method,method_version,review_status,valid_from)
           SELECT event_family_id,1,canonical_name,category,subcategory,description,
                  'initial','canonical_bootstrap',NULL,'system',created_at
           FROM er_event_families WHERE event_family_id=?""",
        [family_id],
    )
    return 1


def append_family_version(con, family_id: str, change_at: datetime) -> int:
    latest = ensure_family_baseline(con, family_id)
    con.execute(
        "UPDATE er_event_family_versions SET valid_to=? WHERE event_family_id=? AND version_no=? AND valid_to IS NULL",
        [change_at, family_id, latest],
    )
    new_version = latest + 1
    con.execute(
        """INSERT INTO er_event_family_versions(
             event_family_id,version_no,canonical_name,category,subcategory,description,
             change_type,method,method_version,review_status,valid_from)
           SELECT event_family_id,?,canonical_name,category,subcategory,description,
                  'taxonomy_normalization','event_ontology',?,'system',?
           FROM er_event_families WHERE event_family_id=?""",
        [new_version, TAXONOMY_VERSION, change_at, family_id],
    )
    return new_version


def run_ontology_pass() -> dict:
    con = connect()
    changed = 0
    family_changed = 0
    revisions = 0
    try:
        rows = con.execute(
            """SELECT canonical_event_id,event_family_id,canonical_name,canonical_definition,
                      event_class,predicate,threshold_value,threshold_unit,taxonomy_version
               FROM er_canonical_events"""
        ).fetchall()
        for event_id, family_id, name, definition, old_class, old_predicate, old_value, old_unit, old_version in rows:
            ensure_event_baseline(con, event_id)
            text = f"{name or ''}\n{definition or ''}"
            event_class = classify(text)
            predicate, value, unit = extract_threshold(text)
            new_tuple = (event_class, predicate, value, unit, TAXONOMY_VERSION)
            old_tuple = (old_class, old_predicate, old_value, old_unit, old_version)
            if new_tuple == old_tuple:
                continue
            change_at = now_utc()
            con.execute(
                """UPDATE er_canonical_events SET event_class=?,predicate=?,threshold_value=?,threshold_unit=?,
                          taxonomy_version=?,updated_at=? WHERE canonical_event_id=?""",
                [event_class, predicate, value, unit, TAXONOMY_VERSION, change_at, event_id],
            )
            append_event_version(con, event_id, change_at, "taxonomy_normalization")
            con.execute(
                """INSERT INTO er_revisions(
                     revision_id,object_type,object_id,revision_type,reason,previous_version,new_version,effective_at)
                   VALUES (?,?,?,?,?,?,?,?)""",
                [f"rev_{uuid.uuid4().hex}", "canonical_event", event_id, "taxonomy_normalization",
                 "Deterministic Layer 2 event classification/threshold normalization",
                 str(old_tuple), str(new_tuple), change_at],
            )
            changed += 1
            revisions += 1

        families = con.execute(
            "SELECT event_family_id,category FROM er_event_families"
        ).fetchall()
        for family_id, old_category in families:
            ensure_family_baseline(con, family_id)
            classes = [r[0] for r in con.execute(
                "SELECT event_class FROM er_canonical_events WHERE event_family_id=? AND event_class IS NOT NULL",
                [family_id],
            ).fetchall()]
            if not classes:
                continue
            category = Counter(classes).most_common(1)[0][0]
            if category == old_category:
                continue
            change_at = now_utc()
            con.execute(
                "UPDATE er_event_families SET category=?,updated_at=? WHERE event_family_id=?",
                [category, change_at, family_id],
            )
            append_family_version(con, family_id, change_at)
            family_changed += 1
        return {
            "events_normalized": changed,
            "families_normalized": family_changed,
            "revisions_recorded": revisions,
            "taxonomy_version": TAXONOMY_VERSION,
        }
    finally:
        con.close()


if __name__ == "__main__":
    print(run_ontology_pass())
