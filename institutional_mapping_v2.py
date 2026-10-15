"""Scaled canonical-event candidate generation for EventRisk.

Contracts are compared only when they share meaningful normalized tokens across sources.
Automatic merges are deliberately rare: title, deadline *and* normalized resolution rules
must all be compatible. Missing/different rules force human review.
"""

from __future__ import annotations

from collections import defaultdict

import institutional_mapping as base

METHOD_VERSION = "mapping_rules_v2_blocked"
GENERIC = {
    "market", "price", "above", "below", "more", "less", "than", "happen", "occur",
    "reach", "yes", "no", "win", "between", "end", "year", "month", "day",
}


def meaningful_tokens(title: str) -> set[str]:
    tokens = set(base.normalize(title).split())
    return {t for t in tokens if len(t) >= 3 and t not in GENERIC}


def normalized_rules(value: str | None) -> str:
    return base.normalize(value or "")


def generate_candidates(con) -> int:
    contracts = con.execute(
        """SELECT source_contract_id,source_id,current_title,closes_at,resolves_at,current_resolution_rules
           FROM er_source_contracts WHERE active=TRUE ORDER BY source_id,source_contract_id"""
    ).fetchall()
    if not contracts:
        return 0

    token_index: dict[str, set[int]] = defaultdict(set)
    token_sets: list[set[str]] = []
    for idx, row in enumerate(contracts):
        tokens = meaningful_tokens(row[2])
        token_sets.append(tokens)
        for token in tokens:
            token_index[token].add(idx)

    seen_pairs: set[tuple[int, int]] = set()
    inserted = 0
    for i, left in enumerate(contracts):
        candidates: set[int] = set()
        for token in token_sets[i]:
            candidates.update(token_index[token])
        for j in candidates:
            if j <= i:
                continue
            right = contracts[j]
            if left[1] == right[1]:
                continue
            pair = (i, j)
            if pair in seen_pairs:
                continue
            seen_pairs.add(pair)

            shared = token_sets[i] & token_sets[j]
            required_shared = 1 if min(len(token_sets[i]), len(token_sets[j])) <= 2 else 2
            if len(shared) < required_shared:
                continue

            title_sim = base.title_similarity(left[2], right[2])
            if title_sim < 0.72:
                continue
            hours = base.deadline_hours(left[4] or left[3], right[4] or right[3])
            if hours is not None and hours > 24 * 120:
                continue

            left_rules = normalized_rules(left[5])
            right_rules = normalized_rules(right[5])
            rules_both = bool(left_rules and right_rules)
            rules_sim = base.title_similarity(left_rules, right_rules) if rules_both else None
            # Automatic equivalence requires exact normalized source rules. Similar-but-not-
            # identical rules remain reviewable candidates rather than silent merges.
            rules_compatible = rules_both and left_rules == right_rules

            exact_title = base.normalize(left[2]) == base.normalize(right[2])
            exact_deadline = hours is None or hours <= 24
            suggestion = "same_event" if exact_title and exact_deadline and rules_compatible else "related_only"

            if rules_sim is None:
                confidence = title_sim * (1.0 if exact_deadline else 0.9)
            else:
                confidence = 0.60 * title_sim + 0.30 * rules_sim + (0.10 if exact_deadline else 0.0)
            confidence = min(1.0, max(0.0, confidence))

            candidate_id = base.stable_id(
                "cand", min(left[0], right[0]), max(left[0], right[0]), METHOD_VERSION
            )
            con.execute(
                """INSERT INTO er_mapping_candidates(
                     candidate_id,left_source_contract_id,right_source_contract_id,suggested_relationship,
                     title_similarity,deadline_distance_hours,overall_confidence,candidate_method,method_version,
                     resolution_similarity,resolution_compatible,rules_available_both)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(candidate_id) DO NOTHING""",
                [candidate_id, left[0], right[0], suggestion, title_sim, hours, confidence,
                 "token_block+title+deadline+resolution_rules", METHOD_VERSION,
                 rules_sim, rules_compatible, rules_both],
            )
            inserted += 1
    return inserted


# Patch the base orchestration. base.auto_accept_exact can only see `same_event` candidates;
# this module assigns that label only after the stricter resolution-rule gate above.
base.generate_candidates = generate_candidates
base.METHOD_VERSION = METHOD_VERSION

review_candidate = base.review_candidate
bootstrap_native_events = base.bootstrap_native_events
auto_accept_exact = base.auto_accept_exact
run_mapping_pass = base.run_mapping_pass


if __name__ == "__main__":
    print(run_mapping_pass())
