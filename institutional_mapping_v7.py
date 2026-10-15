"""Indexed incremental canonical mapping v7 with cheaper pair filtering.

v7 preserves v6 candidate/review semantics and semantic-version history. The only judgement-
neutral execution change is in candidate blocking: SQL still discovers exact-title and rare-token
cross-source pair IDs, but the meaningful-token overlap threshold is evaluated in Python from the
already fetched normalized titles instead of rejoining every pair to the 2.6M-row token index and
grouping shared tokens in MotherDuck. This removes the dominant pair-shared aggregation while
producing the same eligible pair set.
"""

from __future__ import annotations

import institutional_mapping as base
import institutional_mapping_v2 as v2
import institutional_mapping_v3 as v3
import institutional_mapping_v4 as v4
import institutional_mapping_v5 as v5
import institutional_mapping_v6 as v6

METHOD_VERSION = "mapping_rules_v7_python_overlap"
RELATIONSHIP_TYPES = v6.RELATIONSHIP_TYPES
DIRECTIONS = v6.DIRECTIONS

for module in (base, v2, v3, v4, v5, v6):
    module.METHOD_VERSION = METHOD_VERSION


def _candidate_pair_rows(con, changed_rows: list[dict]) -> tuple[list[dict], dict]:
    if not changed_rows:
        return [], {"pairs_blocked": 0, "pairs_evaluated": 0, "token_frequency_rows": 0}

    v5._register_changed_ids(con, changed_rows)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _mapping_token_freq AS
        SELECT t.token,count(*)::BIGINT AS token_frequency
        FROM er_mapping_token_index t
        JOIN er_mapping_contract_index i USING(source_contract_id)
        WHERE i.active=TRUE
        GROUP BY t.token
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _mapping_changed_rare_tokens AS
        WITH ranked AS (
          SELECT t.source_contract_id,t.token,f.token_frequency,
                 row_number() OVER(
                   PARTITION BY t.source_contract_id
                   ORDER BY f.token_frequency,t.token
                 ) AS rn
          FROM er_mapping_token_index t
          JOIN _mapping_changed_ids c USING(source_contract_id)
          JOIN _mapping_token_freq f USING(token)
          WHERE f.token_frequency<=?
        )
        SELECT source_contract_id,token,token_frequency
        FROM ranked WHERE rn<=?
    """, [v5.MAX_BLOCK_MEMBERS, v5.MAX_BLOCK_TOKENS])
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _mapping_pair_ids AS
        SELECT DISTINCT least(left_id,right_id) AS left_id,greatest(left_id,right_id) AS right_id
        FROM (
          SELECT c.source_contract_id AS left_id,p.source_contract_id AS right_id
          FROM _mapping_changed_ids ch
          JOIN er_mapping_contract_index c USING(source_contract_id)
          JOIN er_mapping_contract_index p
            ON p.active=TRUE
           AND p.source_id<>c.source_id
           AND p.source_contract_id<>c.source_contract_id
           AND p.normalized_title=c.normalized_title
           AND p.deadline_bucket=c.deadline_bucket
          WHERE c.active=TRUE AND c.normalized_title<>''

          UNION ALL

          SELECT ct.source_contract_id AS left_id,pt.source_contract_id AS right_id
          FROM _mapping_changed_rare_tokens ct
          JOIN er_mapping_token_index pt USING(token)
          JOIN er_mapping_contract_index c ON c.source_contract_id=ct.source_contract_id
          JOIN er_mapping_contract_index p ON p.source_contract_id=pt.source_contract_id
          WHERE c.active=TRUE AND p.active=TRUE
            AND c.source_id<>p.source_id
            AND c.source_contract_id<>p.source_contract_id
        ) pairs
    """)
    blocked = int(con.execute("SELECT count(*) FROM _mapping_pair_ids").fetchone()[0])
    rows = v5._rows_dict(con.execute("""
        SELECT p.left_id,p.right_id,
               l.source_id AS left_source,r.source_id AS right_source,
               l.semantic_version_no AS left_semantic_version_no,
               r.semantic_version_no AS right_semantic_version_no,
               l.normalized_title AS left_normalized_title,
               r.normalized_title AS right_normalized_title,
               l.normalized_rules AS left_normalized_rules,
               r.normalized_rules AS right_normalized_rules,
               l.deadline_at AS left_deadline,r.deadline_at AS right_deadline,
               l.meaningful_token_count AS left_token_count,
               r.meaningful_token_count AS right_token_count
        FROM _mapping_pair_ids p
        JOIN er_mapping_contract_index l ON l.source_contract_id=p.left_id
        JOIN er_mapping_contract_index r ON r.source_contract_id=p.right_id
        ORDER BY p.left_id,p.right_id
    """))

    eligible: list[dict] = []
    for row in rows:
        left_tokens = v2.meaningful_tokens(row["left_normalized_title"] or "")
        right_tokens = v2.meaningful_tokens(row["right_normalized_title"] or "")
        required_shared = 1 if min(len(left_tokens), len(right_tokens)) <= 2 else 2
        shared_count = len(left_tokens & right_tokens)
        if shared_count < required_shared:
            continue
        row["shared_token_count"] = shared_count
        eligible.append(row)

    freq_rows = int(con.execute("SELECT count(*) FROM _mapping_token_freq").fetchone()[0])
    return eligible, {
        "pairs_blocked": blocked,
        "pairs_evaluated": len(eligible),
        "token_frequency_rows": freq_rows,
        "overlap_filter": "python_meaningful_tokens_v1",
    }


def run_mapping_pass() -> dict:
    v5._candidate_pair_rows = _candidate_pair_rows
    # Keep v6's semantic-versioned candidate materialization/history and exact-review sync.
    v5._candidate_rows = v6._candidate_rows
    v5._persist_candidates = v6._persist_candidates
    v3.auto_accept_exact = v6._auto_accept_exact
    result = v5.run_mapping_pass()
    result["method_version"] = METHOD_VERSION
    result["candidate_history"] = "semantic_versioned_v1"
    result["blocking_execution"] = "python_overlap_v1"
    return result


review_candidate = v6.review_candidate


if __name__ == "__main__":
    print(run_mapping_pass())
