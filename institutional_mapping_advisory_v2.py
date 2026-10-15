"""Advisory-only cross-source relationship discovery v2.

Judgement and review semantics remain the same as mapping v8/advisory v1. The execution
boundary changes so the expensive candidate-pair construction no longer materializes large
pair/frequency temp tables inside the shared MotherDuck warehouse while the hot ingestion lane
is writing.

The warehouse is used to:
- maintain the incremental mapping index/watermark;
- read the current changed-contract tokens and matching active index rows;
- persist the compact candidate/history/state result.

Token-frequency counting, rare-token ranking, pair construction and overlap filtering execute in
Python on the enrichment Flight. This is advisory only: native canonical bootstrap and automatic
same-event identity merges remain disabled. Deterministic identity is owned exclusively by
``institutional_mapping_identity_v1``.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Iterable

import pyarrow as pa

ADVISORY_EXECUTION_VERSION = "mapping_advisory_v2_local_pair_discovery"
_FETCH_BATCH_ROWS = 50_000


def _rows_batched(cursor, batch_rows: int = _FETCH_BATCH_ROWS) -> Iterable[tuple]:
    while True:
        rows = cursor.fetchmany(batch_rows)
        if not rows:
            return
        yield from rows


def _candidate_pair_rows_local(con, changed_rows: list[dict]) -> tuple[list[dict], dict]:
    """Build the exact v8 candidate-pair input without remote pair materialization.

    The rarity threshold, max blocking tokens, exact-title/deadline block, opposite-source rule,
    pair de-duplication and meaningful-token overlap threshold intentionally match mapping v8.
    """
    import institutional_mapping_v2 as meaning
    import institutional_mapping_v5 as v5

    if not changed_rows:
        return [], {
            "pairs_blocked": 0,
            "pairs_evaluated": 0,
            "token_frequency_rows": 0,
            "changed_token_rows": 0,
            "frequency_scope": "changed_tokens_only_local",
            "overlap_filter": "python_meaningful_tokens_v1",
            "execution_location": "enrichment_flight_local_python",
            "remote_token_rows_fetched": 0,
            "remote_metadata_rows_fetched": 0,
        }

    v5._register_changed_ids(con, changed_rows)

    index_rows = v5._rows_dict(con.execute("""
        SELECT i.source_contract_id,i.source_id,i.semantic_version_no,i.normalized_title,
               i.normalized_rules,i.deadline_at,i.deadline_bucket,i.meaningful_token_count
        FROM er_mapping_contract_index i
        JOIN _mapping_changed_ids c USING(source_contract_id)
        WHERE i.active=TRUE
    """))
    changed_index = {row["source_contract_id"]: row for row in index_rows}

    changed_tokens_by_id: dict[str, set[str]] = defaultdict(set)
    changed_token_values: set[str] = set()
    for source_contract_id, token in _rows_batched(con.execute("""
        SELECT t.source_contract_id,t.token
        FROM er_mapping_token_index t
        JOIN _mapping_changed_ids c USING(source_contract_id)
    """)):
        if token is None:
            continue
        token = str(token)
        changed_tokens_by_id[str(source_contract_id)].add(token)
        changed_token_values.add(token)

    token_frequency: dict[str, int] = defaultdict(int)
    token_members: dict[str, list[tuple[str, str]]] = defaultdict(list)
    remote_token_rows = 0
    if changed_token_values:
        token_table = pa.Table.from_pylist(
            [{"token": token} for token in sorted(changed_token_values)],
            schema=pa.schema([("token", pa.string())]),
        )
        con.register("_advisory_changed_tokens_arrow", token_table)
        try:
            cur = con.execute("""
                SELECT t.source_contract_id,t.source_id,t.token
                FROM er_mapping_token_index t
                JOIN _advisory_changed_tokens_arrow x USING(token)
                JOIN er_mapping_contract_index i USING(source_contract_id)
                WHERE i.active=TRUE
            """)
            for source_contract_id, source_id, token in _rows_batched(cur):
                remote_token_rows += 1
                token = str(token)
                token_frequency[token] += 1
                frequency = token_frequency[token]
                if frequency <= v5.MAX_BLOCK_MEMBERS:
                    token_members[token].append((str(source_contract_id), str(source_id)))
                elif frequency == v5.MAX_BLOCK_MEMBERS + 1:
                    # The token can no longer participate in rare-token blocking. Drop retained
                    # members immediately so common tokens cannot dominate Flight memory.
                    token_members.pop(token, None)
        finally:
            con.unregister("_advisory_changed_tokens_arrow")

    pair_ids: set[tuple[str, str]] = set()

    # Preserve v8's exact normalized-title + exact deadline-bucket block. SQL equality does not
    # match NULL buckets, so exclude them here as well.
    exact_keys = {
        (row.get("normalized_title") or "", row.get("deadline_bucket"))
        for row in changed_index.values()
        if (row.get("normalized_title") or "") and row.get("deadline_bucket") is not None
    }
    exact_members: dict[tuple[str, str], list[tuple[str, str]]] = defaultdict(list)
    if exact_keys:
        exact_table = pa.Table.from_pylist(
            [
                {"normalized_title": title, "deadline_bucket": bucket}
                for title, bucket in sorted(exact_keys)
            ],
            schema=pa.schema([
                ("normalized_title", pa.string()),
                ("deadline_bucket", pa.string()),
            ]),
        )
        con.register("_advisory_exact_keys_arrow", exact_table)
        try:
            for contract_id, source_id, title, bucket in _rows_batched(con.execute("""
                SELECT i.source_contract_id,i.source_id,i.normalized_title,i.deadline_bucket
                FROM er_mapping_contract_index i
                JOIN _advisory_exact_keys_arrow k
                  ON k.normalized_title=i.normalized_title
                 AND k.deadline_bucket=i.deadline_bucket
                WHERE i.active=TRUE
            """)):
                exact_members[(str(title), str(bucket))].append((str(contract_id), str(source_id)))
        finally:
            con.unregister("_advisory_exact_keys_arrow")

    for contract_id, row in changed_index.items():
        source_id = str(row["source_id"])
        title = row.get("normalized_title") or ""
        bucket = row.get("deadline_bucket")
        if title and bucket is not None:
            for partner_id, partner_source in exact_members.get((title, str(bucket)), []):
                if partner_id == contract_id or partner_source == source_id:
                    continue
                pair_ids.add(tuple(sorted((contract_id, partner_id))))

        ranked_tokens = sorted(
            (
                (token_frequency[token], token)
                for token in changed_tokens_by_id.get(contract_id, set())
                if 0 < token_frequency[token] <= v5.MAX_BLOCK_MEMBERS
            ),
            key=lambda item: (item[0], item[1]),
        )[: v5.MAX_BLOCK_TOKENS]
        for _frequency, token in ranked_tokens:
            for partner_id, partner_source in token_members.get(token, []):
                if partner_id == contract_id or partner_source == source_id:
                    continue
                pair_ids.add(tuple(sorted((contract_id, partner_id))))

    blocked = len(pair_ids)
    if not pair_ids:
        return [], {
            "pairs_blocked": 0,
            "pairs_evaluated": 0,
            "token_frequency_rows": len(token_frequency),
            "changed_token_rows": len(changed_token_values),
            "frequency_scope": "changed_tokens_only_local",
            "overlap_filter": "python_meaningful_tokens_v1",
            "execution_location": "enrichment_flight_local_python",
            "remote_token_rows_fetched": remote_token_rows,
            "remote_metadata_rows_fetched": len(changed_index),
        }

    pair_contract_ids = sorted({contract_id for pair in pair_ids for contract_id in pair})
    ids_table = pa.Table.from_pylist(
        [{"source_contract_id": contract_id} for contract_id in pair_contract_ids],
        schema=pa.schema([("source_contract_id", pa.string())]),
    )
    con.register("_advisory_pair_contracts_arrow", ids_table)
    try:
        metadata_rows = v5._rows_dict(con.execute("""
            SELECT i.source_contract_id,i.source_id,i.semantic_version_no,i.normalized_title,
                   i.normalized_rules,i.deadline_at,i.meaningful_token_count
            FROM er_mapping_contract_index i
            JOIN _advisory_pair_contracts_arrow p USING(source_contract_id)
            WHERE i.active=TRUE
        """))
    finally:
        con.unregister("_advisory_pair_contracts_arrow")
    metadata = {row["source_contract_id"]: row for row in metadata_rows}

    eligible: list[dict] = []
    for left_id, right_id in sorted(pair_ids):
        left = metadata.get(left_id)
        right = metadata.get(right_id)
        if left is None or right is None:
            continue
        if left["source_id"] == right["source_id"]:
            continue
        left_tokens = meaning.meaningful_tokens(left.get("normalized_title") or "")
        right_tokens = meaning.meaningful_tokens(right.get("normalized_title") or "")
        required_shared = 1 if min(len(left_tokens), len(right_tokens)) <= 2 else 2
        shared_count = len(left_tokens & right_tokens)
        if shared_count < required_shared:
            continue
        eligible.append({
            "left_id": left_id,
            "right_id": right_id,
            "left_source": left["source_id"],
            "right_source": right["source_id"],
            "left_semantic_version_no": left.get("semantic_version_no"),
            "right_semantic_version_no": right.get("semantic_version_no"),
            "left_normalized_title": left.get("normalized_title") or "",
            "right_normalized_title": right.get("normalized_title") or "",
            "left_normalized_rules": left.get("normalized_rules") or "",
            "right_normalized_rules": right.get("normalized_rules") or "",
            "left_deadline": left.get("deadline_at"),
            "right_deadline": right.get("deadline_at"),
            "left_token_count": left.get("meaningful_token_count") or len(left_tokens),
            "right_token_count": right.get("meaningful_token_count") or len(right_tokens),
            "shared_token_count": shared_count,
        })

    return eligible, {
        "pairs_blocked": blocked,
        "pairs_evaluated": len(eligible),
        "token_frequency_rows": len(token_frequency),
        "changed_token_rows": len(changed_token_values),
        "frequency_scope": "changed_tokens_only_local",
        "overlap_filter": "python_meaningful_tokens_v1",
        "execution_location": "enrichment_flight_local_python",
        "remote_token_rows_fetched": remote_token_rows,
        "remote_metadata_rows_fetched": len(metadata_rows),
    }


def _stamp_execution_provenance(pass_id: str, result: dict) -> None:
    import json
    from db import connect

    con = connect()
    try:
        row = con.execute("SELECT details FROM er_mapping_passes WHERE pass_id=?", [pass_id]).fetchone()
        if row is None:
            raise RuntimeError(f"mapping pass {pass_id} missing while stamping advisory provenance")
        try:
            details = json.loads(row[0]) if row[0] else {}
        except Exception:
            details = {"prior_details_unparsed": str(row[0])[:4000]}
        details.update({
            "execution_version": ADVISORY_EXECUTION_VERSION,
            "identity_owner": "institutional_mapping_identity_v1",
            "identity_mutations_disabled": True,
            "native_bootstrap_disabled": True,
            "auto_exact_identity_disabled": True,
            "candidate_methodology": "mapping_rules_v8_changed_token_frequency",
            "pair_execution_location": "enrichment_flight_local_python",
            "pair_execution_stats": {
                key: result.get(key)
                for key in (
                    "remote_token_rows_fetched",
                    "remote_metadata_rows_fetched",
                    "frequency_scope",
                    "changed_token_rows",
                )
                if key in result
            },
        })
        con.execute(
            "UPDATE er_mapping_passes SET details=? WHERE pass_id=?",
            [json.dumps(details, sort_keys=True, default=str), pass_id],
        )
    finally:
        con.close()


def run_mapping_pass() -> dict:
    # Imports are local because historical mapping generations use compatibility hooks.
    import institutional_mapping_v3 as v3
    import institutional_mapping_v4 as v4
    import institutional_mapping_v5 as v5
    import institutional_mapping_v6 as v6
    import institutional_mapping_v8 as v8

    prior_bootstrap = v4._bootstrap_native_events_batch
    prior_auto_accept = v3.auto_accept_exact
    prior_pair_rows = v5._candidate_pair_rows
    prior_candidate_rows = v5._candidate_rows
    prior_persist_candidates = v5._persist_candidates

    def no_identity_bootstrap(con) -> dict:
        return {"contracts": 0, "families": 0, "events": 0, "mappings": 0}

    def no_auto_identity_merge(con) -> int:
        return 0

    try:
        v4._bootstrap_native_events_batch = no_identity_bootstrap
        v5._candidate_pair_rows = _candidate_pair_rows_local
        v5._candidate_rows = v6._candidate_rows
        v5._persist_candidates = v6._persist_candidates
        v3.auto_accept_exact = no_auto_identity_merge

        result = v5.run_mapping_pass()
        result["method_version"] = v8.METHOD_VERSION
        result["execution_version"] = ADVISORY_EXECUTION_VERSION
        result["identity_owner"] = "institutional_mapping_identity_v1"
        result["identity_mutations_disabled"] = True
        result["native_events_created"] = 0
        result["auto_exact_mappings"] = 0
        result["pair_execution_location"] = "enrichment_flight_local_python"
        _stamp_execution_provenance(result["pass_id"], result)
        return result
    finally:
        v4._bootstrap_native_events_batch = prior_bootstrap
        v3.auto_accept_exact = prior_auto_accept
        v5._candidate_pair_rows = prior_pair_rows
        v5._candidate_rows = prior_candidate_rows
        v5._persist_candidates = prior_persist_candidates


if __name__ == "__main__":
    import json
    print(json.dumps(run_mapping_pass(), default=str, sort_keys=True))
