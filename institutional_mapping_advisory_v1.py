"""Advisory-only cross-source relationship discovery v1.

This lane is deliberately *not* a canonical-identity owner. It reuses mapping v8's indexed,
semantic-version-aware candidate discovery and immutable review history, but disables the two
identity-mutating behaviors inherited from the historical monolithic mapper:

- source-native canonical/event-mapping bootstrap;
- automatic acceptance/merge of exact same-event candidates.

Deterministic source-native identity and strict exact cross-source identity are owned exclusively by
`institutional_mapping_identity_v1` on the hot path. This module may only update advisory mapping
indexes/work watermarks and candidate/review-history tables. Candidate suggestions remain proposals
until an explicit review action records a relationship; inferred/related semantics never become
implicit canonical identity here.

The historical mapping modules use module-global compatibility hooks. This module is therefore
loaded lazily by the enrichment worker; callers that need multiple mapping generations in one
Python process should execute this pass in its dedicated enrichment process.
"""

from __future__ import annotations

ADVISORY_EXECUTION_VERSION = "mapping_advisory_v1_candidate_only"


def _stamp_execution_provenance(pass_id: str) -> None:
    """Persist the wrapper/mutation boundary on the durable mapping-pass audit record."""
    import json
    from db import connect

    con = connect()
    try:
        row = con.execute(
            "SELECT details FROM er_mapping_passes WHERE pass_id=?", [pass_id]
        ).fetchone()
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
        })
        con.execute(
            "UPDATE er_mapping_passes SET details=? WHERE pass_id=?",
            [json.dumps(details, sort_keys=True, default=str), pass_id],
        )
    finally:
        con.close()


def run_mapping_pass() -> dict:
    # Imports are intentionally local. Importing mapping v8 activates its version-scoped helper
    # hooks, which is appropriate inside the dedicated enrichment execution process but should not
    # happen merely by importing this wrapper from a test/research process.
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
        # Preserve v8 discovery semantics/history while making the mutation boundary explicit.
        v4._bootstrap_native_events_batch = no_identity_bootstrap
        v5._candidate_pair_rows = v8._candidate_pair_rows
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
        _stamp_execution_provenance(result["pass_id"])
        return result
    finally:
        # Keep the wrapper polite inside long-lived/manual Python processes even though production
        # enrichment is a one-shot process. The method-version globals activated by importing v8
        # remain v8 by design; only callable hooks changed by this wrapper are restored here.
        v4._bootstrap_native_events_batch = prior_bootstrap
        v3.auto_accept_exact = prior_auto_accept
        v5._candidate_pair_rows = prior_pair_rows
        v5._candidate_rows = prior_candidate_rows
        v5._persist_candidates = prior_persist_candidates


if __name__ == "__main__":
    import json

    print(json.dumps(run_mapping_pass(), default=str, sort_keys=True))
