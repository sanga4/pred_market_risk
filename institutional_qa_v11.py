"""Institutional QA v11: lifecycle-aware mapping coverage.

v11 preserves QA v10's snapshot-safe two-phase evaluation/publication model and complete
institutional rule set while fixing two mapping false-positive classes exposed by full-universe
operation:

* active mapping cardinality applies only to active source contracts. Retired/inactive contracts
  may correctly have no live canonical mapping.
* point-in-time mapping coverage allows intentional interval gaps (for example the transparent
  legacy-to-institutional cutover) when no institutional-era observation falls in the gap, and it
  allows an inactive contract's final mapping interval to be closed. Overlaps and observations
  outside the mapping coverage remain critical failures.

No quality threshold is weakened for served data: every active contract must still have exactly
one live canonical mapping, and every institutional-era observation must remain covered by exactly
one non-overlapping mapping interval.
"""

from __future__ import annotations

import institutional_qa_v4 as v4
import institutional_qa_v10 as v10

QA_VERSION = "institutional_qa_v11_lifecycle_aware_mapping_coverage"
POINT_IN_TIME_MAPPING_METHOD = "observation_aware_interval_coverage_v2"


ACTIVE_MAPPING_SQL = r"""
SELECT c.source_contract_id AS object_id,
       'Expected exactly one active canonical mapping for active source contract; found ' ||
       count(m.mapping_id) AS details
FROM er_source_contracts c
LEFT JOIN er_event_mappings m
  ON m.source_contract_id=c.source_contract_id AND m.valid_to IS NULL
WHERE c.active=TRUE
GROUP BY c.source_contract_id
HAVING count(m.mapping_id)<>1
"""


POINT_IN_TIME_MAPPING_SQL = r"""
WITH ordered AS (
  SELECT source_contract_id,mapping_id,valid_from,valid_to,
         lead(valid_from) OVER (
           PARTITION BY source_contract_id ORDER BY valid_from,mapping_id
         ) AS next_valid_from,
         row_number() OVER (
           PARTITION BY source_contract_id ORDER BY valid_from DESC,mapping_id DESC
         ) AS rev_rn
  FROM er_event_mappings
), interval_overlap AS (
  SELECT source_contract_id AS object_id,
         'Mapping intervals overlap or a non-final mapping remains open' AS details
  FROM ordered
  WHERE next_valid_from IS NOT NULL
    AND (valid_to IS NULL OR valid_to>next_valid_from)
  GROUP BY source_contract_id
), gaps AS (
  SELECT source_contract_id,valid_to AS gap_start,next_valid_from AS gap_end
  FROM ordered
  WHERE valid_to IS NOT NULL
    AND next_valid_from IS NOT NULL
    AND valid_to<next_valid_from
), observed_gap AS (
  SELECT g.source_contract_id AS object_id,
         'Institutional observation falls inside a mapping interval gap' AS details
  FROM gaps g
  JOIN er_market_observations o
    ON o.source_contract_id=g.source_contract_id
   AND o.received_at>=g.gap_start
   AND o.received_at<g.gap_end
  WHERE coalesce(o.methodology_version,'')<>'legacy_collector_v0.3'
  GROUP BY g.source_contract_id
), observation_bounds AS (
  SELECT source_contract_id,
         min(received_at) AS first_observation_at,
         max(received_at) AS last_observation_at
  FROM er_market_observations
  WHERE coalesce(methodology_version,'')<>'legacy_collector_v0.3'
  GROUP BY source_contract_id
), mapping_start AS (
  SELECT source_contract_id,min(valid_from) AS first_mapping_at
  FROM er_event_mappings
  GROUP BY source_contract_id
), final_mapping AS (
  SELECT source_contract_id,valid_from AS final_mapping_from,valid_to AS final_mapping_to
  FROM ordered
  WHERE rev_rn=1
), uncovered_start AS (
  SELECT o.source_contract_id AS object_id,
         'First institutional observation predates the first mapping interval' AS details
  FROM observation_bounds o
  LEFT JOIN mapping_start m USING(source_contract_id)
  WHERE m.first_mapping_at IS NULL OR o.first_observation_at<m.first_mapping_at
), uncovered_end AS (
  SELECT o.source_contract_id AS object_id,
         'Institutional observation occurs after the final closed mapping interval' AS details
  FROM observation_bounds o
  JOIN final_mapping m USING(source_contract_id)
  WHERE m.final_mapping_to IS NOT NULL
    AND o.last_observation_at>=m.final_mapping_to
), violations AS (
  SELECT object_id,details FROM interval_overlap
  UNION ALL
  SELECT object_id,details FROM observed_gap
  UNION ALL
  SELECT object_id,details FROM uncovered_start
  UNION ALL
  SELECT object_id,details FROM uncovered_end
)
SELECT object_id,string_agg(details,'; ' ORDER BY details) AS details
FROM violations
GROUP BY object_id
"""


def check_active_mapping_cardinality(con) -> int:
    return v4._sync_rule(
        con,
        object_type="source_contract",
        rule_code="active_mapping_cardinality",
        severity="critical",
        sql=ACTIVE_MAPPING_SQL,
        resolution_note="Active source contract has exactly one live canonical mapping",
    )


def check_point_in_time_mapping_cardinality(con) -> int:
    return v4._sync_rule(
        con,
        object_type="source_contract",
        rule_code="point_in_time_mapping_cardinality",
        severity="critical",
        sql=POINT_IN_TIME_MAPPING_SQL,
        resolution_note="Institutional observations are covered by non-overlapping mapping intervals",
    )


def run_qa(stale_minutes: int = 20) -> dict:
    """Run the v10 snapshot-safe engine with corrected mapping-rule semantics."""
    original_active = v4.check_active_mapping_cardinality
    original_point_in_time = v10.check_point_in_time_mapping_cardinality
    v4.check_active_mapping_cardinality = check_active_mapping_cardinality
    v10.check_point_in_time_mapping_cardinality = check_point_in_time_mapping_cardinality
    try:
        result = v10.run_qa(stale_minutes=stale_minutes)
    finally:
        v4.check_active_mapping_cardinality = original_active
        v10.check_point_in_time_mapping_cardinality = original_point_in_time

    result["qa_version"] = QA_VERSION
    result["point_in_time_mapping_method"] = POINT_IN_TIME_MAPPING_METHOD
    result["mapping_lifecycle_policy"] = {
        "active_contract_live_mapping_required": True,
        "inactive_contract_live_mapping_required": False,
        "unobserved_mapping_gaps_allowed": True,
        "observed_mapping_gaps_allowed": False,
        "mapping_overlaps_allowed": False,
        "legacy_observations_excluded_from_institutional_coverage": True,
    }
    return result


if __name__ == "__main__":
    import json
    print(json.dumps(run_qa(), default=str, sort_keys=True))
