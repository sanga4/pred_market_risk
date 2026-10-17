# EventRisk explorer

The institutional service serves the barebones UI at `/explorer`. It uses the same-origin,
authenticated `/v1/explorer` endpoints. There is no frontend build, extra production
dependency, embedded customer key or warehouse credential. A separate enrichment lane persists the
versioned semantic hierarchy; the public service remains read-only. Existing retail deployment
and institutional source/identity contracts remain available. See [HIERARCHY.md](HIERARCHY.md).

## Hierarchy and definitions

| UI concept | Existing data | Meaning / limit |
| --- | --- | --- |
| Event | `er_canonical_events` + live mappings + active source contracts | A source-backed, currently active canonical outcome. |
| Subject | `er_hierarchy_event_subject_mappings` → `er_hierarchy_subjects` | Primary semantic topic from a pinned transformer or reviewed override; uncertainty is explicit. |
| Theme | `er_hierarchy_subject_theme_mappings` → `er_hierarchy_themes` | Immutable governed parent of a subject, within a taxonomy version. |
| Event family | `er_event_families` | Separate group of related questions/horizons; does not establish outcome equivalence. |
| Named mention | `er_graph_edges` + `er_edge_evidence` + `er_entities` | Current source-asserted `mentions` with evidence. Never promoted to formal subject, issuer or causal exposure. |
| Security | `er_instrument_reference_current` | Separate reference universe; no exposure mapping is implied. |

The event detail shows the complete current contract-mapping set, including mapping IDs,
types, confidence, methods/versions, review state and validity. An optional audit toggle
adds closed/future mappings. Current-price comparisons exclude those mappings and inactive
contracts. Current observations shown on historical mappings are explicitly labeled and
are not historical reconstructions. Use `/as-known` or `/history-point-in-time` for that.

## Discovery API

All routes below require the existing `read` scope when production authentication is enabled.

| Route | Parameters / response |
| --- | --- |
| `GET /v1/explorer/summary` | Optional `source`. Global live coverage, theme counts, source health, instrument coverage and definitions. |
| `GET /v1/explorer/events` | `q`, `theme`, `subject_id`, `source`, `after`, `limit` (1–100). `items`, `count`, `next_after`, `as_of`. |
| `GET /v1/explorer/subjects` | `q`, `theme`, `source`, `after`, `limit` (1–100). Semantic topics and live matching event/contract counts, same page envelope. |
| `GET /v1/explorer/events/{event_id}/mappings` | Optional `include_expired`. Event → semantic subject → governed theme, mapping rows, per-contract current observations, named mention evidence and descriptive insights. |

Search is case-insensitive literal substring search over canonical event, semantic topic or family names or an
exact event ID. It does not execute user SQL or treat `%`/`_` as wildcards. Source filters
are applied before aggregation. Paging is by stable EventRisk ID. Pages are separate live
reads; ingestion can change membership between requests. Summary and quality metrics each use one database snapshot. Pages and panels are separate live reads.

Discovery reads `er_market_current_state` instead of scanning the observation-history views.
`received_at` describes the current material observation; `last_polled_at` is also exposed
in mapping detail because repeated unchanged polls do not necessarily create observations.
No cross-source range is reported unless at least two distinct sources supply a current
price. Prices are never averaged into an EventRisk consensus.

The existing `/source-comparison` summary now counts distinct sources in `source_count`,
and separately exposes `priced_source_count` and `priced_contract_count`. Previously,
multiple priced contracts from one venue could be mislabeled as multiple sources.

The hierarchy routes `/v1/hierarchy/metrics`, `/taxonomy`, `/events/{id}?at=...`,
`/review`, `/review-datasets` and `/review-datasets/{id}` use the same authentication.
The UI shows provenance, candidates, processing-time history, a review queue and measured quality.
Missing independent production precision/recall are displayed as unavailable.

## UI behavior and credentials

The UI shell and CSS/JS are public; data remains behind the existing API-key boundary.
Supply an existing EventRisk key. The key is sent in the Authorization header only to this
service's origin and retained only in JavaScript memory. Disconnect clears loaded private
data and ignores in-flight responses. No local/session storage, analytics, external script
or credential-bearing URL is used. The page applies a restrictive CSP and prevents framing.
The API inspector retains at most 30 request/response records in the current tab; cURL
examples contain a placeholder instead of the key.

## Original audit findings, 16 October 2026 (before semantic upgrade)

Reviewed product contracts, all Python files structurally (186 before this change), the
59-migration schema, active implementation paths and test coverage. Inspected the live
Railway services and MotherDuck warehouses/scheduled Flight topology. Baseline: 209 tests
passed. The running institutional service reported healthy Kalshi/Polymarket feeds and zero
open critical quality issues. The duplicate empty Railway project was left untouched.

Important product gaps remain visible rather than being silently filled:

- Formal `subject_entity_id` was empty on all 603,597 canonical records at the initial audit.
  The original UI used families as a presentation alias. The new hierarchy replaces that alias with semantic topics. Named-entity role extraction remains separate.
- The entity master contained 12 active seed entities and approximately 33,000 current
  source-asserted mention edges. This is limited alias coverage, not a full entity universe.
- Roughly 84% of canonical events had event class `other`. Much of the full source universe
  falls outside the current financial/geopolitical regex taxonomy. Broad/unknown theme
  coverage is disclosed as a coverage measure, not treated as fabricated classification.
- All current contract mappings at audit time were `source_native`. The strict identity
  policy preserves separate events unless resolution semantics establish equivalence.
  Related events must not be collapsed just to manufacture cross-source comparisons.
- There were 7,192 reference instruments and zero established issuer links. Completing
  subjects/themes → securities requires a sourced issuer/reference layer and a reviewed or
  explicitly inferred exposure model. The new UI intentionally stops at themes as requested.
- Existing customer key material is hashed; connected OAuth tools expose Railway variable
  names without secret values. Reusing an existing key in the UI avoids resetting credentials
  or weakening production auth.

Counts are audit snapshots; the live UI reports current values.

## Validation

```bash
pip install -r requirements-dev.txt
python -m pytest -q tests
node --check static/explorer/explorer.js
npm ci --prefix tests/ui --ignore-scripts --no-audit --no-fund
npm test --prefix tests/ui
```

The Python integration fixtures cover full keyset traversal, literal search, source/theme/
subject filters, missing mappings, auth, read-only current-state queries, expired/future
mapping exclusion and typed graph-entity evidence. Node DOM tests cover connection,
filtering/paging, mapping inspection, safe source rendering, auth errors and disconnect
while requests are in flight. They do not replace visual browser QA.

`institutional_explorer_smoke.py` performs read-only acceptance against an explicitly selected
warehouse without migrations, client-key creation or background jobs. A temporary bootstrap
key exists only inside that process to exercise its TestClient/auth path. Set
`EVENTRISK_PUBLIC_URL` after deployment to also check hosted assets and anonymous 401s.
This does not verify a production customer key over HTTP; use an existing key in the UI
for that check. No real-browser QA was available in the managed environment.
