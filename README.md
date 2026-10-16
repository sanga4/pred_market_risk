# EventRisk

EventRisk is an event-data platform built around prediction-market observations from Kalshi and Polymarket. The repository contains two deliberately separated products:

1. **Institutional data plane** — immutable raw source lineage, versioned source contracts, canonical events, point-in-time mappings, observed market curves, entity/instrument reference data, provenance-aware graph edges, quality controls, exports, change feeds and a read-only API.
2. **Retail research UI** — the original equity/event relevance experience backed by the separate `event_risk` MotherDuck database.

The institutional platform is the system to use for new data integrations and downstream institutional applications. It does not prescribe trades, portfolio weights, scenario P&L or investment decisions.

## Institutional API

Production base URL:

```text
https://institutional-api-production-7306.up.railway.app
```

Interactive documentation:

```text
https://institutional-api-production-7306.up.railway.app/docs
```

Operational health is available at `/health`. The `/v1` surface includes events, point-in-time history and curves, source comparison, event families and term structure, entities, instruments, graph relationships, quality state, releases, and resumable change feeds. WebSocket streaming is exposed at `/v1/stream`.

Example:

```bash
BASE=https://institutional-api-production-7306.up.railway.app
curl -s "$BASE/health" | jq
curl -s "$BASE/v1/events?status=active&limit=5" | jq
```

When API authentication is enabled, send either `Authorization: Bearer <key>` or `X-API-Key: <key>`.

Python:

```python
from eventrisk_sdk import EventRisk

client = EventRisk(
    "https://institutional-api-production-7306.up.railway.app",
    api_key=None,  # provide a key when production auth is required
)

events = client.events(limit=20)
curve = client.event_curve(events["items"][0]["canonical_event_id"], mapping_basis="as_known")
```

## Institutional architecture

```text
Kalshi API ─────┐
                ├─> source adapters ─> immutable raw snapshots ─> source contracts/versions
Polymarket APIs ┘                                             │
                                                              ├─> market observations
                                                              ├─> deterministic canonical identity
                                                              └─> semantic contract versions
                                                                        │
                                                                        ├─> ontology/lifecycle
                                                                        ├─> provenance graph
                                                                        ├─> QA/readiness
                                                                        └─> read-only API/exports/stream

verified raw payload bundles ───────────────────────────────────────────> RawArchive
```

The institutional warehouse is `event_risk_institutional`. It is populated directly from supported source APIs; `geo_odds` is not the upstream source for this pipeline.

Core modules:

- `institutional_universe_adapters.py` — full-universe Kalshi/Polymarket adapters.
- `institutional_ingest*.py` / `institutional_worker*.py` — immutable collection and deterministic identity hot path.
- `institutional_mapping_identity_v1.py` — deterministic canonical identity ownership.
- `institutional_mapping_advisory_v2.py` — advisory candidate generation that cannot mutate canonical identity.
- `institutional_ontology_v2.py` / `institutional_lifecycle_v2.py` — deterministic normalization and lifecycle.
- `institutional_graph_v4.py` — version-aware, provenance-preserving, non-inferential entity mention graph.
- `institutional_qa_v11.py` — snapshot-safe lifecycle-aware institutional QA.
- `institutional_archive_v3.py` — bounded, collision-safe, set-wise raw archive maintenance.
- `institutional_public_service.py` — stateless read-only public service.
- `eventrisk_sdk.py` — Python client for the public API.

## Data guarantees

The institutional data contract is intentionally conservative:

- source contracts are never silently collapsed;
- raw observations are append-only;
- source, receipt and processing clocks are distinct;
- canonical mappings and semantic metadata are versioned with validity intervals;
- cross-source disagreement is preserved;
- revisions are recorded rather than rewritten silently;
- inferred relationships must be explicitly labelled with methodology/version metadata;
- a `mentions` relationship is not an `affects` claim;
- graph edges retain evidence and provenance;
- historical queries support both current-taxonomy restatement and `as_known` reconstruction;
- stable EventRisk IDs are primary; external IDs are interoperability mappings;
- source redistribution entitlement is tracked independently from technical availability.

See `docs/INSTITUTIONAL_DATA_PLATFORM.md` and `docs/DATA_DICTIONARY_V1.md` for the contract and schema semantics.

## Institutional runtime

MotherDuck Flights own scheduled ingestion and enrichment. Railway hosts the stateless public API and the independent raw-archive worker. The public API is not permitted to run ingestion, migrations or background collection.

Raw payloads are first stored with their immutable MotherDuck lineage record. The archive worker writes verified tar.gz bundles to S3-compatible object storage, records bundle/member hashes, and only then may compact the duplicate JSON body from the analytical warehouse. Archive failure leaves the database payload intact.

## Retail application

The original retail application remains isolated in `event_risk` and is served by the Railway `web` service. Its main components are:

- `collector.py` / `collector_runner.py` — bounded retail collection.
- `pipeline.py` / `pipeline_runner.py` / `production_pipeline.py` — retail event/security features and models.
- `relevance.py` — retail event/ticker relevance.
- `app.py` / `app_entry.py` / `static/` — FastAPI retail UI.

The original `geo_odds` database is a legacy/read-only source used by the retail lineage; it is not the institutional live feed.

## Development and CI

Install dependencies and run the institutional suite:

```bash
pip install -r requirements.txt pytest
python -m pytest -q tests
```

Institutional schema installation for a local DuckDB database:

```bash
DB=event_risk_institutional.duckdb DB_FORCE_READ_WRITE=true python migrate_institutional.py
```

Never point local development commands at production MotherDuck credentials. Production schema/data changes must go through the controlled migration/worker paths and readiness gates.

## Product boundary

EventRisk supplies observed event-market data, canonical identity, normalized semantics, provenance and explicitly labelled relationships. Downstream institutions remain responsible for scenarios, security-impact models, portfolio construction and investment decisions.
