# Event Risk MVP

Retail event-risk intelligence for equities. Search a ticker and see the unresolved prediction-market events most relevant to it, live probabilities, probability changes, exposure explanations, and—when statistically supported—estimated ticker sensitivity per +1 percentage-point move in event probability.

## Production architecture

The original `geo_odds` database is a read-only MotherDuck share. The MVP uses an owned writable MotherDuck database named `event_risk`; its `ticks` table was seeded from `geo_odds.ticks`, and all new collection/model data is written to `event_risk`.

- `collector.py` — Kalshi + Polymarket collection primitives.
- `collector_runner.py` — production collector; caps Kalshi to the most liquid/relevant 1,000 contracts rather than ingesting its entire open universe.
- `schema.sql` / `migrate.py` — additive MotherDuck schema.
- `pipeline.py` — canonical-event, security, market-price and model logic.
- `pipeline_runner.py` — production-safe market features, history-backed 1h/24h deltas and coefficient SQL.
- `production_pipeline.py` — applies hardened event classification/relevance to scheduled jobs.
- `relevance.py` — severity-aware event themes and ticker relevance.
- `universe.py` — broad U.S. equity search universe (~7k names); only a smaller calibration basket is priced/modelled continuously.
- `enrich.py` — Polymarket CLOB token metadata.
- `backfill.py` — historical Polymarket probability backfill.
- `app.py` / `app_entry.py` / `static/index.html` — FastAPI + responsive retail search UI.

## Refresh cadence

MotherDuck Flights are the primary scheduler:

- **Every 5 minutes:** collect bounded Kalshi + Polymarket snapshots and rebuild canonical events / market features.
- **Hourly at :17:** refresh the security universe, Polymarket metadata/history, stock bars, relevance mappings, and coefficients.
- **Frontend:** the viewed ticker automatically refreshes every minute.

The GitHub Actions workflows are retained as manual fallbacks only.

## Data model

Main tables in `event_risk`:

- `ticks` — append-only raw prediction snapshots
- `market_metadata`
- `canonical_events`
- `event_markets`
- `market_features`
- `probability_history`
- `securities`
- `ticker_prices`
- `event_security_map`
- `market_security_coefficients`
- `model_runs`

Views:

- `probability_history_all`
- `ticker_event_feed`

## Model semantics

`beta_per_1pp` is the estimated abnormal ticker return associated with a **+1 percentage-point change in prediction-market probability**, after removing a first-pass SPY market component. It is **not** the predicted stock move if the event itself occurs.

Confidence is deliberately conservative. Coefficients that do not meet sample/significance requirements are marked low-confidence or omitted; the UI says `calibrating` when no coefficient is supportable yet.

The current stock-price source is the public Yahoo chart endpoint for MVP calibration. Replace it with a licensed market-data provider before relying on the product commercially.

## Environment

- `DB=md:event_risk`
- `MOTHERDUCK_TOKEN=...` for external runtimes such as Railway
- `PORT` is supplied by Railway

MotherDuck Flights use their built-in MotherDuck execution credential and do not require a copied token.

## Railway

The `web` service deploys `main` and starts:

```bash
uvicorn app_entry:app --host 0.0.0.0 --port $PORT
```

Railway only needs database read access; scheduled writes/modeling happen inside MotherDuck Flights.

## Local run

```bash
pip install -r requirements.txt
DB=event_risk.duckdb python migrate.py
DB=event_risk.duckdb python collector_runner.py
DB=event_risk.duckdb python production_pipeline.py all
DB=event_risk.duckdb uvicorn app_entry:app --reload
```
