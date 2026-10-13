# Event Risk MVP

Retail event-risk intelligence for equities. Search a ticker and see the unresolved prediction-market events most relevant to it, live probabilities, probability changes, exposure explanations, and—when statistically supported—estimated ticker sensitivity per +1 percentage-point move in event probability.

## Architecture

- `collector.py` — append-only Kalshi + Polymarket snapshots into `geo_odds.ticks`.
- `migrate.py` / `schema.sql` — additive MotherDuck schema; never rewrites `ticks`.
- `pipeline.py markets` — refreshes canonical events and 1h/24h market features.
- `enrich.py` — captures Polymarket token/market metadata needed for history backfills.
- `backfill.py` — best-effort CLOB probability-history backfill for liquid Polymarket markets.
- `pipeline.py models` — refreshes tracked equity prices, ticker-event relevance, and robust OLS probability-sensitivity coefficients.
- `app.py` + `static/index.html` — FastAPI plus responsive retail UI.

## Model semantics

`beta_per_1pp` is the estimated abnormal ticker return associated with a **+1 percentage-point change in prediction-market probability**, after controlling for SPY in the first model version. It is **not** the predicted stock move if the event itself occurs. Missing coefficients are intentionally displayed as `calibrating` until the minimum sample requirements are met.

## Environment

- `DB=md:geo_odds`
- `MOTHERDUCK_TOKEN=...` (required for MotherDuck)
- `PORT` is provided by Railway.

## Refresh cadence

GitHub Actions refresh prediction markets and derived market features every five minutes. The model workflow refreshes market metadata, stock histories, relevance mappings, and coefficients hourly. The app refreshes the currently viewed ticker once per minute.

The production Railway service should receive a MotherDuck read-capable token as `MOTHERDUCK_TOKEN`. Keep the existing write-capable token in GitHub Actions for ingestion and model jobs.

## Local run

```bash
pip install -r requirements.txt
DB=geo_odds.duckdb python migrate.py
DB=geo_odds.duckdb python collector.py
DB=geo_odds.duckdb python pipeline.py all
DB=geo_odds.duckdb uvicorn app:app --reload
```
