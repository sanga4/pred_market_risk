import os
from datetime import datetime, timezone

import db


def setup_database(monkeypatch, tmp_path):
    monkeypatch.setattr(db, "DB", str(tmp_path / "readiness-v4.duckdb"))
    os.environ["DB_FORCE_READ_WRITE"] = "true"
    os.environ["EVENTRISK_COLLECTION_SCOPE"] = "full"
    from institutional_migrations_v2 import ensure_schema
    ensure_schema()


def snapshot(source_id, market_id):
    from institutional_ingest_v3 import ContractSnapshot
    close = datetime(2027, 12, 31, tzinfo=timezone.utc)
    title = "Will China blockade Taiwan before 2028?"
    rules = "Resolves YES if China begins a blockade of Taiwan before 2028."
    return ContractSnapshot(
        source_id=source_id, source_market_id=market_id, source_event_id=f"{source_id}-event",
        title=title, description="Canonical identity readiness fixture", status="active",
        opens_at=datetime(2026, 1, 1, tzinfo=timezone.utc), closes_at=close, resolves_at=close,
        resolution_authority="test", resolution_rules=rules, source_url=None,
        observed_at=datetime.now(timezone.utc), yes_bid=0.20, yes_ask=0.22,
        no_bid=None, no_ask=None, last_trade=0.21, displayed_probability=0.21,
        volume=1000.0, volume_24h=100.0, open_interest=500.0, liquidity=200.0,
        orderbook_depth=None,
        raw_event={"id": f"{source_id}-event", "title": title, "rules": rules},
        raw_market={"id": market_id, "question": title, "bestBid": 0.20, "bestAsk": 0.22},
    )


def test_layered_readiness_core_does_not_depend_on_advisory_enrichment(monkeypatch, tmp_path):
    setup_database(monkeypatch, tmp_path)
    import institutional_ingest_v10 as ingest
    from institutional_mapping_identity_v1 import run_identity_pass
    from institutional_readiness_v4 import (
        evaluate_enrichment_readiness,
        evaluate_identity_readiness,
        evaluate_observed_readiness,
        evaluate_readiness,
    )

    fixtures = {
        "kalshi": [snapshot("kalshi", "KX-TW")],
        "polymarket": [snapshot("polymarket", "PM-TW")],
    }

    class Adapter:
        def __init__(self, source):
            self.source = source
        def iter_snapshots(self):
            yield from fixtures[self.source]

    monkeypatch.setitem(ingest.ADAPTERS, "kalshi", lambda: Adapter("kalshi"))
    monkeypatch.setitem(ingest.ADAPTERS, "polymarket", lambda: Adapter("polymarket"))
    monkeypatch.setattr(ingest, "CHUNK_ROWS", 1)

    assert ingest.ingest_source("kalshi")["status"] == "success"
    assert ingest.ingest_source("polymarket")["status"] == "success"
    identity_pass = run_identity_pass()
    assert identity_pass["exact_mappings_accepted"] == 1

    observed = evaluate_observed_readiness(stale_minutes=20, require_archive=False)
    identity = evaluate_identity_readiness()
    enrichment = evaluate_enrichment_readiness()
    combined = evaluate_readiness(stale_minutes=20, require_archive=False)

    assert observed["ready"] is True, observed
    assert identity["ready"] is True, identity
    # No fuzzy/ontology/graph pass has run in this fixture; advisory enrichment should report that
    # independently instead of turning the observed/identity data feed red.
    assert enrichment["ready"] is False
    assert combined["core_ready"] is True
    assert combined["ready"] is True
    assert combined["enrichment_ready"] is False


def test_readiness_api_defines_independent_layer_routes():
    from institutional_readiness_api import router
    paths = {route.path for route in router.routes}
    assert "/v1/readiness" in paths
    assert "/v1/readiness/observed" in paths
    assert "/v1/readiness/identity" in paths
    assert "/v1/readiness/enrichment" in paths
