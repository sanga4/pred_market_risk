from __future__ import annotations


def test_kalshi_full_scope_does_not_exclude_categories(monkeypatch):
    import institutional_universe_adapters as adapters

    adapters.COLLECTION_SCOPE = "full"
    adapters.KALSHI_FILTER_CATEGORIES = {"politics"}
    calls = []

    def fake_get_json(url, **params):
        calls.append(params)
        return {
            "events": [
                {
                    "event_ticker": "KXSPORTS",
                    "title": "Sports event",
                    "category": "Sports",
                    "markets": [
                        {
                            "ticker": "KXSPORTS-YES",
                            "title": "Will team win?",
                            "status": "open",
                            "yes_bid_dollars": "0.40",
                            "yes_ask_dollars": "0.44",
                            "updated_time": "2026-10-14T20:00:00Z",
                        }
                    ],
                }
            ],
            "cursor": None,
        }

    monkeypatch.setattr(adapters.base, "get_json", fake_get_json)
    rows = list(adapters.KalshiUniverseAdapter().iter_snapshots())
    assert len(rows) == 1
    assert rows[0].source_market_id == "KXSPORTS-YES"
    assert calls[0]["status"] == "open"
    assert calls[0]["limit"] == 200


def test_polymarket_full_scope_is_untagged_and_keyset_paginated(monkeypatch):
    import institutional_universe_adapters as adapters

    adapters.COLLECTION_SCOPE = "full"
    adapters.POLY_FILTER_TAGS = ["geopolitics"]
    calls = []

    first = {
        "events": [
            {
                "id": "evt-1",
                "title": "Event one",
                "markets": [
                    {
                        "id": "mkt-1",
                        "question": "Market one?",
                        "active": True,
                        "closed": False,
                        "bestBid": 0.20,
                        "bestAsk": 0.22,
                        "updatedAt": "2026-10-14T20:00:00Z",
                    }
                ],
            }
        ],
        "next_cursor": "cursor-2",
    }
    second = {
        "events": [
            {
                "id": "evt-2",
                "title": "Event two",
                "markets": [
                    {
                        "id": "mkt-2",
                        "question": "Market two?",
                        "active": True,
                        "closed": False,
                        "bestBid": 0.30,
                        "bestAsk": 0.34,
                        "updatedAt": "2026-10-14T20:01:00Z",
                    }
                ],
            }
        ],
        "next_cursor": None,
    }

    def fake_get_json(url, **params):
        calls.append(params)
        return first if len(calls) == 1 else second

    monkeypatch.setattr(adapters.base, "get_json", fake_get_json)
    rows = list(adapters.PolymarketUniverseAdapter().iter_snapshots())
    assert [row.source_market_id for row in rows] == ["mkt-1", "mkt-2"]
    assert "tag_slug" not in calls[0]
    assert calls[0]["after_cursor"] is None
    assert calls[1]["after_cursor"] == "cursor-2"


def test_filtered_scope_is_explicit_not_default(monkeypatch):
    import institutional_universe_adapters as adapters

    adapters.COLLECTION_SCOPE = "filtered"
    adapters.KALSHI_FILTER_CATEGORIES = {"politics"}
    adapters.POLY_FILTER_TAGS = ["geopolitics", "economy"]
    policy = adapters.collection_policy()
    assert policy == {
        "scope": "filtered",
        "kalshi_categories": ["politics"],
        "polymarket_tags": ["geopolitics", "economy"],
        "default_behavior": "explicit source filters",
    }
