import pytest


def test_kalshi_repeated_cursor_fails_closed(monkeypatch):
    import institutional_universe_adapters as adapters

    calls = []

    def fake_get_json(url, **params):
        calls.append(params.get("cursor"))
        return {"events": [], "cursor": "repeat-me"}

    monkeypatch.setattr(adapters.base, "get_json", fake_get_json)
    with pytest.raises(RuntimeError, match="repeated pagination cursor"):
        list(adapters.KalshiUniverseAdapter().iter_snapshots())
    assert calls == [None, "repeat-me"]


def test_polymarket_repeated_cursor_fails_closed(monkeypatch):
    import institutional_universe_adapters as adapters

    calls = []

    def fake_get_json(url, **params):
        calls.append(params.get("after_cursor"))
        return {"events": [], "next_cursor": "repeat-me"}

    monkeypatch.setattr(adapters.base, "get_json", fake_get_json)
    with pytest.raises(RuntimeError, match="repeated pagination cursor"):
        list(adapters.PolymarketUniverseAdapter().iter_snapshots())
    assert calls == [None, "repeat-me"]


def test_cursor_guard_accepts_normal_completion(monkeypatch):
    import institutional_universe_adapters as adapters

    pages = iter([
        {"events": [], "cursor": "page-2"},
        {"events": [], "cursor": None},
    ])
    monkeypatch.setattr(adapters.base, "get_json", lambda *args, **kwargs: next(pages))
    assert list(adapters.KalshiUniverseAdapter().iter_snapshots()) == []
