import json

import institutional_archive_backfill as backfill


def test_run_once_processes_configured_number_of_bounded_cycles(monkeypatch, capsys):
    calls = []

    def fake_archive_pending(*, max_rows, bundle_rows):
        calls.append((max_rows, bundle_rows))
        return {
            "configured": True,
            "archived": 20000,
            "failed": 0,
            "compacted": 20000,
            "bundles": 4,
        }

    monkeypatch.setattr(backfill, "archive_pending", fake_archive_pending)
    monkeypatch.setattr(backfill.time, "sleep", lambda _: None)
    monkeypatch.setenv("RAW_ARCHIVE_RUN_ONCE", "true")
    monkeypatch.setenv("RAW_ARCHIVE_CYCLES_PER_RUN", "3")
    monkeypatch.setenv("RAW_ARCHIVE_MAX_ROWS", "20000")
    monkeypatch.setenv("RAW_ARCHIVE_BATCH_SIZE", "5000")
    monkeypatch.setenv("RAW_ARCHIVE_BACKFILL_PAUSE_SECONDS", "0")

    backfill.main()

    assert calls == [(20000, 5000)] * 3
    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert lines[-1]["bounded_run_complete"] is True
    assert lines[-1]["cycles"] == 3
    assert lines[-1]["archived"] == 60000
    assert lines[-1]["compacted"] == 60000
    assert lines[-1]["bundles"] == 12


def test_worker_stops_early_when_backlog_is_empty(monkeypatch, capsys):
    results = iter([
        {"configured": True, "archived": 20000, "failed": 0, "compacted": 20000, "bundles": 4},
        {"configured": True, "archived": 0, "failed": 0, "compacted": 0, "bundles": 0},
    ])
    monkeypatch.setattr(backfill, "archive_pending", lambda **_: next(results))
    monkeypatch.setattr(backfill.time, "sleep", lambda _: None)
    monkeypatch.setenv("RAW_ARCHIVE_RUN_ONCE", "true")
    monkeypatch.setenv("RAW_ARCHIVE_CYCLES_PER_RUN", "6")
    monkeypatch.setenv("RAW_ARCHIVE_BACKFILL_PAUSE_SECONDS", "0")

    backfill.main()

    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert lines[-1]["complete"] is True
    assert lines[-1]["cycles"] == 2
    assert lines[-1]["archived"] == 20000
