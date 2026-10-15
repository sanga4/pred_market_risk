from pathlib import Path

import institutional_readiness_v6 as v6

ROOT = Path(__file__).resolve().parents[1]


class FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class FakeConnection:
    def __init__(self, rows):
        self.rows = rows
        self.sql = []

    def execute(self, sql, params=None):
        self.sql.append(sql)
        return FakeCursor(self.rows)

    def close(self):
        pass


def _base():
    return {
        "layer": "observed",
        "ready": False,
        "controls": [
            {"name": "observed_schema_complete", "passed": True, "severity": "critical"},
            {"name": "latest_source_runs_full_and_successful", "passed": False, "severity": "critical"},
            {"name": "observed_sources_fresh", "passed": False, "severity": "critical"},
            {"name": "raw_evidence_present", "passed": True, "severity": "critical"},
        ],
        "failed_controls": ["latest_source_runs_full_and_successful", "observed_sources_fresh"],
        "policy": {"readiness_version": "v5", "stale_minutes": 20},
    }


def test_v6_uses_started_at_order_and_accepts_newer_success_over_older_recovery(monkeypatch):
    # Row shape: source, run_id, status, complete, scope, started_at, finished_at, age_minutes.
    # The selected row represents a newer successful invocation whose transaction-stable
    # finished_at can be older than a later recovery timestamp on an older abandoned run.
    rows = [
        ("kalshi", "new_success", "success", True, "full", "2026-10-15T07:20:00Z", "2026-10-15T07:20:01Z", 4),
        ("polymarket", "new_success_2", "success", True, "full", "2026-10-15T07:21:00Z", "2026-10-15T07:21:01Z", 3),
    ]
    fake = FakeConnection(rows)
    monkeypatch.setattr(v6.v5, "evaluate_observed_readiness", lambda **kwargs: _base())
    monkeypatch.setattr(v6, "connect", lambda read_only=True: fake)

    result = v6.evaluate_observed_readiness(stale_minutes=20)

    assert result["ready"] is True
    assert result["failed_controls"] == []
    latest = next(c for c in result["controls"] if c["name"] == "latest_source_runs_full_and_successful")
    assert latest["passed"] is True
    assert [x["run_id"] for x in latest["observed"]] == ["new_success", "new_success_2"]
    sql = "\n".join(fake.sql)
    assert "ORDER BY started_at DESC, run_id DESC" in sql
    assert "ORDER BY finished_at DESC" not in sql
    assert result["policy"]["transaction_stable_finished_at_safe"] is True


def test_v6_still_fails_closed_on_newest_completed_failure(monkeypatch):
    rows = [
        ("kalshi", "new_failed", "failed", False, "full", "2026-10-15T07:20:00Z", "2026-10-15T07:20:01Z", 4),
        ("polymarket", "new_success", "success", True, "full", "2026-10-15T07:21:00Z", "2026-10-15T07:21:01Z", 3),
    ]
    fake = FakeConnection(rows)
    monkeypatch.setattr(v6.v5, "evaluate_observed_readiness", lambda **kwargs: _base())
    monkeypatch.setattr(v6, "connect", lambda read_only=True: fake)

    result = v6.evaluate_observed_readiness(stale_minutes=20)
    assert result["ready"] is False
    assert "latest_source_runs_full_and_successful" in result["failed_controls"]


def test_worker_v5_combines_readiness_v6_and_singleflight():
    body = (ROOT / "institutional_worker_v5.py").read_text()
    assert "from institutional_readiness_v6 import evaluate_identity_readiness, evaluate_observed_readiness" in body
    assert "acquire_runtime_lease" in body
    assert "release_runtime_lease" in body
    assert "hot_path_v5_v11_identity_singleflight_readiness6" in body
    assert "institutional_worker_v3" not in body
