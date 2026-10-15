from pathlib import Path

import institutional_qa_v9 as qa_v9


ROOT = Path(__file__).resolve().parents[1]


class FakeResult:
    def __init__(self, row=(0,)):
        self._row = row

    def fetchone(self):
        return self._row


class FakeConnection:
    def __init__(self, name):
        self.name = name
        self.statements = []
        self.closed = False

    def execute(self, sql, params=None):
        self.statements.append((sql.strip(), params))
        if "count(*) FROM er_quality_issues WHERE status='open' AND severity='critical'" in sql:
            return FakeResult((0,))
        if "count(*) FROM er_quality_issues WHERE status='open'" in sql:
            return FakeResult((1,))
        return FakeResult((0,))

    def close(self):
        self.closed = True


def _ready_fingerprint(tag="same"):
    return {
        "identity": {"ready": True, "identity_work_queue": 0, "active_mapping_cardinality_bad": 0},
        "latest_completed_source_runs": [{"source_id": "kalshi", "run_id": tag}],
        "latest_identity_pass": [tag],
        "active_contracts": 10,
        "active_mappings": 10,
    }


def test_changed_core_fingerprint_defers_without_opening_write_connection(monkeypatch):
    read = FakeConnection("read")
    barrier = FakeConnection("barrier")
    calls = []

    def fake_connect(read_only=False):
        calls.append(read_only)
        assert read_only is True, "QA must not open a write connection after a changed fingerprint"
        return read if len(calls) == 1 else barrier

    fingerprints = iter([_ready_fingerprint("before"), _ready_fingerprint("after")])
    monkeypatch.setattr(qa_v9, "connect", fake_connect)
    monkeypatch.setattr(qa_v9, "_fingerprint", lambda con: next(fingerprints))
    monkeypatch.setattr(
        qa_v9,
        "_collect_rules",
        lambda con, stale: ({"probability_bounds": 0}, [], {"source_contract:probability_out_of_bounds": 0.01}),
    )

    result = qa_v9.run_qa(stale_minutes=20)

    assert result["deferred"] is True
    assert result["deferred_reason"] == "core_fingerprint_changed_during_read_evaluation"
    assert calls == [True, True]
    assert read.closed and barrier.closed
    assert not any(sql.startswith("INSERT INTO er_quality_issues") for sql, _ in read.statements + barrier.statements)


def test_stable_snapshot_uses_read_only_evaluation_then_short_write_sync(monkeypatch):
    read = FakeConnection("read")
    barrier = FakeConnection("barrier")
    write = FakeConnection("write")
    connections = [read, barrier, write]
    modes = []
    synced = []

    def fake_connect(read_only=False):
        modes.append(read_only)
        return connections.pop(0)

    fingerprint = _ready_fingerprint("stable")
    rules = [qa_v9.CollectedRule(
        object_type="source_contract",
        rule_code="example_rule",
        severity="critical",
        resolution_note="resolved",
        rows=[],
    )]
    monkeypatch.setattr(qa_v9, "connect", fake_connect)
    monkeypatch.setattr(qa_v9, "_fingerprint", lambda con: fingerprint)
    monkeypatch.setattr(
        qa_v9,
        "_collect_rules",
        lambda con, stale: ({"example": 0}, rules, {"source_contract:example_rule": 0.02}),
    )
    monkeypatch.setattr(
        qa_v9,
        "_sync_collected_rule",
        lambda con, rule: synced.append((con.name, rule.rule_code)) or 0,
    )

    result = qa_v9.run_qa(stale_minutes=20)

    assert result["deferred"] is False
    assert result["critical_issues"] == 0
    assert result["open_issues"] == 1
    assert result["execution"] == "read_only_snapshot_then_short_issue_sync"
    assert modes == [True, True, False]
    assert synced == [("write", "example_rule")]
    assert any(sql == "BEGIN TRANSACTION" for sql, _ in write.statements)
    assert any(sql == "COMMIT" for sql, _ in write.statements)


def test_qa_v9_source_enforces_two_phase_boundary():
    body = (ROOT / "institutional_qa_v9.py").read_text()
    worker = (ROOT / "institutional_enrichment_worker_v8.py").read_text()
    schema = (ROOT / "institutional_schema_051.sql").read_text()
    migrations = (ROOT / "institutional_migrations_v2.py").read_text()
    assert "connect(read_only=True)" in body
    assert "core_fingerprint_changed_during_read_evaluation" in body
    assert "read_only_snapshot_then_short_issue_sync" in body
    assert "from institutional_qa_v9 import run_qa" in worker
    assert "enrichment_path_v8_local_pairs_two_phase_qa" in worker
    assert "institutional_qa_v9_two_phase_read_snapshot" in schema
    assert '"institutional_schema_051.sql"' in migrations
