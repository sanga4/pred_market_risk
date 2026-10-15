from pathlib import Path

import institutional_auth as auth


ROOT = Path(__file__).resolve().parents[1]


class FakeResult:
    def __init__(self, row=None):
        self.row = row

    def fetchone(self):
        return self.row


class FakeConnection:
    def __init__(self):
        self.statements = []
        self.closed = False

    def execute(self, sql, params=None):
        self.statements.append((sql, params))
        if sql.startswith("SELECT client_id"):
            return FakeResult(("client_1", "Test Institution", "read,stream", True))
        return FakeResult()

    def close(self):
        self.closed = True


def test_authenticate_key_can_be_strictly_read_only(monkeypatch):
    connection = FakeConnection()
    calls = []

    def fake_connect(read_only=False):
        calls.append(read_only)
        return connection

    monkeypatch.setattr(auth, "connect", fake_connect)
    monkeypatch.delenv("EVENTRISK_API_KEY", raising=False)

    result = auth.authenticate_key("er_live_example", "read", record_usage=False)

    assert result["client_id"] == "client_1"
    assert calls == [True]
    assert not any(sql.startswith("UPDATE er_api_clients") for sql, _ in connection.statements)
    assert connection.closed


def test_authenticate_key_usage_touch_remains_opt_in(monkeypatch):
    connection = FakeConnection()
    calls = []

    def fake_connect(read_only=False):
        calls.append(read_only)
        return connection

    monkeypatch.setattr(auth, "connect", fake_connect)
    monkeypatch.delenv("EVENTRISK_API_KEY", raising=False)

    result = auth.authenticate_key("er_live_example", "read", record_usage=True)

    assert result["client_id"] == "client_1"
    assert calls == [False]
    assert any(sql.startswith("UPDATE er_api_clients") for sql, _ in connection.statements)


def test_public_service_explicitly_disables_authentication_writes():
    body = (ROOT / "institutional_public_service.py").read_text()
    assert 'authenticate_key(supplied, "read", record_usage=False)' in body
    assert "institutional_admin_api" not in body
    assert "institutional_ops_api" not in body
