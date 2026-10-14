from datetime import datetime, timezone

from institutional_stream import Cursor, decode_cursor, encode_cursor


def test_stream_cursor_round_trip():
    cursor = Cursor(
        datetime(2026, 10, 14, 21, 30, 0, 123456, tzinfo=timezone.utc),
        "sc_contract_9",
        datetime(2026, 10, 14, 21, 29, 59, tzinfo=timezone.utc),
    )
    encoded = encode_cursor(cursor)
    decoded = decode_cursor(encoded)
    assert decoded == cursor


def test_invalid_cursor_falls_back_to_explicit_since():
    decoded = decode_cursor("not-a-valid-token", "2026-10-14T21:00:00Z")
    assert decoded.received_at == datetime(2026, 10, 14, 21, 0, tzinfo=timezone.utc)
    assert decoded.source_contract_id == ""
    assert decoded.observed_at == datetime(1970, 1, 1, tzinfo=timezone.utc)
