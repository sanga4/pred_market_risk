from pathlib import Path

import institutional_public_service as service

ROOT = Path(__file__).resolve().parents[1]


def text(name: str) -> str:
    return (ROOT / name).read_text()


def test_public_service_is_stateless_and_has_no_write_side_startup():
    body = text("institutional_public_service.py")
    forbidden = [
        "ensure_schema",
        "recover_abandoned_runs",
        "institutional_ingest_v11",
        "run_identity_pass",
        "archive_pending",
        "collector_loop",
    ]
    for value in forbidden:
        assert value not in body
    assert "connect(read_only=True)" in body
    assert "ENABLE_BACKGROUND_COLLECTOR must be false" in body
    assert "source collection is owned by MotherDuck Flights" in body


def test_public_service_blocks_and_excludes_internal_control_plane_routes():
    body = text("institutional_public_service.py")
    assert 'request.url.path.startswith("/internal/")' in body
    assert 'status_code=404' in body
    assert "_include_public_routes" in body

    paths = set(service.app.openapi()["paths"])
    assert not any(path == "/internal" or path.startswith("/internal/") for path in paths)


def test_public_service_runtime_exposes_investor_data_surfaces():
    paths = set(service.app.openapi()["paths"])
    expected = {
        "/v1/events",
        "/v1/change-feed",
        "/v1/readiness",
        "/v1/sources",
        "/v1/quality",
        "/v1/releases",
        "/v1/stream",
    }
    assert expected <= paths

    body = text("institutional_public_service.py")
    assert 'X-EventRisk-Decision-Boundary' in body
    assert '"mode": "stateless-read-api"' in body
