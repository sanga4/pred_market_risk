from pathlib import Path

import institutional_public_service as service
from institutional_stream import router as stream_router

ROOT = Path(__file__).resolve().parents[1]


def text(name: str) -> str:
    return (ROOT / name).read_text()


def test_public_service_is_stateless_and_has_no_write_side_imports():
    body = text("institutional_public_service.py")
    forbidden = [
        "ensure_schema",
        "recover_abandoned_runs",
        "institutional_ingest_v11",
        "run_identity_pass",
        "archive_pending",
        "collector_loop",
        "institutional_ops_api",
        "institutional_admin_api",
        "institutional_mapping",
    ]
    for value in forbidden:
        assert value not in body
    assert "institutional_public_queries_api" in body
    assert "connect(read_only=True)" in body
    assert "ENABLE_BACKGROUND_COLLECTOR must be false" in body
    assert "source collection is owned by MotherDuck Flights" in body


def test_public_service_blocks_and_excludes_internal_control_plane_routes():
    body = text("institutional_public_service.py")
    assert 'request.url.path.startswith("/internal/")' in body
    assert 'status_code=404' in body

    paths = set(service.app.openapi()["paths"])
    assert not any(path == "/internal" or path.startswith("/internal/") for path in paths)


def test_public_service_runtime_exposes_investor_http_and_websocket_surfaces():
    http_paths = set(service.app.openapi()["paths"])
    expected_http = {
        "/v1/events",
        "/v1/change-feed",
        "/v1/readiness",
        "/v1/sources",
        "/v1/quality",
        "/v1/releases",
    }
    assert expected_http <= http_paths

    assert any(getattr(route, "path", None) == "/v1/stream" for route in stream_router.routes)
    body = text("institutional_public_service.py")
    assert "app.include_router(stream_router)" in body
    assert 'X-EventRisk-Decision-Boundary' in body
    assert '"mode": "stateless-read-api"' in body


def test_pure_public_query_router_has_no_write_side_dependencies():
    body = text("institutional_public_queries_api.py")
    for forbidden in [
        "institutional_mapping",
        "institutional_archive",
        "institutional_auth",
        "institutional_exports",
        "generate_release",
        "review_candidate",
        "create_client_key",
        "disable_client",
    ]:
        assert forbidden not in body
    assert "connect(read_only=True)" in body
