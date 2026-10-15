from pathlib import Path

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


def test_public_service_blocks_internal_control_plane_routes():
    body = text("institutional_public_service.py")
    assert 'request.url.path.startswith("/internal/")' in body
    assert 'status_code=404' in body
    assert "institutional_ops_api" in body
    assert "institutional_admin_api" in body


def test_public_service_exposes_investor_data_surfaces():
    body = text("institutional_public_service.py")
    for router in [
        "data_router",
        "curve_router",
        "changefeed_router",
        "ops_router",
        "history_router",
        "readiness_router",
        "release_router",
        "stream_router",
    ]:
        assert f"app.include_router({router})" in body
    assert 'X-EventRisk-Decision-Boundary' in body
    assert '"mode": "stateless-read-api"' in body
