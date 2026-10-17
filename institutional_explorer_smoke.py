"""Read-only acceptance against a configured warehouse; never migrates or creates client keys.

Run with DB explicitly set. A temporary process-local bootstrap key exercises the
same HTTP/auth path against the real warehouse without changing production key state.
Set EVENTRISK_PUBLIC_URL to additionally verify the deployed shell and auth boundary.
"""

from __future__ import annotations

import os
import secrets
import time


def main():
    if not os.environ.get("DB"):
        raise RuntimeError("Set DB explicitly to the intended acceptance warehouse")
    os.environ["DB_FORCE_READ_WRITE"] = "false"
    os.environ["ENABLE_BACKGROUND_COLLECTOR"] = "false"
    os.environ["EVENTRISK_REQUIRE_AUTH"] = "true"
    local_key = secrets.token_urlsafe(32)
    os.environ["EVENTRISK_API_KEY"] = local_key

    from fastapi.testclient import TestClient
    from institutional_public_service import app

    with TestClient(app) as client:
        def get(path, **params):
            started = time.monotonic()
            response = client.get(path, params=params, headers={"Authorization": f"Bearer {local_key}"})
            print("GET", path, response.status_code, round(time.monotonic() - started, 3), "seconds", flush=True)
            assert response.status_code == 200, response.text[:1000]
            return response.json()

        assert client.get("/v1/explorer/summary").status_code == 401
        assert client.get("/v1/hierarchy/metrics").status_code == 401
        quality = get("/v1/hierarchy/metrics")
        assert not any(quality["integrity"].values())
        assert len(get("/v1/hierarchy/taxonomy")["definition"]["subjects"]) == 54
        get("/v1/hierarchy/review",limit=2)
        summary = get("/v1/explorer/summary")
        assert summary["coverage"]["events"] > 0
        assert sum(t["event_count"] for t in summary["themes"]) == summary["coverage"]["events"]
        print("coverage", summary["coverage"], "reference", summary["reference"], flush=True)
        first = get("/v1/explorer/events", limit=2)
        assert first["count"] == 2
        second = get("/v1/explorer/events", limit=2, after=first["next_after"])
        assert not {e["canonical_event_id"] for e in first["items"]} & {e["canonical_event_id"] for e in second["items"]}
        subjects = get("/v1/explorer/subjects", limit=2)
        assert subjects["items"]
        subject_id = subjects["items"][0]["subject_id"]
        related = get("/v1/explorer/events", subject_id=subject_id, limit=2)
        assert all(e["subject_id"] == subject_id for e in related["items"])
        event_id = first["items"][0]["canonical_event_id"]
        detail = get(f"/v1/explorer/events/{event_id}/mappings")
        assert detail["event"]["canonical_event_id"] == event_id
        assert detail["hierarchy_mapping"]
        hierarchy = get(f"/v1/hierarchy/events/{event_id}")
        assert hierarchy["mapping"]["subject_id"] == detail["subject"]["subject_id"]
        assert hierarchy["mapping"]["theme_id"] == detail["theme"]["theme_id"]
        assert detail["source_contracts"]
        assert all(m["valid_to"] is None for m in detail["source_contracts"])
        filtered = get("/v1/explorer/events", theme="rates", source="kalshi", limit=2)
        assert all(e["theme_id"] == "rates" and e["sources"] == "kalshi" for e in filtered["items"])

        from db import connect
        con = connect(read_only=True)
        try:
            mention = con.execute("""
                SELECT g.source_node_id FROM er_graph_edges g
                JOIN er_canonical_events e ON e.canonical_event_id=g.source_node_id
                WHERE e.status='active' AND g.valid_to IS NULL AND g.valid_from<=now()
                  AND g.edge_class='source_asserted' AND g.relationship_type='mentions'
                ORDER BY g.source_node_id LIMIT 1
            """).fetchone()
        finally:
            con.close()
        if mention:
            mentions = get(f"/v1/explorer/events/{mention[0]}/mappings")["entity_mentions"]
            assert mentions and any(edge["evidence"] for edge in mentions)
            print("evidenced_mentions", len(mentions), flush=True)

    base = os.environ.get("EVENTRISK_PUBLIC_URL", "").rstrip("/")
    if base:
        import requests
        for path in ("/explorer", "/explorer-assets/explorer.js", "/explorer-assets/explorer.css"):
            response = requests.get(base + path, timeout=45)
            assert response.status_code == 200, (path, response.status_code)
            print("deployed_public", path, response.status_code, flush=True)
        for path in ("/v1/explorer/summary", "/v1/explorer/events", "/v1/explorer/subjects", "/v1/hierarchy/metrics", "/v1/hierarchy/taxonomy", "/v1/hierarchy/review-datasets"):
            assert requests.get(base + path, timeout=45).status_code == 401, path
        print("DEPLOYED_EXPLORER_AUTH_BOUNDARY_OK", flush=True)
    print("EXPLORER_WAREHOUSE_ACCEPTANCE_OK", flush=True)


if __name__ == "__main__":
    main()
