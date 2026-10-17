"""Exercise discovery against a migrated warehouse with no historical observations."""

import pytest
from fastapi.testclient import TestClient

import db
import institutional_explorer_api as api
import institutional_public_service as service


@pytest.fixture
def client(monkeypatch, tmp_path):
    monkeypatch.setattr(db, "DB", str(tmp_path / "explorer.duckdb"))
    monkeypatch.setenv("DB_FORCE_READ_WRITE", "true")
    monkeypatch.setenv("ENABLE_BACKGROUND_COLLECTOR", "false")
    monkeypatch.setenv("EVENTRISK_REQUIRE_AUTH", "true")
    monkeypatch.setenv("EVENTRISK_API_KEY", "test-explorer-key")
    from institutional_migrations_v2 import ensure_schema
    ensure_schema()
    from institutional_hierarchy_schema import ensure_schema as ensure_hierarchy, register_model
    ensure_hierarchy(); register_model()
    con = db.connect()
    try:
        con.execute("""
            INSERT INTO er_event_families(event_family_id,canonical_name,category)
            VALUES ('fam_1','Federal Reserve decisions','rates'),('fam_2','Uncovered topic','other');
            INSERT INTO er_canonical_events(canonical_event_id,event_family_id,canonical_name,canonical_definition,event_class,status)
            VALUES ('evt_1','fam_1','Fed cuts by December?','Rules 1','rates','active'),
                   ('evt_2','fam_1','Fed cuts by January?','Rules 2','rates','active'),
                   ('evt_3','fam_2','Other topic?','Rules 3','other','active'),
                   ('evt_4','missing_family','Orphan event?','Rules 4','unclassified','active'),
                   ('evt_5','fam_1','Unserved event','No contracts','rates','active'),
                   ('evt_6','fam_1','Superseded event','Retired','rates','superseded');
            INSERT INTO er_source_contracts(source_contract_id,source_id,source_market_id,current_title,active)
            VALUES ('c1','kalshi','m1','Fed cuts?',TRUE),('c2','polymarket','m2','Fed cuts?',TRUE),
                   ('c3','kalshi','m3','Fed January?',TRUE),('c4','polymarket','m4','Other?',TRUE),
                   ('c5','kalshi','m5','Orphan?',TRUE),('c6','kalshi','m6','Retired?',TRUE),
                   ('c7','kalshi','m7','Inactive?',FALSE);
            INSERT INTO er_event_mappings(mapping_id,source_contract_id,canonical_event_id,mapping_type,confidence,mapping_method,review_status,valid_from)
            VALUES ('map1','c1','evt_1','source_native',1,'deterministic_identity','source_native','2026-01-01'),
                   ('map2','c2','evt_1','same_event',1,'reviewed','human_reviewed','2026-01-01'),
                   ('map3','c3','evt_2','source_native',1,'deterministic_identity','source_native','2026-01-01'),
                   ('map4','c4','evt_3','source_native',1,'deterministic_identity','source_native','2026-01-01'),
                   ('map5','c5','evt_4','source_native',1,'deterministic_identity','source_native','2026-01-01'),
                   ('map6','c6','evt_6','source_native',1,'deterministic_identity','source_native','2026-01-01'),
                   ('map7','c7','evt_1','source_native',1,'deterministic_identity','source_native','2026-01-01');
            INSERT INTO er_event_mappings(mapping_id,source_contract_id,canonical_event_id,mapping_type,mapping_method,valid_from,valid_to)
            VALUES ('old_map','c3','evt_1','same_event','reviewed','2025-01-01','2025-12-31'),
                   ('future_map','c3','evt_1','same_event','reviewed','2099-01-01',NULL);
            INSERT INTO er_market_current_state(source_contract_id,source_id,observed_at,received_at,displayed_probability,last_polled_at)
            VALUES ('c1','kalshi',now(),now(),0.4,now()),('c2','polymarket',now(),now(),0.55,now()),
                   ('c3','kalshi',now(),now(),0.7,now()),('c4','polymarket',now(),now(),NULL,now()),
                   ('c5','kalshi',now(),now(),0.2,now()),('c7','kalshi',now(),now(),0.99,now());
            INSERT INTO er_entities(entity_id,entity_type,canonical_name) VALUES ('org_fed_test','organization','Federal Reserve');
            INSERT INTO er_graph_edges(edge_id,source_node_id,source_node_type,relationship_type,target_node_id,target_node_type,edge_class,valid_from,valid_to)
            VALUES ('edge1','evt_1','canonical_event','mentions','org_fed_test','organization','source_asserted','2026-01-01',NULL),
                   ('edge_expired','evt_1','canonical_event','mentions','org_fed_test','organization','source_asserted','2025-01-01','2025-12-31'),
                   ('edge_inferred','evt_1','canonical_event','mentions','org_fed_test','organization','inferred','2026-01-01',NULL);
            INSERT INTO er_edge_evidence(edge_id,evidence_id,evidence_type,source_id,excerpt)
            VALUES ('edge1','ev1','source_contract','kalshi','Explicit Federal Reserve reference.');
        """)
    finally:
        con.close()
    from institutional_hierarchy_worker import infer, project
    class Classifier:
        def predict(self, texts):
            return [{"subject_id": "central_bank_rates" if "Fed" in text else "unclassified", "theme_id": "rates" if "Fed" in text else "unclassified", "candidate_subject_id": "central_bank_rates", "similarity": .7, "margin": .1, "abstained": "Fed" not in text, "reason": "fixture", "top_candidates": []} for text in texts]
    infer(classifier=Classifier()); project()
    monkeypatch.setenv("DB_FORCE_READ_WRITE", "false")
    with TestClient(service.app) as test_client:
        yield test_client


AUTH = {"Authorization": "Bearer test-explorer-key"}


def get(client, path, **params):
    response = client.get(path, params=params, headers=AUTH)
    assert response.status_code == 200, response.text
    return response.json()


def test_explorer_shell_is_public_but_all_data_routes_require_auth(client):
    page = client.get("/explorer")
    assert page.status_code == 200
    assert "Events → Subjects → Themes" in page.text
    assert page.headers["Cache-Control"] == "no-store"
    assert "frame-ancestors 'none'" in page.headers["Content-Security-Policy"]
    assert client.get("/explorer-assets/explorer.js").status_code == 200
    for path in ["summary", "events", "subjects", "events/evt_1/mappings"]:
        assert client.get("/v1/explorer/" + path).status_code == 401
    assert client.get("/").json()["explorer"] == "/explorer"


def test_summary_counts_current_active_coverage_without_history(client):
    result = get(client, "/v1/explorer/summary")
    assert result["coverage"]["events"] == 4
    assert result["coverage"]["subjects"] == 2
    assert result["coverage"]["themes"] == 2
    assert result["coverage"]["cross_source_events"] == 1
    assert result["coverage"]["uncategorized_events"] == 2
    assert result["coverage"]["events_without_family"] == 1
    assert result["coverage"]["formal_subject_events"] == 0
    filtered = get(client, "/v1/explorer/summary", source="kalshi")
    assert filtered["coverage"]["events"] == 3
    assert filtered["coverage"]["cross_source_events"] == 0
    assert sum(t["event_count"] for t in result["themes"]) == 4


def test_search_filters_and_keyset_pagination_cover_the_universe(client):
    first = get(client, "/v1/explorer/events", limit=2)
    second = get(client, "/v1/explorer/events", limit=2, after=first["next_after"])
    assert [e["canonical_event_id"] for e in first["items"] + second["items"]] == ["evt_1", "evt_2", "evt_3", "evt_4"]
    assert second["next_after"] is None
    assert get(client, "/v1/explorer/events", q="Federal Reserve")["count"] == 2
    assert get(client, "/v1/explorer/events", q="%_'")["count"] == 0
    result = get(client, "/v1/explorer/events", theme="rates", source="polymarket", subject_id="central_bank_rates")
    assert result["count"] == 1
    assert result["items"][0]["source_count"] == 1
    assert result["items"][0]["sources"] == "polymarket"
    assert client.get("/v1/explorer/events?limit=0", headers=AUTH).status_code == 422


def test_subjects_are_semantic_topics_with_complete_mapping_counts(client):
    result = get(client, "/v1/explorer/subjects", theme="rates", limit=1)
    assert result["items"][0]["subject_id"] == "central_bank_rates"
    assert result["items"][0]["event_count"] == 2
    assert result["items"][0]["contract_count"] == 3
    assert "semantic topic" in get(client, "/v1/explorer/summary")["definitions"]["subject"]
    assert get(client, "/v1/explorer/subjects", source="polymarket", theme="rates")["items"][0]["event_count"] == 1


def test_mapping_lineage_preserves_evidence_and_excludes_expired_inferred_edges(client):
    result = get(client, "/v1/explorer/events/evt_1/mappings")
    assert result["subject"]["subject_id"] == "central_bank_rates"
    assert result["event_family"]["event_family_id"] == "fam_1"
    assert result["theme"]["taxonomy_version"] == "eventrisk_semantic_topics_v1"
    assert result["theme"]["theme_id"] == "rates"
    assert {m["mapping_id"] for m in result["source_contracts"]} == {"map1", "map2", "map7"}
    assert result["insights"]["current_contract_count"] == 2
    assert result["insights"]["probability_range"] == pytest.approx(0.15)
    assert len(result["entity_mentions"]) == 1
    assert result["entity_mentions"][0]["target_node_type"] == "organization"
    assert result["entity_mentions"][0]["evidence"][0]["excerpt"] == "Explicit Federal Reserve reference."
    historic = get(client, "/v1/explorer/events/evt_1/mappings", include_expired=True)
    assert {m["mapping_id"] for m in historic["source_contracts"]} >= {"old_map", "future_map"}
    assert historic["insights"]["probability_range"] == pytest.approx(0.15)


def test_missing_mapping_and_one_source_are_reported_without_invented_links(client):
    orphan = get(client, "/v1/explorer/events/evt_4/mappings")
    assert orphan["subject"]["subject_id"] == "unclassified"
    assert orphan["event_family"] is None
    assert orphan["theme"]["theme_id"] == "unclassified"
    assert orphan["insights"]["theme_needs_review"]
    assert orphan["insights"]["probability_range"] is None
    assert orphan["entity_mentions"] == []
    assert client.get("/v1/explorer/events/missing/mappings", headers=AUTH).status_code == 404


def test_source_comparison_counts_distinct_venues_not_contracts(monkeypatch):
    import institutional_public_queries_api as queries

    class Result:
        description = [("source_id",), ("displayed_probability",)]
        def fetchall(self):
            return [("kalshi", 0.4), ("kalshi", 0.5), ("polymarket", None)]

    class Connection:
        def execute(self, *args):
            return Result()
        def close(self):
            pass

    monkeypatch.setattr(queries, "db_read", Connection)
    result = queries.source_comparison("evt_1")["descriptive_summary"]
    assert result["source_count"] == 2
    assert result["priced_source_count"] == 1
    assert result["priced_contract_count"] == 2
