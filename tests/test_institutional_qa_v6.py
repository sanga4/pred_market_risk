from pathlib import Path

import duckdb

import institutional_qa_v6 as qa_v6

ROOT = Path(__file__).resolve().parents[1]


def text(name: str) -> str:
    return (ROOT / name).read_text()


def test_reviewed_graph_edge_uses_edge_evidence_namespace():
    con = duckdb.connect(":memory:")
    try:
        con.execute("""
            CREATE TABLE er_graph_edges(
              edge_id VARCHAR PRIMARY KEY,
              human_reviewed BOOLEAN,
              inference_method VARCHAR,
              method_version VARCHAR
            );
            CREATE TABLE er_edge_evidence(
              edge_id VARCHAR,
              evidence_id VARCHAR,
              PRIMARY KEY(edge_id,evidence_id)
            );
            CREATE TABLE er_event_relationship_evidence(
              relationship_id VARCHAR,
              evidence_id VARCHAR,
              PRIMARY KEY(relationship_id,evidence_id)
            );
            CREATE TABLE er_quality_issues(
              quality_issue_id VARCHAR PRIMARY KEY,
              object_type VARCHAR,
              object_id VARCHAR,
              rule_code VARCHAR,
              severity VARCHAR,
              status VARCHAR,
              details VARCHAR,
              detected_at TIMESTAMPTZ,
              resolved_at TIMESTAMPTZ,
              resolution_notes VARCHAR
            );
            INSERT INTO er_graph_edges VALUES ('edge-1', TRUE, 'reviewed_mapping', 'v1');
        """)

        assert qa_v6.check_reviewed_relationship_provenance(con) == 1
        assert con.execute(
            "SELECT status FROM er_quality_issues WHERE object_id='edge-1'"
        ).fetchone()[0] == "open"

        con.execute("INSERT INTO er_edge_evidence VALUES ('edge-1','ev-1')")
        assert qa_v6.check_reviewed_relationship_provenance(con) == 0
        assert con.execute(
            "SELECT status FROM er_quality_issues WHERE object_id='edge-1'"
        ).fetchone()[0] == "resolved"
    finally:
        con.close()


def test_qa_v6_keeps_evidence_namespaces_distinct_and_is_versioned():
    qa = text("institutional_qa_v6.py")
    worker = text("institutional_enrichment_worker_v3.py")
    schema = text("institutional_schema_042.sql")
    migrations = text("institutional_migrations_v2.py")

    assert "LEFT JOIN er_edge_evidence r USING(edge_id)" in qa
    assert "LEFT JOIN er_event_relationship_evidence r USING(edge_id)" not in qa
    assert "institutional_qa_v6_graph_evidence" in qa
    assert "from institutional_qa_v6 import run_qa" in worker
    assert "enrichment_path_v3_provenance_qa" in worker
    assert "institutional_qa_v6_graph_evidence" in schema
    assert '"institutional_schema_042.sql"' in migrations
