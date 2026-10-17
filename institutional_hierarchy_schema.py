"""Checksum-guarded additive migrations for the advisory hierarchy namespace.

The core migration chain remains at its existing version so already-pinned ingestion
and QA runners remain valid while the new lane is promoted independently.
"""
from pathlib import Path
import hashlib

from db import connect
from institutional_semantic_model import canonical_json, digest, load_taxonomy, manifest


def ensure_schema():
    con = connect()
    try:
        con.execute("""CREATE TABLE IF NOT EXISTS er_hierarchy_schema_migrations(
            migration_name VARCHAR PRIMARY KEY,content_sha256 VARCHAR NOT NULL,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT now())""")
        name = "hierarchy_schema_001.sql"
        sql = Path(__file__).with_name(name).read_text()
        checksum = hashlib.sha256(sql.encode()).hexdigest()
        prior = con.execute("SELECT content_sha256 FROM er_hierarchy_schema_migrations WHERE migration_name=?", [name]).fetchone()
        if prior and prior[0] != checksum:
            raise RuntimeError("Hierarchy migration checksum drift; add a migration instead")
        if not prior:
            con.execute("BEGIN")
            try:
                con.execute(sql)
                con.execute("INSERT INTO er_hierarchy_schema_migrations VALUES (?,?,now())", [name, checksum])
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise
        return {"hierarchy_migrations": 1, "applied_now": not bool(prior)}
    finally:
        con.close()


def register_model():
    import pyarrow as pa
    taxonomy = load_taxonomy()
    model = manifest()
    taxonomy_json = canonical_json(taxonomy)
    manifest_json = canonical_json(model)
    con = connect()
    try:
        prior = con.execute("SELECT content_sha256 FROM er_hierarchy_taxonomies WHERE taxonomy_version=?", [taxonomy["version"]]).fetchone()
        if prior and prior[0] != digest(taxonomy_json):
            raise RuntimeError("Taxonomy version is immutable; create a new version")
        prior_model = con.execute("SELECT manifest_sha256 FROM er_hierarchy_models WHERE model_id=?", [model["model_id"]]).fetchone()
        if prior_model and prior_model[0] != digest(manifest_json):
            raise RuntimeError("Model manifest is immutable")
        con.execute("BEGIN")
        try:
            con.execute("INSERT INTO er_hierarchy_taxonomies(taxonomy_version,content_sha256,definition_json) VALUES (?,?,?) ON CONFLICT DO NOTHING", [taxonomy["version"], digest(taxonomy_json), taxonomy_json])
            con.execute("INSERT INTO er_hierarchy_models(model_id,taxonomy_version,manifest_sha256,manifest_json) VALUES (?,?,?,?) ON CONFLICT DO NOTHING", [model["model_id"], taxonomy["version"], digest(manifest_json), manifest_json])
            tables = {
                "_hierarchy_themes": [{"taxonomy_version": taxonomy["version"], "theme_id": k, "canonical_name": v} for k, v in taxonomy["themes"].items()],
                "_hierarchy_subjects": [{"taxonomy_version": taxonomy["version"], "subject_id": s["id"], "canonical_name": s["name"], "definition": " ".join(s["anchors"]) if s["anchors"] else "Abstention or pending inference; no semantic claim."} for s in taxonomy["subjects"]],
                "_hierarchy_links": [{"taxonomy_version": taxonomy["version"], "subject_id": s["id"], "theme_id": s["theme"], "definition_sha256": digest(canonical_json(s))} for s in taxonomy["subjects"]],
            }
            for name, rows in tables.items():
                con.register(name, pa.Table.from_pylist(rows))
            con.execute("INSERT INTO er_hierarchy_themes BY NAME SELECT * FROM _hierarchy_themes ON CONFLICT DO NOTHING")
            con.execute("INSERT INTO er_hierarchy_subjects BY NAME SELECT * FROM _hierarchy_subjects ON CONFLICT DO NOTHING")
            con.execute("INSERT INTO er_hierarchy_subject_theme_mappings BY NAME SELECT * FROM _hierarchy_links ON CONFLICT DO NOTHING")
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        return model
    finally:
        con.close()
