"""MotherDuck Flight entrypoint. Deploy this file with a flattened pinned lock."""
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile


def main():
    ref = os.environ["EVENTRISK_GIT_REF"]
    database = os.environ["EVENTRISK_DB"]
    operation = os.environ.get("HIERARCHY_OPERATION", "cycle")
    if not re.fullmatch(r"[0-9a-f]{40}", ref):
        raise ValueError("Pin an immutable full Git commit SHA")
    if database not in {"event_risk_institutional", "event_risk_institutional_staging"}:
        raise ValueError("Unrecognized hierarchy database")
    if operation not in {"init", "infer", "project", "cycle", "evaluate", "accept", "seed-staging", "export-review", "recover-stopped"}:
        raise ValueError("Unrecognized hierarchy operation")
    if operation == "seed-staging" and database != "event_risk_institutional_staging":
        raise ValueError("Staging seed must never target production")
    with tempfile.TemporaryDirectory(prefix="eventrisk-hierarchy-") as directory:
        repo = Path(directory) / "repo"
        subprocess.run(["git","clone","--depth","1","https://github.com/sanga4/pred_market_risk.git",str(repo)],check=True)
        subprocess.run(["git","-C",str(repo),"fetch","--depth","1","origin",ref],check=True)
        subprocess.run(["git","-C",str(repo),"checkout","--detach",ref],check=True)
        os.environ["DB"] = "md:" + database
        os.environ["DB_FORCE_READ_WRITE"] = "true"
        os.environ["ENABLE_BACKGROUND_COLLECTOR"] = "false"
        os.environ.setdefault("EVENTRISK_MODEL_THREADS", "2")
        os.environ["OPENBLAS_NUM_THREADS"] = "1"
        os.environ.setdefault("OMP_NUM_THREADS", "2")
        os.environ["TOKENIZERS_PARALLELISM"] = "false"
        os.environ["PYTHONPATH"] = str(repo)
        os.chdir(repo); sys.path.insert(0,str(repo))
        from db import connect
        from institutional_semantic_model import canonical_json
        if operation in {"init","infer","project","cycle"}:
            from institutional_hierarchy_worker import main as worker
            sys.argv = ["institutional_hierarchy_worker.py",operation]
            worker()
        elif operation == "recover-stopped":
            from institutional_hierarchy_worker import recover_stopped
            print(canonical_json(recover_stopped(json.loads(os.environ["HIERARCHY_STOPPED_RUNS"]))),flush=True)
        elif operation == "evaluate":
            from institutional_hierarchy_evaluate import run
            result = run(persist=True)
            print(canonical_json({k:v for k,v in result.items() if k != "predictions"}),flush=True)
        elif operation == "seed-staging":
            con = connect()
            try:
                # Copy a bounded, repeatable sample, preserving source IDs and originals.
                # The target is staging only; production is read-only in this operation.
                con.execute("""CREATE OR REPLACE TEMP TABLE _seed_events AS
                    SELECT * FROM event_risk_institutional.er_canonical_events
                    WHERE status='active' ORDER BY sha256(canonical_event_id) LIMIT 1000""")
                con.execute("INSERT INTO er_event_families BY NAME SELECT f.* FROM event_risk_institutional.er_event_families f WHERE event_family_id IN (SELECT event_family_id FROM _seed_events) ON CONFLICT DO NOTHING")
                con.execute("INSERT INTO er_canonical_events BY NAME SELECT * FROM _seed_events ON CONFLICT DO NOTHING")
                con.execute("""CREATE OR REPLACE TEMP TABLE _seed_mappings AS
                    SELECT * FROM event_risk_institutional.er_event_mappings
                    WHERE canonical_event_id IN (SELECT canonical_event_id FROM _seed_events) AND valid_to IS NULL AND valid_from<=now()""")
                con.execute("INSERT INTO er_source_contracts BY NAME SELECT * FROM event_risk_institutional.er_source_contracts WHERE source_contract_id IN (SELECT source_contract_id FROM _seed_mappings) ON CONFLICT DO NOTHING")
                con.execute("INSERT INTO er_event_mappings BY NAME SELECT * FROM _seed_mappings ON CONFLICT DO NOTHING")
                con.execute("INSERT INTO er_market_current_state BY NAME SELECT * FROM event_risk_institutional.er_market_current_state WHERE source_contract_id IN (SELECT source_contract_id FROM _seed_mappings) ON CONFLICT DO NOTHING")
                print(json.dumps({"staging_sample_events":con.execute("SELECT count(*) FROM _seed_events").fetchone()[0]}),flush=True)
            finally: con.close()
        elif operation == "export-review":
            from institutional_hierarchy_admin import export_review
            result = export_review(count=1000)
            # Persisted blinded sample is retrievable through the read-only API.
            print(canonical_json({"dataset_id":result["dataset_id"],"examples":len(result["examples"]),"protocol":result["protocol"]}),flush=True)
        else:
            from institutional_hierarchy_queries import operational_metrics
            con = connect()
            try:
                con.execute("BEGIN")
                result = operational_metrics(con)
                print(json.dumps(result,default=str,sort_keys=True),flush=True)
                assert not any(result["integrity"].values()),result["integrity"]
                assert con.execute("SELECT count(*) FROM er_schema_migrations").fetchone()[0] == 59
                assert result["coverage"]["mapped_events"] > 0
            finally: con.close()
            from institutional_explorer_smoke import main as smoke
            smoke()
            print("SEMANTIC_HIERARCHY_ACCEPTANCE_OK",flush=True)


if __name__ == "__main__":
    main()
