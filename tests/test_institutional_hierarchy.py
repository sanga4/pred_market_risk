import copy
import json

import pytest
from fastapi.testclient import TestClient

import db
from institutional_semantic_model import digest, load_taxonomy, manifest
from institutional_hierarchy_worker import infer, project


class Classifier:
    def __init__(self): self.calls = 0

    def predict(self, texts, **kwargs):
        self.calls += len(texts)
        return [{"subject_id": "central_bank_rates" if "Fed" in t else "unclassified",
                 "theme_id": "rates" if "Fed" in t else "unclassified", "candidate_subject_id": "central_bank_rates",
                 "similarity": .7, "margin": .1, "abstained": "Fed" not in t,
                 "reason": "fixture", "top_candidates": [{"subject_id": "central_bank_rates", "similarity": .7}]} for t in texts]


@pytest.fixture
def warehouse(monkeypatch, tmp_path):
    monkeypatch.setattr(db, "DB", str(tmp_path / "hierarchy.duckdb"))
    monkeypatch.setenv("DB_FORCE_READ_WRITE", "true")
    from institutional_migrations_v2 import ensure_schema
    from institutional_hierarchy_schema import ensure_schema as hierarchy_schema, register_model
    ensure_schema(); hierarchy_schema(); register_model()
    con = db.connect()
    con.execute("""INSERT INTO er_canonical_events(canonical_event_id,canonical_name,event_family_id,status,canonical_definition)
        VALUES ('e1','Fed rate cut in December?','f1','active','Rules'),('e2','Fed rate cut in December?','f1','active','Rules'),
               ('e3','Unknown question','f2','active','Rules')""")
    yield con
    con.close()


def test_cached_inference_and_idempotent_projection_preserve_core(warehouse):
    con = warehouse; classifier = Classifier()
    core = con.execute("SELECT count(*) FROM er_schema_migrations").fetchone()[0]
    assert infer(classifier=classifier)["inputs_processed"] == 2
    assert classifier.calls == 2
    assert infer(classifier=classifier)["inputs_processed"] == 0
    assert project()["mappings_changed"] == 3
    assert project()["mappings_changed"] == 0
    assert con.execute("SELECT count(*) FROM er_hierarchy_current").fetchone()[0] == 3
    assert con.execute("SELECT count(*) FROM er_schema_migrations").fetchone()[0] == core == 59
    from institutional_hierarchy_queries import operational_metrics
    result = operational_metrics(con)
    assert result["structurally_ready"] and result["inference_complete"]
    assert not any(result["integrity"].values())
    assert result["production_precision"] is None and result["production_recall"] is None
    assert not result["institutional_validation_ready"]


def test_pending_abstained_and_title_changes_are_versioned_without_backdating(warehouse):
    con = warehouse
    project()
    old = con.execute("SELECT mapping_id,valid_from FROM er_hierarchy_current WHERE canonical_event_id='e3'").fetchone()
    assert con.execute("SELECT assignment_status FROM er_hierarchy_current WHERE canonical_event_id='e3'").fetchone()[0] == "pending_inference"
    infer(classifier=Classifier()); project()
    assert con.execute("SELECT assignment_status FROM er_hierarchy_current WHERE canonical_event_id='e3'").fetchone()[0] == "abstained"
    end = con.execute("SELECT valid_to FROM er_hierarchy_event_subject_mappings WHERE mapping_id=?", [old[0]]).fetchone()[0]
    assert end > old[1]
    con.execute("UPDATE er_canonical_events SET canonical_name='Fed future rate decision?' WHERE canonical_event_id='e3'")
    project()
    assert con.execute("SELECT assignment_status FROM er_hierarchy_current WHERE canonical_event_id='e3'").fetchone()[0] == "pending_inference"
    infer(classifier=Classifier()); project()
    assert con.execute("SELECT subject_id FROM er_hierarchy_current WHERE canonical_event_id='e3'").fetchone()[0] == "central_bank_rates"


def test_review_override_is_preserved_and_invalidated_when_title_changes(warehouse):
    from institutional_hierarchy_admin import override
    con = warehouse; infer(classifier=Classifier()); project()
    with pytest.raises(ValueError, match="input changed"):
        override("e1","consumer_prices","analyst","Reviewed source",expected_input_hash="stale",con=con)
    override("e1","consumer_prices","analyst","Reviewed source",expected_input_hash=digest("Fed rate cut in December?"),con=con)
    project(); project()
    assert con.execute("SELECT subject_id,theme_id,review_status FROM er_hierarchy_current WHERE canonical_event_id='e1'").fetchone() == ("consumer_prices","inflation","human_reviewed")
    override("e1","consumer_prices","second analyst","New supporting evidence",expected_input_hash=digest("Fed rate cut in December?"),con=con)
    assert project()["mappings_changed"] == 1
    assert "second analyst" == con.execute("SELECT reviewer FROM er_hierarchy_overrides WHERE valid_to IS NULL AND canonical_event_id='e1'").fetchone()[0]
    from institutional_hierarchy_api import event_hierarchy
    reviewed = event_hierarchy("e1")["mapping"]["review"]
    assert reviewed["reviewer"] == "second analyst" and reviewed["reason"] == "New supporting evidence"
    con.execute("UPDATE er_canonical_events SET canonical_name='Fed leadership change?' WHERE canonical_event_id='e1'")
    infer(classifier=Classifier()); project()
    assert con.execute("SELECT review_status FROM er_hierarchy_current WHERE canonical_event_id='e1'").fetchone()[0] == "unreviewed"


def test_hash_partitions_cover_inputs_once(warehouse):
    classifier = Classifier()
    total = sum(infer(partition_id=i,partition_count=4,classifier=classifier)["inputs_processed"] for i in range(4))
    assert total == classifier.calls == 2
    with pytest.raises(ValueError): infer(partition_count=3,classifier=classifier)


def test_stopped_worker_recovery_requires_terminal_evidence_and_exact_owner(warehouse):
    from institutional_hierarchy_worker import _lease, recover_stopped
    lease = _lease("hierarchy_inference_4_0")
    warehouse.execute("INSERT INTO er_hierarchy_runs(run_id,operation,model_id,partition_id,partition_count,status) VALUES ('interrupted-test','inference',?,0,4,'running')", [manifest()["model_id"]])
    record = {"run_id":"interrupted-test","expected_owner_id":lease.owner_id,"flight_run_number":8,"terminal_status":"CANCELLED"}
    with pytest.raises(ValueError,match="terminal Flight"): recover_stopped([record])
    record["confirmed_terminal"] = True
    with pytest.raises(ValueError,match="owner"): recover_stopped([{**record,"expected_owner_id":"replacement"}])
    assert recover_stopped([record])["recovered_runs"] == ["interrupted-test"]
    assert warehouse.execute("SELECT status FROM er_hierarchy_runs WHERE run_id='interrupted-test'").fetchone()[0] == "interrupted"
    assert recover_stopped([record])["recovered_runs"] == []


def test_blind_review_rejects_cherry_picking_and_requires_two_reviewers(warehouse):
    from institutional_hierarchy_admin import export_review, import_review, evaluate_gold, approve
    payload = export_review(count=10,con=warehouse)
    assert len(payload["examples"]) == 2  # one per family, no duplicated horizon
    assert "candidate_subject_id" not in json.dumps(payload)
    with pytest.raises(ValueError,match="independently reviewed"):
        evaluate_gold(payload["dataset_id"],classifier=Classifier(),con=warehouse)
    payload["independent_review_attestation"] = True
    incomplete = copy.deepcopy(payload); incomplete["examples"].pop()
    with pytest.raises(ValueError,match="cherry picking"): import_review(incomplete,con=warehouse)
    with pytest.raises(ValueError,match="Two distinct"): import_review(payload,con=warehouse)
    for e in payload["examples"]:
        e.update(reviewer_a="a",reviewer_b="b",subject_a="central_bank_rates",subject_b="central_bank_rates",subject_id="central_bank_rates")
    tampered = copy.deepcopy(payload); tampered["examples"][0]["input_text"] = "changed"
    with pytest.raises(ValueError,match="remain unchanged"): import_review(tampered,con=warehouse)
    assert import_review(payload,con=warehouse)["reviewed"] == 2
    evaluated = evaluate_gold(payload["dataset_id"],classifier=Classifier(),con=warehouse)
    assert evaluated["metrics"]["is_independent_production_gold"]
    assert not evaluated["metrics"]["promotion_gate"]["ready"]
    with pytest.raises(ValueError,match="blocked"): approve(evaluated["evaluation_id"],"owner","Review",con=warehouse)
    with pytest.raises(ValueError,match="immutable"): import_review(payload,con=warehouse)
    with pytest.raises(ValueError,match="blocked"): approve("not-an-independent-evaluation","owner","Review",con=warehouse)


def test_auth_history_boundary_and_read_only_metrics(warehouse, monkeypatch):
    import institutional_public_service as service
    infer(classifier=Classifier()); project()
    first = warehouse.execute("SELECT valid_from FROM er_hierarchy_current WHERE canonical_event_id='e1'").fetchone()[0]
    warehouse.execute("UPDATE er_canonical_events SET canonical_name='Changed question' WHERE canonical_event_id='e1'")
    project()
    boundary = warehouse.execute("SELECT valid_to FROM er_hierarchy_event_subject_mappings WHERE canonical_event_id='e1' AND version_no=1").fetchone()[0]
    monkeypatch.setenv("EVENTRISK_REQUIRE_AUTH","true"); monkeypatch.setenv("EVENTRISK_API_KEY","test-hierarchy")
    monkeypatch.setenv("ENABLE_BACKGROUND_COLLECTOR","false")
    auth = {"Authorization":"Bearer test-hierarchy"}
    from institutional_hierarchy_admin import export_review
    pending = export_review(count=10,con=warehouse)
    with TestClient(service.app) as client:
        assert client.get("/v1/hierarchy/metrics").status_code == 401
        for path in ["metrics","taxonomy","review"]:
            assert client.get("/v1/hierarchy/"+path,headers=auth).status_code == 200
        dataset = client.get("/v1/hierarchy/review-datasets/"+pending["dataset_id"],headers=auth)
        assert dataset.status_code == 200
        assert not dataset.json()["independent_review_attestation"]
        assert "candidate_subject_id" not in dataset.text
        assert len(dataset.json()["examples"]) == 2
        old = client.get("/v1/hierarchy/events/e1",params={"at":first.isoformat()},headers=auth).json()
        new = client.get("/v1/hierarchy/events/e1",params={"at":boundary.isoformat()},headers=auth).json()
        assert old["mapping"]["version_no"] == 1
        assert new["mapping"]["version_no"] == 2
        assert client.get("/v1/hierarchy/events/missing",headers=auth).status_code == 404


def test_hand_calculated_metrics_and_authored_provenance():
    from institutional_hierarchy_metrics import evaluate, promotion_gate, _roc_auc, _average_precision
    examples = [{"subject_id":s,"group_id":str(i)} for i,s in enumerate(["central_bank_rates","central_bank_rates","consumer_prices","unclassified"])]
    predictions = [{"subject_id":s,"theme_id":"rates","abstained":s=="unclassified","top_candidates":[{"subject_id":s}]} for s in ["central_bank_rates","unclassified","central_bank_rates","unclassified"]]
    metrics = evaluate(examples,predictions,load_taxonomy(),dataset_kind="authored_challenge",bootstrap_replicates=20)
    assert metrics["subject"]["accuracy"] == .5
    assert metrics["subject"]["per_class"]["central_bank_rates"]["precision"] == .5
    assert metrics["subject"]["per_class"]["central_bank_rates"]["recall"] == .5
    assert metrics["unknown_detection_recall"] == 1
    assert metrics["production_gold_recall"] is None
    assert metrics["calibration"]["brier_score"] is None
    assert not promotion_gate(metrics)["ready"]
    assert _roc_auc([1,0],[.5,.5]) == .5
    assert _average_precision([1,0],[.5,.5]) == .5


def test_manifest_is_pinned_and_taxonomy_has_exactly_one_parent():
    model = manifest(); taxonomy = load_taxonomy()
    assert model["model_revision"] == "1110a243fdf4706b3f48f1d95db1a4f5529b4d41"
    assert model["model_id"] == "hmodel_6f49091fdc1373386625098c"
    assert len(taxonomy["subjects"]) == 54
    assert all(s["theme"] in taxonomy["themes"] for s in taxonomy["subjects"])
