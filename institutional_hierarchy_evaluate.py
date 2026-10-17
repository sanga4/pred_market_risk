"""Reproducible evaluation; authored challenge data is never promoted to human gold."""
import argparse
import json
from pathlib import Path
import time
import uuid

from institutional_semantic_model import canonical_json, digest, load_taxonomy, manifest
from institutional_hierarchy_metrics import evaluate, promotion_gate, stability


def challenge_examples():
    raw = Path(__file__).with_name("hierarchy_challenge_v1.json").read_text()
    dataset = json.loads(raw)
    examples = [{"example_id": f"{subject}_{i}", "group_id": subject,
                 "input_text": text, "subject_id": subject, "split": "challenge"}
                for subject, texts in dataset["cases"].items() for i, text in enumerate(texts)]
    return dataset, examples, digest(raw)


def run(*, persist=False, classifier=None):
    from institutional_semantic_model import SemanticClassifier
    classifier = classifier or SemanticClassifier()
    dataset, examples, checksum = challenge_examples()
    texts = [e["input_text"] for e in examples]
    started = time.monotonic()
    predictions = classifier.predict(texts, full_scores=True)
    elapsed = time.monotonic() - started
    metrics = evaluate(examples, predictions, load_taxonomy(), dataset_kind=dataset["dataset_kind"])
    repeated = classifier.predict(texts, batch_size=1)
    metrics["repeatability"] = stability(predictions, repeated)
    metrics["casefold_stability"] = stability(predictions, classifier.predict([t.lower() for t in texts]))
    metrics["throughput"] = {"examples": len(texts), "seconds": elapsed,
                             "titles_per_second": len(texts) / elapsed, "scope": "small authored benchmark; not a production load test"}
    metrics["promotion_gate"] = promotion_gate(metrics)
    metrics["protocol"] = {"dataset_sha256": checksum, "description": dataset["description"],
        "model": manifest(), "thresholds_tuned_on_challenge": False,
        "sample_design": "Two authored examples per known subject plus eight unknown/adversarial examples; deliberately not production weighted."}
    result = {"dataset_id": dataset["dataset_id"], "model_id": manifest()["model_id"],
              "metrics": metrics,
              "predictions": [{**example, **prediction, "expected_subject_id": example["subject_id"]} for example, prediction in zip(examples, predictions)]}
    if persist:
        import pyarrow as pa
        from db import connect
        con = connect()
        try:
            old = con.execute("SELECT content_sha256 FROM er_hierarchy_evaluation_sets WHERE dataset_id=?", [dataset["dataset_id"]]).fetchone()
            if old and old[0] != checksum:
                raise RuntimeError("Evaluation dataset ID is immutable; use a new version")
            con.execute("BEGIN")
            try:
                con.execute("INSERT INTO er_hierarchy_evaluation_sets(dataset_id,dataset_kind,content_sha256,description) VALUES (?,?,?,?) ON CONFLICT DO NOTHING", [dataset["dataset_id"], dataset["dataset_kind"], checksum, dataset["description"]])
                con.register("_hierarchy_benchmark_labels", pa.Table.from_pylist([
                    {"dataset_id": dataset["dataset_id"], **e, "review_status": "authored_not_independent"} for e in examples]))
                con.execute("INSERT INTO er_hierarchy_labels BY NAME SELECT * FROM _hierarchy_benchmark_labels ON CONFLICT DO NOTHING")
                evaluation_id = "heval_" + uuid.uuid4().hex
                con.execute("""INSERT INTO er_hierarchy_evaluations(evaluation_id,dataset_id,model_id,metrics_json,predictions_json,evaluation_kind)
                    VALUES (?,?,?,?,?,?)""", [evaluation_id, dataset["dataset_id"], result["model_id"], canonical_json(metrics), canonical_json(result["predictions"]), dataset["dataset_kind"]])
                con.execute("COMMIT")
                result["evaluation_id"] = evaluation_id
            except Exception:
                con.execute("ROLLBACK")
                raise
        finally:
            con.close()
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--persist", action="store_true")
    parser.add_argument("--output")
    args = parser.parse_args()
    result = run(persist=args.persist)
    if args.output:
        Path(args.output).write_text(json.dumps(result, indent=2))
    metrics = result["metrics"]
    print(canonical_json({"model_id": result["model_id"], "evaluation_id": result.get("evaluation_id"),
        "subject_accuracy": metrics["subject"]["accuracy"], "macro_precision": metrics["subject"]["macro_precision"],
        "macro_recall": metrics["subject"]["macro_recall"], "macro_f1": metrics["subject"]["macro_f1"],
        "theme_accuracy": metrics["theme"]["accuracy"], "abstention_rate": metrics["abstention_rate"],
        "repeatability": metrics["repeatability"], "promotion_gate": metrics["promotion_gate"]}), flush=True)
