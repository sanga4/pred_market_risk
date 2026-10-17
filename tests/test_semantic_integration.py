"""Opt-in real-weight regression. Authored cases do not certify production accuracy."""
import json
import os
from pathlib import Path

import pytest


@pytest.mark.skipif(os.getenv("EVENTRISK_RUN_SEMANTIC_INTEGRATION") != "true", reason="Real model runs in dedicated pinned-dependency CI job")
def test_real_pinned_weights_repeat_frozen_decisions_across_batch_sizes():
    from institutional_semantic_model import SemanticClassifier, manifest
    from institutional_hierarchy_evaluate import challenge_examples
    from institutional_hierarchy_metrics import stability
    report = json.loads(Path("reports/hierarchy_challenge_v1.json").read_text())
    _, examples, _ = challenge_examples()
    classifier = SemanticClassifier()
    texts = [e["input_text"] for e in examples]
    actual = classifier.predict(texts)
    expected = report["expected_predictions"]
    assert report["model_id"] == manifest()["model_id"]
    assert [p["subject_id"] for p in actual] == [p["subject_id"] for p in expected]
    assert [p["theme_id"] for p in actual] == [p["theme_id"] for p in expected]
    repeated = classifier.predict(texts,batch_size=1)
    assert stability(actual,repeated)["subject_agreement"] == 1
    assert stability(actual,repeated)["theme_agreement"] == 1
