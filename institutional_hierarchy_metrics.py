"""Metrics with explicit label provenance, denominators and clustered intervals.

No production accuracy is inferred from coverage, model scores, legacy regex labels
or an authored challenge set. Missing independent labels remain null, not 100%.
"""
from __future__ import annotations

from collections import Counter, defaultdict
import math
import random


def _ratio(a, b):
    return a / b if b else None


def _classification(truth, predicted, labels):
    per_class = {}
    confusion = Counter(zip(truth, predicted))
    for label in labels:
        tp = confusion[label, label]
        fp = sum(n for (a, b), n in confusion.items() if b == label and a != label)
        fn = sum(n for (a, b), n in confusion.items() if a == label and b != label)
        support = sum(a == label for a in truth)
        precision, recall = _ratio(tp, tp + fp), _ratio(tp, tp + fn)
        f1 = _ratio(2 * tp, 2 * tp + fp + fn)
        per_class[label] = {"precision": precision, "recall": recall, "f1": f1,
                            "support": support, "predicted": tp + fp, "tp": tp, "fp": fp, "fn": fn}
    supported = [v for v in per_class.values() if v["support"]]
    accuracy = _ratio(sum(a == b for a, b in zip(truth, predicted)), len(truth))
    return {
        "n": len(truth), "accuracy": accuracy, "micro_precision": accuracy,
        "micro_recall": accuracy, "micro_f1": accuracy,
        "macro_precision": sum(v["precision"] or 0 for v in supported) / len(supported) if supported else None,
        "macro_recall": sum(v["recall"] or 0 for v in supported) / len(supported) if supported else None,
        "macro_f1": sum(v["f1"] or 0 for v in supported) / len(supported) if supported else None,
        "weighted_f1": sum((v["f1"] or 0) * v["support"] for v in supported) / len(truth) if truth else None,
        "per_class": per_class,
        "confusion": [{"actual": a, "predicted": b, "count": n} for (a, b), n in sorted(confusion.items())],
    }


def _average_precision(binary_truth, ranking_scores):
    positives = sum(binary_truth)
    if not positives:
        return None
    ordered = sorted(zip(ranking_scores, binary_truth), reverse=True)
    # Group ties, so arbitrary row ordering cannot improve a reported ranking metric.
    groups = defaultdict(list)
    for score, yes in ordered:
        groups[score].append(yes)
    found = total = 0
    ap = 0.0
    for score in sorted(groups, reverse=True):
        ys = groups[score]
        found += sum(ys); total += len(ys)
        ap += (sum(ys) / positives) * found / total
    return ap


def _roc_auc(binary_truth, ranking_scores):
    positive = [s for y, s in zip(binary_truth, ranking_scores) if y]
    negative = [s for y, s in zip(binary_truth, ranking_scores) if not y]
    if not positive or not negative:
        return None
    return sum(1 if a > b else .5 if a == b else 0 for a in positive for b in negative) / (len(positive) * len(negative))


def evaluate(examples, predictions, taxonomy, *, dataset_kind, bootstrap_replicates=300):
    if len(examples) != len(predictions) or not examples:
        raise ValueError("Evaluation needs a nonempty aligned label/prediction set")
    parents = {s["id"]: s["theme"] for s in taxonomy["subjects"]}
    truth = [e["subject_id"] for e in examples]
    guessed = [p["subject_id"] for p in predictions]
    if any(s not in parents for s in truth + guessed):
        raise ValueError("Label is not part of this pinned taxonomy")
    subject = _classification(truth, guessed, list(parents))
    theme = _classification([parents[s] for s in truth], [parents[s] for s in guessed], list(taxonomy["themes"]))
    accepted = [i for i, p in enumerate(predictions) if not p["abstained"]]
    known = [i for i, y in enumerate(truth) if y != "unclassified"]
    unknown = [i for i, y in enumerate(truth) if y == "unclassified"]
    correct_accepted = sum(truth[i] == guessed[i] for i in accepted if truth[i] != "unclassified")
    overlap = sum(int(a == b) + int(parents[a] == parents[b]) for a, b in zip(truth, guessed))
    rankings = {}
    if all("all_scores" in p for p in predictions):
        for label in parents:
            if label == "unclassified":
                continue
            binary = [y == label for y in truth]
            scores = [p["all_scores"][label] for p in predictions]
            rankings[label] = {"average_precision": _average_precision(binary, scores), "roc_auc": _roc_auc(binary, scores)}
    groups = defaultdict(list)
    for i, example in enumerate(examples):
        groups[example["group_id"]].append(i)
    rng = random.Random(1729)
    keys = sorted(groups)
    boot = {"accuracy": [], "macro_recall": [], "macro_f1": []}
    for _ in range(bootstrap_replicates):
        indices = [i for key in rng.choices(keys, k=len(keys)) for i in groups[key]]
        sample = _classification([truth[i] for i in indices], [guessed[i] for i in indices], list(parents))
        for metric in boot:
            boot[metric].append(sample[metric])
    intervals = {}
    for metric, values in boot.items():
        values.sort()
        intervals[metric] = {"lower": values[int(.025 * (len(values)-1))], "upper": values[int(.975 * (len(values)-1))]}
    return {
        "label_provenance": dataset_kind, "is_independent_production_gold": dataset_kind == "human_reviewed_production",
        "subject": subject, "theme": theme,
        "hierarchical_precision": overlap / (2 * len(examples)), "hierarchical_recall": overlap / (2 * len(examples)),
        "coverage": len(accepted) / len(examples), "abstention_rate": 1 - len(accepted) / len(examples),
        "selective_precision": _ratio(correct_accepted, len(accepted)),
        "selective_recall": _ratio(correct_accepted, len(known)),
        "unknown_detection_recall": _ratio(sum(guessed[i] == "unclassified" for i in unknown), len(unknown)),
        "unknown_false_accept_rate": _ratio(sum(guessed[i] != "unclassified" for i in unknown), len(unknown)),
        "top_3_subject_recall_known": _ratio(sum(truth[i] in [c["subject_id"] for c in predictions[i]["top_candidates"][:3]] for i in known), len(known)),
        "ranking_metrics": rankings,
        "cluster_bootstrap_95pct": {"method": "group resampling; percentile interval", "groups": len(keys), "replicates": bootstrap_replicates, "intervals": intervals},
        "calibration": {"brier_score": None, "log_loss": None, "ece": None,
                        "reason": "Cosine similarities are not calibrated class probabilities. Requires independently labeled calibration and test sets."},
        "production_gold_precision": subject["micro_precision"] if dataset_kind == "human_reviewed_production" else None,
        "production_gold_recall": subject["micro_recall"] if dataset_kind == "human_reviewed_production" else None,
    }


def stability(reference, repeated):
    if len(reference) != len(repeated) or not reference:
        raise ValueError("Need aligned nonempty repeated predictions")
    n = len(reference)
    return {
        "n": n,
        "subject_agreement": sum(a["subject_id"] == b["subject_id"] for a, b in zip(reference, repeated)) / n,
        "theme_agreement": sum(a["theme_id"] == b["theme_id"] for a, b in zip(reference, repeated)) / n,
        "abstention_agreement": sum(a["abstained"] == b["abstained"] for a, b in zip(reference, repeated)) / n,
        "max_absolute_similarity_delta": max(abs(a["similarity"] - b["similarity"]) for a, b in zip(reference, repeated)),
    }


def promotion_gate(metrics, *, overrides=None):
    """Initial configurable release policy, not a universal institutional standard."""
    policy = {"minimum_gold_examples": 500, "minimum_per_subject": 10,
              "minimum_micro_precision": .95, "minimum_macro_recall": .85,
              "minimum_repeat_agreement": 1.0, **(overrides or {})}
    per_class = metrics.get("subject", {}).get("per_class", {})
    controls = {
        "independent_production_gold": metrics.get("is_independent_production_gold") is True,
        "adequate_total_support": metrics.get("subject", {}).get("n", 0) >= policy["minimum_gold_examples"],
        "adequate_per_subject_support": bool(per_class) and all(v["support"] >= policy["minimum_per_subject"] for k, v in per_class.items() if k != "unclassified"),
        "precision_target": (metrics.get("subject", {}).get("micro_precision") or 0) >= policy["minimum_micro_precision"],
        "macro_recall_target": (metrics.get("subject", {}).get("macro_recall") or 0) >= policy["minimum_macro_recall"],
        "repeatability": (metrics.get("repeatability", {}).get("subject_agreement") or 0) >= policy["minimum_repeat_agreement"],
    }
    return {"ready": all(controls.values()), "controls": controls,
            "failed_controls": [k for k, v in controls.items() if not v], "policy": policy}
