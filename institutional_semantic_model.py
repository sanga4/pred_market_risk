"""Pinned CPU sentence transformer + transparent semantic prototypes.

Inference dependencies belong only to the enrichment runner. Importing the public
API never loads ONNX, a tokenizer or model weights. Similarity is NOT probability.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

MODEL_REPO = "sentence-transformers/all-MiniLM-L6-v2"
MODEL_REVISION = "1110a243fdf4706b3f48f1d95db1a4f5529b4d41"
ARTIFACTS = {
    "model.onnx": ("onnx/model.onnx", "6fd5d72fe4589f189f8ebc006442dbb529bb7ce38f8082112682524616046452"),
    "tokenizer.json": ("tokenizer.json", "be50c3628f2bf5bb5e3a7f17b1f74611b2561a3a27eeab05e5aa30f411572037"),
}
ALGORITHM_VERSION = "minilm_fp32_prototype_abstention_v1"
MAX_TOKENS = 128
POLICY = {"min_similarity": 0.32, "min_margin": 0.025, "max_tokens": MAX_TOKENS, "score_kind": "cosine_similarity_not_probability"}
TAXONOMY_PATH = Path(__file__).with_name("hierarchy_taxonomy_v1.json")


def canonical_json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def digest(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def load_taxonomy():
    data = json.loads(TAXONOMY_PATH.read_text())
    ids = [s["id"] for s in data["subjects"]]
    if len(ids) != len(set(ids)) or "unclassified" not in ids:
        raise ValueError("Subject IDs must be unique and include unclassified")
    for subject in data["subjects"]:
        if subject["theme"] not in data["themes"]:
            raise ValueError("Dangling subject-to-theme reference")
    return data


def manifest():
    taxonomy = load_taxonomy()
    body = {
        "model_repo": MODEL_REPO, "model_revision": MODEL_REVISION,
        "artifacts": ARTIFACTS, "algorithm_version": ALGORITHM_VERSION,
        "taxonomy_version": taxonomy["version"], "taxonomy_sha256": digest(canonical_json(taxonomy)),
        "policy": POLICY, "input": "canonical_event_title_utf8_v1",
    }
    return {"model_id": "hmodel_" + digest(canonical_json(body))[:24], **body}


def ensure_artifacts(directory=None):
    import requests
    root = Path(directory or os.environ.get("EVENTRISK_MODEL_CACHE", "/tmp/eventrisk-minilm-pinned"))
    root.mkdir(parents=True, exist_ok=True)
    for name, (remote, expected) in ARTIFACTS.items():
        path = root / name
        if path.exists() and hashlib.sha256(path.read_bytes()).hexdigest() == expected:
            continue
        response = requests.get(f"https://huggingface.co/{MODEL_REPO}/resolve/{MODEL_REVISION}/{remote}", timeout=120)
        response.raise_for_status()
        if hashlib.sha256(response.content).hexdigest() != expected:
            raise RuntimeError(f"Pinned model artifact checksum mismatch: {name}")
        temporary = root / (name + ".download")
        temporary.write_bytes(response.content)
        temporary.replace(path)
    return root


class SemanticClassifier:
    def __init__(self, directory=None):
        import numpy as np
        import onnxruntime as ort
        from tokenizers import Tokenizer

        self.np = np
        root = ensure_artifacts(directory)
        self.tokenizer = Tokenizer.from_file(str(root / "tokenizer.json"))
        self.tokenizer.enable_truncation(max_length=MAX_TOKENS)
        self.tokenizer.enable_padding()
        options = ort.SessionOptions()
        options.intra_op_num_threads = max(1, min(2, int(os.environ.get("EVENTRISK_MODEL_THREADS", "2"))))
        options.inter_op_num_threads = 1
        options.execution_mode = ort.ExecutionMode.ORT_SEQUENTIAL
        self.session = ort.InferenceSession(str(root / "model.onnx"), sess_options=options, providers=["CPUExecutionProvider"])
        self.taxonomy = load_taxonomy()
        self.subjects = [s for s in self.taxonomy["subjects"] if s["id"] != "unclassified"]
        anchors = [a for s in self.subjects for a in s["anchors"]]
        self.anchor_offsets = []
        offset = 0
        for subject in self.subjects:
            self.anchor_offsets.append((offset, offset + len(subject["anchors"])))
            offset += len(subject["anchors"])
        self.anchors = self.encode(anchors)

    def encode(self, texts, batch_size=64):
        np = self.np
        chunks = []
        names = {v.name for v in self.session.get_inputs()}
        # Length bucketing reduces padding work for mixed short/long source titles.
        # Restore original row order before scoring; no input or policy is changed.
        order = sorted(range(len(texts)), key=lambda i: (len(texts[i]), i))
        ordered = [texts[i] for i in order]
        for begin in range(0, len(ordered), batch_size):
            encoded = self.tokenizer.encode_batch(ordered[begin:begin + batch_size])
            feed = {
                "input_ids": np.asarray([e.ids for e in encoded], dtype=np.int64),
                "attention_mask": np.asarray([e.attention_mask for e in encoded], dtype=np.int64),
                "token_type_ids": np.asarray([e.type_ids for e in encoded], dtype=np.int64),
            }
            outputs = self.session.run(None, {k: v for k, v in feed.items() if k in names})
            tokens = outputs[0]
            mask = feed["attention_mask"][..., None].astype(np.float32)
            pooled = (tokens * mask).sum(axis=1) / np.maximum(mask.sum(axis=1), 1e-9)
            chunks.append(pooled / np.maximum(np.linalg.norm(pooled, axis=1, keepdims=True), 1e-9))
        return np.concatenate(chunks)[np.argsort(order)] if chunks else np.empty((0, 384), dtype=np.float32)

    def predict(self, texts, batch_size=64, full_scores=False):
        np = self.np
        embeddings = self.encode(texts, batch_size)
        similarities = embeddings @ self.anchors.T
        scores = np.stack([similarities[:, a:b].max(axis=1) for a, b in self.anchor_offsets], axis=1)
        results = []
        for text, row in zip(texts, scores):
            # Fixed rounding and label-ID tie break. Model/revision/policy changes create a new ID.
            ranked = sorted(((round(float(score), 6), s["id"]) for score, s in zip(row, self.subjects)), key=lambda p: (-p[0], p[1]))
            top, runner_up = ranked[:2]
            margin = round(top[0] - runner_up[0], 6)
            reason = None
            if not text or len(text.strip()) < 8:
                reason = "insufficient_title_context"
            elif top[0] < POLICY["min_similarity"]:
                reason = "low_similarity"
            elif margin < POLICY["min_margin"]:
                reason = "ambiguous_subject"
            primary = top[1] if reason is None else "unclassified"
            parent = next(s["theme"] for s in self.taxonomy["subjects"] if s["id"] == primary)
            results.append({
                "subject_id": primary, "theme_id": parent, "candidate_subject_id": top[1],
                "similarity": top[0], "margin": margin, "abstained": reason is not None,
                "reason": reason or "prototype_similarity_thresholds_passed",
                "score_kind": POLICY["score_kind"],
                "top_candidates": [{"subject_id": sid, "similarity": score} for score, sid in ranked[:5]],
            })
            if full_scores:
                results[-1]["all_scores"] = {sid: score for score, sid in ranked}
        return results
