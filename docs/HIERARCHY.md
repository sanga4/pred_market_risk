# Versioned semantic hierarchy

The database now persists **event → semantic subject → governed theme**. The old
explorer used event families and regex categories as presentation aliases. Families
still group related question horizons; they are separate from semantic topics.

This is a provisional, auditable neural baseline. The engineering controls and
evaluation workflow do not certify prediction quality for investment decisions.
Independent production gold is absent, so institutional promotion is blocked.

## Mapping method

1. Read the canonical event's UTF-8 title, without changing its identity, contracts,
   resolution rules, observations or existing family. Hash the exact input.
2. Encode the title and three definition anchors per known subject using
   `sentence-transformers/all-MiniLM-L6-v2`, immutable revision
   `1110a243fdf4706b3f48f1d95db1a4f5529b4d41`. Verify both artifact SHA256s before use.
   CPU float32 ONNX, 128-token truncation, attention-mask mean pooling and L2
   normalization produce a 384-dimensional vector.
3. Each subject's score is its maximum anchor cosine similarity. Sort rounded
   scores to six decimals with a subject-ID tie break. Accept the top subject when
   score ≥ 0.32 and its lead over the runner-up ≥ 0.025. Titles shorter than eight
   characters, weak matches and ambiguous matches explicitly abstain to
   `unclassified`. Keep the top five candidates and reasons. A similarity is not
   a probability. This is prototype-based zero-shot inference, not a fine-tuned
   classifier or calibrated confidence model.
4. Look up that subject's single governed theme in taxonomy
   `eventrisk_semantic_topics_v1`: 53 known subjects plus `unclassified`, 18 themes
   including the unknown bucket. The catalog and model manifest are immutable.
5. Cache by `(model_id, input_hash)`. Atomically publish one current primary path
   per event. Preserve every replaced mapping with `[valid_from, valid_to)`
   processing-time intervals. Old knowledge is never backdated.

Example: a Federal Reserve rate-cut question can map to `central_bank_rates`
(Central bank policy rates), which belongs to `rates` (Interest rates and central
banks). “Fed decisions” as an event family is still returned separately. Named
entity mentions and security exposures are distinct relationships.

One primary subject is a deliberate first-release contract. Alternative candidates
are evidence for review, not additional accepted labels or security exposures.
Title-only inference can miss context in resolution text, shorthand team names,
multilingual inputs and events outside the catalog. Abstention does not prove a
question is irrelevant. No outcome probabilities are averaged by this pipeline.

## Storage and consistency

| Object | Grain / purpose |
| --- | --- |
| `er_hierarchy_taxonomies` | One immutable catalog definition and hash per version |
| `er_hierarchy_subjects`, `er_hierarchy_themes` | Versioned controlled vocabularies |
| `er_hierarchy_subject_theme_mappings` | One governed parent per subject per version |
| `er_hierarchy_models` | Manifest, pinned artifacts, policy and validation state |
| `er_hierarchy_prediction_cache` | Immutable output per model/input; candidates, scores and reason |
| `er_hierarchy_event_subject_mappings` | Event assignment versions, provenance, input hash and validity |
| `er_hierarchy_current` | Current event→subject→theme paths with model validation status |
| `er_hierarchy_overrides` | Reviewer, rationale and input-bound validity for explicit overrides |
| `er_hierarchy_runs` | Operation, bounded partitions, runtime and changed-row counts |
| `er_hierarchy_evaluation_sets`, `er_hierarchy_labels` | Frozen label datasets, group/split, reviewer evidence |
| `er_hierarchy_evaluations` | Append-only metric and prediction snapshots |
| `er_hierarchy_model_approvals` | Accountable approval tied to a passing independent evaluation |

An independent checksum-guarded hierarchy migration ledger preserves the existing
59-step core migration chain. Old pinned ingestion/QA workers remain compatible.
No destructive core migration is required. The public API never initializes schema,
downloads weights, runs inference or writes usage records.

Inference is bounded and resumable, with disjoint input-hash partitions and bulk
cache writes. Projection uses a lease and a single transaction. A crash rolls back
the assignment change; a retry reuses cached predictions. Leases expire after 15
minutes; configured Flight runtime is at most 10 minutes. Normal completion releases
the lease immediately. Operators should not overlap different partition layouts.

An unchanged input/model uses its persisted output. A title or model change creates
a new version. An override survives replay for the reviewed input hash, but cannot
silently follow a changed title. A stale input is counted in quality metrics and
excluded from discovery's current semantic join until projection catches up.
Pending events have an explicit `pending_inference` path; abstained events have a
different status. Neither is silently presented as a confident classification.

## Evaluation and release policy

`hierarchy_challenge_v1.json` has 114 assistant-authored examples: two per known
subject and eight unknown/adversarial cases. Anchors and thresholds were frozen
before this challenge was scored. It is not blind, independent, random production,
temporally held out or production weighted. Never call its scores production recall.

Measured frozen baseline (see `reports/hierarchy_challenge_v1.json`):

| Metric | Value |
| --- | ---: |
| Subject accuracy / micro precision / micro recall | 83.33% |
| Subject macro precision | 91.67% |
| Subject macro recall | 84.49% |
| Subject macro F1 | 85.81% |
| Theme accuracy | 85.96% |
| Abstention | 13.16% |
| Subject/theme repeat agreement, batch 64 versus 1 | 100% on 114 cases |
| Maximum cosine delta in that repeat | 0.0 at recorded precision |
| Independent production precision / recall | Unavailable |

The report also includes confusion matrices, per-class precision/recall/F1/support,
selective precision/recall, unknown false-accept/detection rates, top-three recall,
per-class ranking AP/AUC, hierarchical path precision/recall and group bootstrap
95% intervals. Single-label micro metrics equal accuracy and include abstentions
as unknown predictions. Macro averages use supported truth classes and zero for
undefined class precision. Bootstrap intervals describe this particular sample;
two authored examples per subject do not support precise production inference.
Brier score, log loss and ECE remain null because cosine scores are uncalibrated.

Float32 was selected after the quantized candidate showed larger score variation
across batch sizes. The repeat check does not establish invariance to all hardware,
dependency versions, paraphrases or future source drift. CI verifies pinned weights,
frozen challenge decisions and batch-size label agreement. Production metrics expose
coverage, pending inputs, staleness, distribution, interval/orphan/duplicate integrity,
recent operation runs and persisted evaluations separately from accuracy.

The initial configurable gate requires independently reviewed production labels,
at least 500 test examples, ten per known subject, micro precision ≥95%, macro recall
≥85%, repeat agreement 100%, and explicit accountable approval. These are project
release targets, not a universal institutional standard. The authored baseline fails
the gate and is intentionally marked `provisional_not_independently_validated`.
Calibration, out-of-time validation, source/language slices, stress testing, source
licensing, security exposure research and model-risk sign-off still require further
evidence before a claim of institutional fitness.

## Human review and model governance

Run controlled tooling with `DB` explicitly set to the intended warehouse:

```bash
python institutional_hierarchy_admin.py export-review --count 1000 --seed review-v1 --output review.json
python institutional_hierarchy_admin.py import-review completed-review.json
python institutional_hierarchy_admin.py evaluate-gold DATASET_ID
python institutional_hierarchy_admin.py approve EVALUATION_ID --reviewer analyst --notes 'Validation rationale'
```

The blinded export samples active event families deterministically, one event per
family, and freezes exact text, IDs, hashes, model, catalog and group-disjoint 80/20
test/development splits. Predictions are omitted. A read-key holder can download a
pending sample through `/v1/hierarchy/review-datasets/{id}`. Existing samples cannot
be replaced with easier cases. A fresh seed/sample is needed for a changed model.
Complete every sampled case with independent reviewers A/B, their separate labels,
an agreed/adjudicated subject and notes. Disagreements need a third distinct reviewer.
Set `independent_review_attestation` to true only after actual independent review.
Import rejects altered inputs, splits, missing/duplicated examples and absent
reviewer evidence. Reviewed labels become immutable. Only test labels enter the
production-gold evaluation; development labels must not enter reported test metrics.
Human review cannot be manufactured by rerunning a model or copying its prediction.

To correct an individual current mapping, use `override EVENT_ID SUBJECT_ID
--input-hash HASH --reviewer NAME --reason EVIDENCE`, then run projection. This is
an operational correction, not an independent evaluation label. No public mutation
endpoint is exposed.

## Operation and rollout

```bash
pip install -r requirements-semantic.txt
python institutional_hierarchy_worker.py init
python institutional_hierarchy_worker.py infer --partition-count 4 --partition-id 0 --max-rows 250000
python institutional_hierarchy_worker.py project
python institutional_hierarchy_evaluate.py --persist
```

`ops/hierarchy_flight.py` is the allowlisted MotherDuck entrypoint. Pin a full Git
SHA, target database, operation and bounded row limit. Initial deployment seeds and
validates 1,000 active production events in staging without changing production
source data. Then initialize the additive production namespace, backfill partitions,
publish paths, persist measured evaluation, export a blinded review sample and run
read-only warehouse/API acceptance before switching the Railway API commit.

Incremental scheduled cycles infer only unseen model/input pairs, then project.
The UI's metrics, queue, raw API inspector and provenance expose remaining gaps.
To roll back the serving release, pin Railway to its previous commit. To stop the
hierarchy writer, disable its Flight schedule. Preserve hierarchy tables/history;
rollback never deletes source data or rewrites prior model knowledge.

Read-only routes use the existing key and read scope: `/v1/hierarchy/metrics`,
`/taxonomy`, `/events/{id}?at=...`, `/review`, `/review-datasets` and
`/review-datasets/{id}`. `at` queries hierarchy as known at processing time; source
observation point-in-time research remains on the existing historical API.
