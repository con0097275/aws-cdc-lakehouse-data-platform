# AI Data Contracts

What every later AI phase compiles against. Authored by **AI-P1**, 2026-08-26.
Implementation: `aiplatform/`. Decisions: ADR-047 … ADR-060.

**Every field below names its consumer.** A field with no consumer is not in the contract —
unused metadata is cost, drift and a false sense of completeness.

---

## 1. Config layout — one system, not several

```
aiplatform/
  __init__.py        why this is not inside ai/ and not inside reporting/
  versioning.py      deterministic, content-addressed version ids
  classification.py  security classification + cost class (both fail closed)
  knowledge.py       KnowledgeSource / KnowledgeDocument / Chunk
  features.py        FeatureDefinition / FeatureGroup + the point-in-time rule
  models.py          TrainingDataset / ModelDefinition
  agents.py          ToolContract / AgentDefinition
  evaluation.py      EvalQuestion / EvaluationDataset
  ops.py             OPS table contracts + DDL rendering
  compile.py         YAML -> schema -> semantics -> canonical -> sha256

  knowledge/*.yaml   authored config
  features/*.yaml
  agents/*.yaml
  models/*.yaml
  evaluations/*.yaml
```

Pipeline, matching `reporting/compile.py` deliberately:

```
Git YAML  ->  structural validation  ->  semantic validation (dataclass __post_init__)
          ->  canonical JSON  ->  sha256  ->  plan_hash
```

### Why `aiplatform/` and not `ai/`

`ai/` is the assistant, and one of its load-bearing properties is that it is **deletable** —
no pipeline code imports it, and a test asserts that. Putting feature or ops contracts under
`ai/` would force `spark/features/*` to import it and quietly destroy that property.

`ai/` may import `aiplatform`. **`aiplatform` never imports `ai/`**, and a test enforces it.

### Why not extend `reporting/`

`reporting/` is keyed on the five reporting FLOW MODES. A knowledge source and an agent tool
have no flow mode. Bolting them on would either loosen the reporting schema for everyone or
invent a sixth pseudo-mode. The **pattern** is reused; the tree is not.

---

## 2. Metadata ownership — three kinds of state

| | Where | What | Rule |
|---|---|---|---|
| **STATIC CONFIG** | Git (`aiplatform/**.yaml`) | knowledge sources, features, agents, models, eval sets | versioned by content hash; never written at runtime |
| **HOT RUNTIME** | DynamoDB (ADR-036 pattern) | live lifecycle | **AI-P1 declares none** — nothing has one yet, and an empty table is a maintenance cost with no reader |
| **IMMUTABLE AUDIT** | Iceberg `ops.*` | what ran, over what, producing what | append-only |

**High-frequency agent traces do not go in Iceberg.** Per-token timings, prompt bodies and
score vectors go to CloudWatch. A table written per model call is a small-files generator
whose compaction costs more than the telemetry is worth (ADR-058).

---

## 3. Versioning — content hashes, never counters

| Version | Kind | Derived from |
|---|---|---|
| corpus | `corpus:` | source definition / published corpus content |
| chunking | `chunking:` | chunking strategy |
| embedding | `embedding:` | model id + config |
| feature | `feature:` | the feature definition |
| feature group | `feature_group:` | the whole group |
| dataset | `dataset:` | window, label, groups, split |
| model | `model:` | algorithm, seed, dataset version |
| agent | `agent:` | model, prompt, sorted tool versions |
| prompt / tool | `prompt:` / `tool:` | their own content |
| evaluation | `evaluation:` | the question set |

A random or hand-incremented version cannot answer *"is this the same corpus as yesterday?"*,
and a stale counter is worse than none because downstream trusts it. A content hash
re-derives itself and **cannot disagree with the content**.

`generated_at` is excluded from `plan_hash` — otherwise every compile is a new version.

---

## 4. Knowledge contracts

**Source** — `source_id`, `document_type`, `include`/`exclude`, `owner`, `domain`,
`classification`. Rejects any include pattern reaching an excluded path, and rejects
`RESTRICTED` outright.

**Document** — `document_id`, `document_type`, `source_path`, `source_uri`, `git_commit`,
`content_hash`, `owner`, `domain`, `environment`, `classification`, `table_name`,
`database_name`, `architecture_version`, `knowledge_version`, `created_at`, `updated_at`.

`git_commit` must be a hex sha: a document stamped with a commit its content does not match
is worse than one with no commit at all.

**Chunk** — `chunk_id`, `document_id`, `chunk_index`, `content_hash`, `chunking_version`,
`embedding_version` (None until AI-P3), `metadata`.

`chunk_id` is deterministic: same document + same text ⇒ same id.

> A chunk whose classification cannot be derived defaults to **RESTRICTED** and is
> therefore withheld. An unknown sensitivity is a reason to withhold, not to share.

**Secrets never enter the corpus.** Exclusion by path is the primary control; redaction is
the backstop (ADR-048).

---

## 5. Feature contracts

Group: `name`, `entity_keys`, `event_time_column`, `owner`, `domain`, `classification`,
`features[]`, `batch`, `streaming`, `online_store`, `partition_by`.
Feature: `name`, `type`, `description`, `owner`, `version`, `source_lineage`,
`freshness_sla_minutes`.

Offline row shape:

```
entity_id · feature_event_time · feature_version
  · <feature columns> ·
source_cob_date · source_watermark · job_run_id · created_at
```

Rejected at compile time:

- no `event_time_column` — without it there is no PIT boundary and every training set leaks
- `event_time_column` that is a processing-time column
- a processing-time column as a feature
- `partition_by` including an entity key (CLAUDE.md §6: one file per entity per day)
- duplicate feature names
- `streaming_enabled: true` (ADR-053)
- an online store with no offline source
- `RESTRICTED` classification

---

## 6. Point-in-time rule

```
feature_event_time <= label_event_time
```

`<=`, not `<`: a feature stamped exactly at T **was** knowable at T.

`feature_event_time` is EVENT time. `created_at` is PROCESSING time — recorded for audit,
**never** a join key. They diverge under backfill and late arrival, and joining on the wrong
one leaks the future.

Selection order is `(feature_event_time DESC, feature_version DESC)` — the same shape as the
EOD rule in `CLAUDE.md` §5.6, where the tie-breaker exists because two rows *can* share a
timestamp and picking arbitrarily makes a rerun non-reproducible.

### The three mandatory tests

1. **Future feature rejected** — a feature at `T+1` never joins to a label at `T`.
2. **Processing time is never a join key** — `assert_join_key_is_event_time` refuses it.
3. **Label horizon excluded** — for a forward label over `[T, T+30d]`, no feature drawn from
   that window is selected.

Test 3 is the one that catches real leakage. Plain `<=` does **not** catch it: such a
feature can satisfy `feature_event_time <= label_event_time` while being derived from the
outcome window. This is why the ML pilot is a forward-window label and not anomaly
detection — an unsupervised pilot has no horizon and would never exercise the boundary.

---

## 7. Agent tool contracts

`tool_name`, `tool_version`, `description`, `input_schema`, `output_schema`,
`authorization_class`, `read_only`, `timeout_seconds`, `max_rows`, `max_result_bytes`,
`audited`, `cost_class`.

| Authorization class | V1 |
|---|---|
| `public_metadata` — docs, ADRs, runbooks, lineage | allowed |
| `governed_read` — MART/SERVING via the Athena executor | allowed |
| `ops_read` — ops tables, runtime state | allowed |
| `mutating` | **rejected** |
| `shell` | **rejected, permanently** |

Rejected at contract level: `read_only=False`; `audited=False`; an `input_schema` with no
`type`/`properties`; timeouts outside 1–300s; `max_rows` outside 1–10 000; an agent with an
unpinned `model_id`; `session_memory=True`; an agent with no tools.

Forbidden actions, in any version: `terraform_apply`, `terraform_destroy`, `sql_mutation`,
`airflow_rerun`, `watermark_mutation`, `kafka_offset_reset`, `checkpoint_delete`,
`source_db_mutation`, `s3_delete`, `shell_exec`.

> Auditing cannot be disabled. The audit record is **part of the control**, so an
> unauditable action must not proceed. Metrics may fail open; the audit fails closed.

---

## 8. Evaluation contracts

`question_id`, `question`, `expect_mode` (`tool` | `retrieval` | `refusal`),
`expect_source`, `expect_tool`, `expect_contains`, `adversarial`.

- a `tool` question without `expect_tool` is rejected — otherwise a routing regression
  cannot be detected
- a `retrieval` question without `expect_source` is rejected — groundedness means the
  evidence came from the right document, not that the prose reads well
- a dataset with **no adversarial questions** is rejected
- `meets_retrieval_gate()` enforces ≥50 questions before any retrieval change is judged

Routing is scored **separately** from accuracy: a free lookup silently becoming a paid call
is a cost regression no accuracy metric would notice.

---

## 9. OPS contracts

| Table | Consumer | Created |
|---|---|---|
| `ops.ai_knowledge_sync` | AI-P2 staleness; AI-P4 joins a score to its corpus | **yes** |
| `ops.ai_eval_run` | AI-P4/P10 regression detection; AI-P16 acceptance | **yes** |
| `ops.ai_agent_execution_hist` | AI-P11 security review; AI-P14/15 no-mutation proof | **yes** |
| `ops.model_training_run` | AI-P6 lineage; `get_model_status`; AI-P16 | **yes** |
| `ops.feature_materialization_run` | — | **deferred** |

`feature_materialization_run` is deliberately **not created**. If feature groups register as
reporting jobs — which is the plan, so ordering, waves, watermarks and rerun idempotency come
from ADR-042/043 rather than being rewritten — then `ops.job_master_execution_hist` already
records every materialisation with the same correlation ids. A second execution-history table
with no distinct purpose is the duplicate-state anti-pattern. **AI-P5 must justify it or
reuse the existing table.**

`ops.ai_agent_execution_hist` stores a **question hash, not the question text**.

DDL is generated from `aiplatform/ops.py` into `spark/common/ddl/ai_ops.sql`, so the
contract and the schema cannot drift.


---

## 10. Corpus publication pipeline (AI-P2)

```
allow-listed registry  ->  per-file exclusion  ->  read + classify
    ->  SECRET SCAN (quarantine whole documents)
    ->  normalize to the document model
    ->  heading-aware chunking (versioned)
    ->  content-addressed corpus_version
    ->  ai/knowledge/<corpus_version>/{documents,chunks,manifest}
```

```bash
make ai-corpus         # report only, writes nothing  (DEFAULT)
make ai-corpus-build   # write ai/knowledge/<corpus_version>/
```

### Allow-list, not walk-and-exclude

A walk-and-exclude design **fails open**: a new directory is indexed until someone
remembers to exclude it. The registry in `ai/knowledge/sources.py` fails closed — a new
directory is invisible until someone decides it is knowledge. Per-file exclusion is the
second control, because a glob can still reach somewhere it should not.

Two defects this design caught during AI-P2, both of which would have been silent:

- `lstrip("./")` strips a **character set**, not a prefix, so `.git/config` became
  `git/config` and slipped past the `.git` root exclusion entirely. Same for `.terraform`
  and `.env`. Now a `removeprefix` loop, with a test per path.
- A bare `secret` path pattern excluded **`docs/adr/ADR-031-secret-store.md`** — an ADR
  operators need — while excluding nothing that held a credential. Now matches
  secret-bearing *files* (`secrets/`, `secrets.yml`, `*.secrets.json`), not the word.

### Quarantine, never redact-and-publish

A redactor that rewrites the match and publishes the rest is a bet that the pattern list is
complete. It is not. A document that trips the scan is **withheld whole**, and the operator
is told which file and which rule. An explicit gap beats a silent leak.

The scanner must also not quarantine documentation *about* secrets — this corpus is full of
security prose. `ALLOWED_CONTEXTS` exempts `${file:…}` / `${ssm:…}` references,
`$(DBZ_PASSWORD)` template variables, `aws ssm get-parameter` commands, `<placeholder>`
forms and already-masked values. Nine detection rules and six exemption cases are tested.

Findings never carry the raw value out of the module — only a masked excerpt.

### Chunking

`chunking:v2-heading-aware`. Sections are delimited by Markdown headings and carry their
heading path; YAML/JSON splits on top-level keys so one dataset's entry stays whole.

- fenced code blocks are **never split** — half a command is not a command
- a `# comment` inside a fence does **not** start a section
- oversized sections split on line boundaries with a 200-char overlap
- deterministic for the same input, asserted by test

The chunking version is part of every `chunk_id`, so changing the strategy changes every id
— which is what makes "re-embed only what changed" decidable in AI-P3 rather than a guess.

### dbt metadata — one document per model, never one blob

`manifest.json` is 688 KB of mostly compiled SQL and internal graph state. As one chunk it
matches every query and answers none; split by characters it yields fragments of JSON.

`ai/knowledge/dbt_meta.py` reads it as structured data and renders one small document per
documented model: description, columns, declared `refs`/`sources`, tests, tags and meta.
Models with neither a description nor documented columns are **skipped** — a stub teaches
nothing the SQL does not already say and only dilutes retrieval.

**Dependency truth is not duplicated.** `refs`/`sources` are recorded as the model's own
declared inputs — what a person asking "what feeds this mart" wants — and the rendered text
says explicitly that the authoritative execution order is the compiled plan, not this list.

### Versioning and reproducibility

`corpus_version` is a content hash over every document's content hash plus the chunking and
architecture versions. Never a timestamp, never a counter. `generated_at` is excluded from
the hash, or every build would be a new version.

A **dirty working tree is recorded** (`git_dirty`), not hidden: stamping a commit the
content does not match is worse than having no commit at all.

Deleted documents need no tombstone — documents are discovered each build, never carried
forward.

### No AWS in this phase

`ai/knowledge/publish.py` constructs no AWS client, and a test asserts `boto3` does not
appear in it. The manifest records the intended `s3_target`; publication is AI-P13, after
the prefix exists in Terraform.


---

## 11. Feature platform (AI-P5)

```
EOD / CURATED / MART  ->  batch materialization  ->  feature_offline (Iceberg)
                                                          |
                                                          +-> point-in-time join -> training set
                                                          +-> online store (optional, OFF)
```

```bash
make ai-features          # validate the registry, print the pilot group + version
make ai-feature-lineage   # resolve feature lineage against the dbt manifest
python3 spark/features/pilot_customer_behavior.py --cob-date <d> --group-version <v> --dry-run
```

### Source policy is enforced, not documented

`resolve_source_layer` permits **EOD / CURATED / MART / SNAPSHOT** for batch and refuses
`FULL_CDC` / `REALTIME`. A batch feature job reading the change stream would make the
feature store a second interpretation of it, and two interpretations of the same events is
precisely what ADR-052 exists to prevent. Streaming features may read REALTIME or FULL_CDC —
and are not built at all (ADR-053).

### Offline row shape

```
entity_id · feature_event_time · feature_version
  · <feature columns> ·
source_cob_date · source_watermark · materialization_run_id · created_at
```

Partitioned by `source_cob_date`, **never** by `entity_id`: `CLAUDE.md` §6 forbids
partitioning on a high-cardinality key, and it would produce one file per entity per day.

`created_at` is processing time — recorded for audit, refused as a join key in code.

### Idempotence

MERGE on **`(entity_id, feature_event_time, feature_version)`**.

`materialization_run_id` is stamped on every row but is deliberately **not** in the merge
key: including it would make every rerun insert new rows and destroy the idempotence the run
id exists to audit.

A version bump produces a **distinct row** rather than overwriting, because point-in-time
reconstruction of an older training set depends on the old values still existing.

### Point-in-time join — one implementation

`spark/features/point_in_time.py`. For each label row at `T`, the last feature with
`feature_event_time <= T`, ordered `(event_time DESC, feature_version DESC)`.

Two copies of a cutoff rule drift, and the drift is silent because both still return rows —
training would then learn on one rule and inference score on another, surfacing as
unexplained production degradation rather than an error.

Three assertions, matching the AI-P1 contracts:

| Check | Catches |
|---|---|
| `assert_event_time_column` | a processing-time column used as the join key |
| `assert_no_future_leakage` | a feature stamped after its label |
| `assert_no_horizon_leakage` | **a feature drawn from a forward label's own outcome window** |

The third is the one that catches real leakage; `feature_event_time <= label_event_time`
does not, because such a feature satisfies the inequality while encoding the answer.

The join is a **left** join: a label with no feature survives with nulls. An inner join
would silently shrink the training set and move the class balance.

### Online store — interface built, backend off

`DisabledOnlineStore` is the default and **raises** rather than returning `None`: a caller
that assumed an online store exists finds out at the call site instead of silently seeing no
features. `InMemoryOnlineStore` is the only implementation the tests exercise.

`DynamoDbOnlineStore` is the approved backend when a consumer appears — $0 idle, and a
pattern ADR-036 already operates here. Its `put` is conditional on `feature_event_time`, so
a late write cannot regress a newer value; out-of-order arrival is normal and silently going
backwards is not. **SageMaker Feature Store is deliberately not the default**; if ever
adopted it goes behind this same interface and nothing above it changes.

### Lineage is a projection, never a second graph

`spark/features/lineage.py` resolves the registry's `source_lineage` against
`dbt/target/manifest.json`. Unresolvable references are **reported** in `unresolved`, not
dropped — an unresolvable claim is a registry defect, and hiding it makes the graph look
complete when it is not.

That reporting immediately earned itself: the pilot's three references resolved as `false`
because the resolver indexed dbt **models** only, while `fact_transaction` and
`fact_account_daily_snapshot` are dbt **sources**. A resolver that knows half the graph
reports the other half as broken. Fixed, with a regression test.
