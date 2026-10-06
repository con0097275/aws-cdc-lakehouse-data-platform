"""OPS metadata contracts for the AI plane.

THREE KINDS OF STATE, DELIBERATELY SEPARATED
--------------------------------------------
  STATIC CONFIG    Git. Knowledge sources, features, agents, models, eval sets.
                   Versioned by content hash; never written at runtime.
  HOT RUNTIME      DynamoDB (the ADR-036 pattern). Only where a later phase needs a
                   live lifecycle. AI-P1 declares NONE -- nothing here has one yet, and
                   an empty table is a maintenance cost with no reader.
  IMMUTABLE AUDIT  Iceberg `ops.*`. Append-only history: what ran, over what, producing what.

WHAT DOES NOT GO IN ICEBERG
---------------------------
High-frequency agent traces -- per-token timings, prompt bodies, retrieval score vectors.
Those belong in CloudWatch. An Iceberg table written per model call is a small-files
generator whose compaction costs more than the telemetry is worth, and `ops.*` is queried
for audit, not for tailing.

EVERY TABLE HERE MUST NAME ITS CONSUMER. `docs/AI_TARGET_ARCHITECTURE.md` lists candidate
tables as EXAMPLES; a table with no reader is not created just because it appeared in a
document.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class OpsTableContract:
    name: str
    purpose: str
    consumer: str                     # who reads it, by name. Mandatory.
    partition_by: str
    columns: tuple[tuple[str, str], ...]
    create: bool = True

    def __post_init__(self) -> None:
        if not self.consumer.strip():
            raise ValueError(
                f"{self.name}: every ops table names its consumer. A table nobody reads is "
                "schema, storage and a migration burden with no benefit")


#: Correlation columns every AI ops table carries, matching the existing reporting
#: convention so an AI run can be joined to the platform run that triggered it.
_AUDIT = (
    ("run_id",        "STRING NOT NULL"),
    ("started_at",    "TIMESTAMP NOT NULL"),
    ("finished_at",   "TIMESTAMP"),
    ("status",        "STRING NOT NULL"),
    ("git_commit",    "STRING"),
    ("created_at",    "TIMESTAMP NOT NULL"),
)

AI_KNOWLEDGE_SYNC = OpsTableContract(
    name="ops.ai_knowledge_sync",
    purpose="Corpus builds: which commit, how many chunks, which embedding model.",
    consumer="AI-P2 staleness check; AI-P4 joins a score to the corpus that produced it.",
    partition_by="sync_date",
    columns=_AUDIT + (
        ("corpus_version",   "STRING NOT NULL"),
        ("chunking_version", "STRING NOT NULL"),
        ("embedding_version","STRING"),
        ("document_count",   "BIGINT NOT NULL"),
        ("chunk_count",      "BIGINT NOT NULL"),
        ("source_breakdown", "MAP<STRING,BIGINT>"),
        ("sync_date",        "DATE NOT NULL"),
    ),
)

AI_EVAL_RUN = OpsTableContract(
    name="ops.ai_eval_run",
    purpose="Evaluation results over time, versioned against what produced them.",
    consumer="AI-P4/AI-P10 regression detection; AI-P16 acceptance.",
    partition_by="eval_date",
    columns=_AUDIT + (
        ("evaluation_version","STRING NOT NULL"),
        ("corpus_version",    "STRING"),
        ("agent_version",     "STRING"),
        ("model_id",          "STRING"),
        ("total_questions",   "BIGINT NOT NULL"),
        ("passed",            "BIGINT NOT NULL"),
        ("routing_correct",   "BIGINT NOT NULL"),   # scored separately, on purpose
        ("refusals_expected", "BIGINT NOT NULL"),
        ("refusals_actual",   "BIGINT NOT NULL"),
        ("cost_usd",          "DECIMAL(12,6)"),
        ("eval_date",         "DATE NOT NULL"),
    ),
)

AI_AGENT_EXECUTION_HIST = OpsTableContract(
    name="ops.ai_agent_execution_hist",
    purpose="Durable audit of what the agent was asked and which tools it reached.",
    consumer="AI-P11 security review; AI-P14/15 proof that no mutation occurred.",
    partition_by="execution_date",
    columns=_AUDIT + (
        ("agent_version",    "STRING NOT NULL"),
        ("question_hash",    "STRING NOT NULL"),   # hash, NOT the question text
        ("route",            "STRING NOT NULL"),
        ("tools_called",     "ARRAY<STRING>"),
        ("athena_query_ids", "ARRAY<STRING>"),
        ("bytes_scanned",    "BIGINT"),
        ("guard_outcome",    "STRING NOT NULL"),
        ("refused",          "BOOLEAN NOT NULL"),
        ("execution_date",   "DATE NOT NULL"),
    ),
)

MODEL_TRAINING_RUN = OpsTableContract(
    name="ops.model_training_run",
    purpose="Training runs: metrics, artifact URI, and the feature versions consumed.",
    consumer="AI-P6 lineage; the agent's get_model_status tool; AI-P16 acceptance.",
    partition_by="training_date",
    columns=_AUDIT + (
        ("model_version",        "STRING NOT NULL"),
        ("dataset_version",      "STRING NOT NULL"),
        ("feature_group_versions","ARRAY<STRING>"),
        ("train_start",          "DATE NOT NULL"),
        ("train_end",            "DATE NOT NULL"),
        ("label_horizon_days",   "INT NOT NULL"),
        ("seed",                 "INT NOT NULL"),
        ("metrics",              "MAP<STRING,DOUBLE>"),
        ("artifact_uri",         "STRING NOT NULL"),
        ("training_date",        "DATE NOT NULL"),
    ),
)

# DELIBERATELY NOT CREATED -----------------------------------------------------
# The architecture lists `ops.feature_materialization_run` as a candidate. If feature
# groups register as reporting jobs -- which is the plan, so ordering, waves, watermarks
# and rerun idempotency come from ADR-042/043 rather than being rewritten -- then
# `ops.job_master_execution_hist` ALREADY records every materialisation, with the same
# columns and the same correlation ids.
#
# A second execution-history table with no distinct purpose is the duplicate-state
# anti-pattern the architecture forbids. AI-P5 decides from evidence: create this only if
# it can name something the existing table cannot record.
FEATURE_MATERIALIZATION_RUN = OpsTableContract(
    name="ops.feature_materialization_run",
    purpose="Feature materialisation runs.",
    consumer="DEFERRED -- ops.job_master_execution_hist already covers this if feature "
             "groups register as reporting jobs. AI-P5 must justify it or reuse.",
    partition_by="materialization_date",
    columns=_AUDIT,
    create=False,
)

#: Only the tables with a named consumer AND create=True.
TABLES_TO_CREATE = tuple(t for t in (
    AI_KNOWLEDGE_SYNC, AI_EVAL_RUN, AI_AGENT_EXECUTION_HIST, MODEL_TRAINING_RUN,
    FEATURE_MATERIALIZATION_RUN) if t.create)

ALL_TABLES = (AI_KNOWLEDGE_SYNC, AI_EVAL_RUN, AI_AGENT_EXECUTION_HIST,
              MODEL_TRAINING_RUN, FEATURE_MATERIALIZATION_RUN)


def render_ddl(t: OpsTableContract, *, catalog: str = "glue_catalog") -> str:
    """Iceberg v2 DDL, matching spark/common/ddl/*.sql conventions."""
    cols = ",\n".join(f"  {n:<26} {ty}" for n, ty in t.columns)
    return (f"-- {t.purpose}\n-- consumer: {t.consumer}\n"
            f"CREATE TABLE IF NOT EXISTS {catalog}.{t.name} (\n{cols}\n)\n"
            f"USING iceberg\nPARTITIONED BY ({t.partition_by})\n"
            "TBLPROPERTIES (\n  'format-version' = '2',\n"
            "  'write.parquet.compression-codec' = 'zstd'\n);")
