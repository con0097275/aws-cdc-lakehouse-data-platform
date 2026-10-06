"""The DQ check catalogue, by layer — and the publish gate that makes a BLOCKER mean something.

DRP5. Two halves.

**The catalogue** says which checks each layer owes, derived from the CDC registry so a new
table inherits them rather than needing a hand-written rules entry. That is the direct
remedy for what DRP0 measured: `governance/dq/rules.yml` named seven datasets of which one
existed, because it was maintained by hand while the platform became config-driven.

**The gate** fixes an ordering defect that is invisible until it bites:

    transform -> COMMIT -> DQ -> reconciliation -> certification -> successful watermark

The commit happens FIRST and is not undone. A blocker after a successful commit therefore
leaves a physical snapshot that exists and must not be read: the gate marks it invalid and
refuses to advance the watermark. Deleting it instead would destroy the evidence anyone needs
to work out what went wrong, and advancing the watermark would let the next run treat a
corrupt partition as its starting point.

WHY FOUR SEVERITIES
-------------------
    BLOCKER  do not certify AND do not advance the watermark -- unusable
    ERROR    do not certify; the watermark may advance -- correctable later
    WARN     recorded
    INFO     observability

At 2 a.m. the difference between ERROR and BLOCKER is the difference between "this number is
not certified" and "nothing downstream may read this partition". Conflating them either
strands a recoverable day or lets a corrupt one through.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

from .assets import AssetId, AssetKind
from .config_loader import load_config
from .contracts import CheckType, Severity
from .models import ConfigError
from .quality import ResultStatus, summarise

ROOT = Path(__file__).resolve().parents[1]


class Layer(str, Enum):
    SOURCE = "source"
    KAFKA = "kafka"
    FULL_CDC = "full_cdc"
    REALTIME_LATEST_STATE = "realtime_latest_state"
    REALTIME_EVENT_WINDOW = "realtime_event_window"
    EOD = "eod"
    CURATED = "curated"
    MART = "mart"
    SERVING = "serving"


@dataclass(frozen=True)
class CheckSpec:
    key: str
    check_type: CheckType
    severity: Severity
    rule: str
    #: Why this severity and not the one above or below it. A severity with no argument is a
    #: severity nobody can defend when it fires at 2 a.m.
    reason: str

    def payload(self) -> dict:
        return {"key": self.key, "check_type": self.check_type.value,
                "severity": self.severity.value, "rule": self.rule, "reason": self.reason}


def _c(key, ct, sev, rule, reason):
    return CheckSpec(key, ct, sev, rule, reason)


#: What each layer owes. Derived per table by `checks_for()`, never re-typed per dataset.
LAYER_CHECKS: dict = {
    Layer.SOURCE: (
        _c("source_schema_declared", CheckType.SCHEMA, Severity.BLOCKER,
           "the captured columns match the declared contract",
           "an undeclared column change silently reshapes every layer below"),
        _c("source_primary_key", CheckType.BUSINESS_KEY, Severity.BLOCKER,
           "a primary key is declared, or `primary_key_unsupported` is explicit",
           "without a key nothing downstream can deduplicate, and the shrug has to be explicit"),
        _c("source_cdc_enabled", CheckType.CUSTOM, Severity.BLOCKER,
           "supplemental logging / CDC capture is on for this table",
           "the connector reports RUNNING and the topic stays at offset 0"),
    ),
    Layer.KAFKA: (
        _c("decode_success", CheckType.CUSTOM, Severity.BLOCKER,
           "every consumed record decodes against the registry schema",
           "an undecodable record that slips the filter reaches FULL_CDC as a null row"),
        _c("operation_is_known", CheckType.ACCEPTED_VALUES, Severity.BLOCKER,
           "op in (c, u, d, r)", "an unknown op is applied by no layer and dropped by all"),
        _c("canonical_key_present", CheckType.NULLABILITY, Severity.BLOCKER,
           "the message key is the canonical PK",
           "same PK on two partitions breaks per-key ordering, silently (CLAUDE.md 5.1)"),
        _c("kafka_metadata_present", CheckType.NULLABILITY, Severity.ERROR,
           "topic, partition and offset are populated",
           "recoverable by replay; not a reason to stop the world"),
        _c("schema_compatibility", CheckType.SCHEMA, Severity.BLOCKER,
           "the registry accepts the payload under BACKWARD compatibility",
           "an incompatible schema is a producer defect, not a data defect"),
        _c("quarantine_rate", CheckType.VOLUME, Severity.WARN,
           "quarantined share of the window is within tolerance",
           "a rising rate is a signal; a single poison record is not an outage"),
    ),
    Layer.FULL_CDC: (
        _c("dv_event_id_unique", CheckType.UNIQUENESS, Severity.BLOCKER,
           "dv_event_id is unique",
           "it is the idempotency key: a duplicate means replay is no longer safe"),
        _c("dv_src_event_id_present", CheckType.NULLABILITY, Severity.ERROR,
           "dv_src_event_id is populated", "needed to trace back to the source event"),
        _c("pk_completeness", CheckType.NULLABILITY, Severity.BLOCKER,
           "the primary key columns are non-null",
           "a null key collapses rows in every downstream join"),
        _c("event_date_not_null", CheckType.NULLABILITY, Severity.BLOCKER,
           "event_date is non-null",
           "a null partition column is the signature of a record that slipped quarantine"),
        _c("transport_duplicate_rate", CheckType.VOLUME, Severity.WARN,
           "at-least-once redelivery is within tolerance",
           "expected under at-least-once; only a spike is informative"),
        _c("source_position_available", CheckType.NULLABILITY, Severity.ERROR,
           "SCN / LSN is populated",
           "without it event_order falls back to timestamps and ties become guesses"),
        _c("freshness", CheckType.FRESHNESS, Severity.ERROR,
           "max(event time) within the table's SLA",
           "late is not corrupt -- the rows are right, they just have not all arrived, and holding the watermark would strand a day that will complete on its own"),
        _c("event_count_reconciliation", CheckType.RECONCILIATION, Severity.ERROR,
           "counts agree with the source window where the comparison is valid",
           "a break needs explaining before certification, not before the watermark"),
    ),
    Layer.REALTIME_LATEST_STATE: (
        _c("one_row_per_business_key", CheckType.UNIQUENESS, Severity.BLOCKER,
           "exactly one row per dv_pk_hash",
           "the entire contract of latest_state; two rows means a consumer reads either"),
        _c("source_order_monotonic", CheckType.CUSTOM, Severity.BLOCKER,
           "the stored order key never moves backwards",
           "a late out-of-order event would overwrite newer state"),
        _c("tombstones_preserved", CheckType.CUSTOM, Severity.BLOCKER,
           "deleted keys remain as tombstones",
           "removing the tombstone lets a late event resurrect a deleted key"),
        _c("cursor_continuity", CheckType.CUSTOM, Severity.ERROR,
           "the recorded snapshot cursor is an ancestor of the current snapshot",
           "a stale cursor is wrong SILENTLY; the fallback is a full scan, not a failure"),
        _c("freshness", CheckType.FRESHNESS, Severity.ERROR,
           "overlay lag within the SLA", "a stale overlay is stale, not corrupt"),
    ),
    Layer.REALTIME_EVENT_WINDOW: (
        _c("event_uniqueness", CheckType.UNIQUENESS, Severity.BLOCKER,
           "dv_event_id is unique within the window",
           "append + a replayed range must be a no-op, not a duplicate"),
        _c("cursor_coverage", CheckType.CUSTOM, Severity.ERROR,
           "the appended range is contiguous with the previous one",
           "a gap is recoverable by a window scan"),
        _c("retention_extent", CheckType.VOLUME, Severity.WARN,
           "physical extent equals the materialised window",
           "a longer extent keeps data the layer never promised"),
        _c("freshness", CheckType.FRESHNESS, Severity.ERROR, "window lag within the SLA",
           "the overlay is bounded by design; being behind is its normal state near the edge"),
    ),
    Layer.EOD: (
        _c("one_active_row_per_key", CheckType.UNIQUENESS, Severity.BLOCKER,
           "one active row per key for the COB",
           "the EOD invariant. Two active rows means every downstream join multiplies, and the mart is wrong in a direction nobody notices"),
        _c("source_native_ordering", CheckType.CUSTOM, Severity.BLOCKER,
           "the last event per key is chosen by SCN/LSN, not by arrival",
           "arrival order makes two runs disagree about which version is real"),
        _c("delete_semantics", CheckType.ACCEPTED_VALUES, Severity.BLOCKER,
           "the table's declared delete policy is applied",
           "CLAUDE.md 5.7 -- an ambiguous tombstone is never acceptable"),
        _c("cob_cutoff_respected", CheckType.CUSTOM, Severity.BLOCKER,
           "every row satisfies source_commit_ts < cutoff",
           "without it the close is not reproducible, which is what certified means"),
        _c("source_readiness", CheckType.CUSTOM, Severity.BLOCKER,
           "FULL_CDC is current past the cutoff",
           "closing early certifies a day that is still arriving"),
        _c("freshness", CheckType.FRESHNESS, Severity.ERROR, "the close ran within its SLA",
           "a late close is a scheduling problem; the figure it produces is still correct"),
        _c("full_cdc_to_eod_reconciliation", CheckType.RECONCILIATION, Severity.ERROR,
           "key counts agree with FULL_CDC as of the cutoff",
           "a break blocks certification and is explainable without discarding the snapshot"),
        _c("rerun_determinism", CheckType.CUSTOM, Severity.ERROR,
           "a rerun for the same COB produces the same key set",
           "non-determinism means the certified number depends on when it was run"),
    ),
    Layer.CURATED: (
        _c("not_null", CheckType.NULLABILITY, Severity.ERROR, "declared non-null columns hold",
           "dbt test; a mart is not certified on it but the rows are usable"),
        _c("unique", CheckType.UNIQUENESS, Severity.BLOCKER, "the declared grain is unique",
           "a duplicated grain multiplies every measure that joins to it"),
        _c("relationships", CheckType.REFERENTIAL_INTEGRITY, Severity.ERROR,
           "foreign keys resolve", "an unresolved key becomes UNKNOWN_SK, not a lost row"),
        _c("accepted_values", CheckType.ACCEPTED_VALUES, Severity.ERROR,
           "coded columns hold declared values", "a new code is a contract change to review"),
        _c("grain", CheckType.BUSINESS_KEY, Severity.BLOCKER,
           "one row per declared grain", "the definition of the table"),
        _c("freshness", CheckType.FRESHNESS, Severity.ERROR, "within SLA",
           "a stale mart is stale, not incorrect; BI shows the COB it was built for"),
    ),
    Layer.MART: (
        _c("not_null", CheckType.NULLABILITY, Severity.ERROR, "declared non-null columns hold",
           "a null in a declared-non-null measure makes an aggregate silently smaller rather than absent, so it blocks certification but not the rows"),
        _c("unique", CheckType.UNIQUENESS, Severity.BLOCKER, "the declared grain is unique",
           "a duplicated grain multiplies every measure"),
        _c("relationships", CheckType.REFERENTIAL_INTEGRITY, Severity.ERROR,
           "dimension keys resolve", "unresolved becomes UNKNOWN_SK"),
        _c("business_rules", CheckType.CUSTOM, Severity.ERROR,
           "declared business predicates hold",
           "a business-rule break is a number to explain, not a partition to quarantine"),
        _c("volume", CheckType.VOLUME, Severity.ERROR, "row count within bounds",
           "an empty mart that passes every other check is the classic silent failure"),
        _c("eod_to_mart_reconciliation", CheckType.RECONCILIATION, Severity.ERROR,
           "measures agree with EOD within tolerance",
           "a mart that disagrees with the layer it was built from is a number to explain before anyone relies on it, not a partition to quarantine"),
        _c("freshness", CheckType.FRESHNESS, Severity.ERROR, "within SLA",
           "a stale mart is stale, not incorrect; BI shows the COB it was built for"),
    ),
    Layer.SERVING: (
        _c("certified_upstream", CheckType.CUSTOM, Severity.BLOCKER,
           "every upstream mart is certified for this COB",
           "serving an uncertified number to BI is the failure the ladder exists to prevent"),
        _c("freshness_sla", CheckType.FRESHNESS, Severity.ERROR, "within the published SLO",
           "the promise a consumer relies on"),
        _c("semantic_measurable", CheckType.CUSTOM, Severity.WARN,
           "declared semantic checks that can actually be measured hold",
           "a semantic claim nobody can measure is documentation, not a check"),
    ),
}

#: Which layer a governed asset belongs to. `realtime` splits by SHAPE, because the two
#: shapes have genuinely different invariants -- one row per key versus every event.
KIND_TO_LAYER = {
    AssetKind.SOURCE_TABLE: Layer.SOURCE,
    AssetKind.KAFKA_TOPIC: Layer.KAFKA,
    AssetKind.FULL_CDC: Layer.FULL_CDC,
    AssetKind.EOD: Layer.EOD,
    AssetKind.CURATED: Layer.CURATED,
    AssetKind.MART: Layer.MART,
    AssetKind.SERVING_VIEW: Layer.SERVING,
}


def layer_for(asset: AssetId, *, shape: str = "") -> Layer:
    if asset.kind is AssetKind.REALTIME:
        if shape == "latest_state":
            return Layer.REALTIME_LATEST_STATE
        if shape == "event_window":
            return Layer.REALTIME_EVENT_WINDOW
        raise ConfigError(
            f"{asset}: REALTIME needs its `shape` to choose a check set. `latest_state` owes "
            f"one row per key; `event_window` owes event uniqueness. They are not the same "
            f"table with a different setting.")
    try:
        return KIND_TO_LAYER[asset.kind]
    except KeyError:
        raise ConfigError(f"{asset}: no DQ layer for kind {asset.kind.value}") from None


def checks_for(asset: AssetId, *, shape: str = "") -> tuple:
    return LAYER_CHECKS[layer_for(asset, shape=shape)]


def compile_catalogue(registry: Path | None = None) -> dict:
    """asset id -> its check specs, DERIVED from the CDC registry.

    A new table onboarded through the registry gets its checks automatically. That is the
    whole point: `governance/dq/rules.yml` had to be edited by hand and therefore was not.
    """
    from .assets import assets_for_table

    cfg = load_config(registry or (ROOT / "cdc" / "registry" / "sources.yaml"))
    out: dict = {}
    for table in cfg.tables:
        shape = getattr(table.realtime, "shape", "") or ""
        for asset in assets_for_table(table):
            if asset.kind is AssetKind.REALTIME and not table.realtime.enabled:
                continue
            out[str(asset)] = checks_for(asset, shape=shape)
    return out


# --------------------------------------------------------------------------- #
# The publish gate
# --------------------------------------------------------------------------- #

class GateOutcome(str, Enum):
    CERTIFIED = "CERTIFIED"
    PUBLISHED_UNCERTIFIED = "PUBLISHED_UNCERTIFIED"
    INVALID = "INVALID"


@dataclass(frozen=True)
class GateDecision:
    outcome: GateOutcome
    advance_watermark: bool
    certify: bool
    mark_snapshot_invalid: bool
    reasons: tuple = field(default_factory=tuple)
    dq: dict = field(default_factory=dict)
    recon: dict = field(default_factory=dict)

    def payload(self) -> dict:
        return {"outcome": self.outcome.value, "advance_watermark": self.advance_watermark,
                "certify": self.certify,
                "mark_snapshot_invalid": self.mark_snapshot_invalid,
                "reasons": list(self.reasons), "dq": self.dq, "recon": self.recon}


def evaluate_publish(*, committed: bool, dq_results=(), recon_results=()) -> GateDecision:
    """The order is the design: commit, THEN judge.

    A commit that already happened cannot be undone by a failed check, so the gate's job is
    to decide what the world is allowed to believe about it:

        BLOCKER  -> snapshot marked INVALID, watermark held, not certified
        ERROR    -> published, watermark advances, NOT certified
        clean    -> certified and the watermark advances
    """
    if not committed:
        return GateDecision(GateOutcome.INVALID, False, False, False,
                            ("the transform did not commit; there is nothing to judge",))

    dq, recon = list(dq_results), list(recon_results)
    reasons: list = []

    blocking_wm = [r for r in (*dq, *recon) if getattr(r, "blocks_watermark", False)]
    blocking_pub = [r for r in (*dq, *recon) if r.blocks_publish]

    if not dq:
        # An empty suite is not a clean one. Same rule as `suite_blocks_publish`, one level up.
        reasons.append("no DQ check produced a verdict; an unevaluated suite is not a clean one")

    if blocking_wm:
        reasons += [f"BLOCKER {getattr(r, 'check_id', getattr(r, 'metric', '?'))}"
                    for r in blocking_wm]
        return GateDecision(GateOutcome.INVALID, False, False, True, tuple(reasons),
                            summarise(dq), summarise(recon))

    if blocking_pub or not dq:
        reasons += [f"blocking {getattr(r, 'check_id', getattr(r, 'metric', '?'))}"
                    for r in blocking_pub]
        # The rows are there and a later correction can supersede them, so the watermark
        # advances. Holding it would strand a day that is recoverable.
        return GateDecision(GateOutcome.PUBLISHED_UNCERTIFIED, True, False, False,
                            tuple(reasons), summarise(dq), summarise(recon))

    if not recon:
        return GateDecision(GateOutcome.PUBLISHED_UNCERTIFIED, True, False, False,
                            ("no reconciliation result; an absent comparison is not a passed one",),
                            summarise(dq), summarise(recon))

    return GateDecision(GateOutcome.CERTIFIED, True, True, False, (),
                        summarise(dq), summarise(recon))


def assertion_mcps(client, asset: AssetId, specs, *, results=()) -> list:
    """Publish the contract's checks as DataHub assertions, and their outcomes as runs.

    OPS stays authoritative: these carry the check identity and the verdict, never the
    evidence. `dq_run_id` points back at the ledger row that holds it.
    """
    from .datahub_client import MetadataChangeProposal

    urn = client.minter.dataset_urn(asset)
    out = [MetadataChangeProposal(
        urn, "assertionInfo",
        {"type": s.check_type.value, "description": s.rule, "severity": s.severity.value,
         "scope": "DATASET", "key": s.key}) for s in specs]
    by_status = {ResultStatus.PASS: "SUCCESS", ResultStatus.WARN: "SUCCESS"}
    for r in results:
        out.append(MetadataChangeProposal(
            urn, "assertionRunEvent",
            {"assertionKey": r.check_id, "status": "COMPLETE",
             "result": by_status.get(r.status, "FAILURE"),
             "runId": r.dq_run_id, "nativeResultType": r.status.value}))
    return out
