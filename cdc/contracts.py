"""The executable data contract — what a PRODUCER promises, expressed as checkable rules.

DRP1. A contract here is producer-oriented on purpose: it states what the job writing the
table guarantees, so a consumer can depend on it and a check can verify it. That makes the
boundary sharp:

    GOVERNANCE METADATA          EXECUTABLE CONTRACT
    owner, description, tags     schema, keys, nullability, uniqueness,
    domain, glossary, PII        accepted values, referential integrity,
    classification, criticality  freshness, volume, custom rules, reconciliation

`GovernanceMetadata` answers "who is responsible and what is this". `DataContract` answers
"what must be TRUE, and how would you find out". They are separate types, and this module
deliberately has no `owner` field -- a test asserts that. The reason is not tidiness:

    An owner is not falsifiable. A uniqueness rule is.

Mixing them produces a contract whose "violations" include things no query can detect,
which trains everyone to ignore the ones that matter. It also produces the inverse failure
the DRP0 audit found -- governance attributes maintained in a file that nothing executes,
drifting for 38 sessions while looking maintained.

CHECK IDENTITY IS DERIVED, NEVER TYPED
---------------------------------------
Every rule renders to a `ContractCheck` whose `check_id` is a deterministic function of the
rule's own content. DQ results are keyed on `check_id` across runs, so a hand-typed id that
someone renames silently starts a NEW check with no history -- the old one stops reporting
and nothing says so. A derived id cannot be renamed without changing the rule it names.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from enum import Enum

from .assets import AssetId
from .models import ConfigError, SchemaEvolutionPolicy


class Severity(str, Enum):
    """Four levels, because DRP5 found there are four distinct BEHAVIOURS, not two.

        BLOCKER  the data is unusable. Do not certify AND do not advance the watermark --
                 the physical snapshot exists and must be marked invalid, or the next run
                 reads from it as though it were good.
        ERROR    do not certify. The watermark may still advance: the rows are there and a
                 later correction can supersede them.
        WARN     recorded, never blocks.
        INFO     observability only; not a judgement about the data at all.

    DRP1 shipped two levels with the note that "a third level would need a third behaviour".
    It does, and DRP5 found it: `BLOCKER` and `ERROR` differ in whether the SUCCESSFUL
    WATERMARK advances, which is the difference between "this number is not certified" and
    "nothing downstream may read this partition".
    """

    BLOCKER = "BLOCKER"
    ERROR = "ERROR"
    WARN = "WARN"
    INFO = "INFO"


#: Severities that stop a certified publish.
BLOCKING_SEVERITIES = (Severity.BLOCKER, Severity.ERROR)

#: Severities that additionally stop the successful watermark from advancing.
WATERMARK_BLOCKING_SEVERITIES = (Severity.BLOCKER,)


class CheckType(str, Enum):
    SCHEMA = "schema"
    BUSINESS_KEY = "business_key"
    NULLABILITY = "nullability"
    UNIQUENESS = "uniqueness"
    ACCEPTED_VALUES = "accepted_values"
    REFERENTIAL_INTEGRITY = "referential_integrity"
    FRESHNESS = "freshness"
    VOLUME = "volume"
    CUSTOM = "custom"
    RECONCILIATION = "reconciliation"


@dataclass(frozen=True, order=True)
class FieldSpec:
    name: str
    type: str
    nullable: bool = True
    description: str = ""     # documentation only; never rendered into a check

    def payload(self) -> dict:
        return {"name": self.name, "type": self.type, "nullable": self.nullable}


@dataclass(frozen=True)
class FreshnessRule:
    timestamp_column: str
    max_lag_minutes: int
    severity: Severity = Severity.ERROR


@dataclass(frozen=True)
class VolumeRule:
    """`min_rows` guards the failure the DQ engine was built around: an empty result that
    looks like a clean pass. `max_pct_change` guards the opposite -- a join that fanned out.
    """

    min_rows: int | None = None
    max_rows: int | None = None
    max_pct_change: float | None = None
    severity: Severity = Severity.ERROR


@dataclass(frozen=True)
class NullabilityRule:
    column: str
    max_null_pct: float = 0.0
    severity: Severity = Severity.ERROR


@dataclass(frozen=True)
class UniquenessRule:
    columns: tuple[str, ...]
    severity: Severity = Severity.ERROR


@dataclass(frozen=True)
class AcceptedValuesRule:
    column: str
    values: tuple[str, ...]
    severity: Severity = Severity.ERROR


@dataclass(frozen=True)
class ReferentialRule:
    column: str
    parent_asset: str          # an AssetId in `kind:name` form
    parent_column: str
    severity: Severity = Severity.ERROR


@dataclass(frozen=True)
class CustomCheck:
    name: str
    expression: str            # a boolean SQL predicate that must hold for every row
    severity: Severity = Severity.ERROR


@dataclass(frozen=True)
class ReconRequirement:
    """A contract may REQUIRE reconciliation against a counterpart. Declaring it here rather
    than only in a DQ config means "this table must be reconciled" survives someone deleting
    the reconciliation job's config -- the requirement is part of the promise, not of the
    tooling that happens to check it."""

    counterpart_asset: str
    metric: str                # row_count | sum:<column> | distinct:<column>
    tolerance_pct: float = 0.0
    severity: Severity = Severity.ERROR


@dataclass(frozen=True)
class ContractCheck:
    """One executable assertion, with a derived stable identity."""

    check_id: str
    check_type: CheckType
    severity: Severity
    expected_rule: str
    columns: tuple[str, ...] = ()

    def payload(self) -> dict:
        return {"check_id": self.check_id, "check_type": self.check_type.value,
                "severity": self.severity.value, "expected_rule": self.expected_rule,
                "columns": list(self.columns)}


def _check_id(asset: AssetId, check_type: CheckType, *parts: str) -> str:
    body = "|".join([str(asset), check_type.value, *[str(p) for p in parts]])
    return f"{check_type.value}.{hashlib.sha256(body.encode()).hexdigest()[:12]}"


@dataclass(frozen=True)
class DataContract:
    """What the producer of one asset guarantees. No owner, no tags, no description of the
    asset itself -- those are `GovernanceMetadata`."""

    asset: AssetId
    version: str = "1"
    schema: tuple[FieldSpec, ...] = ()
    business_key: tuple[str, ...] = ()
    schema_evolution: SchemaEvolutionPolicy = SchemaEvolutionPolicy.ADDITIVE_ONLY
    freshness: FreshnessRule | None = None
    volume: VolumeRule | None = None
    nullability: tuple[NullabilityRule, ...] = ()
    uniqueness: tuple[UniquenessRule, ...] = ()
    accepted_values: tuple[AcceptedValuesRule, ...] = ()
    referential: tuple[ReferentialRule, ...] = ()
    custom_checks: tuple[CustomCheck, ...] = ()
    reconciliation: tuple[ReconRequirement, ...] = ()

    @property
    def columns(self) -> tuple[str, ...]:
        return tuple(f.name for f in self.schema)

    def validate(self) -> tuple[str, ...]:
        """Returns every problem, not the first. Same reasoning as `governance.validate_asset`."""
        errs: list[str] = []
        known = set(self.columns)
        if len(known) != len(self.schema):
            dupes = sorted({c for c in self.columns if self.columns.count(c) > 1})
            errs.append(f"duplicate column(s) in schema: {', '.join(dupes)}")

        if not self.business_key:
            errs.append("business_key is required; without a grain, uniqueness and "
                        "reconciliation have nothing to be stated about")
        for col in self.business_key:
            if col not in known:
                errs.append(f"business_key column {col!r} is not in the schema")
            else:
                spec = next(f for f in self.schema if f.name == col)
                if spec.nullable:
                    errs.append(
                        f"business_key column {col!r} is nullable; a NULL key silently "
                        f"collapses rows in every downstream join and cannot be deduplicated")

        def _cols(label: str, cols) -> None:
            for c in cols:
                if c not in known:
                    errs.append(f"{label} references column {c!r}, which is not in the schema")

        for r in self.nullability:
            _cols("nullability", [r.column])
            if not 0.0 <= r.max_null_pct <= 100.0:
                errs.append(f"nullability[{r.column}].max_null_pct must be 0-100, got {r.max_null_pct}")
        for r in self.uniqueness:
            if not r.columns:
                errs.append("uniqueness rule with no columns")
            _cols("uniqueness", r.columns)
        for r in self.accepted_values:
            _cols("accepted_values", [r.column])
            if not r.values:
                errs.append(f"accepted_values[{r.column}] declares no values; an empty set "
                            f"accepts nothing and would fail every row")
        for r in self.referential:
            _cols("referential", [r.column])
            try:
                AssetId.parse(r.parent_asset)
            except ConfigError as exc:
                errs.append(f"referential[{r.column}].parent_asset: {exc}")
        for r in self.reconciliation:
            try:
                AssetId.parse(r.counterpart_asset)
            except ConfigError as exc:
                errs.append(f"reconciliation.counterpart_asset: {exc}")
            if not 0.0 <= r.tolerance_pct <= 100.0:
                errs.append(f"reconciliation tolerance_pct must be 0-100, got {r.tolerance_pct}")
        if self.freshness:
            _cols("freshness", [self.freshness.timestamp_column])
            if self.freshness.max_lag_minutes <= 0:
                errs.append("freshness.max_lag_minutes must be positive")
        if self.volume and self.volume.min_rows is not None and self.volume.max_rows is not None:
            if self.volume.min_rows > self.volume.max_rows:
                errs.append("volume.min_rows exceeds volume.max_rows")
        for c in self.custom_checks:
            if not c.expression.strip():
                errs.append(f"custom check {c.name!r} has an empty expression")

        ids = [c.check_id for c in self._checks_unchecked()]
        dupe_ids = sorted({i for i in ids if ids.count(i) > 1})
        if dupe_ids:
            errs.append(f"duplicate check_id(s): {', '.join(dupe_ids)} -- two rules with one "
                        f"identity share one history and overwrite each other's results")
        return tuple(errs)

    def _checks_unchecked(self) -> tuple[ContractCheck, ...]:
        a = self.asset
        out: list[ContractCheck] = []

        if self.schema:
            cols = ",".join(sorted(f"{f.name}:{f.type}" for f in self.schema))
            out.append(ContractCheck(
                _check_id(a, CheckType.SCHEMA, self.schema_evolution.value, cols),
                CheckType.SCHEMA, Severity.ERROR,
                f"schema matches {len(self.schema)} declared field(s) under "
                f"{self.schema_evolution.value}",
                tuple(sorted(self.columns))))

        if self.business_key:
            key = ",".join(self.business_key)
            out.append(ContractCheck(
                _check_id(a, CheckType.BUSINESS_KEY, key), CheckType.BUSINESS_KEY,
                Severity.ERROR, f"exactly one row per ({key})", tuple(self.business_key)))

        for r in self.nullability:
            out.append(ContractCheck(
                _check_id(a, CheckType.NULLABILITY, r.column), CheckType.NULLABILITY,
                r.severity, f"{r.column} null rate <= {r.max_null_pct}%", (r.column,)))

        for r in self.uniqueness:
            key = ",".join(r.columns)
            out.append(ContractCheck(
                _check_id(a, CheckType.UNIQUENESS, key), CheckType.UNIQUENESS, r.severity,
                f"({key}) is unique", tuple(r.columns)))

        for r in self.accepted_values:
            out.append(ContractCheck(
                _check_id(a, CheckType.ACCEPTED_VALUES, r.column), CheckType.ACCEPTED_VALUES,
                r.severity, f"{r.column} in ({', '.join(sorted(r.values))})", (r.column,)))

        for r in self.referential:
            out.append(ContractCheck(
                _check_id(a, CheckType.REFERENTIAL_INTEGRITY, r.column, r.parent_asset,
                          r.parent_column),
                CheckType.REFERENTIAL_INTEGRITY, r.severity,
                f"{r.column} exists in {r.parent_asset}.{r.parent_column}", (r.column,)))

        if self.freshness:
            out.append(ContractCheck(
                _check_id(a, CheckType.FRESHNESS, self.freshness.timestamp_column),
                CheckType.FRESHNESS, self.freshness.severity,
                f"max({self.freshness.timestamp_column}) within "
                f"{self.freshness.max_lag_minutes}m", (self.freshness.timestamp_column,)))

        if self.volume:
            bits = []
            if self.volume.min_rows is not None:
                bits.append(f">= {self.volume.min_rows}")
            if self.volume.max_rows is not None:
                bits.append(f"<= {self.volume.max_rows}")
            if self.volume.max_pct_change is not None:
                bits.append(f"change <= {self.volume.max_pct_change}%")
            if bits:
                out.append(ContractCheck(
                    _check_id(a, CheckType.VOLUME, *bits), CheckType.VOLUME,
                    self.volume.severity, "row count " + " and ".join(bits)))

        for c in self.custom_checks:
            out.append(ContractCheck(
                _check_id(a, CheckType.CUSTOM, c.name), CheckType.CUSTOM, c.severity,
                c.expression))

        for r in self.reconciliation:
            out.append(ContractCheck(
                _check_id(a, CheckType.RECONCILIATION, r.counterpart_asset, r.metric),
                CheckType.RECONCILIATION, r.severity,
                f"{r.metric} matches {r.counterpart_asset} within {r.tolerance_pct}%"))

        return tuple(out)

    def checks(self) -> tuple[ContractCheck, ...]:
        """The executable form. Refuses to render an invalid contract.

        Rendering first and validating later would hand a runtime a list of checks derived
        from a contract nobody confirmed was coherent -- for instance a uniqueness rule on a
        column that does not exist, which evaluates to NOT_EVALUATED and therefore BLOCKS,
        looking like a data defect rather than a config one.
        """
        errs = self.validate()
        if errs:
            raise ConfigError(
                f"{self.asset}: contract is invalid, refusing to render checks:\n  - "
                + "\n  - ".join(errs))
        return self._checks_unchecked()

    def payload(self) -> dict:
        return {"asset": str(self.asset), "version": self.version,
                "schema_evolution": self.schema_evolution.value,
                "business_key": list(self.business_key),
                "schema": [f.payload() for f in self.schema],
                "checks": [c.payload() for c in self._checks_unchecked()]}


#: Fields a contract must NEVER carry. Asserted by a test against the dataclass itself, so
#: adding one of these to `DataContract` fails the build rather than quietly turning an
#: unfalsifiable attribute into something the DQ engine is asked to evaluate.
FORBIDDEN_CONTRACT_FIELDS = ("owner", "technical_owner", "business_owner", "domain",
                             "subdomain", "tags", "glossary_terms", "classification",
                             "criticality", "pii_fields", "description")
