"""Governance metadata: what it is, where it comes from, and what it refuses to compile.

DRP1. The DRP0 audit measured a hand-maintained registry describing 12 datasets of which 1
existed, while 72 of 73 live tables carried no owner, classification, retention or SLA.
This module is the answer, and its shape is a direct consequence of that measurement:

    governance metadata is INHERITED and DERIVED, never re-typed.

FOUR LEVELS, MOST SPECIFIC WINS
--------------------------------
    global      `cdc/registry/sources.yaml`  -> defaults.governance
    domain      `governance/registry/domains.yaml` -> domains.<name>.defaults
    source      `cdc/registry/sources.yaml`  -> sources[].defaults.governance
    asset       the table's own `governance:` block, or the derived-asset overlay

Every resolved field carries its PROVENANCE -- which level supplied it. That is not a
nicety: "who owns this table" and "who owns everything in this domain" are different
answers to the same question, and an audit that cannot tell them apart cannot tell a
deliberate assignment from an unreviewed default.

ACCUMULATE vs OVERRIDE -- the distinction that makes inheritance safe
---------------------------------------------------------------------
Scalars override. `tags`, `glossary_terms` and `pii_fields` ACCUMULATE.

If tags overrode, a table that declares `tags: [reconciled]` would silently drop the
domain's `[regulated]` -- the asset would look less governed for having said something
about itself. Accumulation means a domain-level control cannot be shed by a local edit,
which is the only behaviour a governance default can safely have. `pii_fields` accumulates
per column, so an asset may refine a column's category but cannot un-declare one.

TWO NUMBERS THAT MUST NOT DISAGREE
-----------------------------------
DRP0 open question 4 found `freshness_sla_minutes: 60` in the CDC registry and
`freshness_sla_minutes: 1440` in the governance registry -- two values, neither measured,
no way to prefer one. This module resolves it by giving them different jobs rather than
picking a winner:

    dq.freshness_sla_minutes   the EXECUTABLE threshold -- what a check evaluates
    freshness_slo_minutes      the PUBLISHED promise    -- what a consumer may rely on

and then refusing the combination that is actually wrong: an SLO tighter than the SLA you
measure at. Promising 60 minutes while only checking at 1440 is a promise nothing tests.
Retention is handled the same way: `retention_class` is the governance band,
`eod.retention_days` stays the executable number, and the two are checked for consistency
instead of being stored twice.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

import yaml

from .assets import AssetId
from .models import Classification, ConfigError

DEFAULT_VOCABULARY_FILE = (Path(__file__).resolve().parents[1]
                           / "governance" / "registry" / "domains.yaml")


class RetentionClass(str, Enum):
    """Governance retention bands. The executable number stays in the layer's own policy
    (`eod.retention_days`, `realtime.retention_hours`); this says which band it must fall in,
    so a band and a number cannot drift into disagreement unnoticed."""

    TRANSIENT = "transient"        # <= 7 days
    SHORT = "short"               # <= 35 days
    STANDARD = "standard"         # <= 400 days
    LONG = "long"                 # <= 2600 days (~7 years)
    REGULATORY = "regulatory"     # no upper bound; deletion requires an approval


#: Inclusive upper bound in days, or None for unbounded. Used to CHECK a declared band
#: against the executable retention, never to replace it.
RETENTION_MAX_DAYS: dict[RetentionClass, int | None] = {
    RetentionClass.TRANSIENT: 7,
    RetentionClass.SHORT: 35,
    RetentionClass.STANDARD: 400,
    RetentionClass.LONG: 2600,
    RetentionClass.REGULATORY: None,
}


class Criticality(str, Enum):
    """How much depends on this asset being right. Drives the certification gate and the
    blast-radius limit, so it is a closed set."""

    TIER_1 = "tier_1"   # certified reporting; a defect is externally visible
    TIER_2 = "tier_2"   # internal decision-making
    TIER_3 = "tier_3"   # exploratory / operational telemetry


#: Criticality that forces the strictest ownership and certification rules.
CRITICAL_TIERS = (Criticality.TIER_1,)


class PiiCategory(str, Enum):
    """What KIND of personal data a column holds. `pii: true` was never enough -- a name
    and a national id need different masking, different retention and different approval
    to export, and a boolean cannot express that."""

    DIRECT_IDENTIFIER = "direct_identifier"     # name, national id, passport
    CONTACT = "contact"                         # email, phone, address
    FINANCIAL = "financial"                     # account number, card, balance
    BEHAVIOURAL = "behavioural"                 # device, session, location trail
    PSEUDONYMOUS = "pseudonymous"               # hashed or tokenised identifier


@dataclass(frozen=True, order=True)
class PiiField:
    column: str
    category: PiiCategory

    def payload(self) -> dict:
        return {"column": self.column, "category": self.category.value}


#: Field-by-field merge behaviour. Anything not listed here overrides.
ACCUMULATING_FIELDS = ("tags", "glossary_terms", "pii_fields")

#: Governance fields, in the order they appear in the resolved payload. Declared once so
#: the payload, the provenance map and the validation all walk the same list -- a field
#: added to the dataclass but forgotten in one of the three is the classic way a governance
#: attribute becomes unvalidated.
GOVERNANCE_FIELDS = (
    "owner", "technical_owner", "business_owner",
    "domain", "subdomain", "description",
    "classification", "pii_fields",
    "retention_class", "freshness_slo_minutes", "criticality",
    "tags", "glossary_terms",
    "contract_policy", "dq_policy", "reconciliation_policy", "certification_policy",
)


@dataclass(frozen=True)
class GovernanceMetadata:
    """Resolved governance for ONE asset. Immutable; produced only by `resolve()`."""

    owner: str = ""
    technical_owner: str = ""
    business_owner: str = ""
    domain: str = ""
    subdomain: str = ""
    description: str = ""
    classification: Classification = Classification.INTERNAL
    pii_fields: tuple[PiiField, ...] = ()
    retention_class: RetentionClass = RetentionClass.STANDARD
    #: `None` means NOT DECLARED -- distinct from 0, which would promise instant freshness.
    #: The same absence/emptiness distinction `MaintenancePolicy.actions` already makes.
    freshness_slo_minutes: int | None = None
    criticality: Criticality = Criticality.TIER_3
    tags: tuple[str, ...] = ()
    glossary_terms: tuple[str, ...] = ()
    contract_policy: str = ""
    dq_policy: str = ""
    reconciliation_policy: str = ""
    certification_policy: str = ""
    #: field name -> the level that supplied it: "global" | "domain:<d>" | "source:<s>" | "asset"
    provenance: dict = field(default_factory=dict, compare=False)

    @property
    def effective_technical_owner(self) -> str:
        """`technical_owner` falls back to `owner` rather than being blank.

        A blank technical owner on an asset that HAS an owner is not missing information --
        it is the same person wearing one hat. Reporting it as absent would make the
        coverage number lie in the pessimistic direction, which erodes trust in it just as
        fast as lying optimistically.
        """
        return self.technical_owner or self.owner

    @property
    def effective_business_owner(self) -> str:
        return self.business_owner or self.owner

    @property
    def has_pii(self) -> bool:
        return bool(self.pii_fields)

    def payload(self) -> dict:
        """Deterministic, JSON-safe. Sorted everywhere a set would otherwise leak ordering."""
        out: dict = {}
        for name in GOVERNANCE_FIELDS:
            value = getattr(self, name)
            if name == "pii_fields":
                out[name] = [p.payload() for p in sorted(value)]
            elif isinstance(value, Enum):
                out[name] = value.value
            elif isinstance(value, tuple):
                out[name] = sorted(value)
            else:
                out[name] = value
        return out


# --------------------------------------------------------------------------- #
# Vocabulary -- the closed sets a governance reference is checked against
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class Vocabulary:
    """Domains, subdomains, glossary terms and policy names.

    Every governance REFERENCE is checked against this. An unknown domain is a compile
    error, not an unrecognised string that quietly becomes its own domain of one -- which
    is how `unassigned` ends up being the largest domain in a catalogue.
    """

    domains: dict = field(default_factory=dict)          # name -> {defaults, subdomains, description}
    glossary: dict = field(default_factory=dict)         # term -> definition
    contract_policies: tuple[str, ...] = ()
    dq_policies: tuple[str, ...] = ()
    reconciliation_policies: tuple[str, ...] = ()
    certification_policies: tuple[str, ...] = ()

    @classmethod
    def load(cls, path: Path | None = None) -> "Vocabulary":
        path = path or DEFAULT_VOCABULARY_FILE
        if not path.exists():
            raise ConfigError(
                f"{path}: the governance vocabulary is missing. Domains, glossary terms and "
                f"policy names are checked against it, so without it every reference would "
                f"be accepted -- which is the DRP0 drift, re-enabled.")
        doc = yaml.safe_load(path.read_text()) or {}
        return cls.from_payload(doc)

    @classmethod
    def from_payload(cls, doc: dict) -> "Vocabulary":
        domains = doc.get("domains") or {}
        if not isinstance(domains, dict) or not domains:
            raise ConfigError("governance vocabulary: `domains` is required and must be a mapping")
        policies = doc.get("policies") or {}
        return cls(
            domains=domains,
            glossary={k: (v or {}).get("definition", "") if isinstance(v, dict) else str(v)
                      for k, v in (doc.get("glossary") or {}).items()},
            contract_policies=tuple(sorted(policies.get("contract") or ())),
            dq_policies=tuple(sorted(policies.get("dq") or ())),
            reconciliation_policies=tuple(sorted(policies.get("reconciliation") or ())),
            certification_policies=tuple(sorted(policies.get("certification") or ())),
        )

    def domain_defaults(self, domain: str) -> dict:
        return dict((self.domains.get(domain) or {}).get("defaults") or {})

    def subdomains(self, domain: str) -> tuple[str, ...]:
        return tuple((self.domains.get(domain) or {}).get("subdomains") or ())


# --------------------------------------------------------------------------- #
# Resolution
# --------------------------------------------------------------------------- #

def _coerce(name: str, raw, *, where: str):
    if raw is None:
        return None
    if name == "classification":
        return _enum_value(Classification, raw, what=f"{where}.classification")
    if name == "retention_class":
        return _enum_value(RetentionClass, raw, what=f"{where}.retention_class")
    if name == "criticality":
        return _enum_value(Criticality, raw, what=f"{where}.criticality")
    if name == "freshness_slo_minutes":
        if not isinstance(raw, int) or isinstance(raw, bool) or raw <= 0:
            raise ConfigError(
                f"{where}.freshness_slo_minutes: expected a positive integer, got {raw!r}. "
                f"Omit the key to leave it undeclared; 0 would promise instant freshness.")
        return raw
    if name == "pii_fields":
        return _pii(raw, where=where)
    if name in ("tags", "glossary_terms"):
        if isinstance(raw, str):
            raise ConfigError(f"{where}.{name}: expected a list, got a string {raw!r}")
        return tuple(str(x) for x in raw)
    return str(raw)


def _enum_value(cls, raw, *, what: str):
    try:
        return cls(raw)
    except ValueError:
        raise ConfigError(
            f"{what}: {raw!r} is not one of "
            f"{', '.join(m.value for m in cls)}") from None


def _pii(raw, *, where: str) -> tuple[PiiField, ...]:
    if isinstance(raw, dict):
        raw = [{"column": k, "category": v} for k, v in raw.items()]
    out = []
    for entry in raw or ():
        if not isinstance(entry, dict) or "column" not in entry:
            raise ConfigError(
                f"{where}.pii_fields: each entry needs `column` and `category`, got {entry!r}. "
                f"A bare column name cannot say WHICH kind of personal data it holds, and the "
                f"masking and retention rules differ by kind.")
        out.append(PiiField(str(entry["column"]),
                            _enum_value(PiiCategory, entry.get("category"),
                                        what=f"{where}.pii_fields[{entry['column']}].category")))
    return tuple(out)


def _merge_level(current: dict, prov: dict, raw: dict | None, level: str, *, where: str) -> None:
    """Apply one inheritance level in place. Accumulating fields union; the rest override."""
    for name, value in (raw or {}).items():
        if name not in GOVERNANCE_FIELDS:
            raise ConfigError(
                f"{where}: unknown governance field {name!r}. Known fields: "
                f"{', '.join(GOVERNANCE_FIELDS)}. A misspelt field would be silently "
                f"ignored, and an ignored governance attribute looks exactly like a "
                f"declared one to anyone reading the file.")
        coerced = _coerce(name, value, where=where)
        if coerced is None:
            continue
        if name in ACCUMULATING_FIELDS:
            merged = list(current.get(name) or ())
            if name == "pii_fields":
                by_col = {p.column: p for p in merged}
                for p in coerced:
                    by_col[p.column] = p          # a later level may REFINE a column's category
                merged = sorted(by_col.values())
            else:
                merged = sorted(set(merged) | set(coerced))
            current[name] = tuple(merged)
            prov[name] = f"{prov.get(name)}+{level}" if name in prov else level
        else:
            current[name] = coerced
            prov[name] = level


def resolve(*, vocabulary: Vocabulary, global_defaults: dict | None = None,
            source_defaults: dict | None = None, asset_override: dict | None = None,
            where: str = "governance") -> GovernanceMetadata:
    """The four-level merge. Domain defaults are pulled from the vocabulary once the domain
    is known, which is why they are applied SECOND and not passed in by the caller.

    The domain can be set at any level, so it is resolved first from the most specific
    level that names one -- otherwise a table that chooses its own domain would inherit the
    defaults of the domain it was moved out of.
    """
    domain = ""
    for raw in (asset_override, source_defaults, global_defaults):
        if (raw or {}).get("domain"):
            domain = str(raw["domain"])
            break
    if domain and domain not in vocabulary.domains:
        raise ConfigError(
            f"{where}: unknown domain {domain!r}. Known domains: "
            f"{', '.join(sorted(vocabulary.domains))}. An unchecked domain string becomes "
            f"its own domain of one, which is how `unassigned` grows.")

    current: dict = {}
    prov: dict = {}
    _merge_level(current, prov, global_defaults, "global", where=f"{where}[global]")
    if domain:
        _merge_level(current, prov, vocabulary.domain_defaults(domain), f"domain:{domain}",
                     where=f"{where}[domain:{domain}]")
    _merge_level(current, prov, source_defaults, "source", where=f"{where}[source]")
    _merge_level(current, prov, asset_override, "asset", where=f"{where}[asset]")

    meta = GovernanceMetadata(**current, provenance=prov)
    if meta.subdomain:
        known = vocabulary.subdomains(meta.domain)
        if known and meta.subdomain not in known:
            raise ConfigError(
                f"{where}: subdomain {meta.subdomain!r} is not declared for domain "
                f"{meta.domain!r}. Declared: {', '.join(known) or '(none)'}")
    unknown_terms = [t for t in meta.glossary_terms if t not in vocabulary.glossary]
    if unknown_terms:
        raise ConfigError(
            f"{where}: unknown glossary term(s) {', '.join(sorted(unknown_terms))}. "
            f"A term that resolves to nothing is a definition the reader cannot look up.")
    for attr, known, label in (
            ("contract_policy", vocabulary.contract_policies, "contract"),
            ("dq_policy", vocabulary.dq_policies, "dq"),
            ("reconciliation_policy", vocabulary.reconciliation_policies, "reconciliation"),
            ("certification_policy", vocabulary.certification_policies, "certification")):
        value = getattr(meta, attr)
        if value and value not in known:
            raise ConfigError(
                f"{where}.{attr}: {value!r} is not a declared {label} policy. Declared: "
                f"{', '.join(known) or '(none)'}")
    return meta


# --------------------------------------------------------------------------- #
# Validation -- the rules DRP1 requires, each with the failure it prevents
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class GovernanceFinding:
    asset: str
    rule: str
    message: str

    def __str__(self) -> str:
        return f"{self.asset}: [{self.rule}] {self.message}"


def validate_asset(asset: AssetId, meta: GovernanceMetadata, *,
                   retention_days: int | None = None,
                   executable_freshness_minutes: int | None = None,
                   certifies: bool = False) -> tuple[GovernanceFinding, ...]:
    """Every rule returns a finding rather than raising, so ONE compile reports ALL of them.

    A validator that raises on the first problem turns a 73-asset backlog into 73 sequential
    edit-and-rerun cycles, and the person doing it stops after a few.
    """
    out: list[GovernanceFinding] = []
    a = str(asset)

    if not meta.owner.strip():
        out.append(GovernanceFinding(a, "owner_required",
                                     "every governed asset needs an owner; an unowned asset "
                                     "has nobody to route an incident to"))
    if meta.criticality in CRITICAL_TIERS:
        for label, value in (("technical_owner", meta.effective_technical_owner),
                             ("business_owner", meta.effective_business_owner)):
            if not value.strip():
                out.append(GovernanceFinding(
                    a, "critical_requires_named_owners",
                    f"{meta.criticality.value} requires {label}; at this tier a defect is "
                    f"externally visible and 'the data team' is not a routable owner"))
        if not meta.description.strip():
            out.append(GovernanceFinding(
                a, "critical_requires_description",
                f"{meta.criticality.value} requires a description; a consumer cannot judge "
                f"fitness for use from a name"))

    if certifies:
        if not meta.dq_policy:
            out.append(GovernanceFinding(
                a, "certified_requires_dq",
                "an asset that certifies must declare a dq_policy; certification without a "
                "quality verdict means 'the job finished', not 'the data was checked'"))
        if not meta.contract_policy:
            out.append(GovernanceFinding(
                a, "certified_requires_contract",
                "an asset that certifies must declare a contract_policy; without a contract "
                "there is no stated shape for the check to be a check OF"))

    if meta.has_pii and meta.classification in (Classification.PUBLIC, Classification.INTERNAL):
        out.append(GovernanceFinding(
            a, "pii_requires_classification",
            f"{len(meta.pii_fields)} PII column(s) declared but classification is "
            f"{meta.classification.value}; the masking views and the AI deny list read "
            f"classification, not the PII list, so this asset would be exposed"))

    if retention_days is not None:
        cap = RETENTION_MAX_DAYS[meta.retention_class]
        if cap is not None and retention_days > cap:
            out.append(GovernanceFinding(
                a, "retention_band_mismatch",
                f"executable retention is {retention_days}d but band "
                f"{meta.retention_class.value} caps at {cap}d; the band is what a reviewer "
                f"reads and the number is what actually deletes"))

    if executable_freshness_minutes is not None and meta.freshness_slo_minutes is not None:
        if executable_freshness_minutes > meta.freshness_slo_minutes:
            out.append(GovernanceFinding(
                a, "slo_tighter_than_sla",
                f"published SLO is {meta.freshness_slo_minutes}m but the check only "
                f"evaluates at {executable_freshness_minutes}m; the promise is not measured"))

    return tuple(out)


# --------------------------------------------------------------------------- #
# The compiled inventory
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class GovernedAsset:
    asset: AssetId
    governance: GovernanceMetadata
    #: Free-form, deterministic. Carries the executable numbers the governance bands were
    #: checked against, so the compiled artefact explains its own verdict.
    facts: dict = field(default_factory=dict)

    def payload(self) -> dict:
        return {"asset": str(self.asset), "kind": self.asset.kind.value,
                "layer": self.asset.layer, "lineage_key": self.asset.lineage_key,
                "governance": self.governance.payload(),
                "provenance": {k: self.governance.provenance[k]
                               for k in sorted(self.governance.provenance)},
                "facts": {k: self.facts[k] for k in sorted(self.facts)}}


@dataclass(frozen=True)
class GovernanceInventory:
    assets: tuple[GovernedAsset, ...]
    findings: tuple[GovernanceFinding, ...] = ()

    @property
    def ok(self) -> bool:
        return not self.findings

    def by_id(self) -> dict:
        return {str(a.asset): a for a in self.assets}

    def payload(self) -> dict:
        return {"assets": [a.payload() for a in self.assets],
                "findings": [{"asset": f.asset, "rule": f.rule, "message": f.message}
                             for f in self.findings]}

    def config_version(self) -> str:
        """A stable hash of the governance metadata ONLY -- findings excluded.

        Findings describe the state of the world at compile time; the config version must
        identify the CONFIGURATION, so that fixing an unrelated asset does not invalidate
        every DQ result already stamped with this version.
        """
        body = json.dumps([a.payload() for a in self.assets], sort_keys=True,
                          separators=(",", ":"))
        return "gv1:" + hashlib.sha256(body.encode()).hexdigest()[:16]

    def coverage(self) -> dict:
        """The number DRP1 is measured by: how many assets are actually governed."""
        total = len(self.assets)
        owned = sum(1 for a in self.assets if a.governance.owner.strip())
        classified = sum(1 for a in self.assets
                         if a.governance.classification is not Classification.INTERNAL
                         or "classification" in a.governance.provenance)
        described = sum(1 for a in self.assets if a.governance.description.strip())
        return {"assets": total, "owned": owned, "classified": classified,
                "described": described,
                "owned_pct": round(100.0 * owned / total, 1) if total else 0.0}
