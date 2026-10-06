"""Metric registry — the contract a metric question is answered against.

A metric the copilot will discuss must be DECLARED: what it measures, at what grain, over
which date column, which dimensions may explain it, and who owns it. Without this, "why is
the balance small" degenerates into free-text SQL generation, which cannot be reviewed and
cannot be wrong in a detectable way.

Every metric names its `additive` behaviour explicitly. Summing an average across segments
produces a number that looks plausible and is meaningless, and it is the single most common
way a metric layer lies.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class MetricSpec:
    name: str
    dataset: str                      # governed table, e.g. mart.mart_account_balance_daily
    measure: str                      # SQL aggregate, e.g. SUM(closing_balance)
    date_column: str
    dimensions: tuple[str, ...]       # columns that EXIST on `dataset` right now
    owner: str
    grain: str
    #: SUM/COUNT are additive across segments; AVG/ratios are NOT. A non-additive metric
    #: cannot be decomposed by simple contribution, so diagnose() refuses rather than
    #: producing a decomposition whose parts do not sum to the whole.
    additive: bool = True
    unit: str = ""
    description: str = ""
    #: Relative change beyond this is worth surfacing unprompted.
    alert_rel_change: float = 0.15
    tags: tuple[str, ...] = field(default_factory=tuple)
    #: Conceptually valid dimensions that are NOT yet columns on `dataset` -> why. Kept
    #: distinct from `dimensions` rather than merged and skipped, because "this dimension
    #: does not exist" and "this dimension exists but I chose not to list it" look identical
    #: to a caller unless the reason is named. Mirrors the available/blocked_reason pattern
    #: already proven in aiplatform/metrics/business_metrics.yaml -- this registry never had
    #: it, so `dimensions` held three names (product_code, currency, branch_code/status)
    #: that are not columns on kafka_dev_lab_dev_mart.mart_account_balance_daily at all.
    #: validate_dimension() used to accept them, and the query then failed at Athena as
    #: COLUMN_NOT_FOUND -- after being billed -- which reads as "the platform is broken"
    #: rather than "this dimension needs dim_account built first". Found live 2026-09-03
    #: running the Diagnose tab against the deployed mart.
    blocked_dimensions: dict[str, str] = field(default_factory=dict)

    def qualified(self) -> str:
        """The relation to actually query.

        `AI_MART_RELATION` redirects the account-balance mart to a seeded table (see
        `scripts/seed-demo-mart.py`). It is applied HERE rather than at construction so the
        declared `dataset` stays what the deployed contract says, and so the two registries
        -- this one and the business metric layer in `semantic.py` -- cannot end up reading
        different tables. They did: the Ask tab honoured the override and Diagnose did not,
        so the same page showed a 90-date series and "no rows for this date".
        """
        import os
        override = os.environ.get("AI_MART_RELATION", "").strip()
        if override and self.dataset.endswith(".mart_account_balance_daily"):
            return override
        return self.dataset

    def validate_dimension(self, dim: str) -> str:
        if dim in self.blocked_dimensions:
            raise ValueError(
                f"{self.name}: dimension {dim!r} is declared but NOT YET QUERYABLE -- "
                f"{self.blocked_dimensions[dim]}. Refusing before this reaches Athena, "
                "where it would fail as COLUMN_NOT_FOUND after the query was already billed.")
        if dim not in self.dimensions:
            extra = (f"; not yet available: {list(self.blocked_dimensions)}"
                    if self.blocked_dimensions else "")
            raise ValueError(
                f"{self.name}: {dim!r} is not a declared dimension. "
                f"Queryable now: {list(self.dimensions)}{extra}. Explaining a metric with "
                "an undeclared column is how a spurious correlation becomes a reported cause.")
        return dim


#: The registry ships with the metrics this lakehouse actually models. Adding one is a
#: deliberate act with an owner, not a side effect of someone writing a query.
#:
#: Every `dimensions` entry below is a column PROVEN to exist on the live table (checked
#: against `aws glue get-table --name mart_account_balance_daily` 2026-09-03):
#:   account_sk, customer_sk, business_date, closing_balance, debit_amount, credit_amount,
#:   txn_count, processing_status, input_cutoff, run_id, certified_at, built_at,
#:   execution_id, coordinator_run_id, config_version, source_flow_mode
#: product_code, currency and branch_id/branch_code are NOT among them -- they require
#: curated.dim_account / dim_customer, which BAI-P0 found unmaterialised. Declared in
#: `blocked_dimensions` instead, so the reason is visible rather than the dimension simply
#: being absent (indistinguishable from "nobody thought of it").
_DIM_ACCOUNT_GAP = "requires curated.dim_account, not materialised (BAI-P0 gap 1)"
_DIM_CUSTOMER_GAP = "requires curated.dim_customer, not materialised (BAI-P0 gap 1)"
_NO_ACCOUNT_STATUS = ("the source account-status column (Oracle COREBANK.ACCOUNT.STATUS) is "
                     "not carried through EOD into the curated fact -- only "
                     "`processing_status` (the row's certification tier) survives. Use "
                     "that dimension; it answers a different question, not the same one.")

METRICS: dict[str, MetricSpec] = {
    "total_account_balance": MetricSpec(
        name="total_account_balance",
        dataset="kafka_dev_lab_dev_mart.mart_account_balance_daily",
        measure="SUM(closing_balance)",
        date_column="business_date",
        dimensions=("processing_status", "source_flow_mode"),
        blocked_dimensions={"product_code": _DIM_ACCOUNT_GAP, "currency": _DIM_ACCOUNT_GAP,
                            "branch_code": _DIM_CUSTOMER_GAP, "status": _NO_ACCOUNT_STATUS},
        owner="risk-data", grain="one row per account per business_date",
        additive=True, unit="VND",
        description="Closing balance summed across all accounts for a business date.",
        alert_rel_change=0.10, tags=("finance", "daily")),
    "active_account_count": MetricSpec(
        name="active_account_count",
        dataset="kafka_dev_lab_dev_mart.mart_account_balance_daily",
        # was COUNT(DISTINCT account_id) -- no such column; the PK is account_sk (same
        # correction the EOD job and every other layer in this lakehouse use).
        measure="COUNT(DISTINCT account_sk)",
        date_column="business_date",
        dimensions=("processing_status", "source_flow_mode"),
        blocked_dimensions={"product_code": _DIM_ACCOUNT_GAP, "currency": _DIM_ACCOUNT_GAP,
                            "branch_code": _DIM_CUSTOMER_GAP, "status": _NO_ACCOUNT_STATUS},
        owner="risk-data", grain="one row per account per business_date",
        additive=False,   # COUNT(DISTINCT) does not sum across segments
        description="Distinct accounts present on a business date.",
        tags=("finance", "daily")),
    "avg_account_balance": MetricSpec(
        name="avg_account_balance",
        dataset="kafka_dev_lab_dev_mart.mart_account_balance_daily",
        measure="AVG(closing_balance)",
        date_column="business_date",
        dimensions=("processing_status", "source_flow_mode"),
        blocked_dimensions={"product_code": _DIM_ACCOUNT_GAP, "currency": _DIM_ACCOUNT_GAP,
                            "status": _NO_ACCOUNT_STATUS},
        owner="risk-data", grain="one row per account per business_date",
        additive=False,   # averages are not additive -- decomposition would be a lie
        unit="VND", description="Mean closing balance per account.",
        tags=("finance", "daily")),
}


def get_metric(name: str) -> MetricSpec:
    if name not in METRICS:
        raise KeyError(
            f"unknown metric {name!r}. Declared: {sorted(METRICS)}. "
            "The copilot answers about DECLARED metrics only; inventing one on demand is "
            "how two dashboards end up disagreeing.")
    return METRICS[name]
