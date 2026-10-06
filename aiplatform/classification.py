"""Security classification and cost class — two small enums that gate a lot of behaviour.

CLASSIFICATION mirrors `governance/catalog/domains.yml` exactly, because a second
classification vocabulary is a second access-control policy drifting invisibly from the
first. The registry is the source; this is the typed view of it.

THE DEFAULT IS THE MOST RESTRICTIVE VALUE. A chunk whose classification cannot be derived
must not fall through to `internal` and become retrievable — an unknown sensitivity is a
reason to withhold, not to share.
"""

from __future__ import annotations

from enum import Enum


class Classification(str, Enum):
    RESTRICTED   = "restricted"     # raw PII — FULL_CDC / REALTIME. BI and AI denied.
    CONFIDENTIAL = "confidential"   # curated with controlled PII
    INTERNAL     = "internal"       # aggregates, no direct identifiers — marts
    OPERATIONAL  = "operational"    # pipeline telemetry — ops.*

    @classmethod
    def most_restrictive(cls) -> "Classification":
        return cls.RESTRICTED

    @classmethod
    def parse(cls, value: str | None) -> "Classification":
        """Unknown or missing -> RESTRICTED. Fail closed, always."""
        if value is None:
            return cls.most_restrictive()
        try:
            return cls(str(value).strip().lower())
        except ValueError:
            return cls.most_restrictive()

    def retrievable_by_ai(self) -> bool:
        """Which classifications the AI plane may retrieve or query.

        RESTRICTED is denied here AND at the IAM layer (ADR-060). This method is the second
        control, never the only one.
        """
        return self in (Classification.INTERNAL, Classification.OPERATIONAL,
                        Classification.CONFIDENTIAL)


class CostClass(str, Enum):
    """Every AI component must declare one. ADR-060 forbids ALWAYS_ON by default."""
    ALWAYS_ON      = "always_on"
    PER_REQUEST    = "per_request"
    PER_JOB        = "per_job"
    METERED_WINDOW = "metered_window"

    def requires_cost_review(self) -> bool:
        """ALWAYS_ON is the one that cannot be adopted silently at a $30/month budget."""
        return self is CostClass.ALWAYS_ON


# Resources that may never be provisioned without a written, operator-approved cost review.
# Named here so a config referencing one is rejected at compile time rather than at apply.
FORBIDDEN_WITHOUT_COST_REVIEW = (
    "opensearch_serverless",
    "sagemaker_realtime_endpoint",
    "eks_cluster",
    "nat_gateway",
    "persistent_ec2_ai_runtime",
    "large_online_feature_store",
)
