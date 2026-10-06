"""Online feature serving — an ABSTRACTION, with no backend deployed.

ADR-053: no consumer in this project needs sub-100 ms feature lookup. Building an online
store before a low-latency consumer exists is the failure the policy exists to prevent, so
the interface is here and every real backend is off.

`DynamoDbOnlineStore` is the approved backend when one is needed -- $0 idle
(`PAY_PER_REQUEST`) and a pattern ADR-036 already operates in this repository. SageMaker
Feature Store would need its own ADR, cost envelope and destroy verification to earn the
same position, so it is deliberately NOT the default; if it is ever adopted it goes behind
this same interface and nothing above it changes.

`InMemoryOnlineStore` is the test double. It is the ONLY implementation the test suite
exercises, because a test that needs AWS is a test that does not run.
"""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any


class OnlineStoreDisabled(RuntimeError):
    """Raised when a lookup is attempted with no online store enabled."""


@dataclass(frozen=True)
class FeatureRecord:
    entity_id: str
    feature_group: str
    feature_event_time: datetime
    feature_version: str
    values: dict[str, Any]


class OnlineStore(ABC):
    """Serving interface. Deliberately narrow: get/put by entity, nothing else.

    No scan, no query-by-range, no delete-all. An online store that can be scanned becomes
    an analytics store by accident, and then two systems disagree about history.
    """
    name: str = "abstract"

    @abstractmethod
    def put(self, record: FeatureRecord) -> None: ...

    @abstractmethod
    def get(self, feature_group: str, entity_id: str) -> FeatureRecord | None: ...

    def get_many(self, feature_group: str, entity_ids: list[str]) -> dict[str, FeatureRecord]:
        return {e: r for e in entity_ids
                if (r := self.get(feature_group, e)) is not None}


class InMemoryOnlineStore(OnlineStore):
    """Test double. Last-write-wins per (group, entity), which is what serving means."""
    name = "in_memory"

    def __init__(self) -> None:
        self._d: dict[tuple[str, str], FeatureRecord] = {}

    def put(self, record: FeatureRecord) -> None:
        key = (record.feature_group, record.entity_id)
        prev = self._d.get(key)
        # Serving holds the CURRENT value. An older event must not overwrite a newer one --
        # out-of-order arrival is normal, and silently regressing a feature is worse than
        # dropping the late write.
        if prev is None or record.feature_event_time >= prev.feature_event_time:
            self._d[key] = record

    def get(self, feature_group: str, entity_id: str) -> FeatureRecord | None:
        return self._d.get((feature_group, entity_id))

    def __len__(self) -> int:
        return len(self._d)


class DisabledOnlineStore(OnlineStore):
    """The DEFAULT. Fails loudly rather than returning None, so a caller that assumed an
    online store exists finds out at the call site instead of silently seeing no features."""
    name = "disabled"

    def put(self, record: FeatureRecord) -> None:
        raise OnlineStoreDisabled(
            "online feature store is disabled (ADR-053: no low-latency consumer). "
            "Set enable_ai_online_feature_store and provide a backend to enable it.")

    def get(self, feature_group: str, entity_id: str) -> FeatureRecord | None:
        raise OnlineStoreDisabled("online feature store is disabled (ADR-053).")


class DynamoDbOnlineStore(OnlineStore):
    """Approved backend when a consumer appears. NOT deployed; no table exists.

    Follows the ADR-036 pattern: `PAY_PER_REQUEST`, narrow IAM, no Scan, no wildcard.
    The client is injected so this class is unit-testable without AWS.
    """
    name = "dynamodb"

    def __init__(self, table_name: str, client=None):
        if not table_name:
            raise ValueError("DynamoDbOnlineStore requires a table name")
        self.table_name = table_name
        self._c = client

    def _client(self):
        if self._c is None:
            raise OnlineStoreDisabled(
                "no DynamoDB client configured — the online store table is not deployed.")
        return self._c

    @staticmethod
    def _pk(group: str, entity_id: str) -> str:
        return f"{group}#{entity_id}"

    def put(self, record: FeatureRecord) -> None:
        self._client().put_item(TableName=self.table_name, Item={
            "feature_key": {"S": self._pk(record.feature_group, record.entity_id)},
            "feature_event_time": {"S": record.feature_event_time.isoformat()},
            "feature_version": {"S": record.feature_version},
            "values": {"S": __import__("json").dumps(record.values, default=str)},
        },
            # Idempotent and order-safe: a late write cannot regress a newer value.
            ConditionExpression="attribute_not_exists(feature_key) OR "
                                "feature_event_time <= :t",
            ExpressionAttributeValues={":t": {"S": record.feature_event_time.isoformat()}})

    def get(self, feature_group: str, entity_id: str) -> FeatureRecord | None:
        r = self._client().get_item(
            TableName=self.table_name,
            Key={"feature_key": {"S": self._pk(feature_group, entity_id)}})
        it = r.get("Item")
        if not it:
            return None
        import json
        return FeatureRecord(
            entity_id=entity_id, feature_group=feature_group,
            feature_event_time=datetime.fromisoformat(it["feature_event_time"]["S"]),
            feature_version=it["feature_version"]["S"],
            values=json.loads(it["values"]["S"]))


def build_online_store(feature_group_name: str, *, enabled: bool | None = None,
                       client=None) -> OnlineStore:
    """Disabled unless BOTH the group opts in and the platform flag is on."""
    if enabled is None:
        enabled = os.environ.get("ENABLE_AI_ONLINE_FEATURE_STORE", "false").lower() == "true"
    if not enabled:
        return DisabledOnlineStore()
    table = os.environ.get("AI_ONLINE_FEATURE_TABLE", "")
    if not table:
        return DisabledOnlineStore()
    return DynamoDbOnlineStore(table, client=client)
