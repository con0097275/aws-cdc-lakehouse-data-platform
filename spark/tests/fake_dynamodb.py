"""A hand-written stand-in for a boto3 `dynamodb.Table`.

`moto` is not installed in this environment and is not required. What the DynamoDB adapter
tests actually need to verify is the ADAPTER: the item shapes it builds, the keys it
constructs, and -- most importantly -- that it sends the condition expressions at all. A
fake that records the calls proves those; a mocked AWS service would prove the same things
more slowly and with a dependency.

The fake implements enough real semantics to be worth testing against:

  * `attribute_not_exists(x)` and `x = :val` conditions are actually evaluated, so a
    conditional put genuinely fails when it should;
  * `get_item` returns `{}` for a miss, exactly as boto3 does, rather than raising;
  * every call is recorded, so a test can assert that a guard was sent rather than only
    that the outcome happened to be right.
"""

from __future__ import annotations

import re
from typing import Any

from dynamodb_state import ConditionalCheckFailed


class FakeTable:
    def __init__(self, name: str, key_names: tuple[str, ...]) -> None:
        self.name = name
        self.key_names = key_names
        self.items: dict[tuple, dict[str, Any]] = {}
        self.calls: list[tuple[str, dict]] = []

    # -- helpers ------------------------------------------------------------ #

    def _key_of(self, item: dict[str, Any]) -> tuple:
        return tuple(item[k] for k in self.key_names)

    def _evaluate(self, condition: str | None, existing: dict | None,
                  names: dict[str, str] | None,
                  values: dict[str, Any] | None) -> bool:
        """Evaluate the small subset of ConditionExpression this framework emits.

        Deliberately not a general parser: supporting more than the framework writes would
        let a test pass against a condition the real service would reject.
        """
        if not condition:
            return True
        resolved = condition
        for placeholder, actual in (names or {}).items():
            resolved = resolved.replace(placeholder, actual)

        for clause in [c.strip() for c in resolved.split(" OR ")]:
            if self._clause_true(clause, existing, values):
                return True
        return False

    def _clause_true(self, clause: str, existing: dict | None,
                     values: dict[str, Any] | None) -> bool:
        match = re.fullmatch(r"attribute_not_exists\((\w+)\)", clause)
        if match:
            return existing is None or match.group(1) not in existing

        match = re.fullmatch(r"(\w+)\s*(<=|=)\s*(:\w+)", clause)
        if match:
            attribute, operator, placeholder = match.groups()
            if existing is None or attribute not in existing:
                return False
            expected = (values or {}).get(placeholder)
            if operator == "=":
                return existing[attribute] == expected
            return existing[attribute] <= expected

        raise AssertionError(
            f"FakeTable cannot evaluate condition clause {clause!r}. Extend the fake "
            f"deliberately rather than loosening it -- an unevaluated condition is a "
            f"test that proves nothing.")

    # -- the boto3 Table surface ------------------------------------------- #

    def put_item(self, *, Item: dict, ConditionExpression: str | None = None,
                 ExpressionAttributeNames: dict | None = None,
                 ExpressionAttributeValues: dict | None = None) -> dict:
        self.calls.append(("put_item", {
            "Item": Item, "ConditionExpression": ConditionExpression,
            "ExpressionAttributeNames": ExpressionAttributeNames,
            "ExpressionAttributeValues": ExpressionAttributeValues}))
        key = self._key_of(Item)
        existing = self.items.get(key)
        if not self._evaluate(ConditionExpression, existing,
                              ExpressionAttributeNames, ExpressionAttributeValues):
            raise ConditionalCheckFailed(
                f"{self.name}: condition {ConditionExpression!r} failed")
        self.items[key] = dict(Item)
        return {}

    def get_item(self, *, Key: dict) -> dict:
        self.calls.append(("get_item", {"Key": Key}))
        item = self.items.get(tuple(Key[k] for k in self.key_names))
        return {"Item": dict(item)} if item else {}

    def update_item(self, **kwargs) -> dict:
        self.calls.append(("update_item", kwargs))
        raise AssertionError(
            "the adapter writes whole items via put_item; update_item is not part of the "
            "contract (see DynamoDbRuntimeStateRepository._update)")

    def query(self, *, IndexName: str, KeyConditionExpression: str,
              ExpressionAttributeValues: dict) -> dict:
        self.calls.append(("query", {
            "IndexName": IndexName,
            "KeyConditionExpression": KeyConditionExpression,
            "ExpressionAttributeValues": ExpressionAttributeValues}))
        attribute = KeyConditionExpression.split("=")[0].strip()
        wanted = list(ExpressionAttributeValues.values())[0]
        return {"Items": [dict(i) for i in self.items.values()
                          if i.get(attribute) == wanted]}

    # -- assertions --------------------------------------------------------- #

    def conditions_sent(self) -> list[str | None]:
        return [payload["ConditionExpression"] for op, payload in self.calls
                if op == "put_item"]

    def operations(self) -> list[str]:
        return [op for op, _ in self.calls]


def make_tables() -> dict[str, FakeTable]:
    """The four tables ADR-036 specifies, with their real key schemas."""
    return {
        "execution_table": FakeTable("job-execution", ("execution_id",)),
        "watermark_table": FakeTable("job-watermark-state", ("watermark_key",)),
        "summary_table": FakeTable("summary-config", ("summary_key",)),
        "streaming_table": FakeTable("streaming-app-state", ("job_id", "deployment_id")),
    }
