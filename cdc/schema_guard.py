"""Safe schema-change detection: what may be applied, and what must stop (section F).

    declared contract + observed table  ->  ALLOW | PLAN | BLOCK, per change

Pure. The judgement is the part that must be reviewable, and a module that also applied the
change could not be exercised without a catalog.

THE ASYMMETRY IS THE WHOLE DESIGN
----------------------------------
Adding a nullable column is recoverable: drop it and nothing was lost. Dropping one is not --
the data goes, and no later change brings it back. A rename is a drop and an add wearing one
name, which is why it is the most dangerous of all: it looks like a modification and behaves
like a deletion. A type change rewrites nothing and REINTERPRETS everything already written.
A primary-key change redefines the grain of every derived layer, so an EOD snapshot built
before it and one built after are not comparable even though both look correct.

So: additive changes are allowed automatically, and every destructive one is blocked and
told to go through a migration. "No silent destructive evolution" is the requirement, and the
way to honour it is to make the destructive path require a person, not a flag.
"""
from __future__ import annotations

from dataclasses import dataclass

ALLOW = "ALLOW"     # apply automatically; recoverable
PLAN = "PLAN"       # safe, but wide enough to want a person to see it first
BLOCK = "BLOCK"     # never automatic; needs a migration

ADD_COLUMN = "add_column"
DROP_COLUMN = "drop_column"
RENAME_COLUMN = "rename_column"
TYPE_CHANGE = "type_change"
PK_CHANGE = "primary_key_change"
NULLABILITY_TIGHTENED = "nullability_tightened"

#: Type changes Iceberg itself calls safe: a widening that cannot lose a value. Anything
#: else -- including every narrowing, and every change between families -- reinterprets data
#: that is already written.
SAFE_WIDENINGS = {
    ("int", "bigint"), ("float", "double"),
    ("smallint", "int"), ("smallint", "bigint"), ("tinyint", "smallint"),
    ("tinyint", "int"), ("tinyint", "bigint"),
    ("date", "timestamp"),
}


@dataclass(frozen=True)
class SchemaChange:
    kind: str
    column: str
    verdict: str
    detail: str
    remediation: str = ""

    @property
    def blocking(self) -> bool:
        return self.verdict == BLOCK


@dataclass(frozen=True)
class SchemaVerdict:
    table_id: str
    changes: tuple

    @property
    def blocking(self) -> tuple:
        return tuple(c for c in self.changes if c.blocking)

    @property
    def allowed(self) -> tuple:
        return tuple(c for c in self.changes if c.verdict == ALLOW)

    @property
    def safe(self) -> bool:
        return not self.blocking

    def payload(self) -> dict:
        return {"table_id": self.table_id, "safe": self.safe,
                "changes": [{"kind": c.kind, "column": c.column, "verdict": c.verdict,
                             "detail": c.detail, "remediation": c.remediation}
                            for c in self.changes]}


#: The SAME type spelled by different engines. Athena/Trino report through
#: `information_schema` in their own vocabulary while Iceberg and Spark use another, so a
#: comparison that does not normalise reports every string column as a type change.
#:
#: Measured live: the first run of `cdc-maintenance.py schema` against the cut-over
#: `oracle/ACCOUNT` BLOCKED nine columns on `varchar -> string`, which is one type under two
#: names. A guard that fires on every table is one nobody reads -- worse than no guard,
#: because its silence stops meaning anything.
TYPE_ALIASES = {
    "varchar": "string", "char": "string",
    "integer": "int",
    "real": "float",
    "varbinary": "binary",
    "row": "struct",
    "array": "array", "map": "map",
    "timestamp(6)": "timestamp", "timestamp(3)": "timestamp",
}


def normalise_type_name(sql_type: str) -> str:
    """The full comparable type: engine spellings folded, PARAMETERS KEPT.

    `varchar` -> `string`, but `decimal(18,2)` stays `decimal(18,2)`. Dropping the
    parameters here would silently permit a decimal scale change.
    """
    raw = str(sql_type).strip().lower()
    raw = raw.replace(" with time zone", "").replace(" without time zone", "")
    if "(" not in raw:
        return TYPE_ALIASES.get(raw, raw)
    head, _, rest = raw.partition("(")
    head = head.strip()
    # `timestamp(6)` is a precision, not a parameter that changes the type's meaning here.
    if head in ("timestamp", "time"):
        return head
    return f"{TYPE_ALIASES.get(head, head)}({rest}"


def _base(sql_type: str) -> str:
    """The comparable base type, with engine spellings folded together."""
    raw = str(sql_type).strip().lower()
    # `timestamp(6) with time zone` and friends: keep the head, drop the qualifiers.
    raw = raw.replace(" with time zone", "").replace(" without time zone", "")
    head = raw.split("(")[0].strip()
    return TYPE_ALIASES.get(raw, TYPE_ALIASES.get(head, head))


def _decimal_parts(sql_type: str):
    t = str(sql_type).strip().lower()
    if not t.startswith("decimal("):
        return None
    try:
        p, s = t[len("decimal("):-1].split(",")
        return int(p), int(s)
    except Exception:                                                  # noqa: BLE001
        return None


def classify_type_change(column: str, old: str, new: str) -> SchemaChange:
    """Whether one column's type may change in place."""
    o, n = _base(old), _base(new)
    if normalise_type_name(old) == normalise_type_name(new):
        # Folds ENGINE SPELLINGS (`varchar` and `string` are one type) while KEEPING
        # parameters. Comparing bases alone would make `decimal(18,2)` and `decimal(18,4)`
        # both "decimal" and return "unchanged" -- allowing the scale change that is the
        # single most destructive thing in this module, because it reinterprets every stored
        # value by a factor of ten per digit. That regression existed for one commit; this
        # is why the decimal test is separate from the alias test.
        return SchemaChange(TYPE_CHANGE, column, ALLOW, "unchanged")

    od, nd = _decimal_parts(old), _decimal_parts(new)
    if od and nd:
        # Iceberg permits widening a decimal's PRECISION at the same scale. Changing the
        # scale reinterprets every stored value by a factor of ten per digit -- the numbers
        # stay, their meaning does not.
        if od[1] == nd[1] and nd[0] >= od[0]:
            return SchemaChange(TYPE_CHANGE, column, ALLOW,
                                f"decimal precision widened {old} -> {new}, scale unchanged")
        return SchemaChange(
            TYPE_CHANGE, column, BLOCK,
            f"decimal {old} -> {new} changes the scale or narrows the precision, which "
            f"reinterprets every value already written",
            remediation="add a new column, backfill it, then retire the old one")

    if (o, n) in SAFE_WIDENINGS:
        return SchemaChange(TYPE_CHANGE, column, ALLOW,
                            f"widening {old} -> {new} cannot lose a value")
    return SchemaChange(
        TYPE_CHANGE, column, BLOCK,
        f"{old} -> {new} is not a safe widening: it reinterprets data that is already "
        f"written, and rewrites nothing",
        remediation="add a new column, backfill it, then retire the old one")


def diff_schema(table_id: str, declared: dict, observed: dict, *,
                declared_pk: tuple = (), observed_pk: tuple = ()) -> SchemaVerdict:
    """Compare the CONTRACT against the TABLE and classify every difference.

    `declared` and `observed` are {column: sql_type}. A rename is detected as the pair of a
    drop and an add rather than trusted from a name: nothing in a catalog records that two
    columns are the same column, and guessing from a type match would call two unrelated
    string columns a rename.
    """
    changes = []

    added = [c for c in declared if c not in observed]
    dropped = [c for c in observed if c not in declared]

    # A drop and an add in the same diff is very likely a rename, and a rename is a drop.
    # Named as such because "rename" is what a person will call it, and the point is to say
    # that the thing they think is a rename will delete data.
    if added and dropped:
        for col in sorted(dropped):
            changes.append(SchemaChange(
                RENAME_COLUMN, col, BLOCK,
                f"{col} disappears while {sorted(added)} appear: if this is a rename, note "
                f"that Iceberg RENAME preserves data but a drop-and-add does NOT, and a "
                f"diff cannot tell the two apart",
                remediation="ALTER TABLE ... RENAME COLUMN as a deliberate migration, or "
                            "add the new column and backfill it"))
        for col in sorted(added):
            changes.append(SchemaChange(
                ADD_COLUMN, col, PLAN,
                f"{col} is new, but appears alongside dropped column(s) {sorted(dropped)} "
                f"-- confirm this is not a rename before applying"))
    else:
        for col in sorted(added):
            changes.append(SchemaChange(
                ADD_COLUMN, col, ALLOW,
                f"{col} {declared[col]} is new and nullable; existing rows read NULL, "
                f"which is accurate -- they were written before it existed"))
        for col in sorted(dropped):
            changes.append(SchemaChange(
                DROP_COLUMN, col, BLOCK,
                f"{col} is in the table and not in the contract. Dropping it destroys data "
                f"that no later change restores",
                remediation="remove it from the table only through a reviewed migration; "
                            "leaving it costs storage and nothing else"))

    for col in sorted(set(declared) & set(observed)):
        change = classify_type_change(col, observed[col], declared[col])
        if change.detail != "unchanged":
            changes.append(change)

    if declared_pk and observed_pk and tuple(declared_pk) != tuple(observed_pk):
        changes.append(SchemaChange(
            PK_CHANGE, ",".join(declared_pk), BLOCK,
            f"primary key {list(observed_pk)} -> {list(declared_pk)} redefines the GRAIN of "
            f"every derived layer: an EOD snapshot built before the change and one built "
            f"after are not comparable, and both look correct",
            remediation="rebuild the EOD history for this table under the new key, or "
                        "onboard it as a new table"))
    return SchemaVerdict(table_id=table_id, changes=tuple(changes))
