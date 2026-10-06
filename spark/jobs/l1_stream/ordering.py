"""Source-position normalisation — the heart of the ordering contract.

docs/DATA_CONTRACTS.md §4.2 is normative and easy to get wrong in OPPOSITE directions for
the two engines:

  Oracle SCN is NUMERIC.        '9' > '10' lexicographically, but 9 < 10 numerically.
  SQL Server LSN is HEX,        already fixed-width zero-padded, so lexicographic compare
  a triplet aaaaaaaa:bbbbbbbb:cccc   works -- but ONLY because of that padding.

So the two engines need opposite treatment: Oracle must be zero-padded to become
sortable, SQL Server must be validated to confirm it already is. A single "just pad it"
helper would silently corrupt one of them.

These functions are pure and are unit-tested in spark/tests/test_ordering.py.
"""

from __future__ import annotations

# Oracle SCN is a 48-bit value; 6 bytes -> max 281,474,976,710,655 (15 digits).
# 24 gives generous headroom and keeps every value the same width forever. The width can
# never be changed after data exists: it would re-sort history.
ORACLE_SCN_WIDTH = 24

# SQL Server LSN: 0000002A:00000C38:0001 -> 8:8:4 hex, colon-separated.
SQLSERVER_LSN_PARTS = (8, 8, 4)

POSITION_TYPE_ORACLE = "oracle_scn"
POSITION_TYPE_SQLSERVER = "sqlserver_lsn"


class PositionFormatError(ValueError):
    """Raised when a source position cannot be normalised.

    Deliberately fatal rather than best-effort: a position that cannot be normalised
    cannot be ordered, and silently passing it through would corrupt L2/L3 ordering in a
    way that only shows up as wrong business numbers much later.
    """


def normalize_oracle_scn(scn: str | int | None) -> str:
    """Zero-pad a numeric Oracle SCN to fixed width so string compare == numeric compare."""
    if scn is None or scn == "":
        raise PositionFormatError("oracle scn is null/empty")
    s = str(scn).strip()
    if not s.isdigit():
        raise PositionFormatError(f"oracle scn is not numeric: {s!r}")
    if len(s) > ORACLE_SCN_WIDTH:
        # Truncating would silently reorder history. Refuse instead.
        raise PositionFormatError(
            f"oracle scn {s!r} exceeds {ORACLE_SCN_WIDTH} digits; widening the field "
            "would re-sort existing data"
        )
    return s.zfill(ORACLE_SCN_WIDTH)


def normalize_sqlserver_lsn(lsn: str | None) -> str:
    """Validate and canonicalise a SQL Server LSN triplet.

    Already fixed-width, so this is validation + case normalisation rather than padding.
    Accepts the colon form Debezium emits; rejects anything else loudly.
    """
    if lsn is None or lsn == "":
        raise PositionFormatError("sqlserver lsn is null/empty")
    s = str(lsn).strip().lower()
    parts = s.split(":")
    if len(parts) != 3:
        raise PositionFormatError(f"sqlserver lsn must have 3 colon-separated parts: {lsn!r}")
    out = []
    for part, width in zip(parts, SQLSERVER_LSN_PARTS):
        if len(part) != width:
            raise PositionFormatError(
                f"sqlserver lsn part {part!r} is {len(part)} chars, expected {width}; "
                "variable width breaks lexicographic ordering"
            )
        try:
            int(part, 16)
        except ValueError as exc:
            raise PositionFormatError(f"sqlserver lsn part {part!r} is not hex") from exc
        out.append(part)
    return ":".join(out)


def normalize_position(position_type: str, primary, secondary):
    """Normalise both position components for one event.

    Returns (position_primary, position_secondary) as fixed-width sortable strings.
    """
    if position_type == POSITION_TYPE_ORACLE:
        return normalize_oracle_scn(primary), normalize_oracle_scn(secondary)
    if position_type == POSITION_TYPE_SQLSERVER:
        # secondary is the event serial number (an int), not an LSN -- pad it numerically.
        prim = normalize_sqlserver_lsn(primary)
        if secondary is None or str(secondary).strip() == "":
            raise PositionFormatError("sqlserver event_serial_no is null/empty")
        sec = str(secondary).strip()
        if not sec.isdigit():
            raise PositionFormatError(f"sqlserver event_serial_no not numeric: {secondary!r}")
        return prim, sec.zfill(10)
    raise PositionFormatError(f"unknown position_type: {position_type!r}")


def order_key(event: dict) -> tuple:
    """The full comparison tuple from DATA_CONTRACTS §4.1, in precedence order.

    Used by tests and by any local reconciliation. Spark applies the same precedence via
    an ORDER BY on the struct fields.
    """
    eo = event["event_order"]
    return (
        eo["position_primary"],
        eo["position_secondary"],
        eo["source_ts_ms"],
        eo["kafka_partition"],
        eo["kafka_offset"],
    )
