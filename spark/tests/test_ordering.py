"""Ordering correctness. These tests RUN -- they are the demonstration, not a description."""
import sys, os, pytest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "jobs", "l1_stream"))
from ordering import (normalize_oracle_scn, normalize_sqlserver_lsn, normalize_position,
                      order_key, PositionFormatError, POSITION_TYPE_ORACLE, POSITION_TYPE_SQLSERVER)


class TestOracleSCN:
    def test_the_trap_raw_string_compare_is_wrong(self):
        # This is DATA_CONTRACTS 4.2's whole point, asserted rather than asserted-about.
        assert "9" > "10"                                    # lexicographic: WRONG
        assert int("9") < int("10")                          # numeric: right
        assert normalize_oracle_scn(9) < normalize_oracle_scn(10)   # normalized: right

    def test_fixed_width(self):
        for v in (1, 42, 999999, 281474976710655):
            assert len(normalize_oracle_scn(v)) == 24

    def test_ordering_holds_across_digit_boundaries(self):
        vals = [9, 10, 99, 100, 999, 1000, 123456789]
        norm = [normalize_oracle_scn(v) for v in vals]
        assert norm == sorted(norm), "normalized SCNs must sort in numeric order"

    def test_rejects_non_numeric(self):
        with pytest.raises(PositionFormatError):
            normalize_oracle_scn("00000C38")

    def test_rejects_null(self):
        for bad in (None, ""):
            with pytest.raises(PositionFormatError):
                normalize_oracle_scn(bad)

    def test_refuses_to_truncate(self):
        # Truncating would silently re-sort history -- must fail instead.
        with pytest.raises(PositionFormatError):
            normalize_oracle_scn("1" * 25)


class TestSQLServerLSN:
    def test_canonicalises_case(self):
        assert normalize_sqlserver_lsn("0000002A:00000C38:0001") == "0000002a:00000c38:0001"

    def test_lexicographic_order_is_correct_because_fixed_width(self):
        a = normalize_sqlserver_lsn("0000002A:00000C38:0001")
        b = normalize_sqlserver_lsn("0000002A:00000C38:0002")
        c = normalize_sqlserver_lsn("0000002B:00000000:0000")
        assert a < b < c

    def test_rejects_wrong_part_width(self):
        # Variable width is exactly what breaks lexicographic ordering.
        with pytest.raises(PositionFormatError):
            normalize_sqlserver_lsn("2A:00000C38:0001")

    def test_rejects_wrong_part_count(self):
        with pytest.raises(PositionFormatError):
            normalize_sqlserver_lsn("0000002A:00000C38")

    def test_rejects_non_hex(self):
        with pytest.raises(PositionFormatError):
            normalize_sqlserver_lsn("0000002G:00000C38:0001")


class TestNormalizePosition:
    def test_oracle_pair(self):
        p, s = normalize_position(POSITION_TYPE_ORACLE, "1000", "999")
        assert len(p) == 24 and len(s) == 24 and s < p

    def test_sqlserver_pair_serial_is_numeric_not_lsn(self):
        # The secondary for SQL Server is an event serial NUMBER, not an LSN. Treating it
        # as an LSN would reject every valid record.
        p, s = normalize_position(POSITION_TYPE_SQLSERVER, "0000002A:00000C38:0001", "3")
        assert p == "0000002a:00000c38:0001"
        assert s == "0000000003"

    def test_unknown_type_rejected(self):
        with pytest.raises(PositionFormatError):
            normalize_position("postgres_lsn", "1", "1")


class TestOrderKeyPrecedence:
    def _ev(self, pp, ps, ts, part, off):
        return {"event_order": {"position_primary": pp, "position_secondary": ps,
                                "source_ts_ms": ts, "kafka_partition": part, "kafka_offset": off}}

    def test_primary_position_dominates_everything(self):
        # A LATER commit position wins even with an earlier timestamp and lower offset.
        later = self._ev(normalize_oracle_scn(200), normalize_oracle_scn(1), 1000, 0, 0)
        earlier = self._ev(normalize_oracle_scn(100), normalize_oracle_scn(999), 9999, 9, 9999)
        assert order_key(later) > order_key(earlier)

    def test_burst_falls_through_to_tiebreakers(self):
        # The scenario-2 case: 500 rows sharing ONE commit SCN. Ordering must then be
        # decided by secondary position, then ts, then partition, then offset.
        a = self._ev(normalize_oracle_scn(100), normalize_oracle_scn(1), 5, 0, 10)
        b = self._ev(normalize_oracle_scn(100), normalize_oracle_scn(2), 5, 0, 11)
        assert order_key(a) < order_key(b)

    def test_offset_is_last_resort_only(self):
        # CLAUDE.md 5.4: offset only increases WITHIN a partition, so it must never
        # outrank a source position.
        hi_off_old_scn = self._ev(normalize_oracle_scn(1), normalize_oracle_scn(1), 1, 0, 999999)
        lo_off_new_scn = self._ev(normalize_oracle_scn(2), normalize_oracle_scn(1), 1, 0, 1)
        assert order_key(lo_off_new_scn) > order_key(hi_off_old_scn)


class TestLivePathSecondaryIsChangeLsn:
    """WHICH field `position_secondary` actually carries, pinned.

    Two modules disagreed and only one of them runs. `spark/jobs/l1_stream/ordering.py`
    normalises `event_serial_no` into the secondary slot; `spark/jobs/full_cdc/job.py` --
    the LIVE canonical path, and the one whose output two cut-over tables now serve --
    writes `source.change_lsn`. `l1_stream` is superseded (its own module docstring says the
    Avro path there was never written).

    `change_lsn` is the right choice for the live path, and this is the reason: SQL Server
    gives every ROW CHANGE its own `change_lsn` within a transaction, while `commit_lsn` is
    shared by the whole transaction. Ordering by (commit_lsn, change_lsn) therefore
    separates every row change. `event_serial_no` only distinguishes the constituent parts
    of ONE change -- the delete/insert pair behind an update -- and the SQL Server connector
    emits those as a single `u` envelope, so the platform never sees two events that share
    both LSNs.

    Pinned rather than left implicit: the two modules will keep looking like a contradiction
    to anyone who reads both, and the next person to "fix" it would change the live path.
    """

    def test_the_live_path_writes_change_lsn_as_the_secondary(self):
        import pathlib as _p
        src = (_p.Path(__file__).resolve().parents[1]
               / "jobs" / "full_cdc" / "job.py").read_text()
        i = src.index('alias("position_secondary")')
        window = src[max(0, i - 400):i]
        assert "change_lsn" in window, (
            "the live FULL_CDC path no longer writes change_lsn into position_secondary; "
            "if that is deliberate, the EOD ordering contract in cdc/eod.py must change "
            "with it")

    def test_the_eod_ordering_names_the_same_two_fields(self):
        import sys as _sys, pathlib as _p
        _sys.path.insert(0, str(_p.Path(__file__).resolve().parents[2]))
        from cdc.eod import ORDERING_BY_ENGINE
        assert ORDERING_BY_ENGINE["sqlserver"].columns == ("commit_lsn", "change_lsn")

    def test_commit_lsn_alone_cannot_order_two_rows_of_one_transaction(self):
        """The property that makes change_lsn necessary: a shared commit_lsn is not a
        tie-break, so dropping the secondary would leave two row changes indistinguishable."""
        a = ("0000002a:00000c38:0001", "0000002a:00000c38:0001")
        b = ("0000002a:00000c38:0001", "0000002a:00000c39:0001")
        assert a[0] == b[0], "same transaction"
        assert a[1] != b[1], "different row change -- only the secondary separates them"

