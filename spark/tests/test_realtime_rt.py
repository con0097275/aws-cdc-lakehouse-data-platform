"""STREAMING_RT: the decisions, tested without Spark, Kafka or AWS.

Each test pins a rule that is easy to get backwards and expensive to get wrong in
production. Where a rule exists because the reference implementation was bitten by the
alternative, the test says so.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "spark" / "realtime"))

from rt_common import (AccountEvent, CustomerDim, PendingEntry, PendingQueue,  # noqa: E402
                       dedup_latest_per_account, detect_dim_changes, enrich,
                       merge_base_and_stream, split_by_completeness)

T0 = datetime(2026, 9, 3, 10, 0, 0, tzinfo=timezone.utc)


def ev(acct: int, cust: int | None, bal: float, ts: datetime) -> AccountEvent:
    return AccountEvent(account_id=acct, customer_id=cust, balance=bal, currency="USD",
                        product_code="SAV", account_status="ACTIVE", source_ts=ts)


DIMS = {100001: CustomerDim(100001, "RETAIL", 1)}


class TestEnrichment:
    def test_a_matched_dimension_marks_the_row_complete(self):
        r = enrich(ev(1, 100001, 10.0, T0), DIMS)
        assert r.segment_code == "RETAIL" and r.branch_id == 1
        assert r.dim_complete is True

    def test_a_missing_dimension_still_publishes_the_balance(self):
        """The row is NOT dropped. A report that silently omits an account is wrong in a
        way nobody can see; a published row flagged incomplete is visible."""
        r = enrich(ev(2, 999999, 55.0, T0), DIMS)
        assert r.balance == 55.0
        assert r.segment_code is None
        assert r.dim_complete is False

    def test_completeness_probes_the_join_not_every_column(self):
        """A dim row present but carrying NULL in the probe column is INCOMPLETE; a dim row
        with NULL in a non-probe column is COMPLETE. Flagging every optional NULL would fill
        the pointer table with work that can never be resolved."""
        dims = {7: CustomerDim(7, None, 3), 8: CustomerDim(8, "SME", None)}
        assert enrich(ev(1, 7, 1.0, T0), dims).dim_complete is False
        assert enrich(ev(2, 8, 1.0, T0), dims).dim_complete is True

    def test_a_null_customer_id_is_incomplete_not_a_crash(self):
        assert enrich(ev(3, None, 1.0, T0), DIMS).dim_complete is False


class TestDedup:
    def test_latest_by_EVENT_time_wins_not_latest_arrival(self):
        """THE rule of the fast path. A retroactive correction for an older effective date
        can arrive after the current one; ordering by arrival would publish the STALE
        balance, silently, with a healthy-looking pipeline."""
        current = ev(1, 100001, 500.0, T0)
        retroactive_arriving_later = ev(1, 100001, 100.0, T0 - timedelta(days=3))
        out = dedup_latest_per_account([current, retroactive_arriving_later])
        assert len(out) == 1
        assert out[0].balance == 500.0

    def test_arrival_order_breaks_ties_within_the_same_event_time(self):
        out = dedup_latest_per_account([ev(1, 100001, 1.0, T0), ev(1, 100001, 2.0, T0)])
        assert out[0].balance == 2.0

    def test_one_row_per_account(self):
        out = dedup_latest_per_account([ev(1, 100001, 1.0, T0), ev(2, 100001, 2.0, T0),
                                        ev(1, 100001, 3.0, T0)])
        assert {r.account_id for r in out} == {1, 2}


class TestMergeView:
    """BASE + STREAM. Getting this backwards resurrects stale fast-path rows after EOD."""

    BASE = [{"account_id": 1, "balance": 10.0, "source_ts": T0 - timedelta(hours=5)},
            {"account_id": 2, "balance": 20.0, "source_ts": T0 - timedelta(hours=5)}]

    def test_stream_wins_for_keys_newer_than_the_watermark(self):
        stream = [{"account_id": 1, "balance": 99.0, "source_ts": T0}]
        out = merge_base_and_stream(self.BASE, stream, watermark=T0 - timedelta(hours=1))
        assert {r["account_id"]: r["balance"] for r in out} == {1: 99.0, 2: 20.0}

    def test_stream_rows_at_or_before_the_watermark_are_ignored(self):
        """After EOD settles a day, that day's fast-path rows are stale by construction."""
        stream = [{"account_id": 1, "balance": 99.0, "source_ts": T0 - timedelta(hours=6)}]
        out = merge_base_and_stream(self.BASE, stream, watermark=T0 - timedelta(hours=1))
        assert {r["account_id"]: r["balance"] for r in out} == {1: 10.0, 2: 20.0}

    def test_an_account_only_in_stream_still_appears(self):
        stream = [{"account_id": 3, "balance": 7.0, "source_ts": T0}]
        out = merge_base_and_stream(self.BASE, stream, watermark=T0 - timedelta(hours=1))
        assert {r["account_id"] for r in out} == {1, 2, 3}

    def test_no_watermark_means_every_stream_row_counts(self):
        stream = [{"account_id": 1, "balance": 99.0, "source_ts": T0 - timedelta(days=9)}]
        out = merge_base_and_stream(self.BASE, stream, watermark=None)
        assert {r["account_id"]: r["balance"] for r in out}[1] == 99.0


class TestRepairedBaseWins:
    """RT-1. A correction repairs BASE without moving the watermark (which is a statement
    about the whole table, not one account). Under the plain rule the repaired row stayed
    masked by the stale STREAM row until the next EOD -- observed live: account 990500 was
    repaired to segment_code=PRIORITY and the view still served dim_complete=false."""

    WM = T0 - timedelta(hours=1)

    def test_a_repair_made_after_the_stream_row_wins(self):
        stream = [{"account_id": 1, "balance": 5.0, "source_ts": T0,
                   "written_at": T0, "dim_complete": False, "segment_code": None}]
        base = [{"account_id": 1, "balance": 5.0, "source_ts": T0,
                 "built_at": T0 + timedelta(minutes=10),      # repaired AFTER
                 "dim_complete": True, "segment_code": "PRIORITY"}]
        out = merge_base_and_stream(base, stream, watermark=self.WM)
        assert out[0]["segment_code"] == "PRIORITY"
        assert out[0]["dim_complete"] is True

    def test_a_newer_stream_row_takes_the_account_back(self):
        """Otherwise the layer freezes at the last correction and stops being realtime."""
        stream = [{"account_id": 1, "balance": 9.0, "source_ts": T0,
                   "written_at": T0 + timedelta(minutes=30), "segment_code": "RETAIL"}]
        base = [{"account_id": 1, "balance": 5.0, "source_ts": T0,
                 "built_at": T0 + timedelta(minutes=10), "segment_code": "PRIORITY"}]
        out = merge_base_and_stream(base, stream, watermark=self.WM)
        assert out[0]["balance"] == 9.0

    def test_missing_provenance_falls_back_to_stream_wins(self):
        """A BASE row with no built_at must not silently outrank live data."""
        stream = [{"account_id": 1, "balance": 9.0, "source_ts": T0}]
        base = [{"account_id": 1, "balance": 5.0, "source_ts": T0}]
        out = merge_base_and_stream(base, stream, watermark=self.WM)
        assert out[0]["balance"] == 9.0


class TestPendingQueue:
    def test_it_refuses_when_full_rather_than_growing(self):
        """Backpressure, not an error. An unbounded in-driver buffer is a driver OOM with
        extra steps -- the reference hit exactly that."""
        q = PendingQueue(max_size=1)
        assert q.add(PendingEntry(ev(1, 9, 1.0, T0), "customer", T0)) is True
        assert q.add(PendingEntry(ev(2, 9, 1.0, T0), "customer", T0)) is False
        assert len(q) == 1

    def test_a_late_dimension_resolves_and_leaves_the_queue(self):
        q = PendingQueue()
        q.add(PendingEntry(ev(1, 100001, 5.0, T0), "customer", T0))
        resolved, still = q.retry_resolvable(DIMS, T0 + timedelta(seconds=5))
        assert [r.account_id for r in resolved] == [1]
        assert still == [] and len(q) == 0

    def test_an_unresolved_entry_ages_and_expires(self):
        q = PendingQueue()
        q.add(PendingEntry(ev(1, 424242, 5.0, T0), "customer", T0))
        assert q.drain_expired(T0 + timedelta(seconds=5)) == []      # still waiting
        expired = q.drain_expired(T0 + timedelta(seconds=25))
        assert len(expired) == 1 and len(q) == 0

    def test_retry_count_increments_so_a_stuck_row_is_visible(self):
        q = PendingQueue()
        q.add(PendingEntry(ev(1, 424242, 5.0, T0), "customer", T0))
        q.retry_resolvable(DIMS, T0 + timedelta(seconds=5))
        q.retry_resolvable(DIMS, T0 + timedelta(seconds=10))
        assert q.drain_expired(T0 + timedelta(seconds=30))[0].retry_count == 2


class TestDimChangeDetection:
    def test_a_changed_value_is_reported(self):
        out = detect_dim_changes({1: CustomerDim(1, "RETAIL", 5)},
                                 {1: CustomerDim(1, "PRIORITY", 5)})
        assert len(out) == 1
        assert out[0]["changed_field"] == "segment_code"
        assert (out[0]["old_value"], out[0]["new_value"]) == ("RETAIL", "PRIORITY")

    def test_a_brand_new_customer_is_NOT_a_change(self):
        """Otherwise AUTOCORRECT re-processes every new customer on every cycle."""
        assert detect_dim_changes({}, {1: CustomerDim(1, "RETAIL", 5)}) == []

    def test_an_unchanged_dimension_reports_nothing(self):
        same = {1: CustomerDim(1, "RETAIL", 5)}
        assert detect_dim_changes(same, dict(same)) == []


class TestSplit:
    def test_both_halves_are_accounted_for(self):
        rows = [enrich(ev(1, 100001, 1.0, T0), DIMS), enrich(ev(2, 777, 2.0, T0), DIMS)]
        full, partial = split_by_completeness(rows)
        assert [r.account_id for r in full] == [1]
        assert [r.account_id for r in partial] == [2]
        assert len(full) + len(partial) == len(rows)


# --------------------------------------------------------------------------- #
# ADR-072: the streaming app and AUTO_CORRECT must FOLLOW the per-table cutover
# --------------------------------------------------------------------------- #

class TestConsumersFollowTheCutover:
    """FULL_CDC became one Iceberg table per source table, and these two consumers did not
    follow it.

    Both read `glue_catalog.kafka_dev_lab_dev_full_cdc.cdc_events` by name with a
    `source_table == '<NAME>'` filter -- the exact pattern per-table storage replaces. Once a
    table's cutover mode is PER_TABLE the ingest stops writing it to the monolith, so this
    read returns the rows that were there before cutover and NOTHING SINCE: a dimension that
    silently stops updating while every job reports SUCCESS.

    AUTO_CORRECT is the worse of the two -- it exists to fix wrong numbers, so re-deriving
    from a stale source is the one failure it must not have.
    """

    CONSUMERS = ("rt_stream_app.py", "rt_autocorrect.py")

    def _body(self, name):
        """Executable body only. `cdc_source.py` DOCUMENTS the old form while explaining that
        it was wrong, and a check that read docstrings would fail on its own explanation."""
        src = (ROOT / "spark" / "realtime" / name).read_text()
        return "\n".join(ln for ln in src.split('"""')[2::2]
                         for ln in ln.splitlines()
                         if not ln.strip().startswith("#"))

    @pytest.mark.parametrize("name", CONSUMERS)
    def test_no_consumer_names_the_monolith(self, name):
        assert "cdc_events" not in self._body(name), (
            f"{name} names the legacy monolith directly. Resolve the table_id through "
            f"cdc_source.full_cdc_for so the read follows the per-table cutover.")

    @pytest.mark.parametrize("name", CONSUMERS)
    def test_no_consumer_uses_the_oracle_only_ordering(self, name):
        """`CAST(position_primary AS DECIMAL(38,0))` is NULL for a SQL Server hex LSN, so
        every row ties and the ranking collapses onto `kafka_offset` -- monotonic only within
        one partition, which CLAUDE.md 5.4 forbids as a comparator."""
        body = self._body(name)
        assert "CAST(position_primary" not in body, f"{name} still casts position to decimal"
        assert "kafka_offset DESC)" not in body, f"{name} tie-breaks on offset across partitions"

    @pytest.mark.parametrize("name", CONSUMERS)
    def test_every_consumer_resolves_through_the_shared_reader(self, name):
        body = self._body(name)
        assert "full_cdc_for(" in body, f"{name} does not resolve its source"
        assert "latest_state_window(" in body, f"{name} does not use the shared ordering"

    def test_the_shared_ordering_is_the_eod_contract_not_a_second_spelling(self):
        """Two spellings of one ordering contract is how they drift. The streaming dimension
        and the certified EOD close must rank identical events identically -- EOD was
        corrected in ADR-065 and these consumers kept the old form, so a dimension could
        disagree with the snapshot about which update was latest."""
        sys.path.insert(0, str(ROOT / "spark" / "realtime"))
        from cdc_source import latest_state_window                 # noqa: E402
        from cdc.eod import order_by_clause                        # noqa: E402
        for engine in ("oracle", "sqlserver"):
            assert order_by_clause(engine) in latest_state_window(engine, "x")

    def test_the_two_engines_get_opposite_treatment(self):
        """Oracle SCN is numeric and needs left-padding; a SQL Server LSN is fixed-width hex
        and needs lowercasing. One 'just pad it' helper would corrupt one of them."""
        sys.path.insert(0, str(ROOT / "spark" / "realtime"))
        from cdc_source import latest_state_window                 # noqa: E402
        assert "lpad(" in latest_state_window("oracle", "x")
        assert "lower(" in latest_state_window("sqlserver", "x")

    def test_a_legacy_mode_read_still_carries_its_mandatory_predicate(self, tmp_path):
        """In LEGACY mode the monolith holds every table, so the predicate is not optional.
        `full_cdc_for` applies it for the caller precisely so it cannot be forgotten --
        forgetting it reads eight tables' events as one and yields a plausible number."""
        sys.path.insert(0, str(ROOT / "spark" / "realtime"))
        from cdc_source import resolve_full_cdc                     # noqa: E402
        r = resolve_full_cdc("oracle.coredb.corebank.customer",
                             plan_path=str(ROOT / "artifacts" / "cdc" / "table-plan.json"))
        if r.legacy:
            assert r.required_predicate, "a legacy read with no predicate reads every table"
            # BOTH sides uppercased: `source_table` carries the engine's own spelling.
            assert "upper(source_table)" in r.required_predicate
        else:
            assert not r.required_predicate, "a per-table read needs no predicate"


class TestLegacyRealtimeRefusesTheUnsafeCombination:
    """The legacy REALTIME entry point applies no `source_table` predicate.

    That was safe when both sides were the legacy monolith. It stopped being safe when
    FULL_CDC became one table per source table: the monolith holds EVERY table, so reading it
    into a per-table REALTIME target writes eight tables' events into one table's window and
    reports success. A plausible number, not an error.
    """

    SRC = ROOT / "spark" / "jobs" / "realtime" / "job.py"

    def test_the_refusal_exists_and_names_the_replacement(self):
        body = self.SRC.read_text()
        assert "REFUSING: --full-cdc names the legacy monolith" in body
        assert "realtime_engine.py --table" in body, (
            "a refusal that does not name the correct command sends the operator looking")

    def test_the_mirror_cases_are_still_allowed(self):
        """Legacy->legacy and per-table->per-table are both self-consistent and must keep
        working; only the CROSS combination is wrong. A guard that refused too much would be
        worked around, and then it would protect nothing."""
        body = self.SRC.read_text()
        guard = body.split("_legacy_source =")[1].split("src = spark.table")[0]
        assert "_legacy_source and _per_table_target" in guard, (
            "the guard must fire on the CROSS case only, not on either side alone")

    def test_the_known_legacy_targets_are_exempted_by_name(self):
        """`rt_account_stream` and `rt_account_base` are the DEPLOYED legacy targets and are
        fed from the monolith by design. Matching on the `rt_` prefix alone would refuse the
        very configuration that is currently running."""
        guard = self.SRC.read_text().split("_per_table_target =")[1].split("if ")[0]
        assert "rt_account_stream" in guard and "rt_account_base" in guard
