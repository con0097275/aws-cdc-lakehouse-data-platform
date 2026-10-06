# Session 10 — mutation checks

Every guard added this session was verified by REVERTING it and confirming the test fails.

| # | Guard reverted | Result |
|---|---|---|
| K1 | point-in-time join → `AND d.is_current` | **FAILED** `test_fact_resolves_the_version_current_at_event_time`, `test_fanout_from_overlapping_versions_is_detected` |
| K2 | validity upper bound `<` → `<=` | **FAILED** `test_boundary_event_matches_exactly_one_version` |
| K3 | `record_hash` tracks `updated_at` too | **FAILED** `test_unchanged_attributes_do_not_create_a_version` |
| K4 | mart status `min` → `max` rank | **FAILED** `test_mart_takes_the_WEAKEST_status_of_its_inputs` |
| K5 | SEMI_ADDITIVE aggregated with `SUM` | **FAILED** `test_semi_additive_over_time_takes_last_not_sum` |
| K6 | dormant accounts dropped (`left` → `inner`) | **FAILED** `test_dormant_account_still_gets_a_row` |

All six caught. No vacuous test this session — unlike Session 09's M3, which passed with its
guard removed and had to be rewritten.

## K1 is the one worth reading

Reverting the point-in-time join to `is_current` is the single most common Kimball defect,
and it produces **no error**. The failing assertion is deliberately paired with a test that
asserts the WRONG answer explicitly:

```python
def test_is_current_would_have_given_the_wrong_answer(...):
    assert got["T_MORNING"] == "PRIVATE", "demonstrates the bug this join avoids"
```

A customer who was RETAIL in the morning and PRIVATE in the evening has their morning
transaction reported under PRIVATE. Row count right, amounts right, every total reconciles.
Only the segmentation is wrong, and only against a historical report — which is why it
survives review so often.

## A defect found while writing, not by a test

`spark/dimensions/scd2.py` was written with a RAW NULL BYTE in the source where the escape
sequence `\x00` was intended, making the module unimportable:

```
ValueError: source code string cannot contain null bytes
```

Caught immediately by the first test run. Recorded because the NULL sentinel it belongs to
is load-bearing: without it, `('A', NULL)` and `(NULL, 'A')` hash identically and a real
attribute change produces no new SCD2 version.
