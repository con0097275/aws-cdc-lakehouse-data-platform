# Session 09 — mutation checks

Every guard added this session was verified by REVERTING it and confirming the test fails.
A test that passes with the fix removed is not protecting anything — Session 08 shipped
four defects behind exactly that.

| # | Guard reverted | Result |
|---|---|---|
| M1 | `collapse_to_grain()` removed from the transform | **FAILED** `test_an_updated_transaction_yields_one_fact_not_two` |
| M2 | `WHEN MATCHED AND <merge_condition>` → `WHEN MATCHED` | **FAILED** `test_late_nrt_batch_cannot_overwrite_a_certified_row`, `test_out_of_order_update_does_not_regress_the_row` |
| M3 | `WHEN NOT MATCHED THEN INSERT *` added to full-fill | **FAILED** `test_full_fill_does_not_insert_an_unmatched_transaction` |

## M3 caught a vacuous test — the important finding

The first version of M3 **PASSED**, meaning the full-fill duplicate test was not testing
what it claimed:

```
=== M3: let full-fill INSERT ===
2 passed, 13 deselected
```

`_run_full_fill` derives its transaction ids **from the mart itself**, so the source can
only ever contain ids that already exist there. `WHEN NOT MATCHED` is unreachable on that
path, and `test_full_fill_resolves_unknown_sk_without_duplicating_facts` passes whether or
not an INSERT clause is present. It asserts a property that holds for a reason other than
the guard.

Fixed by adding `test_full_fill_does_not_insert_an_unmatched_transaction`, which hands
`merge_full_fill` a source row whose id is **not** in the mart. Re-running M3 against it:

```
=== M3 again ===
FAILED test_full_fill_does_not_insert_an_unmatched_transaction
1 failed, 2 passed
```

The end-to-end test is kept — it still verifies the observable behaviour — but the
no-insert rule is now pinned by a test that can actually fail.
