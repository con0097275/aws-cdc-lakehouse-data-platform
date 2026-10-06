# Session 13 — mutation checks

| # | Guard reverted | Result |
|---|---|---|
| T1 | `athena_bi` re-attached to `lake_read` (the original defect D13-1) | **FAILED** `test_athena_bi_is_not_attached_to_lake_read`, `test_athena_bi_is_attached_to_mart_read` |
| T2 | explicit S3 `Deny` on the raw CDC layers removed | **FAILED** `test_mart_read_explicitly_denies_l1_and_l2_objects` |
| T3 | raw `full_name` added to the BI customer view | **FAILED** `test_bi_customer_view_does_not_select_raw_pii` |
| T4 | `enforce_workgroup_configuration = false` | **FAILED** `test_workgroup_configuration_is_enforced` |

All four caught. T1 is the important one: it restores the exact defect this session found,
and the test refuses it.

## Two of my own tests were wrong first

1. `test_mart_read_denies_the_glue_catalog_too` matched `'effect = "Deny"'` literally.
   `terraform fmt` aligns `=` and writes `effect  = "Deny"` with two spaces, so the test
   failed on correct code. Now whitespace-tolerant.

2. `test_bi_customer_view_does_not_select_raw_pii` reported a leak that did not exist: the
   SQL comment stripper removed only FULL-LINE `--` comments, so a trailing comment
   (`age_band,  -- ...instead of dob`) left the word `dob` in what the scanner treated as
   code. Now strips trailing comments too.

The same class of mistake appeared in Sessions 09, 11 and 12 — a scanner that cannot tell
code from prose reports the documentation as the bug.
