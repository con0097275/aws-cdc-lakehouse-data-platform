# Session 14 — mutation checks

| # | Guard reverted | Result |
|---|---|---|
| G1 | `NOT_EVALUATED` treated as a pass (blocks on FAIL only) | **FAILED** 3 tests |
| G2 | uniqueness on an empty table returns `PASS` | **FAILED** `test_uniqueness_on_an_empty_table_is_not_evaluated` |
| G3 | PII table re-opened to BI in the registry | **FAILED** `test_datasets_with_pii_are_denied_to_bi` |
| G4 | IAM deny list drifts from the registry | **FAILED** `test_the_iam_deny_list_matches_the_registry` |

All four caught.

## G1 and G2 are the point of this session

They are the same defect from two directions: a check that examined nothing must not report
health. G1 reverts the *policy* (NOT_EVALUATED stops blocking); G2 reverts the *detection*
(an empty table returns PASS instead of NOT_EVALUATED).

Either alone restores the failure mode this engine exists to prevent — a green DQ report the
morning after a job silently produced no output.

## G3 and G4 protect the registry's authority

The registry is only governance if the other artefacts derive from it. G4 in particular
catches the drift that would otherwise be invisible: the Terraform deny list and the YAML
registry disagreeing while both look maintained.
