# artifacts/validation/data-reliability

## drp2-drp6-live-evidence.json — captured 2026-09-30

The first real evidence in this directory. A local DataHub `v1.7.0.1` was started by the
operator and the metadata plane was exercised against it.

| | |
|---|---|
| smoke test | **9/9 aspects published**, first attempt; all six read back; upstream matched |
| governance | **373 aspects for 84 assets**, config_version `gv1:3d3914603e0fd5a4` |
| lineage | 20 connector aspects + 67 graph aspects, all first attempt |
| multi-hop traversal | **18 assets** from the Oracle source; `mart_account_balance_daily` at **hop 7** |
| defects found | 4 — three fixed, one documented as an operational rule |

The traversal is the part worth reading: it confirms in DataHub's own graph index what the
offline model predicted, including REALTIME and EOD appearing together at hop 3 as siblings
of FULL_CDC rather than as a chain.

## Still empty, and honestly so

There is no evidence here for DRP11 scenarios 1–6. Those need a DQ result, a certification
record and an incident — and `ops.dq_result_v2`, `ops.data_incident`, `ops.recovery_plan`
and `ops.recovery_execution` do not exist yet. A file here that was not produced by a real
run would make `DRP11_FULL_RELIABILITY_E2E_PASS` look reachable from a summary.
