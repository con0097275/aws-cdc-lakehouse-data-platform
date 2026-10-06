# Session 15 — mutation checks

| # | Guard reverted | Result |
|---|---|---|
| O1 | a runbook link removed from an alert | **FAILED** `test_every_alert_has_a_runbook_annotation`, `test_every_runbook_anchor_exists` |
| O2 | NRT freshness escalated from ticket to page | **FAILED** `test_provisional_latency_does_not_page` |
| O3 | `NOT_EVALUATED` excluded from the DQ pass denominator | **FAILED** `test_dq_pass_ratio_counts_not_evaluated_in_the_denominator` |
| O4 | `honor_labels` dropped from the Pushgateway scrape | **FAILED** `test_pushgateway_honors_pushed_labels` |

All four caught.

## O2 is the alert-fatigue guard

Escalating NRT freshness to `page` looks like *more* diligence and is the opposite. NRT
output is explicitly provisional; certified numbers come from EOD. Paging a human at 03:00
for a provisional latency target is how a pager gets ignored — and then the completeness
page, which signals actual data loss, gets ignored with it.

`test_pages_are_the_minority` enforces the same property structurally: if most alerts page,
none of them do.

## O3 is S14-3 reappearing in the monitoring layer

Excluding `NOT_EVALUATED` from the denominator would let a pipeline that produced **nothing**
report a 100% DQ pass rate — the vacuous green, one layer up. The checks that did not run
are counted in the denominator and never in the numerator.

## O4 is subtle and total

Without `honor_labels: true`, Prometheus overwrites the `job` label the batch job pushed with
the scrape job's own name. Every pipeline metric collapses into one series called
"pushgateway", per-job alerting silently stops working, and the dashboard still renders —
with one line where there should be twelve.
