# Power BI on Athena — Import baseline and the DirectQuery gate

- Session: 13
- Date: 2026-08-15
- Status: **connection steps and model documented; NOT connected — nothing is deployed**

```
S3 Iceberg  →  Glue Data Catalog  →  Athena  →  Power BI (Import)
```

---

## 1. Import is the default, and why

`CLAUDE.md` §8: Import is the lab default; DirectQuery only after a benchmark on latency,
concurrency and cost.

The reason is Athena's billing model. Every visual interaction in DirectQuery — a slicer, a
cross-filter, a drill-down — issues a query and **bills bytes scanned**:

| Mode | Queries/day (10-visual page) | Athena cost driver |
|---|---|---|
| **Import**, refreshed daily | ~10 | one scan per table per refresh |
| **Import**, incremental refresh | ~10, one partition each | smallest possible |
| DirectQuery, 15-min refresh | **~960** | every visual, every refresh, every user |

A dashboard nobody is looking at still costs money in DirectQuery. In Import it costs
nothing between refreshes.

At $5/TB the marts here are small enough that Import refreshes are effectively free, which
makes DirectQuery a decision that has to *earn* its cost rather than a default.

## 2. Connecting Power BI Desktop

### Prerequisites

1. **Amazon Athena ODBC driver** (Simba), installed on the Desktop machine.
2. **AWS credentials** — an SSO/assumed-role profile that can assume `*-athena-bi`. No IAM
   user, no access key (`CLAUDE.md` §3.2).
3. **Network path** to Athena. Athena is a regional public API endpoint, so no VPN is
   needed; but this is the point where a corporate proxy usually intervenes.

### Steps

1. Configure an ODBC DSN:

   | Field | Value |
   |---|---|
   | Data source name | `cdc-lakehouse-athena` |
   | AWS region | `ap-southeast-1` |
   | **Workgroup** | `vannk-dev-core` |
   | Catalog | `AwsDataCatalog` |
   | Schema | `mart` |
   | S3 output location | *(leave blank)* |
   | Authentication | Profile / IAM role `*-athena-bi` |

   **Leave the output location blank on purpose.** The workgroup sets it and
   `enforce_workgroup_configuration = true` overrides anything the client sends. A DSN that
   specifies its own S3 location is either ignored or, on a misconfigured workgroup, writes
   unencrypted results outside the lifecycle rule.

2. Power BI Desktop → **Get Data → ODBC → `cdc-lakehouse-athena`**.
3. Choose **Import**. (DirectQuery is offered; see §5 before selecting it.)
4. Select **views only**:

   - `mart.v_customer_360_certified`
   - `mart.v_account_balance_daily`
   - `mart.v_channel_performance_daily`
   - `mart.v_dim_customer_bi`
   - `mart.dim_date`, `mart.dim_channel`

   Do **not** select `fact_transaction` or any `stream`/`full_cdc` table. The BI role cannot
   read L1/L2 anyway — they will not appear, and if they do, that is a finding (D13-1).

## 3. The star schema in Power BI

```
        dim_date ──┐
                   ├──< v_customer_360_certified >── v_dim_customer_bi
     dim_channel ──┘
                   └──< v_account_balance_daily >──── v_dim_customer_bi
```

| Relationship | Cardinality | Direction |
|---|---|---|
| `dim_date[full_date]` → fact `[business_date]` | one-to-many | single |
| `v_dim_customer_bi[customer_sk]` → fact `[customer_sk]` | one-to-many | single |
| `dim_channel[channel_sk]` → fact `[channel_sk]` | one-to-many | single |

**Single-direction filters, and no bidirectional cross-filtering.** Bidirectional filters
create ambiguous paths as soon as a second fact joins the same dimension, and Power BI
resolves the ambiguity silently — producing numbers that change depending on which visual
is on the page.

**Filter `v_dim_customer_bi` to `is_current = true`** unless the report is explicitly
historical. It is an SCD2 dimension, so it has multiple rows per `customer_id`; the fact
already carries the correct point-in-time `customer_sk` (S10-4), so the relationship is on
the surrogate key and the filter only affects slicer lists.

Mark `dim_date` as the official date table.

## 4. Measures

Full DAX in `docs/KIMBALL_MODEL.md` §6. The two that matter:

```dax
-- ADDITIVE: no special handling
Transaction Amount = SUM(v_customer_360_certified[txn_amount])

-- SEMI-ADDITIVE: LAST over a date axis, never SUM
Closing Balance =
CALCULATE(
    SUM(v_account_balance_daily[closing_balance]),
    LASTDATE(dim_date[full_date])
)
```

**`closing_balance` is the one that goes wrong by default.** Dragging it onto a chart with a
date axis produces an implicit `SUM` across dates — roughly 30× a monthly balance, a number
that does not exist. Summing across *accounts* within one day is correct; across *dates* it
is meaningless. Both are `SUM(closing_balance)`; only the grouping differs.

### The certified/provisional badge

```dax
Data Status =
VAR HasProvisional =
    CALCULATE(COUNTROWS(v_customer_360_provisional), ALLSELECTED())
RETURN IF(HasProvisional > 0, "PROVISIONAL — may still change", "CERTIFIED")

Last Updated = MAX(v_customer_360_provisional[last_updated_at])
```

Place `Data Status` as a card on every page that can show day T. `CERTIFIED` is the closing
number for T-1 and earlier; day T is provisional and **will** change (S09-11, S10-12).

## 5. Incremental refresh

Import over a partitioned Iceberg table is where incremental refresh pays off: only the
changed partitions are re-queried, so bytes scanned stays proportional to what moved.

Parameters (must be named exactly this, `DateTime` type):

| Parameter | Value |
|---|---|
| `RangeStart` | `2026-08-01 00:00:00` |
| `RangeEnd` | `2026-08-15 00:00:00` |

Filter each fact/view on `business_date` between `RangeStart` and `RangeEnd`, then:

- **Archive**: 3 years
- **Incremental refresh**: **3 days**
- **Detect data changes**: `certified_at`
- ~~Only refresh complete periods~~: leave **off** for day T; on for certified-only reports.

**Refresh 3 days, not 1.** Late events change an already-published date: L2's sweep picks
them up, L3 rebuilds the partition, and the mart follows (S08-10, DATA_CONTRACTS §6.2). A
1-day window would never re-read the day a late event landed in, so the dashboard would keep
showing the pre-correction figure indefinitely — and it would look stable, which is worse
than looking wrong.

**Use `certified_at` for change detection, not `built_at`.** `built_at` moves on every run
including no-op reruns, which would force a refresh of unchanged partitions and bill the
scan for nothing.

## 6. Power BI Service and the gateway

Refreshing from the **Service** (not Desktop) requires an **on-premises data gateway** in
standard mode, because the Athena ODBC driver is not a cloud connector.

**No gateway is created in this session.** A gateway is a VM that must be running to serve a
refresh; a lab that refreshes once a day would pay for it 24/7. That contradicts the cost
model in `docs/ATHENA.md` §7, so it is a documented requirement rather than a deployed
component.

For the lab: **refresh from Desktop and publish the result.** For production-like use, the
gateway is the first thing to add — and it belongs on the same metered-window schedule as
the Airflow node.

## 7. What is NOT tested

Everything in this document is **`NOT_TESTED`**. Sessions 02–13 are unapplied: there is no
Glue catalog, no Athena workgroup and no data, so no connection has been made.

Specifically unverified: that the ODBC driver authenticates with an assumed role as
described, that the views appear in the navigator, that incremental refresh partitions map
onto Iceberg partitions as expected, and the actual bytes scanned per refresh.

The acceptance criterion is "Power BI Desktop connection steps **reproducible**" — the steps
are written to be followed, but they have not been walked. Claiming otherwise would be
inventing a result.

## 8. When to leave Athena

Per `docs/ATHENA.md` §8, and worth repeating here because BI is where the pain shows first:

**Fix partition pruning before reaching for a bigger engine.** A dashboard that feels slow
because a visual scans the whole table will also be slow — and more expensive — on Redshift.
Check the pruning ratio (`scripts/athena-benchmark.sh`) before opening the 13B question.
