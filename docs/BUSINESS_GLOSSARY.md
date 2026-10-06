# BUSINESS GLOSSARY

- Phase: DRP7 · Source of truth: `governance/registry/domains.yaml` → `glossary:`
- Git-controlled. A term used by an asset must exist here, or the compile fails.

---

## 1. How it is enforced

`glossary_terms` on an asset is checked against this list at compile time:

> *unknown glossary term(s) X. A term that resolves to nothing is a definition the reader
> cannot look up.*

11 terms are defined; **9 are in use** across the compiled inventory. The two unused ones
(`freshness_slo`, `blast_radius`) are platform vocabulary rather than asset labels.

## 2. Platform terms

**`cob_date`** — Close of business date. The business day a certified figure describes.
Distinct from the wall-clock date a job ran: closing COB D on day D+1 is normal, and a
figure's COB never moves because the job was late.

**`certified_number`** — A figure produced by the EOD flow from a closed, cutoff-bounded
read, that has passed its DQ and reconciliation gates. **Spark exiting 0 is not
certification.**

**`current_state`** — Certified EOD baseline **plus** the REALTIME overlay of changes after
its cutoff. The one governed way to answer "what is true now". Reading either half alone
gives a different and defensible-looking answer.

**`event_window`** — The REALTIME contract in which a table holds every event within a
bounded rolling window. Reading it as current state requires re-ranking by event order.

**`latest_state`** — The REALTIME contract in which a table holds one row per business key,
deletes kept as tombstones. Directly readable as "now", within the window's extent.

**`freshness_slo`** — The lag a consumer may rely on. Distinct from the freshness **SLA** a
check evaluates at: the SLA is what is measured, the SLO is what is promised, and the SLO may
never be tighter than the SLA. The compile refuses that combination.

**`blast_radius`** — The set of assets downstream of a defect, through the lineage graph.
Bounds what a recovery may touch; a plan whose radius exceeds the configured limit requires
approval.

## 3. Business terms

**`account_balance`** — Ledger balance of an account as of a stated point in time or COB.
Carried by `curated:banking_account.balance` and every balance mart.

**`channel_engagement`** — Counts and rates of digital events per channel, per customer, per
day. `curated:fact_digital_engagement_daily`, `mart:mart_channel_engagement_daily`.

**`customer_360`** — One row per customer per COB joining banking and digital measures.
`mart:mart_customer_360_daily`. The join crosses the source-system boundary on
`customer_id`, which is deliberately **not** a foreign key in SQL Server.

**`transaction_monitoring`** — Near-real-time transaction aggregates used for surveillance,
**never certified**. `mart:mart_transaction_monitoring_10m` carries
`certification_policy: realtime_overlay` and `tags: [not-certified]` precisely so the ladder
cannot stamp a number that can still move.

## 4. Where terms are attached

| Term | Assets |
|---|---|
| `cob_date`, `certified_number` | the balance and risk marts, `fact_account_daily_snapshot` |
| `current_state` | `customer`, `account`, `banking_customer` |
| `account_balance` | `account`, `banking_account`, the balance marts |
| `channel_engagement` | `digital_event`, the engagement marts |
| `customer_360` | `customer`, `mart_customer_360_daily` |
| `transaction_monitoring`, `event_window` | `transaction`, the 10-minute mart |
| `latest_state` | `app_user` |

## 5. Adding a term

1. Add it to `governance/registry/domains.yaml` under `glossary:` with a `definition`.
2. Reference it from the asset's `glossary_terms`.
3. `compile_inventory()` — an unknown term fails the compile, so step 1 cannot be forgotten.

A definition is a sentence that distinguishes the term from the thing it is most often
confused with. `cob_date` is defined against the run date; `freshness_slo` against the SLA;
`certified_number` against a green exit code. Definitions that do not do that are decoration.
