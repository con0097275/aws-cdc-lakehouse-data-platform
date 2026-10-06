# COST — Envelope, Guardrails and Metered Windows

> **⚠ LIVE AND BILLING since 2026-09-03T09:06:04Z — verified, not estimated.**
>
> ```text
> hourly burn      $1.2340/hr    MSK 3 x m7g.large 0.7650 (62%) · 4 EC2 0.4264
>                                EBS+MSK storage 0.0296 · Glue endpoint 0.0130 · NAT 0
> metered on top   EMR Serverless ARM up to $1.2091/hr WHILE A JOB RUNS
>                  Athena $0.0488 per query at the 10 GiB workgroup ceiling
> hard cap         $8.00 (2h)    $15.00 (4h)   <- abort thresholds
> state            292 Terraform resources
> ```
>
> **MSK cannot be stopped, only destroyed.** `auto_destroy_after` is `2026-08-25T00:00:00Z`
> — expired — so every resource is tagged already-expired and ADR-027 provides no
> protection. A wall-clock alarm is the only teardown trigger.
>
> Two corrections to what `scripts/cdc-window-start.sh` prints: its `CDC_WINDOW_START_TIME`
> came from a stale stamp (`2026-08-16T09:53:12Z`) — the true start is `09:06:04Z`; and its
> hardcoded `$1.1244/hr` constant predates the Glue interface endpoint and the
> airflow/reporting resources, understating the burn by ~10%.
>
> **Previous state, 2026-09-03T08:36Z — nothing billed by the hour:**
>
> ```text
> hourly burn      $0.0000/hr    0 EC2 · 0 MSK · 0 NAT · 0 EMR · 0 DynamoDB
> MTD spend        $0.31
> idle floor       ~$8/month     8 customer KMS CMKs; 4 PendingDeletion
> S3 lake          85.6 MB       1,585 objects, RETAINED — the deliverable
> ```
>
> **Priced envelope for the approved but unapplied live window**
> (`docs/E2E_LIVE_TEST_PLAN.md` §5; arithmetic in
> `artifacts/validation/final-e2e/05-cost-envelope.txt`; unit prices
> `docs/PRICE_REFERENCE.md`, collected 2026-08-12):
>
> | | $/hr | share |
> |---|---|---|
> | MSK 3 × `kafka.m7g.large` | 0.7650 | **62.0%** — cannot be stopped, only destroyed |
> | EC2 × 4 | 0.4264 | 34.6% |
> | EBS 150 GiB + MSK storage 60 GiB | 0.0296 | bills while **stopped** too |
> | Glue interface endpoint | 0.0130 | per AZ |
> | **Baseline** | **1.2340** | |
>
> Metered on top: EMR Serverless ARM $1.2091/hr at max capacity; Athena $0.0488 per query
> at the 10 GiB workgroup ceiling.
>
> | Window | Expected | Hard cap | **Abort threshold** |
> |---|---|---|---|
> | 2 h | ~$4.07 | $7.34 | **$8.00** |
> | 4 h | ~$7.95 | $14.18 | **$15.00** |
>
> Set a wall-clock alarm: the tag-filtered budget reports hours late and cannot stop
> anything.
>
> **Budget of record: $100/month** (ADR-030 as amended 2026-09-06). The code and the
> account now agree — `aws budgets describe-budgets` shows both `My Monthly Cost Budget`
> and `kafka-dev-lab-dev-monthly` at 100.0 USD, and `main.tf`'s assert matches. Between
> 2026-08-23 and 2026-09-06 they did NOT agree: tfvars carried 100 against an assert of 50,
> so every single plan emitted a budget warning. A check that always warns is a check
> people stop reading, which is worse than no check.


## Per-table CDC provisioning and routing (Session 43, ADR-063)

**Cost impact of this phase: $0.00.** Nothing was created, planned or applied. The
deliverable is Python, DDL *text*, and tests that run against a local Iceberg catalog with
no AWS credential and no network call. The provisioner is **dry-run by default**.

What each piece would cost once it is deliberately turned on:

| Item | Billing | Estimated at lab volume |
|---|---|---|
| Provisioning 21 per-table targets (8 FULL_CDC + 5 REALTIME + 8 EOD; three tables disable REALTIME) | S3 storage | **$0.00 until a write lands** — an empty Iceberg table is one metadata.json |
| `ops.cdc_event_index` | S3 storage | coordinates only, no payloads — a small fraction of the tables it indexes |
| `--migration-mode dual_write` | EMR Serverless time | one extra MERGE per topic per window; transient duplicate storage ~$0.01/month at the lake's 300 MB. **The cost is EMR time, not storage** |
| `--migration-mode per_table_only` | EMR Serverless time | comparable to today — the same rows, written to 8 tables instead of 1 |
| Routing itself | — | $0.00. A dict lookup per topic, built once per run from the compiled plan |
| Per-table maintenance | EMR Serverless time | 8 compaction jobs instead of 1, each far smaller. Net EMR time is comparable; they can be scheduled independently and skipped when a table is idle |

Typed payloads carry a second copy of the images (struct + JSON) — deliberate, so an
undeclared field is never lost and a **wrong temporal encoding is fixable by a rerun rather
than a re-capture**, because the unconverted wire value is still in the row. At the lake's
300 MB that duplication is cents; a re-capture is a connector restart and a full replay.
No table in the shipped registry is typed today, so it costs nothing yet.

**Target file size is 128 MiB, not Iceberg's 512 MiB default, and that is a cost decision.**
The Phase 0 audit measured a mean data file of 137 KiB against the 512 MiB default: file
size is set by commit frequency (one MERGE per topic per run), not by the property. Setting
512 MiB would not produce larger files; it would only make the number a reader compares
against wrong. Compaction is the actual remedy and is budgeted as EMR time above.

The default remains `legacy_only`, so none of the rows above is billing today.

## Config-driven REALTIME layer (Session 43d, ADR-064)

**Cost impact of this phase: $0.00.** The engine performs the same read and the same write
as the job it generalises; nothing has run on EMR.

| Item | Billing | Estimate at lab volume |
|---|---|---|
| the generic engine | EMR Serverless seconds per scheduled run | unchanged from the job it replaces |
| `ops.realtime_run` ledger | S3 storage | one small row per table per run; coordinates and outcomes, no payload |
| `calendar_day` vs `rolling_hours` | — | **neutral to slightly cheaper.** A calendar window is usually *smaller*: whole days from midnight rather than N x 24h back from mid-morning |
| `incremental_merge` | EMR seconds | cheaper per run on a large window; needs the prune to run. `full_refresh` stays the default |
| retention prune | EMR seconds | a row-level DELETE per run; a no-op under `full_refresh` and still run, because the config *promises* the retention |
| `--rebuild` | EMR seconds | one extra FULL_CDC read. **No Kafka, no connector, no replay** — which is the cost argument for deriving REALTIME rather than re-consuming the topics |

The cadence, not the window, is the cost driver: `docs/COST.md` already records that a
`*/10 * * * *` refresh dominates this platform's spend. `realtime.schedule` now lives in the
registry beside the window it serves, so the two are declared in one place — a 72h window
refreshed daily is not the same product, or the same bill, as one refreshed every ten
minutes, and nothing else recorded the difference.

Nothing here is always-on. Immaterial against the $100/month budget of record.

## Config-driven EOD close (Session 43e, ADR-065)

**Cost impact of this phase: $0.00.** The engine performs the same read and the same write as
the job it generalises; nothing has run on EMR.

| Item | Billing | Estimate at lab volume |
|---|---|---|
| the generic builder | EMR Serverless seconds per close | unchanged from the job it replaces |
| `ops.eod_run` ledger | S3 storage | one small row per table per COB; coordinates, counts and outcomes — no payload |
| the `event_date` prune predicate | Athena/EMR bytes scanned | **reduces** cost: partition pruning on a table that is otherwise scanned in full for every close |
| per-engine ordering | — | neutral. `lpad`/`lower` are per-row string ops, not a shuffle |
| `rolling_history` (default) | S3 storage | one partition per closed COB, bounded by `eod.retention_days` (365) |
| `latest_state` | S3 storage | cheaper — one partition — at the cost of history that can only be recovered by re-closing each date |
| `--fulfill` | EMR seconds | one extra FULL_CDC read per date rebuilt. **No Kafka, no REALTIME window** — which is the cost argument for EOD being a function of FULL_CDC rather than of a window |

The close is scheduled once per business date per table, so cadence is not the driver here
that it is for REALTIME. The dominant cost remains platform uptime, not this layer.

Nothing here is always-on. Immaterial against the $100/month budget of record.

## Safe CDC table onboarding (Session 43f, ADR-066)

**Cost impact of this phase: $0.00.** Every one of the seven commands is local and read-only;
the two that mutate are printed, not run. No table was onboarded.

| Item | Billing | Estimate |
|---|---|---|
| source precheck | — | **$0** — reads files already in Git; opens no connection by construction |
| capture plan / lifecycle ledger | — | $0. A small JSON file in the repo |
| connector capture apply | connector restart | ~30 s RTO, RPO 0 (offsets live in Kafka, ADR-002). No AWS charge; the cost is capture downtime |
| `changes_only` onboarding | — | $0 extra. Existing rows are never read, which is exactly what makes it cheap and what makes it incomplete |
| `incremental_snapshot` | connector CPU while snapshotting | bounded, concurrent with streaming. **Unavailable today** — no `signal.data.collection` |
| `initial_snapshot` | full replay of **every** captured table | the reason section C rules it out as routine: onboarding one table re-reads all eight |
| acceptance smoke | one REALTIME run + one EOD close + one Athena query | EMR seconds, once per onboarded table |

The recurring cost is the smoke run, once per table onboarded. Nothing here is always-on.
Immaterial against the $100/month budget of record.

## Per-table CDC cutover (Session 43g, ADR-067)

**The pilot ran live on 2026-09-10.** Actual spend: **7 EMR Serverless job runs** (2 vCPU x
4 GB, minutes each) and **~30 Athena queries**, every one under the workgroup's 10 GiB cutoff
— the largest scanned 113 KB. Two new Iceberg tables holding 6,641 rows. Cents, not dollars.

Three of the seven EMR runs were failures caused by the defects the pilot found; the retry
cost is part of the real cost of a first live run and is recorded rather than netted out.

| Item | Billing | Estimate |
|---|---|---|
| flag, resolver, gate, benchmark harness | — | **$0** — local and pure |
| backfill dry run | one read of the legacy slice | seconds of EMR per pilot table |
| backfill execute | one read + one MERGE | at the measured 20,390 rows, minutes not hours |
| `DUAL` window | one extra MERGE per topic per window | plus transient duplicate storage, ~$0.01/month at the lake's 300 MB |
| **the benchmark** | 5 scenarios x 2 paths of Athena + EMR | **the largest single cost in this phase**, and the one section I makes non-optional. Athena bills per byte scanned and the legacy path scans the whole monolith per scenario |
| cutover | — | $0. It is a config flag |
| rollback | — | $0. The monolith is still written and was never dropped |

**Measured, not predicted.** The EOD source scan for `oracle/ACCOUNT` fell from 89,193 to
11,231 bytes (**-87.4%**), and data files from 14 to 2 on both pilot tables. But
`sqlserver/digital_event`'s scan improved only **-7.1%**, because it *is* 12,000 of the
monolith's 20,390 rows — **the pruning benefit is inversely proportional to a table's share
of the monolith**. And `auto_correct` on ACCOUNT got **3x slower**: two small files with a
filter that matches almost nothing is not obviously cheaper than fourteen.

Athena bills per byte, so the scan reductions are a real saving on the read side. They are
not the argument for the phase — commit isolation and per-table IAM are (ADR-062) — and at
this lake's size the absolute amounts are trivial either way.

Immaterial against the $100/month budget of record; the dominant cost remains platform uptime.

## CDC table operations (Session 43i, ADR-068)

**Maintenance is metric-driven precisely because it is the cost driver.** A nightly cron over
eight tables does the same work whether or not there is any; a threshold does it when the
files say so.

| Item | Billing | Estimate |
|---|---|---|
| `measure` | Athena **metadata** reads (`$files`, `$manifests`, `$snapshots`) | kilobytes — these are manifests, not table scans |
| `plan` / `governance` / `observe` / `decommission` | — | **$0**, local and pure |
| `rewrite_data_files` | EMR seconds, proportional to data rewritten | the dominant cost, and the one the file-count floor bounds |
| `rewrite_manifests` | EMR seconds | cheap; metadata only |
| `expire_snapshots` | EMR seconds | cheap, and **the action that actually reclaims storage** |
| `remove_orphan_files` | a full listing of the table's location | off by default; needs a measured orphan count |

**A measured saving:** the freshly backfilled pilot tables hold 2 files each. Without the
file-count floor, every cadence would have spent an EMR run per table to eliminate one file —
forever, for no benefit. The floor turns that into `skip`.

**Batches are bounded** (default 2, worst-first). Running all eight at once would put the
maintenance window on the same EMR capacity as the ingest, at its peak.

Immaterial against the $100/month budget of record; the dominant cost remains platform uptime.

## Reporting metadata framework (Session 22, Phase 3)

**Cost impact of Phase 3: $0.00.** Nothing was created, planned or applied. The whole
deliverable is Python, YAML, JSON Schema and unreleased SQL DDL; the test suite runs
locally with no Spark session, no AWS credential and no network call.

Costed but **NOT APPLIED** — these become real only when `modules/reporting_ops` is written
and applied in Phase 4, which needs the ADR-036 sign-off:

| Resource | Billing | Estimated at lab volume |
|---|---|---|
| 4 DynamoDB tables (`job_execution`, `job_watermark_state`, `summary_config`, `streaming_app_state`) | `PAY_PER_REQUEST` | **$0.00 idle**; well under $0.01/month in use |
| S3 prefixes `ops/config/`, `artifacts/dbt/`, `checkpoints/reporting/` | storage | negligible; `plan.json` is a few hundred KB per version |
| Iceberg `ops.*` config mirror + execution history | storage | one small snapshot per config version, one append per completed execution |

On-demand billing is what preserves the property `PROJECT_STATE.md` records for the current
foundation: an unused window costs nothing. There is no hourly component in any of the
above, which is why the reporting framework does not change the idle figure.

`STREAMING_RT` is the one part of the framework with a genuine hourly cost, and it ships
`enable_streaming_rt = false` with a bounded daily window (ADR-041). It is not implemented
in Phase 3.

### STREAM_BATCH cadence (Session 29, Phase 10)

**Cost impact so far: $0.00** — nothing has been submitted; the flow is unit-tested against
fakes. What the schedule *would* cost is a frequency problem, not a size one, which is why
the control is the cadence and the per-run ceiling rather than the cluster shape.

At the pilot's `*/10 * * * *` and its `small` profile (4 vCPU / 16 GB ARM, 0.302276 USD/hr
from §3's derivation), a run that occupies the application for one minute costs
`0.302276 / 60 ≈ $0.005`. The arithmetic that matters is the multiplier:

| Cadence | Runs/day | ~$/day | ~$/month |
|---|---|---|---|
| `*/30` | 48 | 0.24 | **7.25** |
| `*/10` (pilot) | 144 | 0.73 | **21.76** |
| `*/5` | 288 | 1.45 | **43.53** |
| `*/1` | 1440 | 7.25 | **217.64** |

Against a $30/month budget (ADR-030), a one-minute cadence is 7× the entire budget for one
mart, and even the pilot's ten-minute cadence would consume most of it if left running for a
month. **STREAM_BATCH is therefore a metered-window workload like everything else here**:
the DAG ships `is_paused_upon_creation=True`, and the job's `cost_guard` bounds each run at
5 GB scanned and 8 minutes — a run that exceeds either is a defect, not a big day.

Two properties keep the estimate honest rather than optimistic: an empty window still
submits (the bounded read happens in Spark, so the flow cannot know it is empty first), and
a failed run is re-read in full by the next one. Both trade a little spend for not losing
data, which is the right direction at this budget.

### STREAMING_RT window (Session 30, Phase 11)

**Cost impact so far: $0.00** — the application has never been submitted, and it ships
behind two flags that are both off.

This is the only workload in the framework that bills for **elapsed time rather than work
done**. A STREAM_BATCH run that finds an empty window costs seconds; a STREAMING_RT
application that finds an empty stream costs exactly as much as a busy one, because the
capacity is held either way.

At the `small` profile (4 vCPU / 16 GB ARM, 0.302276 USD/hr from §3's derivation):

Monthly figures convert weeks at 52/12; the STREAM_BATCH table above uses 30-day months,
and the two agree where they meet (24 h/day is 217.64 there and 220.06 here — the same
number under two calendar conventions).

| Window | Hours/day | Days/week | ~$/month |
|---|---|---|---|
| 12 h, Mon–Sat (the configured default) | 12 | 6 | **94.31** |
| 8 h, Mon–Fri | 8 | 5 | **52.39** |
| 4 h, Mon–Fri demo window | 4 | 5 | **26.20** |
| always on | 24 | 7 | **220.06** |

**Every row exceeds or nearly exhausts the $30/month budget (ADR-030) on its own**, and the
configured 12 h × 6 window is over 3× the whole budget. That is the arithmetic behind
ADR-041's decision to ship the mode off, and it is why `enable_streaming_rt` and the job's
`is_enabled` are separate flags: turning this on is a budget decision, not a config tweak.

For a portfolio demonstration the realistic shape is a **single bounded session** — start
the application, show the latency, stop it — at roughly **$0.30/hour**, tagged
`AutoDestroyAfter` like everything else here. `scripts/verify-destroy.sh` should be run
afterwards: an EMR Serverless job run that outlives the demo is the one way this framework
can quietly spend a month's budget in a weekend.


- Session: 01 — **re-derived 2026-08-13 against the real budget**
- Prices: `docs/PRICE_REFERENCE.md`, collected live from the AWS Pricing API on **2026-08-12** for `ap-southeast-1`
- Raw evidence: `artifacts/validation/session-01/pricing/`
- Budget of record: **$30/month** (ADR-030). The account's `My Monthly Cost Budget`
- Status: `static-validated` — arithmetic only. Nothing here has been observed on a real bill.

> **Re-derived 2026-08-13 (ADR-030).** This document previously sized everything against
> **$80/month**, a figure inherited from
> `kafka-aws-production-lab/terraform/terraform.tfvars:26` and never actually decided. The
> account's budget is **$30**. Every window count below has been recomputed; the
> `kafka.t3.small` branch has been **deleted** because OPEN-03 closed negative.

> All figures are **derived**, not quoted. Every unit price is traceable to a saved
> API response. Re-derive the price list with
> `bash scripts/collect-pricing.sh artifacts/validation/session-01/pricing`, and the
> envelope itself with **`python3 scripts/derive-cost-envelope.py --budget 30`** —
> no total in this document is hard-coded. `CLAUDE.md` §4 forbids hard-coded prices, so
> treat these as arithmetic against a dated price list; re-collect before any spend
> decision made more than ~90 days from the collection date.

---

## 1. The headline finding

**The Kafka platform alone, run continuously as currently configured, costs roughly
20× the entire monthly budget.**

`kafka-aws-production-lab/terraform/terraform.tfvars` specifies 3 × `kafka.m7g.large`
with 100 GiB EBS per broker:

| Component | Quantity | Unit price | Hourly | 730-hour month |
|---|---|---:|---:|---:|
| MSK brokers | 3 × `kafka.m7g.large` | 0.2550 USD/hr | 0.7650 | **558.45** |
| MSK broker storage | 300 GiB | 0.1200 USD/GB-Mo | 0.0493 | **36.00** |
| | | | **0.8143** | **594.45** |

Against a **$30** budget, that is **36 broker-hours per month** — if MSK were the only
resource in the account, which it is not.

**MSK is 50.6 % of the full-stack hourly burn** ($0.7749 of $1.5323/hr). Nothing else is
close; EMR Serverless NRT is next at 19.7 %. Every optimisation below is measured against
that fact — and the reason it is not simply removed is that *managed* Kafka is precisely
what this portfolio project exists to demonstrate. Self-managed Kafka on three `t3.small`
EC2 instances would cost $0.079/hr instead of $0.775/hr, and would throw away IAM
authentication, managed KRaft, and the entire point.

This single fact is what makes every `enable_*` flag in this project load-bearing
rather than decorative, and it is why ADR-027 elevates ephemerality from a
cost-saving habit to an architectural constraint. `reference/COST_PROFILES.md:5`
warns about this driver in prose; no document in the guide package had quantified it.

### 1.1 Two levers that are worth taking before anything else

**Lever 1 — broker storage is over-provisioned by 5×.** Kafka retention is 24 hours
(`log.retention.hours=24`, Gap 13). At lab CDC volumes, 100 GiB per broker is
provisioned for data that is deleted a day later. Reducing to 20 GiB per broker:

| | Storage | Monthly |
|---|---:|---:|
| Current (100 GiB × 3) | 300 GiB | 36.00 |
| Proposed (20 GiB × 3) | 60 GiB | **7.20** |
| Saving | | **28.80/month** |

Two cautions, both material:

- **MSK storage cannot be reduced after creation.** It is a one-way ratchet. Getting
  it right at create time is the only opportunity.
- `enable_storage_autoscaling = true` with `storage_autoscaling_max_gib = 200`
  (`terraform.tfvars:18`) means a traffic spike can silently ratchet to
  3 × 200 GiB = 600 GiB = **$72.00/month, permanently**, consuming 90 % of the
  budget in storage alone. Autoscaling on a bounded lab is a liability, not a
  safeguard. ADR-027 sets `enable_storage_autoscaling = false` and
  `broker_ebs_gib = 20`.

**Lever 2 — a cheaper broker. CLOSED: there isn't one.**

This section previously proposed `kafka.t3.small` at 4.4× cheaper and treated it as the
project's highest-value open question. **OPEN-03 closed negative on 2026-08-13.**

CloudTrail holds the MSK API's own answer, from the day repo A was first applied:

```
2026-07-26T13:01:14Z  CreateCluster  kafka.t3.small   -> BadRequestException
                                                         invalidParameter: instanceType
2026-07-26T13:19:09Z  CreateCluster  kafka.m7g.large  -> SUCCESS
```

The rejection enumerated **every** valid instance type in `ap-southeast-1`.
`kafka.t3.small` is absent — the price list carries it, but the API will not create it.
**Presence in a price list is not proof of availability**, and this is the demonstration.

Of what *is* offered, `kafka.m7g.large` at $0.2550/hr is already the cheapest:

| Broker type | Hourly | Verdict |
|---|---:|---|
| **`kafka.m7g.large`** | **0.2550** | **cheapest offered — chosen by elimination** |
| `kafka.m5.large` | 0.2630 | 3 % dearer; Graviton undercuts it |
| `express.m7g.large` | 0.5100 | **2×** — see below |
| `kafka.t3.small` | 0.0578 | **not offered** |

**OPEN-12 — MSK Express, closed negative.** `express.m7g.large` is exactly **2×**
`kafka.m7g.large`. It bundles broker storage, which at 20 GiB is worth $0.0033/hr — so it
charges $0.2550 to save $0.0033, a **77× loss**. MSK Express targets high-throughput
production workloads; it is the wrong shape for a bounded lab. Evidence:
`artifacts/validation/session-00/aws/open-03-settled.txt`.

**Consequence: the broker line is not negotiable by instance type.** The only remaining
broker lever is *count*, and that is a durability decision, not a cost decision — see
ADR-030, which retains 3 brokers / RF=3 and records the 2-broker fallback at $4.91.

---

## 2. Egress: the Gap 8 decision, costed

`docs/GAP_ANALYSIS.md` §3 identified this as the architecture-breaking gap:
`network.tf:71` declares three private route tables with **no routes at all**.
`CLAUDE.md` §4.2 forbids NAT Gateway by default. Four options, priced:

| Option | Configuration | Hourly | Per GB | 48 h/month | Complies with `CLAUDE.md` §4.2 |
|---|---|---:|---:|---:|---|
| **A** | 12 interface endpoints, 1 AZ | 0.1560 | 0.0100 | 7.49 | ✅ |
| **A-min** | **8 interface endpoints, 1 AZ** | **0.1040** | 0.0100 | **4.99** | ✅ |
| **A-lean** | **1 interface endpoint (Glue) + S3/DynamoDB gateway** | **0.0130** | 0.0100 | **0.62** | ✅ |
| **B** | 1 NAT Gateway, ephemeral | 0.0590 | 0.0590 | 2.83 + data | ❌ needs ADR amendment |
| **C** | 12 interface endpoints, 3 AZ | 0.4680 | 0.0100 | 22.46 | ✅ |
| **D** | IGW from public subnet, zero-inbound SG | **0.0000** | 0.0000 | **0.00** | ✅ (no *inbound* rule) |

Read that table carefully, because the intuitive answer is wrong:

- **Option C — the "safe" full endpoint set — costs $341.64/month at 24/7** and
  keeps billing after MSK is destroyed, because endpoints live with the VPC. It is
  **11× the entire budget** for network plumbing.
- **A NAT Gateway is cheaper per hour than 5 interface endpoints** (0.0590 vs
  0.0650). The common belief that endpoints are always the cheap option is only true
  when you need very few of them.
- **Free egress already exists in this account's proven design.** Repo A's toolbox
  sits in a public subnet with a public IP and a security group with **zero inbound
  rules** (`network.tf:29`, audited in `docs/EXISTING_PLATFORM_AUDIT.md`). Egress
  via Internet Gateway costs nothing. `CLAUDE.md` §3.3 forbids inbound `0.0.0.0/0`;
  it does not forbid a public IP with no inbound path.

### 2.1 Decision: A-lean + D, per workload

The chosen approach (ADR-022) exploits the fact that **different workloads have
different egress needs**, so a single uniform answer overpays:

| Workload | Placement | Egress path | Endpoint cost |
|---|---|---|---|
| Toolbox | public subnet, public IP, zero-inbound SG | IGW | 0.00 |
| Source lab (Oracle / SQL Server containers) | public subnet, public IP, zero-inbound SG | IGW for image pulls; SSM over IGW | 0.00 |
| CDC runtime (Connect + Apicurio) | public subnet, public IP, zero-inbound SG | IGW; MSK reached in-VPC by SG-to-SG on 9098 | 0.00 |
| k3s / Airflow | public subnet, public IP, zero-inbound SG | IGW; EMR Serverless API over IGW | 0.00 |
| **EMR Serverless (Spark)** | **private subnets** | **S3 gateway (free) + Glue interface endpoint** | **0.0130/hr** |

EMR Serverless is the one workload that genuinely cannot use option D: its ENIs
never receive public IPs, so an IGW route does not help them. It needs S3 (gateway,
free), Glue Data Catalog (interface, $0.0130/hr) and MSK (in-VPC, SG-to-SG, no
endpoint). Job logs go to **S3 rather than CloudWatch Logs**, which removes the
`logs` endpoint from the required set.

Net egress cost for a 6-hour window: **$0.078**. Compare option C's $2.81 for the
same window, or $341.64/month at 24/7.

### 2.2 What must be verified in Session 02

This design is `planned`, not `deployed`. Three assumptions carry real risk and each
has a concrete test:

1. **EMR Serverless reaches Glue and S3 with only those two endpoints.** Test: submit
   a trivial Iceberg read job with the endpoint set applied and no NAT. Failure mode
   is a job that hangs on STS or KMS rather than failing fast.
2. **KMS is not needed as an interface endpoint.** SSE-KMS on S3 is executed by the
   S3 service on the caller's behalf, so the client should not call KMS directly. If
   Iceberg or EMR calls KMS for anything else, add the endpoint (+$0.0130/hr).
3. **A public subnet with a public IP and zero-inbound SG is acceptable to the
   security review.** It is already the audited, accepted pattern for repo A's
   toolbox, but extending it to four workloads is a broader exposure and belongs in
   `docs/SECURITY.md` explicitly rather than by inheritance. If it is rejected,
   fall back to A-min ($0.1040/hr) and re-run this arithmetic.

---

## 3. `lab_low_cost` — the default profile

```hcl
enable_athena              = true
enable_redshift_serverless = false
enable_trino               = false
```

### 3.1 Per-window marginal cost (one 6-hour metered window)

| Resource | Spec | Rate | 6 h |
|---|---|---:|---:|
| MSK brokers | 3 × `kafka.m7g.large` | 0.7650/hr | 4.590 |
| MSK storage | 60 GiB (prorated) | 0.1200/GB-Mo | 0.059 |
| Source lab EC2 | `t3.xlarge` (Oracle + SQL Server containers) | 0.2112/hr | 1.267 |
| CDC runtime EC2 | `t3.large` (Connect + Apicurio) | 0.1056/hr | 0.634 |
| k3s / Airflow EC2 | `t3.large` | 0.1056/hr | 0.634 |
| EBS gp3 | 150 GiB across 3 instances (prorated) | 0.0960/GB-Mo | 0.118 |
| EMR Serverless — NRT | ARM 4 vCPU / 16 GB, continuous | 0.302276/hr | 1.814 |
| EMR Serverless — EOD + L3 | ARM 8 vCPU / 32 GB, ~20 min | 0.604552/hr | 0.202 |
| Glue interface endpoint | 1 endpoint, 1 AZ | 0.0130/hr | 0.078 |
| Athena | ~100 queries × ~100 MB | 5.0000/TB | 0.050 |
| Athena — per-query ceiling | 10 GiB cutoff (enforced) | 5.0000/TB | **0.049 max/query** |
| Athena — Power BI **Import**, daily refresh | ~10 queries, one partition each | 5.0000/TB | ~0.001 |
| Athena — Power BI **DirectQuery** (NOT enabled) | ~960 queries/day, 10-visual page @15 min | 5.0000/TB | **see note** |

> **Session 13 note.** Athena has **no idle cost** — the workgroup is free and bills only
> bytes scanned. That is why it is the core engine (ADR-012) at a $30 budget.
>
> The DirectQuery row is deliberately left without a figure: it depends entirely on whether
> the visuals prune. A pruned 10 MB visual costs ~$0.05/day; the same visual scanning a
> whole unpartitioned table costs orders of magnitude more, for identical output. That
> uncertainty is the reason DirectQuery is a gated decision (S13-10) rather than a toggle,
> and the reason `scripts/athena-benchmark.sh` reports bytes scanned rather than latency.
>
> **No Power BI gateway is budgeted or created** (S13-14): it must run continuously to serve
> a refresh, so a lab refreshing once a day would pay for it 24/7.
| **Marginal per 6-hour window** | | | **≈ 9.45** |

EMR Serverless ARM derivation: `4 × 0.052585 + 16 × 0.005746 = 0.302276 USD/hr`;
`8 × 0.052585 + 32 × 0.005746 = 0.604552 USD/hr`. ARM is 20 % cheaper than x86 on
both vCPU and memory (`docs/PRICE_REFERENCE.md` §5), and Spark/Iceberg/Java run on
Graviton without source changes — hence ADR-008's `architecture = ARM64`.

### 3.2 Always-on floor — what bills when the lab is "destroyed"

This is the table that prevents the surprise invoice. Each item survives
`terraform destroy` of the compute stack because it lives in a different module or
a different lifecycle.

| Resource | Quantity | Rate | Was | **Now** |
|---|---|---:|---:|---:|
| S3 lake storage | 10 GiB | 0.0250/GB-Mo | 0.250 | 0.250 |
| KMS customer managed keys | **2** (platform + lake; state → SSE-S3) | 1.0000/key-Mo | 3.000 | **2.000** |
| Secrets Manager secrets | **0** — replaced by SSM SecureString (ADR-031) | 0.4000/secret-Mo | 1.600 | **0.000** |
| SSM SecureString parameters | 4 (Oracle, SQL Server, registry, Grafana) | **free, standard tier** | — | **0.000** |
| CloudWatch alarms | **0 always-on** — now ephemeral with the cluster | 0.1000/alarm-Mo | 0.600 | **0.000** |
| CloudWatch Logs storage | 1 GiB, 3-day retention | 0.0300/GB-Mo | 0.150 | **0.030** |
| **Terraform state bucket** | ~1 MiB versioned | 0.0250/GB-Mo | 0.010 | 0.010 |
| **Floor** | | | **5.60** | **≈ 2.28** |

> **Re-derived 2026-08-13 (ADR-030): floor 5.60 → 2.28, a 59 % cut worth 11.1 % of the
> entire $30 budget — recovered without changing a single design decision.** Four moves:
>
> 1. **Secrets Manager → SSM Parameter Store SecureString: −$1.60.** The largest single
>    item. `CLAUDE.md` §3.6 accepts either store, and repo A already uses SSM SecureString
>    for the Grafana password — the project was about to pay $1.60/month to introduce a
>    *second* secret store alongside one that already worked. Rotation, the feature being
>    paid for, is meaningless for credentials whose containers are destroyed every window.
>    Full reasoning and the security analysis: **ADR-031**.
> 2. **State bucket → SSE-S3: −$1.00.** This **amends ADR-021**, which chose a dedicated
>    CMK for key-policy separation. Sound reasoning, but at $30 that boundary costs 3.3 %
>    of the budget for a bucket with exactly one audience. Platform and lake CMKs stay
>    separate — those audiences genuinely differ (S01-11 stands).
> 3. **Alarms become ephemeral: −$0.60.** Six CloudWatch alarms watching an MSK cluster
>    that exists ~15 hours a month are not observability, they are a subscription. They
>    move inside the platform module's `enable_*` flag.
> 4. **Logs 5 GiB/7 d → 1 GiB/3 d: −$0.12.** Shortens the forensic window; acceptable for
>    supervised lab sessions, and explicitly not a production setting.

> *Superseded history, kept so the arithmetic is auditable:* Session 01 costed two CMKs
> and omitted the state key, giving a floor of 4.60. Session 02 Stage A added the ADR-021
> state CMK, giving **5.61** (5.60 by the script; the 0.01 is rounding on the state
> bucket). ADR-030 then took the four cuts above, giving **2.28**.

The state backend remains the one resource **exempt from the ephemeral lifecycle**
(ADR-027): it outlives every window by design, so `verify-destroy` must assert that it
still *exists*. Only its encryption changed, not its lifecycle.

Two floor items are frequently forgotten and are worth stating plainly:

- **EBS volumes bill while they exist, including on stopped instances.** 150 GiB of
  gp3 left behind by `stop` rather than `destroy` adds **$14.40/month** — tripling
  the floor and consuming 18 % of the budget for idle disks. `make stop-ephemeral`
  is therefore **not** a cost control for this project; only `destroy` is. ADR-027
  makes that explicit.
- **Interface endpoints outlive the workloads that need them** unless bundled into
  the same `enable_*` flag. That is Gap 8's trap and `docs/RISK_REGISTER.md` R10.
  One forgotten Glue endpoint is $9.49/month; a forgotten option-C set is $341.64.

### 3.3 The envelope

```text
budget                        = 30.00 USD/month     <- ADR-030, the real budget
always-on floor               =  2.28 USD/month     <- section 3.2, optimised
available for metered windows = 27.72 USD/month
full-stack hourly burn        =  1.5323 USD/hr

affordable lab time           = 27.72 / 1.5323 ≈ 18.1 hours/month
```

**≈ 18 hours of full-stack lab time per month.** Against the old $80/$5.61 assumption
that figure was ~47 hours, so the real budget buys **about 38 % of what was planned** —
and the floor optimisation is what lifted it from 15.5 hours back to 18.1.

**Think in hours, not windows.** Almost every cost here is hourly, so window *length* is
nearly cost-neutral — six 3-hour windows and three 6-hour windows cost the same. What
changes with length is the number of MSK cold starts (~15–25 min each, unbilled attention
but real wall-clock) and the blast radius of a failed window. Choose length for
operational reasons; choose *total hours* for budget.

Sensitivities, re-derived against the $30 budget and the $2.28 floor:

| Change | Lab hours/month | Comment |
|---|---:|---|
| **Baseline** (3 brokers, optimised floor) | **18.1** | |
| Floor left un-optimised at $5.60 | 15.9 | −12 %; the cost of not reading §3.2 |
| Leave EBS behind (`stop`, not `destroy`) | 14.6 | −19 % for idle disks |
| Broker storage left at 100 GiB × 3 | 15.5 | −14 %; **unrecoverable after create** |
| Storage autoscaled to 200 GiB × 3 | 11.3 | −38 %, permanently |
| Option C endpoint set (12 × 3 AZ) | **negative** | floor alone exceeds the budget |
| 2 brokers / 2 AZ (ADR-030 fallback) | 24.9 | +38 %, at RF=2 — **not the default** |
| MSK left running 24/7 for one week | **0** | $130 — 4.3× the monthly budget in seven days |

The last row is the one to internalise. **There is no configuration of this project that
survives leaving MSK on.** At $0.7749/hr the budget is gone in 39 hours.

### 3.4 The core release spans two months — and that is the right answer

Costing all 16 core sessions at one 6-hour window each totals **≈ $85.03**, nearly 3× the
budget. `docs/SESSION_DEPENDENCY_GRAPH.md` §5 batches them into **5 windows for ≈ $25.97**
(down from 7 windows / $43.92), exploiting three facts:

- six sessions (08, 10, 11, 13, 14, 18) need **no Kafka at all** once L1/L2 data exists;
- the old **W0** `t3.small` probe window is **deleted** — OPEN-03 closed without it;
- **W5 and W6 merge**: session 17 must create infrastructure to prove it can be destroyed,
  which is the same stack session 19's end-to-end acceptance needs.

With the $2.28 floor the core release is **≈ $28.25** — which *technically* fits one month
at $30, leaving $1.75. **That is not enough headroom and the plan does not use it.**

W2 builds L1 and L2 — the most failure-prone work in the project — and a single W2 re-run
costs $6.00. A plan whose first retry breaks the budget is not a plan.

**ADR-030 therefore spreads the core release across two months:**

| Month | Windows | Sessions | Cost | Headroom |
|---|---|---|---:|---:|
| **1** | W1, W2, W3 | 03→07, 09 — the Kafka-dependent chain | **$18.02** | **$11.98** |
| **2** | W4, W5 | 08, 10–15, 17–19 | **$12.51** | **$17.49** |

Month 1 carries the sessions most likely to need a second attempt and gives them room for
**two full W2 re-runs**. Month 2 has enough left over for one optional engine session.

The reasoning generalises: **when a budget tightens, spend the schedule before you spend
the architecture.** A second month costs $2.28 in carrying floor. Reducing MSK to two
brokers to force everything into one month would save $4.91 and cost replication factor 3
— the property that makes this a production-grade Kafka design rather than a demo.

---

## 4. `production_bi_demo` — optional, short-lived

```hcl
enable_athena              = true
enable_redshift_serverless = true
enable_trino               = false
```

| Resource | Spec | Rate | 3 h |
|---|---|---:|---:|
| Redshift Serverless compute | 8 RPU base capacity | 0.4500/RPU-hr → 3.6000/hr | **10.800** |
| Redshift managed storage | 5 GiB | 0.0261/GB-Mo | 0.001 |
| `lab_low_cost` stack (must run concurrently) | 6-hour window | — | 9.450 |
| **One benchmark session** | | | **≈ 20.25** |

**A single 3-hour Redshift benchmark costs $20.25 — 68 % of the entire monthly budget**,
and is 2.2× a full 6-hour lab window on its own. At $30 this is no longer a
"budget it deliberately" decision; it is **most of a month**. Consequences, all of which land in ADR-013:

- Redshift Serverless is enabled for **one** benchmark, never left on. At 24/7 and
  8 RPU it is $2,628/month — **88× the budget**.
- `redshift_max_rpu_hours` usage limit is **mandatory**, with
  `breachAction = deactivate` (not `log`). A logged breach is not a control.
- Base capacity minimum needs live verification at Session 13B — 8 RPU is used here
  and may differ by region. Verify before committing to the benchmark.
- Budget the benchmark deliberately: it consumes two lab windows' worth of money.

---

## 5. `federation_demo` — optional, short-lived

```hcl
enable_athena              = true
enable_redshift_serverless = false
enable_trino               = true
```

| Resource | Spec | Rate | 3 h |
|---|---|---:|---:|
| Trino coordinator | `t3.xlarge` | 0.2112/hr | 0.634 |
| Trino workers | 2 × `t3.large` | 0.2112/hr | 0.634 |
| EBS gp3 | 60 GiB (prorated) | 0.0960/GB-Mo | 0.024 |
| `lab_low_cost` stack | 6-hour window | — | 9.450 |
| **One federation session** | | | **≈ 10.74** |

Trino on k3s is **half the cost of the Redshift benchmark** because it is plain EC2
with no managed-service premium. Note this asymmetry when sequencing the optional
sessions: if only one optional module can be afforded, Trino demonstrates more
engineering for less money, while Redshift demonstrates the more common enterprise
serving pattern. Both are `COULD` in `docs/GAP_ANALYSIS.md` §2 and neither is
required for the core release (`MASTER_PLAN.md:53`).

`allow_multiple_optional_query_engines = false` blocks running both at once
(ADR-026). Combined they are $21.5/window — **72 % of the budget for one session**.

---

## 6. Guardrail variables

Concrete defaults for every guardrail named in `reference/COST_PROFILES.md:56-66`.
These are the values Session 02 implements as Terraform variables with `validation`
blocks so they fail at plan time.

| Variable | Default | Rationale |
|---|---|---|
| `monthly_budget_usd` | **`100`** | **ADR-030 as amended 2026-09-06.** $30 (original) -> $50 (2026-08-23, Airflow does not fit in $30) -> $100 (operator-confirmed; the account already carried it). The $80 the original replaced was inherited from repo A's tfvars and never decided |
| `broker_instance_type` | `kafka.m7g.large` | **OPEN-03 closed** — `kafka.t3.small` is not offered; this is the cheapest available (§1.1) |
| `broker_ebs_gib` | `20` | §1.1 Lever 1; unreducible after create |
| `enable_storage_autoscaling` | `false` | §1.1; a one-way ratchet on a bounded budget |
| `max_emr_vcpu` | `16` | caps a runaway job at ~$0.84/hr ARM |
| `max_emr_memory_gb` | `64` | with the vCPU cap, bounds EMR at ~$1.21/hr |
| `emr_architecture` | `ARM64` | 20 % cheaper on vCPU and memory |
| `emr_job_timeout_minutes` | `120` | `CLAUDE.md` §4.6; also sets the orphan-cleanup floor |
| `emr_auto_stop_idle_minutes` | `15` | `CLAUDE.md` §4.6 |
| `emr_pre_initialized_capacity` | `none` | `CLAUDE.md` §4.5 |
| `athena_bytes_scanned_cutoff_gb` | `10` | $0.05 per query ceiling; catches accidental `full_cdc` scans |
| `athena_results_expiration_days` | `7` | query-results lifecycle |
| `checkpoint_expiration_days` | `14` | must exceed the longest replay window |
| `cloudwatch_log_retention_days` | **`3`** | ADR-030 floor cut; logs storage is $0.03/GB-Mo |
| `redshift_max_capacity_rpu` | `16` | with the usage limit, bounds a benchmark |
| `redshift_max_rpu_hours` | `24` | ≈$10.80 ceiling; `breachAction = deactivate` |
| `trino_max_workers` | `2` | §5 |
| `auto_destroy_after` | `<ISO-8601 timestamp>` | required tag; **not** `manual` as repo A has it |
| `allow_multiple_optional_query_engines` | `false` | ADR-026 |

`auto_destroy_after = "manual"` (`terraform.tfvars:5`) is a required tag holding a
value that encodes no deadline. `CLAUDE.md` §4.11 requires the tag; a real timestamp
makes it actionable by `show-cost-resources` and by a cleanup sweep. Changed to an
explicit timestamp per window.

---

## 7. Cost verification commands

Read-only. `make` targets are implemented in Session 02; the raw commands work today.

```bash
# Identity guard first, always (CLAUDE.md section 2)
aws sts get-caller-identity --profile my-aws-profile --region ap-southeast-1

# What is billable right now
aws kafka list-clusters-v2      --profile my-aws-profile --region ap-southeast-1 \
  --query 'ClusterInfoList[].{Name:ClusterName,State:State}'
aws ec2 describe-instances      --profile my-aws-profile --region ap-southeast-1 \
  --query 'Reservations[].Instances[?State.Name!=`terminated`].[InstanceId,InstanceType,State.Name]'
aws ec2 describe-volumes        --profile my-aws-profile --region ap-southeast-1 \
  --query 'Volumes[].[VolumeId,Size,State]'        # bills even when detached
aws ec2 describe-vpc-endpoints  --profile my-aws-profile --region ap-southeast-1 \
  --query 'VpcEndpoints[].[VpcEndpointId,ServiceName,VpcEndpointType]'
aws ec2 describe-nat-gateways   --profile my-aws-profile --region ap-southeast-1 \
  --query 'NatGateways[?State!=`deleted`].NatGatewayId'   # must always be empty
aws emr-serverless list-applications --profile my-aws-profile --region ap-southeast-1
aws redshift-serverless list-workgroups --profile my-aws-profile --region ap-southeast-1
aws kms list-aliases            --profile my-aws-profile --region ap-southeast-1 \
  --query 'AliasList[?starts_with(AliasName,`alias/kafka`)||starts_with(AliasName,`alias/lake`)]'
aws secretsmanager list-secrets --profile my-aws-profile --region ap-southeast-1 \
  --query 'SecretList[].Name'

# Actual spend month-to-date, by service
aws ce get-cost-and-usage --profile my-aws-profile --region us-east-1 \
  --time-period Start=$(date -u +%Y-%m-01),End=$(date -u +%Y-%m-%d) \
  --granularity MONTHLY --metrics UnblendedCost \
  --group-by Type=DIMENSION,Key=SERVICE
```

The last command is the only one that reports truth rather than intent. Everything
above it is a model; Cost Explorer is the bill. Run it after the first metered
window and record the delta against §3.3 in `IMPLEMENTATION_REPORT.md` — a model
that has never been checked against a bill is a guess.

---

## 7b. Cost of a table (Phase 8, ADR-069)

Onboarding a table is one registry entry, so the *engineering* cost is a line of YAML. The
**runtime** cost is not zero and is worth naming, because a config-driven platform makes it
easy to add tables without anyone deciding to spend:

| per registered table | driver |
|---|---|
| 1 Kafka topic | MSK storage + cross-AZ replication at that table's change rate |
| 3 Iceberg tables (FULL_CDC / REALTIME / EOD) | S3 storage; FULL_CDC grows monotonically |
| 1 nightly EOD close | one bounded EMR Serverless run |
| 1 REALTIME refresh cadence | one EMR run per cadence, unless `realtime: {enabled: false}` |
| maintenance | only when measured metrics say there is work (ADR-068) |

`cdc-table-plan.py` prints a **COST IMPACT CLASS** per table for exactly this reason:
`MEDIUM (realtime window maintained)` versus the LOW of a reference table that disables it.
Three of the ten shipped tables set `realtime: {enabled: false}` — a 72h rolling window over a
table that changes twice a year is a standing cost for noise.

Phase 8 added **two** tables (Oracle `loan`, SQL Server `payment_method`): 2 topics, 6 Iceberg
tables, 2 nightly closes. Against the $100/month budget of record this is immaterial, and none
of it is always-on.

**Verification costs nothing.** `make check-all` — shell lint, doc consistency, registry
validation, plan-freshness and the full test suite — makes no AWS call and needs no
running platform. That is deliberate: a verification surface that needs a deployed lab is one
nobody runs before deploying, and it would bill for the privilege.

**Orchestration is the newest cost lever (ADR-071).** The three config-driven DAGs ship with
`ENABLE_CDC_TABLE_PLATFORM=false`, so they carry no schedule and fire nothing — $0 as
deployed. Once enabled the cost is exactly the jobs they submit, which are the same bounded
EMR Serverless runs an operator was launching by hand; what changes is who types the command,
and how often.

Cadence is the thing to watch: `cdc_realtime` at `*/10` is 144 submissions/day multiplied by
the number of REALTIME-enabled tables. That is why the cron is an env var and why REALTIME is
per-table disableable — three of the ten shipped tables already opt out, and each one that
does removes a task rather than scheduling a no-op.

**One cost the topic gap makes concrete**: a table registered but never given a topic costs
nothing and produces nothing — it is the *silent* failure, not an expensive one. The reverse
(an orphan topic with no registry entry) does cost: partitions, replication and retention on
a stream nobody owns. `cdc-topics.py` reports orphans for that reason, and does not delete
them, because deleting a topic discards data.

The Phase 8 EMR runs themselves — provisioning, two EOD closes — are metered job runs with
auto-stop on and a 25-minute timeout, and the Athena verification queries scanned under 1 KB
each against a workgroup with a 10 GiB cutoff.

---

## 7c. FULL_CDC resident streaming (Phase B, ADR-073/074)

**Computed estimate, not a bill.** The inputs are the live application configuration
(`aws emr-serverless get-application`, 2026-09-17: **ARM64**, `emr-7.2.0`, auto-stop 15 min,
max 16 vCPU / 64 GB), the `SubmissionRequest` default sizing, and the ARM unit prices in
`PRICE_REFERENCE.md` (0.0525850 USD/vCPU-hour, 0.0057460 USD/GB-hour). Memory overhead,
storage beyond the free 20 GB per worker, and per-minute billing minimums are **excluded**,
so real figures run somewhat higher.

| sizing (per app) | vCPU | GB | USD/hr per app | 2 apps/hr | 2 apps/day | 2 apps/30 days |
|---|---|---|---|---|---|---|
| default: driver 1/4 + 2 × executor 2/8 | 5 | 20 | 0.2629 + 0.1149 = **0.3778** | **0.7557** | 18.14 | **~544** |
| minimal: driver 1/2 + 1 × executor 1/2 | 2 | 4 | 0.1052 + 0.0230 = **0.1282** | 0.2563 | 6.15 | ~185 |
| `available_now` drain, default sizing, 5 min | 5 | 20 | 0.0315 per drain | 0.0630 | per drain | — |

Against the **$30/month** lab budget (ADR-030), even the minimal resident sizing is about 6×
the budget, and the default is about 18×. **The registry ships `profile: lab_low_cost`,
every lifecycle DAG ships paused and gated on `ENABLE_FULL_CDC_STREAMING`, and nothing in
this phase turns a resident app on.**

The daily lab window in `full_cdc_streaming_lifecycle.py` (01:00–13:00 UTC, 12 h) would cost
**~$9.07/day** at default sizing with both apps. That is a separate decision, not a default.

Cost drivers this phase adds while OFF: **none**. `ops.streaming_app_state` holds one row per
app. With the 5-minute heartbeat a resident app makes at most 288 state commits a day
(instead of 1,440 at a per-batch write), and those commits count toward the table's
snapshot-expiry work, not toward compute.

## 8. What this document does not cover

- **Data transfer between AZs.** MSK replicates across 3 AZs; cross-AZ traffic is
  billed. At lab CDC volumes it is cents, but it is not zero and it is not modelled
  here. Quantify in Session 05 once topic throughput is known.
- **Free-tier offsets.** This account is well past 12 months old, so no launch free
  tier applies, but some services retain perpetual free tiers (Glue Data Catalog's
  first 1M objects/requests, EMR Serverless's 20 GB storage per worker). Figures
  above ignore them, so they are conservative.
- **Observed cost.** Nothing here has met a real invoice. Every number is
  `static-validated` per `CLAUDE.md` §9.7.
---

## Measured spend and the standing costs that are easy to miss (2026-09-23)

Actual, from Cost Explorer, not estimated:

| service | month to date (01–21 Sep) |
|---|---|
| MSK | $28.50 |
| EC2 compute | $15.37 |
| Tax | $5.30 |
| KMS | $3.98 |
| VPC | $1.93 |
| EMR Serverless | $1.58 |
| **total** | **$58.39** |

Daily was **$0.25–0.29 with the stack down** and **~$27/day with it up** — MSK ~$18.4
(3× `kafka.m7g.large`) plus EC2 ~$8.2 (`t3a.xlarge` source-lab, two `t3.large`, one
`t3.small`). Destroying is what stops the $18/day; *stopping* EC2 does not, because the EBS
volume bills whether the node runs or not.

### The two that accumulate quietly

**KMS keys multiply with every destroy/apply.** Each cycle creates a new lake CMK and leaves
the previous one enabled. By 2026-09-23 the account held **13 customer keys** and the lake
bucket held objects under **7 different CMKs across 5,557 objects** — $13/month for one key's
worth of use, and the direct cause of three failed job runs.

```bash
bash scripts/reencrypt-lake-cmk.sh audit     # read-only: the distribution
```

Converge the objects, then schedule deletion of the keys nothing references. Check before
deleting: a key still protecting objects takes the data with it, and KMS blocks decryption
from the moment deletion is *scheduled*, not from the deletion date.

**An unpaused `datamart_stream_batch` submits a real EMR job every 10 minutes.** Correct for
a production posture; a standing charge in a lab. It is the one DAG worth pausing by reflex
at the end of a window:

```bash
kubectl -n airflow exec deploy/airflow-scheduler -c scheduler -- \
  airflow dags pause datamart_stream_batch
bash scripts/airflow-node.sh stop --execute
```

### What costs nothing while idle

The EMR Serverless application has **no pre-initialized capacity**, so a `STARTED`
application with no job running bills $0. Leave it started; it auto-starts per job and
auto-stops after.
