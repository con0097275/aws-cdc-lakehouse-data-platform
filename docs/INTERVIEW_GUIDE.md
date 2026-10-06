# Interview guide

- Session: 18 · Date: 2026-08-15

Answers you can defend, because each maps to something in `docs/CAPABILITY_MATRIX.md`.
Where a question invites a claim the project cannot support, the answer says so.

---

## CDC ordering — the question that separates candidates

**Q: How do you order CDC events correctly?**

Not by Kafka offset. Offsets increase **within a partition**, so a global sort across
partitions silently reorders events — and produces a plausible, wrong answer.

Order by the source's own commit position, with offset as the last tie-breaker:

```
position_primary → position_secondary → source_ts_ms → kafka_partition → kafka_offset
```

**Q: Oracle and SQL Server both give you a position. Same treatment?**

No — **opposite** treatment, which is the trap:

| | Oracle SCN | SQL Server LSN |
|---|---|---|
| Form | numeric | hex triplet `aaaaaaaa:bbbbbbbb:cccc` |
| Problem | `'9' > '10'` as a string | none — already fixed width |
| Fix | **zero-pad** to 24 chars | **validate** the widths, reject anything else |

A single "just pad it" helper corrupts one of them. And an Oracle-only test suite passes
with the SQL Server path completely broken — different width, different alphabet. I found
that gap by asking what a passing test *didn't* cover.

**Q: Why five components?**

Because the ranking must be **total**. Ties make rebuilds non-deterministic, silently. The
last two — partition and offset — are unique per Kafka record, which is what guarantees no
ties exist. Drop either and reproducibility goes away with no error.

## Idempotency and dedup

**Q: How do you make replay safe?**

`event_id = sha256(topic:partition:offset)`. Deterministic from Kafka coordinates, stable
across replays. A UUID would make every rerun produce new ids and defeat the point.

Everything downstream keys off it: L2's MERGE is idempotent on `event_id`, and the mart's
MERGE keys on the *business* key (`transaction_id`), so reprocessing an overlapping window
updates in place rather than inserting.

**Q: L1 keeps every event. How does the mart avoid duplicates?**

It collapses to grain by source position before writing. I missed this initially — an
updated transaction appears several times in any L1 window, so the first write inserted
*both* rows: two facts for one transaction, double-counted in every sum, in a mart that
still looked internally consistent. Once a row existed, the same duplicates made the MERGE
fail with a cardinality violation instead.

## Iceberg

**Q: Why Iceberg over Delta or Hudi?**

Native Athena and EMR support, Glue as the catalog, and hidden partitioning. For this stack
it was the lowest-friction choice — not a claim that it beats the others generally.

**Q: What bit you?**

`overwritePartitions()`. Dynamic partition overwrite only replaces partitions **present in
the written dataframe**. When a late DELETE removed the last key for a date, the result was
empty, no partition was touched, and a stale ACTIVE row survived in a snapshot the job
reported as `CERTIFIED`.

Fix: overwrite **by filter** on the partition column, so an empty result correctly empties
the partition — plus a check that every surviving row carries the current run's id, because
certifying stale data is worse than failing.

**Q: Small files?**

One file per micro-batch per partition. Scheduled compaction, manifest rewrite, snapshot
expiry, and orphan cleanup **last** with a 72-hour retention floor — removing orphans with a
retention shorter than the longest running job deletes files under a live writer, and the
table only becomes unreadable later.

**Q: Concurrency?**

Iceberg's optimistic concurrency retries; sustained conflicts mean genuine contention. The
EOD DAG orders maintenance **after** the completion marker for that reason.

## Spark

**Q: Streaming or batch?**

Both, and the split is deliberate. Two things are resident: the L1 ingest, and the
STREAMING_RT serving layer (`rt_stream_app.py` on a 30 s trigger, `rt_datamart_app.py`
polling at 45 s). Airflow *supervises* those rather than scheduling them — scheduling "run
the stream every 5 minutes" starts a second consumer of the same topics, and two writers
sharing a checkpoint corrupt it.

Everything else is a scheduled batch, because Structured Streaming would otherwise hold
cluster capacity 24/7 to serve a demo that runs for an hour. In the lab even the resident
apps take a `--run-seconds` budget so a demonstration costs minutes; the reference window is
08:00→20:00. Be precise about that in an interview — the shape is resident, the window is
capped, and those are different claims.

**Q: A fact arrives before its dimension. What does the stream publish?**

The row, flagged — `dim_complete=false`, dims NULL — plus a pointer row recording what it
could not resolve. A slower correction pass rebuilds the dimension from the canonical layer
and repairs it.

Not retry-in-batch: the retry holds the micro-batch open, backpressure builds, and one
missing dimension becomes an outage. Not drop: a report that silently omits an account is
wrong in the way nobody can see.

And the pointer is written **before** the row. If the pointer lands and the row does not, a
correction repairs an account that was never published — harmless. Reverse the order and a
crash publishes a dim-incomplete row with nothing flagging it, and it stays wrong until the
next full rebuild.

**Q: You have Kafka offsets. Why dedup by event time?**

Because the source is a history table. A retroactive correction for an older effective date
can arrive *after* the current row, so ordering by arrival keeps the retroactive one and
publishes a stale balance — silently, with every pipeline green. Event time first; arrival
order only breaks ties within the same event time.

**Q: What happens when one write in a `foreachBatch` fails?**

It raises. Swallowing the exception tells Spark the batch succeeded, so Spark commits the
offsets and the rows are gone — not in the target, not in a queue, nowhere. The reference
system lost roughly 100K offsets exactly that way. Raising leaves the offsets uncommitted
and the data still in Kafka for the retry.

**Q: Checkpoints?**

A top-level sibling of the warehouse prefix, never a child — Iceberg deletes unreferenced
files, and a checkpoint under `warehouse/` is exactly that. `remove_orphan_files` walks
table locations, so it would delete live streaming state. The stream app refuses to start
rather than letting that happen (`spark/realtime/rt_stream_app.py:222`).

## Kafka

**Q: Partitioning?**

Topic key is the canonical PK, so the same PK always lands in the same partition and
per-key ordering is preserved. Verified with a real key-distribution check, not by reading
config.

**Q: 24-hour retention — implication?**

It sets the RPO. Inside the window, recovery is a replay and RPO is 0. Outside it, events
are gone and only a Debezium re-snapshot remains — RPO 24h. That's why "consumer lag
growing" pages while "NRT freshness breached" only tickets: growing lag converts a latency
problem into data loss, on a clock.

## Airflow

**Q: Executor?**

`KubernetesExecutor` on Airflow 3 — one pod per task, reaped on completion, nothing idling.
Not Celery: it needs a broker running 24/7 plus always-on workers, which is pure cost for a
lab whose tasks mostly wait on EMR.

*(The name `KubernetesCeleryExecutor` does not exist — the real, older name is
`CeleryKubernetesExecutor`, and it isn't Airflow 3's default.)*

**Q: EKS?**

No. An EKS control plane bills continuously whether or not a DAG runs; a k3s node bills only
while started. Everything being demonstrated — pod isolation, resource limits, RBAC, remote
logging — is identical. What k3s *doesn't* demonstrate is multi-node scheduling and
control-plane HA, and I'd say that rather than let it pass.

**Q: Why is the EOD chain one DAG?**

Because the order **is** the correctness. Dimensions before facts — a fact built first
resolves every new member to the unknown key, producing a table that's complete, plausible
and wrong about who the rows belong to. Reconciliation before maintenance — maintenance
rewrites and expires files, destroying the evidence an investigation needs. Separate
scheduled DAGs would replace each guarantee with a hope about cron offsets.

## Kimball

**Q: SCD2 — what goes wrong?**

Joining facts on `is_current`. It re-attributes all history to today's values: a customer who
was RETAIL in January and PRIVATE in June has January's transactions reported under PRIVATE.
Row counts right, amounts right, every total reconciles — only the segmentation is wrong, and
only against a historical report.

The fix is a point-in-time join on the validity interval, half-open, so a fact landing
exactly on a boundary matches one version rather than duplicating.

**Q: Surrogate keys?**

Deterministic hash of (natural key, effective_from). A sequence assigns a different key to
the same version on every rebuild, orphaning every fact already written. Hashing makes
collisions possible rather than impossible, so validation checks for them and fails.

**Q: Semi-additive measures?**

`closing_balance` summed across accounts for one day is the bank's position — correct.
The same column summed across 30 days is ~30× a balance that doesn't exist. Both are
`SUM(closing_balance)`; only the grouping differs, and Power BI produces the second by
default the moment you drag it onto a date axis.

## Governance

**Q: How do you stop BI reading raw PII?**

Two layers, and I got the first one wrong initially. IAM denies the BI role L1/L2 — necessary
but **not sufficient**, because two curated tables holding unmasked PII sat under
BI-allowed prefixes. A masked *view* doesn't close that: an Athena view requires read access
to the underlying table, so anyone who can query the view can query the base table.

Fix: explicit Deny on the PII tables at both S3 and Glue-catalog level, plus a
**materialized** masked copy BI reads instead.

**Q: Right to erasure vs immutable audit?**

They genuinely conflict. L2 keeps everything for 7 years as the audit record; L3 `_history`
is opt-in per table because retaining deleted records conflicts with erasure; erasure is
executed at the serving layer. That's a documented position, not a solved problem — a
jurisdiction requiring erasure from the audit layer would need L2 rewriting, which Iceberg
can do and I deliberately didn't automate.

## Cost

**Q: How do you keep a lab under $30/month?**

Feature flags default off, no NAT, EMR Serverless auto-stop, a 10 GiB Athena per-query cutoff
with `enforce_workgroup_configuration=true` — the cutoff is advisory without it — and
metered-window scheduling.

The number that drives behaviour: Airflow left running 24/7 is **$79.97/month, 267% of the
budget**. That's why stopping is a script, not a habit.

**Q: Stop or destroy?**

**Stop is cheaper, not free.** A stopped instance bills no compute; its EBS volume bills
regardless. Only terminate removes it. MSK is the exception — there's no stop, only
keep-or-delete, and deleting loses in-flight events.

## Recovery

**Q: L2 is corrupted. What do you do?**

Re-derive it from L1. No truncate — the MERGE is idempotent on `event_id`, so a rerun
converges, and a destructive step is the last thing you want mid-incident.

Recovery here is **re-derivation, not restoration**: L2 is a pure function of L1, L3 of L2,
marts of L3. There are no data backups above L1 by design — a backup can be stale, a
recomputation can't.

**Q: What does this lab NOT survive?**

Region failure. Single-node Airflow, single region, no automated failover. Component failure
it handles well because every layer is re-derivable. I'd rather say that than imply DR I
haven't built.

## The questions I'd ask myself

**Q: Did you deploy it?**

No. Sessions 02–17 are unapplied behind one gate, and total spend is $0.00 — verified live.
Every session ended at an approval gate rather than self-approving spend. So the
transformation logic is genuinely tested against real Spark and Iceberg; the infrastructure
is validated and planned but never applied.

**Q: What would you do differently?**

Deploy earlier. Sixteen sessions of unexercised infrastructure is too many — the correctness
work held up, but every RTO, every SLO threshold and every cost figure is an estimate that a
single real run would have turned into a measurement.

**Q: What's the best thing in it?**

The mutation testing. Every guard gets reverted to confirm its test fails. It caught a
vacuous test, two guards that were never independently verified, and one case where the
*documented reasoning* was wrong rather than the code. A green suite tells you the tests
pass; it doesn't tell you they'd catch anything.
