# Adding a new datamart

A normal new mart needs **three files** and **no DAG**:

```
dbt/models/marts/<job_id>.sql        the SQL
dbt/models/marts/schema.yml          the tests (one entry appended)
reporting/jobs/<job_id>.yaml         the registration
```

That is the whole thing. The four flow DAGs are generic and expand over the compiled plan
(ADR-039), so adding a mart adds zero DAG files. **If a normal mart seems to need a DAG,
the config is wrong, not the DAG.**

## 1. Scaffold

```bash
scripts/create-datamart.py \
  --job-id mart_channel_engagement_daily \
  --grain channel_sk,business_date \
  --source stg_fact_digital_engagement \
  --columns session_count,event_count,active_minutes \
  --eod-table kafka_dev_lab_dev_snapshot.fact_digital_engagement_daily \
  --affected-key-strategy ALL_KEYS_IN_DATE
```

Dry-run by default; add `--execute` to write. It refuses to overwrite an existing model,
job or schema entry.

## 2. Read what it generated — the step that matters

The defaults are **safe, not necessarily right for your grain**. Three decisions the
generator cannot make for you:

| Decision | Default | Change it when |
|---|---|---|
| `affected_key_strategy` | `PRIMARY_KEY` | your mart **aggregates across keys**. One changed row moves a total every key's row depends on, so rebuilding only the changed key leaves the rest stale. Use `ALL_KEYS_IN_DATE`. |
| `lookback_days` / `late_arrival_days` | 3 | your CDC contract allows a different late window. Do not pick a number that "feels safe" — that is how a correction re-reads a quarter every 30 minutes. |
| `safety_overlap_minutes` | 2 | never set it to 0. An event written microseconds before the recorded watermark falls through the gap between two runs and is never picked up. |

Also check that every measure you carried is **additive** at your grain. A non-additive
column (a distinct count, a closing balance) that gets SUMmed downstream bakes a wrong
answer in where no query can see or fix it.

## 3. Validate

```bash
make reporting-compile                 # the plan compiles, the job registers
DBT_PROFILES_DIR=<dir> make dbt-parse  # the model parses
make reporting-compile-dbt             # dependency-sync: ref() edges from the manifest
make reporting-validate                # config + graph checks, writes nothing
```

`dependency-sync` derives dbt `ref()` edges from the manifest. **Do not hand-author them**
in the job YAML — list only what dbt cannot see: an upstream reporting **job**, or a layer
**gate** such as an EOD close.

## 4. Deploy

Nothing mart-specific. The plan is compiled from Git (ADR-034) and the generic DAGs pick
the new job up on their next run.

## The five flows you get

One model serves all of them; they differ only in `incremental_filter()` (which rows) and
the tier they stamp. Writing one SQL file per mode would make a provisional-vs-certified
variance ambiguous between "late data" and "divergent logic", and only one of those is a
bug worth chasing.

| Flow | Reads | Stamps | Default schedule |
|---|---|---|---|
| `EOD` | `EOD` | `CERTIFIED` | `30 2 * * *` |
| `AUTO_CORRECT` | `FULL_CDC` or `EOD` | `PROVISIONAL_CORRECTED` | `*/30 * * * *` |
| `FULFILL` | `EOD` and `FULL_CDC` | `RECONCILED` | manual |
| `STREAM_BATCH` | `REALTIME` | `PROVISIONAL_NRT` | `*/10 * * * *` |
| `STREAMING_RT` | `FULL_CDC` append | `REALTIME` | **off by default** |

`STREAMING_RT` ships `is_enabled: false`. That keeps the contract compiled, validated and
tested while nothing is deployed — the framework can express a latency requirement without
paying for it. Turning it on is **two** deliberate acts (the flag and `ENABLE_STREAMING_RT`)
because it is the one workload that bills per hour rather than per run.

## When you genuinely need something else

A **non-dbt dependency** — an upstream reporting job, or a gate on a layer close — goes in
the `dependencies:` block as a direct edge. The transitive closure and the turn numbers are
computed by the compiler and must never be authored (ADR-037/038).

A **non-SQL transformation** is the one case that leaves this path: set
`transformation_type` and `execution_engine` accordingly, and expect to write code. That is
rare and should stay rare.
