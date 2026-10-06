# Runbook — add a new datamart

**When:** a new mart is needed. **Duration:** ~15 min plus review. **Cost:** $0 until deploy.

Full detail: [`docs/NEW_DATAMART.md`](../NEW_DATAMART.md). This is the short form.

## Steps

```bash
make create-datamart ARGS="--job-id mart_x_daily --grain x_sk,business_date \
  --source stg_x --columns amount,count"        # dry run
# add --execute to write
make reporting-compile
DBT_PROFILES_DIR=<dir> make dbt-parse
make reporting-compile-dbt      # dependency-sync: ref() edges from the manifest
make reporting-validate
```

## Before you deploy

Three decisions the generator cannot make for you:

- **`affected_key_strategy`** — default `PRIMARY_KEY`. If your mart **aggregates across
  keys**, use `ALL_KEYS_IN_DATE`: one changed row moves a total every key's row depends on.
- **`lookback_days`** — default 3. Match your CDC contract, not a number that feels safe.
- **`safety_overlap_minutes`** — never 0. An event written microseconds before the recorded
  watermark falls through the gap between two runs.

Also check every measure is **additive at your grain**. A closing balance or a distinct
count that gets SUMmed downstream bakes a wrong answer in where no query can fix it.

## Expected outcome

Three files changed, **no DAG**:

```
dbt/models/marts/<job_id>.sql
dbt/models/marts/schema.yml
reporting/jobs/<job_id>.yaml
```

If you find yourself editing a DAG or anything under `spark/reporting/`, stop — the config
is wrong, not the orchestration. `test_phase16_datamart_template.py` asserts this.
