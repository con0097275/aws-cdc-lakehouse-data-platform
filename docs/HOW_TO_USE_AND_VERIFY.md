# HOW TO USE AND VERIFY THIS PLATFORM

Every section is **a command you can run and the output that proves it worked**. Where a
step costs money or touches a shared resource, it says so before the command.

- Profile `my-aws-profile` · region `ap-southeast-1` · environment `dev`
- Anything AWS-mutating is **dry-run by default** and needs `--execute`.

---

## 0. Sixty seconds: is the platform telling the truth?

```bash
make test                      # 3,529 tests
python3 scripts/validate-docs.py   # 14 cross-document gates
```

**Expect:** `3407 passed, 0 failed` and `14 passed, 0 failed, 0 skipped`.

If either fails, stop. Everything below assumes these pass.

---

## 1. Governance — who owns what

```bash
python3 -c "
from cdc.governance_plan import compile_inventory
i = compile_inventory()
print(i.config_version(), i.coverage())"
```

**Expect:** `gv1:3d3914603e0fd5a4 {'assets': 84, 'owned': 84, 'owned_pct': 100.0}`

**What it proves:** every governed asset has an owner, a domain and a classification, and
the `config_version` is a content hash — the same inputs always produce the same version, so
a drift is visible as a changed hash rather than as a silent difference.

---

## 2. Lineage — the graph, and whether it is honest

```bash
python3 -c "
from ai.reliability.context import graph_stats
print(graph_stats())"
```

**Expect:** `{'nodes': 81, 'edges': 80, 'column_edges': 67, 'registered_jobs': [...6 jobs...]}`

### Is the column lineage actually true?

This is the important one, because column lineage is what lets a repair be narrow.

```bash
python3 -c "
from ai.reliability.context import get_column_lineage as cl
for c in ('BALANCE', 'STATUS', 'made_up_column'):
    r = cl('eod:oracle_coredb_corebank_account', c)
    print(f\"{c:16s} {r['confidence']:10s} {r['note'][:70]}\")"
```

**Expect:**

```
BALANCE          VALIDATED  ...confirmed against real data by cdc/column_validation.py
STATUS           VALIDATED  ...
made_up_column   ABSENT     no column edge mentions this column
```

**What `VALIDATED` means here, precisely:** a recorded run confirmed **both endpoints of the
edge exist** — either as a top-level Glue column, or as a key genuinely present in that
table's `payload_after` JSON in real rows. CDC layers store the source row as JSON, so a
business column is invisible to a schema lookup while being present in every row.

Re-run that validation yourself (read-only, a few Athena queries):

```bash
python3 - <<'PY'
import json, subprocess, time, sys; sys.path.insert(0,'.')
from ai.reliability.context import _graph_and_impact
from cdc.column_validation import validate_column_edges, summarise, GlueSchemaReader, PayloadJsonReader
def athena(sql):
    q=subprocess.run(["aws","athena","start-query-execution","--work-group","kafka-dev-lab-dev-wg",
        "--query-string",sql,"--query","QueryExecutionId","--output","text"],capture_output=True,text=True).stdout.strip()
    for _ in range(90):
        st=subprocess.run(["aws","athena","get-query-execution","--query-execution-id",q,"--query",
            "QueryExecution.Status.State","--output","text"],capture_output=True,text=True).stdout.strip()
        if st not in ("RUNNING","QUEUED"): break
        time.sleep(1.5)
    if st!="SUCCEEDED": raise RuntimeError(st)
    o=subprocess.run(["aws","athena","get-query-results","--query-execution-id",q,"--query",
        "ResultSet.Rows[1:].Data[*].VarCharValue","--output","json"],capture_output=True,text=True).stdout
    return json.loads(o or "[]")
g,_=_graph_and_impact('dev')
print(summarise(validate_column_edges(g, GlueSchemaReader(), PayloadJsonReader(athena_query=athena))))
PY
```

**Expect:** `28` of `67` validated. The 39 that fail are honest failures, in two classes:

| class | why |
|---|---|
| dbt `int_*` / `stg_*` models | ephemeral — they have no Glue table, so nothing can confirm them |
| some EOD tables | empty, so their `payload_after` keys cannot be read from data |

**Neither is relabelled.** An unvalidated edge stays `DERIVED`, and a `DERIVED` edge cannot
narrow a recovery — which you can see in §6.

---

## 3. Data quality and the publish gate

```bash
python3 scripts/run-dq-athena.py --help          # dry-run by default
```

Read the ledgers directly:

```sql
-- Athena, workgroup kafka-dev-lab-dev-wg
SELECT status, count(*) FROM kafka_dev_lab_dev_ops.dq_result_v2 GROUP BY status;
SELECT * FROM kafka_dev_lab_dev_ops.reconciliation_run ORDER BY started_at DESC LIMIT 5;
SELECT * FROM kafka_dev_lab_dev_ops.dq_quarantine LIMIT 5;
```

**Expect:** 160+ DQ verdicts across `PASS`/`FAIL`, reconciliation rows, and at least one
quarantined record.

**The rule to understand:** a `BLOCKER` severity failure stops certification **and holds the
watermark**. Verify the decision logic without touching data:

```bash
python3 -c "
from cdc.dq_catalog import evaluate_publish
print(evaluate_publish(committed=True, dq_results=[]).outcome)"
```

---

## 4. Airflow runtime lineage

```bash
scripts/airflow-enable-lineage.sh --verify        # read-only
```

**Expect:** image `airflow-openlineage:3.2.2-ol2.20.2`, four `AIRFLOW__OPENLINEAGE__*`
variables, and `OpenLineageProviderPlugin ... 2.20.2` in the listeners column.

To see an actual event, trigger a DAG and scrape the worker pod **while it lives** — there
is no remote logging and `delete_worker_pods` defaults to true. Full recipe in
[`LINEAGE_AND_RECOVERY_TEST_GUIDE.md`](LINEAGE_AND_RECOVERY_TEST_GUIDE.md) §1f.

**Expect in the worker log:**

```
OpenLineageClient will use `console` transport
Successfully emitted OpenLineage `START` event of id ...
```

> **Do not judge this by whether the DAG went green.** Before the fix, the transport was the
> bare string `console`; the provider parses that key as JSON, threw during plugin import,
> and Airflow ran every task successfully while emitting nothing. **Count the events.**

---

## 5. The deterministic recovery loop

```bash
python3 scripts/run-recovery.py --plan-id rp1:45b21f325ca10ec5            # dry run
python3 scripts/run-recovery.py --plan-id rp1:45b21f325ca10ec5 --status
```

**Expect:** the plan's turns, targets and gate decision. Nothing executes without
`--execute`.

---

## 6. The AI copilot

### 6z. Just ask it something

```bash
scripts/reliability-ask.py --samples              # runnable samples
scripts/reliability-ask.py "Who owns EOD ACCOUNT?"
python3 scripts/reliability-ui.py                 # browser, 127.0.0.1 only
```

Both are read-only and plan-only. The UI carries **15 sample questions** as clickable chips;
each one's expected `terminated:` value is tabulated in
[`AI_COPILOT_SAMPLE_QUESTIONS.md`](AI_COPILOT_SAMPLE_QUESTIONS.md) §4c, verified 15/15.

**Restart the UI after changing `ai/reliability/`.** Python does not hot-reload; a stale page
serves the old copilot and looks like an unfixed bug.


### 6a. Resolution refuses rather than guesses

```bash
python3 -c "
from ai.reliability.context import resolve_asset, ResolutionError
print(resolve_asset('EOD ACCOUNT').asset_id)
try: resolve_asset('the thing')
except ResolutionError as e: print('REFUSED:', str(e)[:80])"
```

**Expect:** `eod:oracle_coredb_corebank_account`, then a refusal. **A guessed identifier is
how a recovery lands on the wrong table**, so ambiguity raises instead of picking a winner.

### 6b. The mutation surface is closed

```bash
python3 -c "
from ai.agent_tools.contract import MUTATING_TOOLS, ToolSpec
print(sorted(MUTATING_TOOLS))
S={'type':'object','properties':{}}
try:
    ToolSpec(name='run_shell', version=1, description='d', input_schema=S, output_schema=S,
             authorization_class='recovery_submit', read_only=False,
             requires_approval_ref=True, requires_idempotency_key=True)
except ValueError as e: print('REFUSED:', str(e)[:90])"
```

**Expect:** `['request_recovery_cancel', 'submit_recovery_plan']` then a refusal.

`run_shell`, `execute_sql`, `trigger_any_dag`, `reset_kafka_offset`, `delete_checkpoint`,
`delete_s3`, `terraform_apply`, `spark_submit` **cannot be constructed**. Not reviewed —
constructed.

### 6c. A root cause a rerun cannot fix is refused

```bash
python3 -c "
from ai.reliability.model import DISPOSITION, IncidentCategory as C
for c in (C.BAD_SOURCE_VALUE, C.TRANSFORM_LOGIC_DEFECT, C.DQ_RULE_DEFECT, C.UNKNOWN):
    print(f'{c.value:24s} -> {DISPOSITION[c].value}')"
```

**Expect:** `WAITING_SOURCE_CORRECTION`, `CODE_FIX_REQUIRED`, `RULE_FIX_REQUIRED`,
`NO_AUTOMATIC_RECOVERY`. None of these can produce a plan — construction raises.

### 6d. Column impact does not mean column execution

```bash
python3 -c "
from ai.reliability.registry import CAPABILITIES
for j,c in CAPABILITIES.items():
    print(f'{j:22s} {sorted(k.value for k in c.supports)}')"
```

**Expect:** `eod_build` supports `COB_DATE` but **not** `BUSINESS_KEY_SET`; `reporting`
supports both. So one plan can legitimately rebuild a whole COB for one job and three keys
for another — that is each job's real capability, not an inconsistency.

### 6e. The whole safety surface

```bash
python3 -m pytest spark/tests/test_aigr*.py -q
```

**Expect:** `105 passed`. Cross-reference
[`AI_RECOVERY_SAFETY_MATRIX.md`](validation/AI_RECOVERY_SAFETY_MATRIX.md) — 52 numbered
attacks, each mapped to a test.

---

## 7. Run the whole AI recovery yourself

**This one writes data.** It creates sandbox Iceberg tables, plants a defect, repairs it and
drops them. Cost: a handful of Athena queries.

```bash
python3 scripts/ai-recovery-drill.py            # dry run: prints every step, changes nothing
python3 scripts/ai-recovery-drill.py --execute
```

**Expect, in order:**

```
[1] defect planted. mart total = 1600.0 (correct would be 1000.0)
[3] resolved eod:oracle_coredb_corebank_account
    column lineage confidence = VALIDATED
[4] column lineage VALIDATED -> scope narrowed to COLUMN_LINEAGE
[5] root cause DUPLICATE_CDC_EVENT (BOUNDED_RECOVERY)
[6] plan aigr1:...  turn 0 eod_build EOD_REBUILD COB_DATE
                    turn 1 reporting MART_RERUN  BUSINESS_KEY_SET
[7] policy AUTO_EXECUTE_ALLOWED
[8] execution exe:... SUCCEEDED
[9] mart total 1600.0 -> 1000.0 ; violations 3 -> 0
AIGR10 LIVE PASS
```

**What each line proves:**

| line | proves |
|---|---|
| `[3]` | the asset came from the real 84-asset inventory, not from a model |
| `[4]` | narrowing happened **because** a validation run confirmed the column |
| `[5]` | the category permits recovery; `TRANSFORM_LOGIC_DEFECT` would stop here |
| `[6]` | each job got the smallest scope **it** supports |
| `[7]` | a deterministic policy decided, using no model input |
| `[8]` | the mutation went through the Recovery Control Service, with an approval |
| `[9]` | verification against FULL_CDC, which is the contract — not a threshold |

---

## 8. What to check when something looks wrong

| symptom | first thing to check | usual cause |
|---|---|---|
| a recovery is wider than expected | the `granularity` column in the plan | the job supports only `COB_DATE`; see §6d |
| the copilot refuses to narrow to a column | `get_column_lineage(...)['confidence']` | it is `DERIVED`; validate the mapping, do not relabel it |
| a plan will not build | the root-cause disposition | five categories cannot produce a plan; §6c |
| lineage looks empty | did anything *emit*? | count events; a green DAG proves nothing — §4 |
| a DQ check fails on correct data | the check itself | this is `DQ_RULE_DEFECT`; verify against the contract, not a threshold |
| an EMR job dies "in resolution" | `spark.jars.packages` | this VPC has no NAT; the jar must be staged in S3 |

---

## 9. Things that will refuse you, and should

| refusal | why it is right |
|---|---|
| `EOD_WAITING_SOURCE` | the source has not caught up to the cutoff |
| `EMPTY_WINDOW` | zero events in the window; certifying it would certify nothing |
| `WAITING_SOURCE_CORRECTION` | the value is wrong in the source; a rebuild reproduces it |
| `CODE_FIX_REQUIRED` | the same code gives the same wrong answer, at cost |
| ambiguous asset | a guessed identifier repairs the wrong table |
| unbounded scope | "rebuild everything" is the absence of a scope |
| expired or tampered approval | an approval describes **one** plan hash |

**Do not work around these.** Every one of them has already prevented a wrong answer in this
project's history.

---

## 10. Cost and teardown

```bash
bash scripts/datahub-local.sh down --execute    # stops DataHub, keeps the store
bash scripts/stop-ephemeral.sh                  # what is running and what it costs
```

The metadata and AI programmes have spent **$0** in AWS. EMR Serverless auto-stops; DataHub
is local; the copilot creates no resource.
