# "The pipeline was green and the number is still wrong"

Every DAG succeeded. Every task is green. The number in the report is wrong anyway.

This is the case the whole reliability plane exists for, and the expensive part is not
finding the fault — it is **rerunning only what the fault actually touched**. A platform
whose answer to "something is wrong" is *rebuild everything* is one that cannot afford to be
wrong twice.

```bash
python3 scripts/reliability-ui.py        # http://127.0.0.1:8899 — ask in English
scripts/reliability-recover.py --help    # same engine, one command, prints the plan
```

![Asking who owns an asset](images/reliability-who-owns.png)

Read-only and plan-only: **no Recovery Control Service is wired, so nothing asked through the
UI can change data.** Every line of every answer is labelled `FACT`, `DIAGNOSIS`, `PLAN` or
`LIMITATION`, so a measurement is never mistaken for an inference or for something that has
not happened yet — and the full evidence pack is on the page, because *the account of what
happened is the one output always owed*.

---

## The four commands

### 1. What can I name?

```bash
scripts/reliability-recover.py --list-assets
```

```
  Assets you can name. Any distinctive words work — 'EOD ACCOUNT' resolves
  eod:oracle_coredb_corebank_account.

  curated  (21)   banking_account, banking_customer, dim_account, … and 9 more
  eod      (10)   oracle_coredb_corebank_account, oracle_coredb_corebank_customer, …
  mart      (…)   mart_account_balance_daily, mart_account_balance_monthly, …
```

`--asset` takes **how people speak**, not an identifier. This exists because the first
version required the canonical name, which meant you had to already know the answer to the
question you were asking.

### 2. Diagnose and plan. Nothing runs.

![Investigate: evidence read from four ledgers](images/reliability-investigate-evidence.png)


```bash
scripts/reliability-recover.py --asset "EOD ACCOUNT" --date 2026-09-28
```

The evidence comes from four operational ledgers, not from the wording of your request:

```
  WHAT THE LEDGERS SAY
    read      dq_result_v2, reconciliation_run, dq_quarantine, eod_run
    DQ FAIL   completeness.source_commit_ts          BLOCKER  0/0 rows
    DQ FAIL   reconciliation.full_cdc_to_eod_keys    WARN     0/0 rows
    DQ FAIL   completeness.dv_pk_hash                BLOCKER  0/0 rows
    DQ FAIL   uniqueness.one_active_row_per_key      BLOCKER  1/670 rows
    RECON     row_count: 670.0 vs 669.0
    EOD RUN   BUILT_NOT_CERTIFIED eod-2026-09-28-031453Z

  DIAGNOSIS   DUPLICATE_CDC_EVENT  (confidence 0.85, BOUNDED_RECOVERY)
              DQ check uniqueness.one_active_row_per_key failed (1 of 670 rows)
```

**`0/0 rows` is not a pass.** Three of those checks examined nothing. A check that ran against
an empty partition and found no violations is not evidence of health, so the evidence layer
ranks `1/670` above `0/0` and the real finding is not buried under three vacuous ones.
*Unread is never treated as clean.*

One duplicated key, corroborated by a reconciliation showing 670 source rows against 669 in
the target. **Every DAG was green.**

### 3. Narrow it — this is the part that matters

```bash
scripts/reliability-recover.py --asset "EOD ACCOUNT" --date 2026-09-28 \
    --column BALANCE --keys A001,A002,A003
```

```
  column      BALANCE  (VALIDATED)

  PLAN        aigr1:a9e2950418619084   4 turns, 22 actions
    turn 0  eod_build   EOD_REBUILD   COB_DATE           eod:oracle_coredb_corebank_account
    turn 1  eod_build   CERTIFY       COB_DATE           eod:oracle_coredb_corebank_account
    turn 1  eod_build   DQ_RECHECK    COB_DATE           eod:oracle_coredb_corebank_account
    turn 1  eod_build   RECONCILE     COB_DATE           eod:oracle_coredb_corebank_account
    turn 2  reporting   MART_RERUN    BUSINESS_KEY_SET   …curated.banking_account
    turn 2  reporting   MART_RERUN    BUSINESS_KEY_SET   …curated.dim_account        (×6 assets)
    turn 3  reporting   DQ_RECHECK    BUSINESS_KEY_SET   …                           (×6)
    turn 3  reporting   RECONCILE     BUSINESS_KEY_SET   …                           (×6)
```

![The plan, and the policy gate that holds it](images/reliability-plan-policy-gate.png)

**6 of 16 downstream assets, at `BUSINESS_KEY_SET` — three keys, not a whole day.**

Without `--column` and `--keys` the same incident plans every descendant at `COB_DATE`. With
them, the recovery is scoped to the rows that actually moved.

### 4. Hash-lock the plan and attach an approval

```bash
scripts/reliability-recover.py --asset "EOD ACCOUNT" --date 2026-09-28 --execute
```

The plan is content-hashed. An approval binds to that hash and expires after four hours, so
it cannot be replayed against a plan that has since changed.

---

## How "only what broke" is actually decided

Three independent limits, and the **narrowest wins**.

### 1. Lineage decides *which* assets

16 assets depend on `eod:oracle_coredb_corebank_account`. Only 6 of them are in the plan —
the rest are excluded because nothing that changed reaches them.

### 2. Column lineage decides *whether* the scope may narrow at all

```
  column      BALANCE  (VALIDATED)
```

| Confidence | Means | Effect |
|---|---|---|
| `VALIDATED` | both endpoints confirmed against real data by `cdc/column_validation.py` | may narrow to this column |
| `DERIVED` | declared in `entities.yaml` or the dbt manifest, never checked | **refused** — falls back to table level, and says so |
| `ABSENT` | no column edge names it | table level |

28 of 67 column edges are validated today; the other 39 are treated as unconfirmed. **An
impact analysis that is confidently wrong is worse than one that admits its width** — a
recovery narrowed on a column that turns out not to feed what the graph claimed leaves
corrupt rows in production and reports success.

### 3. The job decides *how finely it can rebuild*

Note that turn 0 is still `COB_DATE` while turn 2 is `BUSINESS_KEY_SET`. That is not an
oversight. Granularity is a property of the **job**, not of your request: `eod_build`
declares it can recompute a whole COB date and nothing finer, so asking it for three keys
would be asking for a capability it does not have.

> Column lineage answers **which jobs** are affected.
> `RecoveryCapability` answers **what a job can recompute**.
> Conflating them produces a plan that looks precise and cannot run.

---

## Three more refusals, each of them correct

<table>
<tr>
<td width="50%"><img src="images/reliability-ambiguous.png" alt="An asset named without a question"></td>
<td width="50%"><img src="images/reliability-layer-ambiguity.png" alt="One table existing at four layers"></td>
</tr>
<tr>
<td><b>An asset, but no question.</b> <code>the customer snapshot</code> → <code>AMBIGUOUS</code>.
It still answers everything it <i>can</i> — owner, 18 downstream assets — then says exactly
what is missing and offers the four things it could do instead. A dead end is a bad answer
even when it is a correct one.</td>
<td><b>One table at four layers.</b> <code>oracle_coredb_corebank_customer</code> exists as
<code>src</code>, <code>full_cdc</code>, <code>realtime</code> and <code>eod</code>. A
<b>describing</b> caller is answered across all four and told so; an <b>acting</b> caller is
refused, because the layer decides what gets rewritten. Rebuilding the wrong layer is a
silent, expensive no-op.</td>
</tr>
</table>

## When a rerun is the wrong answer

```bash
scripts/reliability-ask.py "The source system sent a bad value for EOD ACCOUNT on 2026-09-28"
```

```
  PLAN        No plan was built.
              Six root causes cannot produce a plan at all — bad source value, missing
              source event, schema drift, transform logic defect, dq rule defect, and
              unknown. For those a rerun is the wrong answer, so the plan is refused
              rather than offered.

  ANSWER — TERMINATED: WAITING_SOURCE_CORRECTION
    DIAGNOSIS   BAD_SOURCE_VALUE: the defect is in the source. The platform will not write
                the source, and a rebuild would reproduce the value.
    LIMITATION  Recovery resumes once corrected CDC arrives.
```

![A source defect refuses to plan a rerun](images/reliability-refuses-source-defect.png)

14 incident categories are modelled; 8 are recoverable and **6 cannot produce a plan at
all**. Rebuilding from a source that is still wrong reproduces the wrong number and reports
success — the most expensive possible outcome, because it consumes the budget *and* the
evidence.

Likewise `"EOD ACCOUNT for 2026-09-28 is wrong"` terminates `unknown_root_cause`: **"wrong"
is a symptom.** Rebuilding on a guess can destroy the evidence needed to diagnose the real
fault.

---

## Turn order is a safety property, not a schedule

```
turn 0   rebuild the root
turn 1   CERTIFY + DQ_RECHECK + RECONCILE the root      <- barrier
turn 2   rebuild the affected descendants
turn 3   DQ_RECHECK + RECONCILE the descendants         <- barrier
```

Turn 1 is a **quality barrier**: the root is rebuilt, re-checked, reconciled and certified
*before* any descendant runs. A plan that rebuilt the root and the marts in parallel would
propagate the corruption faster than it repaired it.

Nothing is scheduled that cannot be verified: every rebuild is followed by a check that would
fail if the rebuild did not work.

---

## Nothing runs until a human says so

```
  POLICY GATE
  APPROVAL_REQUIRED
      cost class large exceeds medium
      caller holds no execute role

  ANSWER — TERMINATED: COMPLETED
    PLAN   Plan built. Not submitted: the request asked for a plan, not an execution.
```

![Plan built, not submitted](images/reliability-plan-not-submitted.png)

The gate states **why**, in the terms it decided on. Two independent reasons here: the cost
class and the caller's roles. `--roles execute` shows `AUTO_EXECUTE_ALLOWED` instead, so the
policy can be inspected without holding the privilege.

The vocabulary is nine `ActionType` values — `EOD_REBUILD`, `MART_RERUN`, `STREAM_BATCH_REPLAY`,
`REALTIME_REBUILD`, `AUTO_CORRECT`, `FULFILL`, `DQ_RECHECK`, `RECONCILE`, `CERTIFY` — and not
one of them drops, deletes or truncates anything. Mutation is closed **by name** to a frozen
set of two tools (ADR-092), so a tool added tomorrow is non-mutating until somebody puts it
in that set deliberately.

---

## Run the whole loop end to end

```bash
python3 scripts/ai-recovery-drill.py                        # dry run
python3 scripts/ai-recovery-drill.py --execute              # against sandbox tables
python3 scripts/ai-recovery-drill.py --scenario duplicate   # or: source-update
```

| Also see | |
|---|---|
| [`AI_COPILOT_TRANSCRIPTS.md`](AI_COPILOT_TRANSCRIPTS.md) | annotated sessions from both copilots |
| [`AI_RECOVERY_PLANNER.md`](AI_RECOVERY_PLANNER.md) | turns, barriers, granularity |
| [`AI_RECOVERY_SCOPE_MODEL.md`](AI_RECOVERY_SCOPE_MODEL.md) | how scope is derived and bounded |
| [`AI_AGENT_ACTION_BOUNDARY.md`](AI_AGENT_ACTION_BOUNDARY.md) | what the agent may never do |
| [`AI_RECOVERY_APPROVAL_POLICY.md`](AI_RECOVERY_APPROVAL_POLICY.md) | the gate, and what `--roles` changes |
