# CERTIFICATION MODEL

- Phase: **DRP1** — ladder and gates implemented; not yet wired into the flows (DRP5)
- Code: `cdc/certification.py` (canonical) · re-exported by `spark/common/flows.py`
- Mirror: `dbt/macros/status_priority.sql`, drift-tested by `spark/tests/test_dbt_contract.py`

---

## 1. The rule

> **A Spark job exiting 0 is not certification.**

The DRP0 audit found the live platform certifying on completeness of the EOD close alone:
`ops.data_certification` does not exist, and `dq_result_run_id` is passed as a literal `None`
by its only producer (`spark/reporting/ops_client.py:359`). "Certified" therefore meant *the
close finished* — a statement about the pipeline, not about the data.

## 2. The ladder

| Tier | Rank | Meaning |
|---|---|---|
| `REALTIME` | 1 | the bounded overlay; true now, certified never |
| `PROVISIONAL_NRT` | 2 | incremental, watermark-bounded |
| `PROVISIONAL_CORRECTED` | 3 | whole-of-day relook after late events |
| `RECONCILED` | 4 | agreed against a counterpart |
| `CERTIFIED` | 5 | closed, cutoff-bounded, DQ- and recon-clean |

A lower tier may never overwrite a higher one. Equal **is** allowed, or a correction to a
certified figure could never be applied.

### One definition, and why the numbers moved

`spark/common/flows.py` held a four-tier ladder and its own comment said a second hardcoded
copy is how an anti-downgrade rule stops matching the ladder it enforces. So the fifth tier
was added by making `cdc/certification.py` canonical and having `flows.py` **import** it —
not by writing a second dict.

The ranks shifted from 1–4 to 2–5. That was forced, not cosmetic:

> `flows.py` and the dbt macro both rank an **unrecognised** status `0`, so an unknown value
> can never win a comparison. Numbering `REALTIME` as 0 would have made garbage compare
> **equal** to a real tier.

Adding a tier below the bottom therefore meant shifting the whole ladder up by one. The
relative order of the original four is unchanged, which is the only property any comparison
depends on, and the existing drift tests (`test_dbt_macro_matches_python_status_rank`,
`test_every_rank_maps_back_to_a_name`, `test_unknown_statuses_rank_below_every_real_tier`)
enforced that the Python, the SQL macro and the decoder all moved together.

## 3. Gates

| Gate | Refused because |
|---|---|
| `job_succeeded` | the producing job did not succeed |
| `contract_declared` | no contract, so no stated shape to verify against |
| `dq_evaluated` | no DQ check produced a verdict; an unevaluated suite is not a clean one |
| `dq_not_blocking` | a required check failed, errored, or could not be evaluated |
| `reconciled` | reconciliation against the counterpart did not pass |
| `cutoff_bounded` | the read was not bounded by an explicit cutoff, so the figure is not reproducible |
| `upstream_ready` | an upstream dataset was not ready, so events may have arrived since |
| `source_closed` | the source period is not closed, so late events can still change this figure |

Cumulative up the ladder — asserted by a test, not left as a reading of the table:

```
REALTIME               job_succeeded
PROVISIONAL_NRT      + contract_declared
PROVISIONAL_CORRECTED+ dq_evaluated, dq_not_blocking
RECONCILED           + reconciled
CERTIFIED            + cutoff_bounded, upstream_ready, source_closed
```

## 4. Per-flow ceilings

| Flow | May stamp at most | Older name in `flows.py` |
|---|---|---|
| `EOD` | `CERTIFIED` | `EOD` |
| `FULFILL` | `RECONCILED` | `FULL_FILL` |
| `AUTO_CORRECT` | `PROVISIONAL_CORRECTED` | `AUTO_CORRECT` |
| `STREAM_BATCH` | `PROVISIONAL_NRT` | `NRT` |
| `STREAMING_RT` | `REALTIME` | *(new — postdates that module)* |

A flow cannot choose its tier at runtime; requesting above the ceiling raises. Letting
`STREAM_BATCH` stamp `CERTIFIED` would defeat the ladder from inside, and the check that
prevents it cannot live in the flow itself.

**Two flow vocabularies exist and DRP1 does not merge them.** `LEGACY_FLOW_NAME` records the
mapping and a test keeps them in step; renaming either is a change to running code and
belongs to the phase that owns it.

## 5. Evidence, not assertions

`Evidence` carries the actual `DqResult` and `ReconResult` records, not booleans. A gate that
took someone's word for it is precisely the failure DRP0 found in production.

Two consequences:

- **An empty reconciliation list is not a pass.** `reconciliation_policy: none` is how an
  asset says it reconciles against nothing, and that is expressed by the tier it is allowed
  to reach — not by an absent result silently counting as one.
- **A miss grants nothing**, rather than quietly dropping to a lower tier. A run that aimed
  at `CERTIFIED` and missed is a run somebody should look at; publishing it as
  `PROVISIONAL_NRT` would hide that while the figure stayed on the dashboard.

`evaluate()` returns **every** failed gate with its reason, so one look tells you everything
missing.

## 6. Worked example

`mart_transaction_monitoring_10m` is a ten-minute surveillance aggregate. Its overlay entry
sets `certification_policy: realtime_overlay`, `reconciliation_policy: none`,
`dq_policy: realtime_overlay_dq` and `tags: [not-certified]`.

Declaring `certified_eod` there would let the ladder stamp a number that can still move — and
the mart is explicitly a figure that moves.

## 7. What DRP1 did not do

No flow calls `evaluate()` yet, and no certification is written to a ledger. `ops.data_certification`
still does not exist. DRP5 supplies real `DqResult` records and wires the gate into the EOD
close; until then the model is exercised only by its tests.
