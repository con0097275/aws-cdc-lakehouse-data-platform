# ENGINEERING JOURNAL — Defects, and What Each One Taught

Every defect here was **reproduced** before it was fixed, and every fix is held by a test.
This is the document I would most want read, because a feature list says what was built and
this says how it was judged.

A pattern runs through all of it:

> **The dangerous defects were not the ones that failed. They were the ones that succeeded
> at everything except the thing they were for.**

---

## 1. The ones that shipped green

### 1.1 A configuration that succeeded at everything except working

`AIRFLOW__OPENLINEAGE__TRANSPORT` was the bare string `console`. The provider parses that
key as **JSON**, so it raised `AirflowConfigException` **during plugin import**. Airflow
skipped the plugin, registered no listener, and ran every DAG green:

```
REGISTERED_LISTENERS = []        # three tasks succeeded; zero events emitted
```

**A test asserted this exact value and passed** — it compared `values.yaml` against the
module deriving it, and *both carried the bare string*.

> Agreement between two files in your own repository is not evidence about the third party
> that has to parse them. The replacement tests parse the value the way the provider does.

Recorded as **ADR-091**. Note the Spark listener takes a *flat*
`spark.openlineage.transport.type=console` string — two integrations, two encodings, one
config field, which is why the bare form looked right.

### 1.2 A recipe that could never have authenticated

The generated Kafka ingestion recipe declared `sasl.mechanism: AWS_MSK_IAM`. That is the
**Java** client's mechanism name; DataHub's source uses librdkafka, which answers
`Unsupported SASL mechanism`. Verified against librdkafka directly, not inferred.

It generated cleanly and passed every offline test for months.

> A config value naming a mechanism, a codec or a driver is a **claim about a library**, and
> only that library can confirm it.

The fix is deliberately incomplete and says so: MSK IAM also needs an `oauth_cb`, a Python
*callable*, and YAML cannot carry one.

### 1.3 A job that died having done no work

`spark.jars.packages` cannot resolve in this VPC — there is no NAT, only gateway endpoints.
Ivy retried and the job died **during dependency resolution**, which reads as a Spark
problem rather than a networking one. The pinned jar is now staged in S3, with its SHA1
verified against both Maven's published `.sha1` and the staged object.

Same submission also nearly lost Iceberg: `spark.jars` is one comma-separated key that the
submit script already set, so a second `--conf spark.jars=` would have **silently replaced**
the Iceberg runtime. Last flag wins; every table read would have failed with a
`ClassNotFound` bearing no resemblance to a lineage change.

### 1.4 A quarantine table that had never existed

`ops.dq_quarantine` was declared in DDL and never created, so the quarantine path had
executed **zero times** in the platform's life. Found by trying to drill it.

---

## 2. The ones where a guard failed open

### 2.1 A verification turn that ran a rebuild

With quality gates added to the plan, the executor keyed SQL templates by **job id alone**,
ignoring the action. `DQ_RECHECK` and `CERTIFY` on `eod_build` resolved to `eod_build`'s
**rebuild** SQL — a verification turn silently re-executing `DELETE + INSERT`.

**The drill still reported PASS**, because an idempotent rebuild survives being run twice.

> That is luck, not correctness. A `CERTIFY` that rewrites a table is exactly the action
> nobody would approve.

Found by reading *"7 statements executed"* against a plan whose verification actions should
have read, not written. Templates are now keyed by `(job, action)`, and a missing template
is **refused** rather than falling back.

### 2.2 A classifier that guessed, defeating every gate behind it

The copilot's first root-cause heuristic mapped the word "wrong" to `DUPLICATE_CDC_EVENT`.
A request describing a **source defect** therefore produced a plan — because the guard
forbidding that only fires once the cause is classified as a source defect, and it never
was.

The guard was intact. Nothing ever reached it.

> A cause is now asserted only when the request **states** it or the ledgers **show** it. A
> symptom ("it is wrong") yields `UNKNOWN`, which authorizes nothing.

### 2.3 A repair that ignored its own scope

The first live AI-driven recovery "succeeded" and left the mart **wrong in a new way**:
1600 → 900 against an expected 1000. The executor template repaired by a value heuristic
(`balance > 350`) instead of the planned scope, missing one affected account and corrupting
one that was never touched.

Plan valid, policy allowed, approval valid, execution `SUCCEEDED`. Nothing in the safety
layer was wrong — the statement simply did not implement the plan.

Now a template referencing no scope placeholder is refused, and the repair is a rebuild from
FULL_CDC: correct by construction rather than by a predicate someone chose.

### 2.4 A budget that suppressed its own postmortem

The graph's step budget was charged to `build_evidence`, so a run that exceeded `max_steps`
produced **no evidence pack** — no explanation of the one failure most needing one.

> Exempt the audit path from the limits it records.

---

## 3. The ones about honesty of measurement

### 3.1 A check that examined nothing, treated as a verdict

With the ledgers wired, the copilot diagnosed `MISSING_SOURCE_EVENT` from a `completeness`
check at **0 of 0 rows**, while `uniqueness.one_active_row_per_key` at **1 of 670** sat in
the same result set — it took the first row by timestamp.

That is the `NOT_EVALUATED` shape this platform already refuses to treat as a verdict,
reappearing one layer up. Evidence is now ranked by *examined > 0*, then *failed > 0*, then
severity.

### 3.2 A verification predicate that was itself wrong

After a repair, verification reported a violation: `balance > 350` flagged an account that
legitimately held 400. The data was fully correct and the **check** was wrong — a
`DQ_RULE_DEFECT` met by accident, inside the code written to demonstrate that category.

> Verify against the contract, not a proxy. A threshold approximating a contract will
> eventually disagree with it, and when it does the *data* gets "fixed".

### 3.3 A dry run that destroyed the evidence of a real one

`ai-recovery-drill.py` in dry-run mode returned dummy values and then **wrote the evidence
file**, replacing a genuine capture with zeros that still looked like evidence — and printed
`FAIL` for numbers never measured.

> The cheapest way to destroy the record of a real run is a safe-looking rehearsal of it.

### 3.4 Evidence that no longer described the system

`aigr10-live-e2e.json` recorded a 2-turn rebuild-only plan and was cited by the acceptance
review for *"root DQ/recon before descendants"*. After quality gates landed, the code
produced 4 turns — so the evidence still looked like a clean capture while describing
something that no longer existed.

> An evidence artefact is only evidence of the code that made it.

---

## 4. The ones about talking to humans

### 4.1 A lineage question, diagnosed

*"What does the BALANCE column in EOD ACCOUNT feed?"* returned
`Root cause UNKNOWN. No automatic recovery is permitted.` The graph ran the diagnostic path
unconditionally, so a question that never asked for a recovery was told it could not have
one.

Every safety test passed, because nothing unsafe happened — it was merely absurd. Routing by
intent was in the specification and had not been implemented.

### 4.2 A table name, diagnosed

`"the customer snapshot"` did the same. A bare noun phrase is a **lookup**, not an incident
report. An ambiguous request that nonetheless *resolved an asset* now describes it.

Then the correction that followed: routing **all** ambiguity to knowledge was too broad — a
sentence stating a source defect got an ownership blurb instead of
`WAITING_SOURCE_CORRECTION`. A sentence that **states a defect** is investigated whatever its
verb suggested.

### 4.3 A repair request, answered as a lineage question

Broadening the lineage vocabulary introduced a regression: *"…repair only the affected
**downstream** data"* matched a lineage keyword, and the rules were a flat scan with
`EXPLAIN` second.

> **Acting beats diagnosing beats describing.** A descriptive noun inside an instruction does
> not make it a description.

Found only by running all 15 sample questions end to end — the 21 unit tests already
covering intent did not catch it.

### 4.4 Refusing a question it should have answered

`"the affected downstream of oracle_coredb_corebank_customer"` refused: that name exists as
`src`, `full_cdc`, `realtime` and `eod`, and the resolver saw a four-way tie.

But that is not ambiguity about *which table* — the caller named it exactly and simply did
not say which layer. A `LayerAmbiguity` now carries the candidates: a **describing** caller
answers across all four, an **acting** caller still refuses, because the layer decides what
gets rewritten.

### 4.5 A plan nobody could act on

`--execute` refused with *"no executor wired"* — honest and useless. The operator's real
question is *how do I rerun what this touched*. It now prints the platform's actual commands
per turn, and says *"no registered command"* where none exists rather than printing
something plausible.

### 4.6 A preposition, mistaken for a column

*"What does the BALANCE column **in** EOD ACCOUNT feed?"* answered `column in`.

The extractor read the word *after* `column`; English puts the name before the keyword as
often as after. A substring test then matched that fragment against clos**in**g_balance and
returned `DERIVED` — **not** `ABSENT`. The resolver takes the first non-`ABSENT` candidate
and stops, so `BALANCE`, sitting one position later in the same list, was never tried.

The wrong label was cosmetic. The cost was not: `BALANCE` is `VALIDATED`, and `DERIVED` sits
below the bar that permits narrowing, so the planner fell back to table level. The flagship
*"repair only the affected downstream data"* scenario held the evidence to narrow to one
column and three keys and rebuilt whole dates instead. Nothing failed. Everything was wider
than it needed to be.

**A near-miss that lands on a lower confidence tier is worse than an error.** `ABSENT` would
have been visible — no column, table scope, question over. `DERIVED` is a plausible state, a
real column not yet validated, so it travelled through the planner, the policy gate and the
printed plan without once looking wrong.

`AI_COPILOT_SAMPLE_QUESTIONS.md` had recorded the expected answer as `confidence VALIDATED`
since AIGR2. The documentation was right, the code was wrong, and **no test compared them** —
the existing test used *"what does the BALANCE column feed?"*, with no trailing preposition,
and passed throughout.

Then the fix moved the defect. Dropping filler turned *"the BALANCE **field** in EOD ACCOUNT
is wrong"* from a wrong answer into **no** answer, because only one of the two patterns ever
accepted `field`. That was caught by the regression test written for the first defect, not by
running the fix. ADR-093.

### 4.7 A comparison that was never made, reported as a number

The live mart holds one business date. *"Compare with the previous day"* answered:

> `VERIFIED` `CERTIFIED` `PERIOD_COMPARISON` — Total closing balance was 2,031,880,160 VND.

The intent was a comparison, `compare_metric_periods` ran, the baseline window came back
empty, and `build_answer` fell through to the bare-value branch. `limitations` was `[]`.

Every statement in that answer is true. The answer is still wrong, because the user asked
for a change and **nothing said the change could not be computed**. On a single-date mart
every comparison question answered that way, and each one looked complete.

The fix names the missing window (*"No comparison against the previous day: that period
returned no rows"*) and records it as a limitation. The signal was already in the payload —
two result windows, the second with `total_value: None` — and nothing read it.

### 4.8 Four suggested questions that could not be answered

The UI offers one-click chips. Four of them did not work.

*"Is it abnormal?"* and *"Which account contributed most to the change?"* name no metric, so
they fell out of metric resolution and were answered **from the knowledge corpus** — prose,
badged `UNKNOWN`, in place of the anomaly test and the driver ranking. The subjectless
allowance existed; it listed `PREDICTION, DIAGNOSIS, GOVERNANCE` and not `ANOMALY, DRIVER,
FORECAST, TREND`.

*"What should I investigate?"* returned a KPI comparison. `predict_business_risk` takes
`mart_rows`, an optional injection, and **nothing ever passed it** — `scripts/ai-ui.py`
computed a source and then called `ask_business` without it. The branch was dead in both
modes.

Wiring it was not enough. `materialize()` builds features **for** `as_of`, while the UI asks
as-of the day *after* the last business date so that "yesterday" resolves to it. The model
scored zero rows and returned an empty ranking, silently. It now anchors on the newest date
the data actually has and names that date.

**A question the product puts on a button has to reach the tool that answers it.** Three of
these four returned a confident, well-formed answer to a question nobody asked, which is why
none of them looked like a bug.

---

## 5. Two that were mine to own

**I wrote "3,404 passing, 0 failed" into the README and three other documents while two
tests were failing.** They were the ADR-057 tests asserting no tool may mutate, which
ADR-092 deliberately replaced. Corrected everywhere, and the count is now taken from a
measured run rather than arithmetic.

**I claimed the OpenLineage provider was missing from the Airflow image.** It was not — the
stock image ships 2.17.0 and the plugin was already registered. The earlier probe had run
`pip install …==2.20.2`, which **upgrades** as quietly as it installs, and I read its success
as proof of absence. Nothing had asked the cluster.

> Before concluding a component is missing, query the place it would be missing *from*.

---

## 6. What the pattern is worth

Of the defects above, the ones found by **running something** outnumber the ones found by
**testing something** — and the test suite is 3,529 cases. Tests prove a system cannot be
made to do the wrong thing. Only running it proves it does the right thing.

Both acceptance reviews returned **NOT_READY** before they earned READY, and the AI review
was re-scored after six further defects appeared post-READY. **None of those six was in the
safety boundary.** The boundary held while the parts that make it usable were wrong,
repeatedly, until each was run.

All 93 decisions are recorded in [`../DECISION_LOG.md`](../DECISION_LOG.md), each with a
*why* and a *how to apply*.
