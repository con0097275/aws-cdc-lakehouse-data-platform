# Code walkthrough

Twelve files, chosen because each one carries a decision that is expensive to get wrong and
invisible when it is wrong. For each: the problem it solves, the invariant it protects, the
pattern worth stealing, and who should read it.

**194 Python modules are published** — the whole AI/ML platform, the whole governance and
metadata plane, and the CDC correctness code. The Spark reporting framework, the Airflow
DAGs, the dbt models, the Terraform modules and the operator tooling are deliberately not
here; [`../PORTFOLIO_SCOPE.md`](../PORTFOLIO_SCOPE.md) draws that line and explains it.

These twelve are where to **start**. They were not chosen because they are easiest to show,
but because each is a place where a wrong decision stays invisible until it has corrupted
months of data — which is exactly where reading the code tells you something an architecture
diagram cannot.

---

## 1. [`spark/jobs/l1_stream/ordering.py`](../spark/jobs/l1_stream/ordering.py)

**The problem.** Oracle and SQL Server express "where in the log did this happen" in formats
that need **opposite** treatment. An Oracle SCN is numeric, so `'9' > '10'` as a string but
`9 < 10` as a number. A SQL Server LSN is a zero-padded hex triplet, so string comparison
already works — *but only because of the padding*.

**The invariant.** String comparison of the normalised position must equal source-order
comparison, for both engines, forever.

**The pattern.** Two separate functions, not one "just pad it" helper — a single helper
would silently corrupt one engine. The field width is a constant with a comment saying it
can never change, because widening it would re-sort history that already exists. An
unparseable position raises `PositionFormatError` rather than passing through: a position
that cannot be ordered would corrupt downstream ordering in a way that only surfaces as
wrong business numbers, months later.

**Read this if** you have ever been asked "how do you order CDC events?" and answered
"by timestamp".

## 2. [`spark/jobs/l1_stream/envelope.py`](../spark/jobs/l1_stream/envelope.py)

**The problem.** A Debezium envelope carries more than the row. An early version of this
platform's column list silently discarded nine envelope fields.

**The invariant.** Nothing is dropped, and `before`/`after` stay **JSON strings**, never
structs.

**The pattern.** `assert_no_field_dropped` fails loudly rather than quietly narrowing — the
check is in the code, not in a reviewer's attention. Keeping the payload as a string means a
source DDL change is *not* an Iceberg schema migration on a live streaming table; it is a
JSON key that appears. That one choice is why schema evolution here is boring.

**Read this if** you want the argument for storing CDC payloads as JSON rather than typed
columns at the raw layer.

## 3. [`spark/snapshot/build_snapshot.py`](../spark/snapshot/build_snapshot.py)

**The problem.** "Current state" is easy. *As-of* state that reproduces byte-for-byte when
rebuilt six months later is not.

**The invariant.** Rebuilding an old `snapshot_date` reproduces the original result exactly.

**The pattern.** Three separate properties, each a distinct way to be wrong: the cutoff
filters on `source_commit_ts` (business time) and the window function ranks only rows inside
it; ordering is by source position, never by Kafka offset alone; deletes are handled
explicitly, with the active table excluding a deleted PK and an opt-in `_history` table
keeping it flagged. The ranking is **total** — two events cannot tie on all five
`event_order` components — and that totality is what makes the rebuild deterministic rather
than merely usually-the-same.

**Read this if** you are building a snapshot layer and have not yet decided what a delete
means.

## 4. [`spark/eod/window.py`](../spark/eod/window.py)

**The problem.** A business-date window has three independent ways to be subtly wrong.

**The invariant.** The window is half-open, UTC, and cut on business time.

**The pattern.** `>= start AND < end`, because a closed upper bound puts a midnight-boundary
event in **two** business dates and double-counts it. Everything in UTC, because a local-time
cutoff moves events between days depending on where the job ran. The cut is on
`source_commit_ts`, not `ingest_ts`, because pipeline time makes the same event land on
different dates across reruns. Note the comment recording that the prose spec said `<=` and
the normative contract said `<` — the code follows the contract and says so.

**Read this if** you want to see a half-open interval argued for rather than assumed.

## 5. [`spark/realtime/rt_common.py`](../spark/realtime/rt_common.py)

**The problem.** A streaming layer and a settled layer both have an opinion about the same
account, and only one can be served.

**The invariant.** The watermark decides, except when a repair is strictly newer.

**The pattern.** The merge is written in **plain Python**, not SQL, so its semantics are
unit-testable without a warehouse — 34 tests, no Spark, no Kafka, no AWS. Dedup is by event
time rather than arrival, because the source is a history table and a retroactive correction
arrives late. And `_base_is_newer` degrades *safely*: if either timestamp is missing it
cannot prove BASE is newer, so live data wins — defaulting the other way would let a row
with no provenance outrank the stream.

**Read this if** you want the RT-1 story:
[`REALTIME_STREAMING_RT.md`](REALTIME_STREAMING_RT.md) §7.

## 6. [`spark/realtime/rt_stream_app.py`](../spark/realtime/rt_stream_app.py)

**The problem.** `foreachBatch` makes data loss look like success.

**The invariant.** A partial failure raises. Always.

**The pattern.** Swallowing an exception tells Spark the batch succeeded, so Spark commits
the offsets and the rows are gone — not in the target, not in a queue, nowhere. The
reference system this was modelled on lost ~100K offsets exactly that way. Note also the
startup refusal at line 222: if the checkpoint path sits under `warehouse/`, the app will not
start, because `remove_orphan_files` walks table locations and would delete live streaming
state.

**Read this if** you have a `try/except` around a micro-batch write.

## 7. [`cdc/compile.py`](../cdc/compile.py)

**The problem.** Config-driven platforms fail at 02:00, in production, on a typo.

**The invariant.** Two compiles of the same registry produce **identical bytes**.

**The pattern.** The compiler runs entirely locally — no AWS call, no credential, no cost —
so every config defect is caught in CI instead of by a coordinator at night. Determinism is
not a nicety here: it is what makes the compiled plan reviewable in a pull request, and what
lets `--verify` fail when the committed plan has drifted from the registry. The module-not-
path invocation note is a real bug this avoided, not pedantry.

**Read this if** you are about to let YAML drive infrastructure without a compile step.

## 8. [`cdc/impact.py`](../cdc/impact.py)

**The problem.** Lineage tells you what is affected. It does not tell you what can be
rebuilt.

**The invariant.** An asset name from a catalogue **never becomes a command**.

**The pattern.** The planner intersects two graphs — DataHub lineage for *what*, the job
registry for *how and in what order* — keeps both lists, and records why each exclusion
happened. Every node in `executable_descendants` has resolved to a job the platform already
registers; anything that does not resolve is an operator decision. Without that rule, a tag
edited in a web UI becomes a way to make the platform rewrite data. And a source-side defect
produces **no plan at all**: rerunning reproduces the same wrong answer, more expensively.

**Read this if** you are building automated remediation on top of a metadata catalogue.

## 9. [`ai/reliability/model.py`](../ai/reliability/model.py)

**The problem.** "Only `BALANCE` is wrong" and "Spark can rewrite one column" are different
statements, and conflating them is the main correctness trap in lineage-driven recovery.

**The invariant.** Column lineage selects *which jobs* run; `RecoveryCapability` decides
*what each one can recompute*.

**The pattern.** Both are types, so the distinction cannot be lost in a prompt. The six root
causes that refuse a rerun are **data** in a taxonomy, not a branch in an LLM instruction —
which is why they can be tested, and why a model cannot talk its way past them.

**Read this if** you want the one idea from this project most worth stealing.

## 10. [`ai/reliability/policy.py`](../ai/reliability/policy.py)

**The problem.** An agent that decides its own permissions has none.

**The invariant.** The gate runs before any mutation and takes **no input from the model**.

**The pattern.** Its inputs are facts the platform produced: environment, root cause, scope
size, lineage confidence, descendant count, cost class, prior attempts, the authenticated
caller's roles. Production is conservative by construction — `AUTO_EXECUTE_ALLOWED` requires
*every* condition to hold, and any single failure downgrades, never upgrades.

**Read this if** you are adding write capability to an LLM agent.

## 11. [`ai/agent_tools/contract.py`](../ai/agent_tools/contract.py)

**The problem.** Per-tool safety means the tool that forgets is the one that gets exploited.

**The invariant.** `invoke()` is the only way to call a tool. None can opt out.

**The pattern.** Validation, timeout, result cap and audit live in one place, so they are
structural rather than a convention each author must remember. Audit is treated as **part of
the control, not telemetry**: the record is written for failures too, and a tool whose audit
sink raises does not silently succeed. This file is where "2 of 27 tools may mutate" is
actually enforced — the number in the README is a consequence of this code, not a claim
about it.

**Read this if** you want the difference between a tool allowlist and a tool contract.

## 12. [`cdc/registry/`](../cdc/registry/) and [`spark/tests/test_ordering.py`](../spark/tests/test_ordering.py)

**The problem.** Every claim above is checkable, or it is marketing.

**The invariant.** The config is the source of truth, and the guards are tests.

**The pattern.** Open one registry file and the config-driven claim is either true or it is
not — a new table is a YAML entry, not a new DAG and a new Spark application. Then open
`test_ordering.py` and `test_realtime_rt.py`: the counter-cases are there, including the ones
that would pass if the implementation were subtly wrong. A guard without a failing
counter-case is not a guard.

**Read this if** you review code for a living.

---

## Where to go next

| | |
|---|---|
| The decisions behind these files | [`../DECISIONS.md`](../DECISIONS.md) (83 ADRs) · [`../DECISION_LOG.md`](../DECISION_LOG.md) |
| The defects that produced them | [`ENGINEERING_JOURNAL.md`](ENGINEERING_JOURNAL.md) |
| Running the tests yourself | [`HOW_TO_USE_AND_VERIFY.md`](HOW_TO_USE_AND_VERIFY.md) |
| What is proven versus designed | [`CAPABILITY_MATRIX.md`](CAPABILITY_MATRIX.md) |
