# CV bullets — Senior Data Engineer

Every line below maps to something in `docs/CAPABILITY_MATRIX.md` (the realtime layer is
the *Realtime serving layer (STREAMING_RT)* block there). **No throughput, volume,
user-count or uptime figure appears anywhere, because none was ever measured.** Inventing one
is the fastest way to fail the interview that follows.

---

## The honest framing

Describe it as what it is: **a personal lab built to production-like standards, not a
production system.** That framing is a strength — it lets you talk about engineering
judgement without defending numbers you cannot produce.

> "A CDC lakehouse I built end to end on AWS — Oracle and SQL Server through Debezium and
> Kafka into Iceberg, with a governance and recovery plane on top. It runs on about a dollar
> a day, so the interesting part was never scale; it was correctness. CDC ordering,
> idempotency, late and out-of-order events, and how you *prove* a snapshot is right before
> anyone reports on it."

---

## Scope of this file

These bullets cover **this repository only**. Professional work belongs in its own CV entry,
not folded into a portfolio project — an interviewer who follows the GitHub link and cannot
find the work described will discount both entries. Keep employer code, table names and
schedules out of any public repository.

## The LaTeX entry

```latex
\begin{onecolentry}{
    \textbf{AWS CDC Lakehouse \& Data Reliability Platform} \\
    (\href{https://github.com/con0097275/aws-cdc-lakehouse-data-platform}{github.com/con0097275/aws-cdc-lakehouse-data-platform})
}
\end{onecolentry}

\vspace{0.10 cm}
\begin{onecolentry}
    \begin{highlights}
        \item Built an end-to-end CDC lakehouse on AWS: Oracle and SQL Server $\rightarrow$ Debezium/Kafka Connect $\rightarrow$ MSK (KRaft) $\rightarrow$ Spark Structured Streaming $\rightarrow$ Apache Iceberg v2 on S3 with Glue Data Catalog $\rightarrow$ dbt $\rightarrow$ Athena/Power BI.
        \item Designed FULL\_CDC as the single canonical history, with the near-real-time and end-of-day views derived as \textbf{siblings} rather than a chain --- so rebuilding one never requires rebuilding the other.
        \item Solved CDC ordering correctly: ordering by source-native commit SCN (Oracle) and commit LSN (SQL Server) rather than timestamps, treating Kafka offsets as valid only within a partition, and handling deletes, tombstones, and late and out-of-order events idempotently.
        \item Built a \textbf{resident} realtime serving layer beside the batch path: two long-running Spark Structured Streaming processes (30s micro-batch; a datamart recomputing every 45s) and two finite jobs that settle behind them (a correction pass that repairs what the stream flagged; a nightly rebuild that re-derives the settled table and moves the watermark), on one rule --- \textbf{the stream does not fix its own mistakes}. A fact whose dimension has not landed is published \emph{flagged} and recorded in a pointer table, then re-enriched later from the canonical layer, so the fast path's latency stays bounded instead of degrading into retry backpressure.
        \item Hardened that layer against three failures that leave every pipeline green and the number wrong: dedup by \textbf{source event time, not Kafka arrival} (the source is a history table, so a retroactive correction arrives late and ordering by offset publishes a stale balance); a partial \texttt{foreachBatch} failure that \textbf{raises} rather than letting Spark commit the offsets and lose the rows; and one source materialisation per cycle, so every section of the datamart reads the same snapshot of a table still being written. Run live end to end twice, reconciling to the cent.
        \item Implemented a certification ladder (REALTIME $\rightarrow$ PROVISIONAL $\rightarrow$ RECONCILED $\rightarrow$ CERTIFIED) with data-quality and reconciliation gates, so no figure reaches BI until it has been independently proven.
        \item Built governance on DataHub and OpenLineage: 84 catalogued assets at 100\% ownership, an 81-node lineage graph, and 67 column-level edges validated against live Glue schemas.
        \item Engineered \textbf{bounded recovery}: column-level lineage narrows a repair from a whole business date to the affected keys, cutting a 16-asset blast radius to the 6 that are genuinely downstream.
        \item Built a natural-language reliability copilot (LangGraph state machine, \textbf{deterministic --- no model in the inference path}) that diagnoses incidents from four operational ledgers, plans a bounded recovery behind a human approval gate, and exposes 27 typed tools of which only 2 may mutate --- a surface closed by name, not by code review.
        \item Provisioned the platform with 14 Terraform modules under feature flags, no NAT gateway, SSM Session Manager instead of SSH, KMS encryption throughout, and no static credentials anywhere; runs at roughly \$1.40/day.
        \item Validated with \textbf{3,529 automated tests} across 115 test files, plus a documentation validator and an offline demo mode that exercises the full AI layer at \$0.
        \item \textbf{Tech Stack:} AWS (MSK, EMR Serverless, S3, Glue, Athena, EKS/k3s, KMS, SSM, DynamoDB), Debezium, Apache Kafka, Apache Spark, Apache Iceberg, Airflow (KubernetesExecutor), dbt, Terraform, DataHub, OpenLineage, LangGraph, Python
    \end{highlights}
\end{onecolentry}
```

---

## If you only have room for five bullets

Pick these — they are the five a senior interviewer will actually probe.

1. **CDC correctness.** Source-native ordering (SCN/LSN), not timestamps; Kafka offsets
   ordered only within a partition; deletes, tombstones, late and out-of-order events handled
   idempotently.
2. **FULL\_CDC canonical, REALTIME and EOD as siblings.** One history, two derivations. This
   is the architectural decision the whole platform rests on.
3. **Bounded recovery from column-level lineage.** The repair narrows to the rows that broke
   instead of rebuilding everything, and refuses outright on the six root causes where a
   rerun makes things worse.
4. **A resident streaming layer that does not pretend to be complete.** Late dimension →
   publish the fact flagged, record a pointer, repair it from a fresher source later. The
   write order (pointer before row) is chosen for the asymmetry of its failure modes, and
   that is the kind of reasoning the question is really testing.
5. **A governed AI surface.** 27 typed tools, 2 mutating, closed by name; every plan hashed
   and held at a human approval gate.

If the role is a streaming role, lead with 4 and 1. If it is a platform or reliability role,
lead with 2 and 3.

---

## The two-line recruiter summary

> Built a production-shaped change-data-capture lakehouse on AWS — Oracle and SQL Server into
> Kafka, Spark and Iceberg, with dbt marts served through Athena — and layered governance,
> column-level lineage, data-quality certification and bounded recovery on top of it.
> On top of the batch path there is a resident Spark Structured Streaming layer that
> publishes a fact whose dimension has not landed yet *flagged rather than dropped*, and
> repairs it from a fresher source. An AI copilot diagnoses data incidents in English and
> plans a repair scoped to only the rows that actually broke, behind a human approval gate.
> 3,529 tests; about \$1.40/day.

---

## Questions this project lets you answer well

Rehearse these. Each has a real answer in the repository, with the file to point at.

| Question | Where the answer lives |
|---|---|
| Why order by SCN/LSN and not by timestamp? | `docs/CDC_CONTRACT_IMPLEMENTATION.md` |
| Why are REALTIME and EOD siblings, not a chain? | `docs/L3_SNAPSHOT.md` (architecture correction) |
| How do you know a downstream table is affected? | `docs/COLUMN_LINEAGE_STRATEGY.md` |
| How does recovery avoid rerunning everything? | `docs/RERUN_ONLY_WHAT_BROKE.md` |
| What is the AI forbidden to do, and how is that enforced? | `docs/AI_AGENT_ACTION_BOUNDARY.md`, ADR-092 |
| A dimension arrives after the fact — what does the stream publish? | `docs/REALTIME_STREAMING_RT.md` §5 |
| Why does a partial `foreachBatch` failure raise instead of logging? | `docs/REALTIME_STREAMING_RT.md` §9 |
| Why dedup by event time when Kafka already gives you an order? | `docs/REALTIME_STREAMING_RT.md` §9, `spark/realtime/rt_common.py` |
| How does the view decide between the streamed row and the settled one? | `docs/REALTIME_STREAMING_RT.md` §4, RT-1 in §12 |
| What is *not* proven? | `docs/CAPABILITY_MATRIX.md` |

---

## Say "deterministic", not "AI/ML"

The copilot takes English in and produces diagnoses and recovery plans — but **no model is in
the inference path**, and the code says so plainly:

| Decision | Made by |
|---|---|
| intent | keyword rule groups; `classify_intent(q, *, model=None)` and nothing supplies a model |
| root cause | rows read from `dq_result_v2`, `reconciliation_run`, `dq_quarantine`, `eod_run` |
| recovery plan | lineage graph + capability registry — the planner docstring says "not produced by a model" |
| retrieval | BM25, lexical; the dense/embedding backend is flag-gated and undeployed |
| answer text | template composition; `confidence.summary = "DATA_FACT"` precisely because no model ran |
| generation | `AI_ENABLE_GENERATION=false` by default |

**Frame it as a strength, because it is one:**

> "There's no model in the loop. Intent is rule-based, the diagnosis is read from the
> operations ledgers, and the plan comes out of the lineage graph and a capability registry.
> That's deliberate — it's why every answer is reproducible and carries the Athena query id
> that produced it. The LLM seams are there (`model=None` on every node) but switching one on
> would move the answer from DATA_FACT to LLM_INTERPRETATION, and the evidence pack labels
> that distinction."

Claiming "AI/ML" invites a modelling question you cannot answer and undersells the
engineering you can.

## What not to claim

* **No throughput, events-per-second or volume numbers.** None were measured. The lab holds
  one real business date of CDC history; the 90-date demo series is seeded and labelled as
  such in every row. The only durations anywhere in this file are wall-clock times of a
  single run, and they are stated as that.
* **Not "production".** Say *production-shaped* or *production-like*, and be ready to say
  what is missing: no HA Airflow metadata DB, Bedrock narration disabled, several subsystems
  flag-gated and not proven live.
* **Say which of the two realtime layers you mean.** They are different things and an
  interviewer will catch the conflation:
  * the **REALTIME overlay** (`kafka_dev_lab_dev_stream`, the incremental-cursor window) is
    a finite, Airflow-triggered micro-batch — *not* a resident streaming application. That
    was a deliberate cost decision and it is a better story told accurately.
  * **STREAMING_RT** (`spark/realtime/`) *is* resident: `rt_stream_app.py` and
    `rt_datamart_app.py` own their own loops and run until stopped. `--run-seconds` bounds a
    demonstration to minutes; the reference window is 08:00→20:00. Say "it runs as a
    process, and I capped the window so a demo costs minutes" — that is the accurate claim
    and it is still the stronger one.
* **No latency number for the streaming layer either.** The trigger is 30s and the datamart
  polls at 45s — those are *configured cadences*, not a measured end-to-end freshness. The
  only measured durations are per-cycle datamart times (17.8s → 3.1s) and the 240s/180s run
  budgets.
