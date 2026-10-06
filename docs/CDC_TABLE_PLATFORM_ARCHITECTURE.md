# Config-Driven CDC Table Platform — Architecture

**Status**: production-ready (Phase 8). **Contract**: ADR-062 … ADR-069.

One file decides what the platform captures and how. Everything below it is generic: adding a
table is a registry entry, and no layer of this diagram gains a line of code for it.

```mermaid
flowchart TB
    subgraph CFG["THE ONLY THING A NEW TABLE CHANGES"]
        REG["cdc/registry/sources.yaml<br/><i>one entry per source table</i>"]
        LAY["reporting/layers.yaml<br/><i>the ONLY file naming a physical Glue DB (ADR-033)</i>"]
    end

    REG -->|"cdc.compile — deterministic plan_hash"| PLAN["table-plan.json<br/><i>schema v6 · config_version</i>"]

    subgraph SRC["SOURCES — prerequisites are yours, the platform only READS"]
        ORA[("Oracle CDB<br/>ARCHIVELOG + supplemental log")]
        MSS[("SQL Server<br/>Agent + sp_cdc_enable_table")]
    end

    ORA -->|LogMiner| DBZ["Debezium connectors<br/><i>capture list RENDERED from the registry</i>"]
    MSS -->|CDC tables| DBZ
    DBZ --> APIC["Apicurio<br/>Avro registry"]
    DBZ --> KAF{{"MSK / Kafka<br/><i>key = canonical PK → same PK, same partition</i>"}}

    PLAN -.->|"dynamic task mapping — a table is a ROW, not a DAG"| ORCH["Airflow<br/><i>cdc_realtime · cdc_eod · cdc_maintenance</i>"]
    ORCH -.-> RT
    ORCH -.-> EOD
    ORCH -.-> MAINT

    PLAN -.->|"table is an ARGUMENT"| ROUTE
    KAF --> ROUTE["TableRouter<br/><i>unknown topic ⇒ REFUSE, never auto-create</i>"]

    ROUTE --> FULL[["<b>FULL_CDC</b> — CANONICAL<br/>every I/U/D, full envelope + Kafka metadata<br/>MERGE on dv_event_id ⇒ idempotent rerun"]]

    FULL --> RT[["<b>REALTIME</b><br/>bounded rolling window<br/>lookback + grace ≤ retention"]]
    FULL --> EOD[["<b>EOD</b><br/>1 row per PK per business_date<br/>source_commit_ts &lt; cutoff_utc"]]

    EOD --> DQ{"DQ + reconciliation<br/><i>distinct_keys − deletes == rows</i>"}
    DQ -->|both pass| CERT["CERTIFIED<br/><i>marker written</i>"]
    DQ -->|either fails| HELD["data written,<br/>MARKER WITHHELD"]

    CERT --> ATH["Athena — certified serving"]
    RT --> ATH

    PLAN -.-> MAINT["maintenance<br/><i>metric-driven, not cron-driven</i>"]
    MAINT -.-> FULL
    MAINT -.-> RT
    MAINT -.-> EOD

    LEG[("legacy monolith cdc_events<br/><b>never dropped</b> — reconciliation baseline")]
    LEG -.->|"backfill, one-way"| FULL
    LEG -.->|"benchmark comparison"| ATH

    classDef canon fill:#1f3d5c,stroke:#7fb2e5,color:#fff,stroke-width:2px
    classDef gate  fill:#5c3a1f,stroke:#e5b27f,color:#fff
    classDef cfg   fill:#1f5c3a,stroke:#7fe5b2,color:#fff
    class FULL,RT,EOD canon
    class DQ,CERT,HELD,ROUTE gate
    class REG,LAY,PLAN cfg
    class ORCH gate
```

## The five invariants the diagram encodes

1. **FULL_CDC is canonical; REALTIME and EOD are siblings, never a chain.** EOD reads
   FULL_CDC directly, so a certified balance never depends on a bounded window that has aged
   rows out, nor on another job's schedule.
2. **The ingest never creates a table.** An unknown topic is refused, naming the registration
   step — an auto-created table is an unowned, unclassified, unretained data product.
3. **Certification is a gate, not a label.** Data is written when validation fails; the
   *marker* is withheld.
4. **The legacy monolith is never dropped.** It stays the reconciliation baseline for every
   window already captured (ADR-062).
5. **One file names physical databases.** Everything else names a logical layer.
6. **Tables are data to orchestration.** The DAG task list is dynamic-mapped over the compiled
   plan, so onboarding a table adds a task and not a file. A per-table DAG fails a test.

## What a new table costs

| | |
|---|---|
| new Python | **none** |
| new DAG | **none** — tasks are mapped over the plan (ADR-071) |
| new SQL | **none** |
| config | one registry entry (~6 lines) |
| runtime | 1 Kafka topic, 3 Iceberg tables, 1 nightly close |

Proven twice in Phase 8, on both engines: `oracle.coredb.corebank.loan` and
`sqlserver.digital.dbo.payment_method`.

## Where to start

`docs/CDC_TABLE_QUICKSTART.md`.
