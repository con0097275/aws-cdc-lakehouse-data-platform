# End-to-End Demo — one key traced through every layer

Live run 2026-09-03. Account **990002**: inserted, deleted, then recreated on the same PK —
the sequence that exercises ordering, tombstones, dedup and resurrection in one chain.

## Correlation map

| Layer | Identity | Value |
|---|---|---|
| Source PK | `corebank.account.ACCOUNT_ID` | **990002** |
| Source SCN | insert / delete / recreate | `3101554` / `3101561` / `3101577` |
| Kafka | topic / partition / offsets | `cdc.oracle.COREBANK.ACCOUNT` / **1** / **121, 122, 124** |
| Tombstone | offset **123** | absent from FULL_CDC — counted, not ingested |
| FULL_CDC ops | `c` → `d` → `c` | `position_primary` 3101552 < 3101559 < 3101575 |
| `dv_event_id` | per delivered record | `de00c69bd0c3`, `1daf5eda6fe1`, `6084449f4dfa` |
| `dv_src_event_id` | per source change | `a548f71ea173`, `6ee24864b5f4`, `b8200851ff0e` |
| REALTIME | snapshot | `7340210906418940945`, 72 h window from `2026-08-31 10:13:06Z` |
| EOD | COB / tag / snapshot | `2026-09-03` / `EOD_2026-09-03` / `5452782707029562418` |
| Coordinator | run / execution | `p14b-eod-1788432893` / `…:EOD:mart_account_balance_daily:2026-09-03:1` |
| Turn / attempt | | **1 / 1** |
| Spark app | EMR job | `/applications/00g8g05ud5dj2u25/jobs/00g8g2f9pgg9vg27` |
| dbt | selector / result | `--select mart_account_balance_daily` / `PASS=3 WARN=0 ERROR=0` |
| Mart row | | `990002 | 2026-09-03 | 777.77 | CERTIFIED` |
| Config version | | `cfg-1682b193a20ce2ae` |
| Athena | query ids | `28658a65-…` (chain), `2ffc94db-…` (mart row) |
| Metric | id / version | `total_closing_balance` / `metric:0d1b5e7ea203f9de` |
| Agent | request id | `f7b3804a-6dec-416e-ac1f-2d6a5390ff38` |
| Feature group | version | `feature_group:bded816b072c310d` |
| Tokens / cost | | `0 / 0` · `$0.000000` (generation disabled) |

Account **990001** ran the same path but ended in a delete, and is correctly **absent** from
EOD, the mart and the AI answer.

## Physical AWS

```mermaid
flowchart TB
  subgraph VPC["VPC 10.42.0.0/16 · 3 AZ · NAT 0"]
    subgraph PUB["public subnets x3"]
      TB[toolbox t3.small]
      SL[source-lab t3a.xlarge<br/>Oracle + SQL Server containers]
      CR[cdc-runtime t3.large<br/>Connect + Apicurio]
      AF[airflow t3.large<br/>k3s, 7 pods]
    end
    subgraph PRIV["private subnets x3"]
      MSK[(MSK 3 x m7g.large<br/>KRaft · TLS · CMK)]
      EMR[EMR Serverless<br/>ARM64 · auto-stop 15m]
    end
    EP[["S3 + DynamoDB gateway<br/>Glue interface<br/>NO logs / NO monitoring"]]
  end
  IGW((IGW)) --- PUB
  SL --> CR --> MSK
  MSK --> EMR
  EMR --> S3[(S3 lake<br/>SSE-KMS · versioned)]
  EMR --> GLUE[(Glue catalog<br/>7 databases)]
  S3 --- EP
  GLUE --- EP
  ATH[Athena workgroup<br/>10 GiB cutoff] --> GLUE
  DDB[(DynamoDB x4<br/>watermarks + history)] --- EP
  AF --> EMR
  SSM{{SSM Session Manager<br/>the ONLY human path}} -.-> PUB
```

Operators reach every host through SSM only — no key pair, no port 22, no inbound
`0.0.0.0/0`. The missing `logs` and `monitoring` endpoints are finding G3.

## Governance / control plane

```mermaid
flowchart LR
  REG[governance/catalog/domains.yml<br/>owner · classification · PII<br/>retention · freshness SLA] --> MASK[BI access decision]
  REG --> GUARD[ai/guards.py<br/>inherits the BI role]
  MET[aiplatform/metrics/*.yaml<br/>metric owner + version] --> AGENT[Business AI]
  MAN[dbt manifest] --> LIN[lineage]
  LIN --> AGENT
  GUARD --> AGENT
  CFG[reporting config<br/>cfg-1682b193a20ce2ae] --> COORD[coordinator]
  COORD --> HIST[(execution history<br/>turn · attempt · status)]
  COORD --> WM[(watermarks<br/>advance only on SUCCESS)]
  COORD --> CERT[certification tier<br/>EOD=CERTIFIED]
  CERT --> MART[(MART rows carry<br/>execution_id · config_version)]
  IAM[IAM: first control] --> ATH[(Athena)]
  GUARD -->|second control| ATH
```

Ownership, classification, PII and retention derive from **one** registry, and the AI SQL
guard inherits the BI role's access from that same file rather than keeping a second list
that would drift invisibly.

## Logical CDC / lakehouse

```mermaid
flowchart LR
  subgraph SRC[Sources]
    ORA[(Oracle COREBANK)]
    MSS[(SQL Server digital)]
  end
  ORA -->|LogMiner| DBZ[Debezium Connect]
  MSS -->|CDC tables| DBZ
  DBZ -->|Avro + Apicurio| K[(MSK topics<br/>key = canonical PK)]
  K --> FC[FULL_CDC<br/>canonical, append-only<br/>MERGE on dv_event_id]
  FC --> RT[REALTIME<br/>rolling T-N .. T]
  FC --> EOD[EOD snapshot<br/>1 state per PK as-of COB]
  EOD --> CUR[curated fact]
  CUR --> MART[(MART)]
  MART --> ATH[Athena / serving]
  ATH --> AI[Business AI]
  K -. tombstone .-> X[counted, excluded]
```

FULL_CDC is the **canonical** layer; REALTIME and EOD are siblings derived from it, not a
chain. `FULL_CDC_RAW` is deliberately unbound so the prohibition is nameable (ADR-033).

## Reporting and dependency turns

```mermaid
flowchart TD
  CFG[reporting/*.yaml] --> COMP[compile.py<br/>plan_hash 418d7158…]
  MAN[dbt manifest<br/>61 nodes] --> COMP
  COMP --> G[graph.py<br/>assert_acyclic]
  G --> T1[TURN 1<br/>mart_account_balance_daily<br/>mart_channel_engagement_daily]
  T1 --> T2[TURN 2<br/>mart_account_balance_monthly]
  T1 --> EMR[EMR Serverless<br/>emr_dbt_bootstrap → run_dbt_job]
  EMR --> DBT[dbt build --select …]
  DBT --> MART[(MART)]
  DBT --> WM[(DynamoDB<br/>watermark + execution history)]
```

Two marts share TURN 1 — that is the same-turn parallelism. The monthly roll-up is TURN 2
because it depends on them.

## Business AI control plane

```mermaid
flowchart LR
  Q[User question] --> LG[LangGraph<br/>bounded, 9 nodes]
  LG --> RES[metric resolver<br/>governed registry]
  RES --> TS[timespec<br/>business window]
  TS --> PLAN[deterministic plan<br/>max 2 queries]
  PLAN --> GUARD{guards}
  GUARD -->|read-only, allow-list,<br/>LIMIT, byte ceiling| ATH[(Athena)]
  GUARD -->|refuse| NO[UNSAFE / refused]
  ATH --> EV[EvidencePack<br/>ids, versions, hashes]
  EV --> VER{evidence<br/>sufficient?}
  VER -->|no| SAY[say so — never fabricate]
  VER -->|yes| ANS[answer + certification]
  RAG[(knowledge corpus)] -.explains, never supplies numbers.-> ANS
```

## Failure / recovery

```mermaid
flowchart TD
  F1[Connect restart] -->|offsets in Kafka| R1[RPO 0 · RTO ~30s · no re-snapshot]
  F2[Replay / rerun] -->|MERGE on dv_event_id| R2[zero rows added]
  F3[Job failure] -->|watermark written only on SUCCESS| R3[no watermark advance]
  F4[EOD rerun] -->|deterministic function of FULL_CDC + cutoff| R4[byte-identical output]
  F5[Unsafe / injected request] -->|router + code guards| R5[refused · tool_calls empty]
```

## Reproducing the demo

```bash
bash scripts/cdc-window-start.sh                       # readiness gate, exit 0 required
bash scripts/source-lab.sh workload 1 --execute        # phrase: RUN WORKLOAD 1
# FULL_CDC → REALTIME → EOD on EMR (recipe: artifacts/validation/final-e2e/cdc/02-lakehouse-layers.txt)
python3 scripts/reporting-live-run.py --flow-mode EOD --business-date <COB> \
        --job-id mart_account_balance_daily --execute
python3 /tmp/bai_live.py "what is the total closing balance on <COB>"
```

The full evidence set is frozen under `artifacts/validation/final-e2e/`.
