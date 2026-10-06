# artifacts/validation/final-e2e/realtime

STREAMING_RT — four-app realtime serving layer, run live 2026-09-03 15:44–16:20 UTC.
Design + full evidence: `docs/REALTIME_STREAMING_RT.md`.

## What ran, in order

| # | App | EMR job run | Result |
|---|---|---|---|
| 1 | `rt_ddl.py` | `00g8g7mg3mlero27` | SUCCESS — 7 tables created |
| 2 | `rt_eod_base.py` | `00g8g7o30s7gao27` | SUCCESS — BASE 321 rows, 0 incomplete, watermark `2026-09-03 10:16:40` |
| 3 | `rt_stream_app.py` | `00g8g80h6375mg27` | SUCCESS — 4 batches / 240s, 332 facts, 1 flagged, 43 resolver cycles |
| 4 | `full_cdc_job.py` | `00g8g845o9m3r827` | SUCCESS — late CUSTOMER lands in canonical (206 rows) |
| 5 | `rt_autocorrect.py` | `00g8g85n8ghip027` | SUCCESS — worklist 1, repaired 1, still_incomplete 0 |
| 6 | `rt_datamart_app.py` | `00g8g8727mjsgg27` | SUCCESS — 9 cycles / 180s, 8 metrics each |

## The demonstration

A **late dimension**, created the way it actually happens in CDC: the source stayed
referentially consistent (customer + account in ONE commit — the source has an FK
constraint, so a true orphan is impossible), but the fact reaches the fast path from Kafka
immediately while the dimension is loaded from the canonical layer, which lags until the
next FULL_CDC ingest.

```
STREAM   990500 | 12345.67 | segment NULL | dim_complete false   ← published, not lost
POINTER  990500 | 888888 | customer | retry_count 5 | resolved NULL
AUTOCORR worklist 1 → repaired 1 → still_incomplete 0
BASE     990500 | 12345.67 | PRIORITY | branch 2 | dim_complete TRUE | built_by AUTOCORRECT
POINTER  990500 | resolved_ts 2026-09-03 16:11:15 | resolved_by AUTOCORRECT
```

## Reconciliation — exact

```
view total          2,031,894,187.94   (322 accounts)
session ground truth 2,031,880,937.77  (321 accounts)
difference              13,250.17
  = 12,345.67   new account 990500
  +    904.50   3 accounts x 100.50 x 3 live waves pushed while the stream was up
```

View provenance: `STREAM 4 rows / 3,213,236.17` + `BASE 318 rows / 2,028,680,951.77`.

## Two defects found by running it (both fixed, then re-proven)

1. **`DecimalType` rejected a Python float** at the write boundary
   (`CANNOT_ACCEPT_OBJECT_IN_TYPE`). Fixed by quantising to `Decimal` where money is
   written — which also stops float artefacts (`12345.669999`) reaching the ledger.
2. **The pending resolver only ran inside a micro-batch**, so a flagged row STARVED
   whenever the source went quiet — no new events, no batch, no timeout, empty pointer
   table. Observed live: `queue=1, timed_out=0` for the whole first run. Fixed with a
   dedicated 5s resolver thread (and a lock on the queue, now that two threads touch it),
   matching the reference. Re-run: `timed_out=1, flagged=1, resolver_cycles=43`.

## Submission recipe

```
role      spark-stream   for rt_stream_app (MSK IAM). spark-eod fails: SaslAuthenticationException
role      spark-eod      for ddl / eod / autocorrect / datamart
py-files  s3://<lake>/artifacts/realtime/rtlib.zip
jars      iceberg-spark3-runtime.jar
          + (stream only) spark-sql-kafka-0-10, spark-token-provider-kafka-0-10, spark-avro,
            aws-msk-iam-auth, kafka-clients-3.4.1, commons-pool2-2.11.1
conf      spark.sql.session.timeZone=UTC   (ADR-024)
checkpoint MUST NOT be under warehouse/    (app refuses at startup — CLAUDE.md 5.9)
```
